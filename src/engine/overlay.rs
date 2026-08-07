use crate::manifest::AppManifest;
use anyhow::{Context, Result};
use hex::encode;
use nix::mount::{mount, MsFlags};
use nix::sched::{unshare, CloneFlags};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::task;

lazy_static::lazy_static! {
    static ref OVERLAY_LOCKS: Arc<Mutex<HashMap<String, Arc<Notify>>>> = Arc::new(Mutex::new(HashMap::new()));
    static ref BASE_IMAGE_LOCKS: Arc<Mutex<HashMap<String, Arc<Notify>>>> = Arc::new(Mutex::new(HashMap::new()));
}

pub struct OverlayEngine;

impl OverlayEngine {
    pub fn get_overlay_path(manifest: &AppManifest) -> Option<String> {
        let base_image = manifest.get_base_image();
        
        let packages_dir = match &manifest.packages {
            Some(pkgs) => pkgs,
            None => return None,
        };

        let mut all_pkgs = Vec::new();
        if let Some(apt_pkgs) = &packages_dir.apt {
            all_pkgs.extend(apt_pkgs.clone());
        } else if let Some(apk_pkgs) = &packages_dir.apk {
            all_pkgs.extend(apk_pkgs.clone());
        } else if let Some(dnf_pkgs) = &packages_dir.dnf {
            all_pkgs.extend(dnf_pkgs.clone());
        }

        if all_pkgs.is_empty() {
            return None;
        }

        all_pkgs.sort();
        
        let mut hasher = Sha256::new();
        hasher.update(base_image.as_bytes());
        hasher.update(b":");
        hasher.update(all_pkgs.join(",").as_bytes());
        let hash = encode(hasher.finalize());

        Some(format!("/var/lib/openback/cache/overlays/{}/layer", hash))
    }

    pub async fn ensure_base_image(base_image: &str) -> Result<()> {
        let base_dir = format!("/var/lib/openback/store/images/{}", base_image);
        if std::path::Path::new(&base_dir).exists()
            && std::fs::read_dir(&base_dir)
                .map(|mut d| d.next().is_some())
                .unwrap_or(false)
        {
            crate::dlog!("OverlayEngine", "INFO", "Base image '{}' already cached at {}", base_image, base_dir);
            return Ok(());
        }

        let notify = {
            let mut locks = BASE_IMAGE_LOCKS.lock().await;
            if let Some(n) = locks.get(base_image) {
                crate::dlog!("OverlayEngine", "INFO", "Another task is already downloading '{}', waiting...", base_image);
                Some(n.clone())
            } else {
                let n = Arc::new(Notify::new());
                locks.insert(base_image.to_string(), n.clone());
                None
            }
        };

        if let Some(n) = notify {
            n.notified().await;
            if std::path::Path::new(&base_dir).exists() {
                return Ok(());
            } else {
                anyhow::bail!("Base image download failed in another task");
            }
        }

        let base_name = base_image.to_string();
        let res = task::spawn_blocking(move || {
            std::fs::create_dir_all(&base_dir)?;

            let url = if base_name.starts_with("ubuntu-24.04") {
                "https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.3-base-amd64.tar.gz"
            } else if base_name.starts_with("alpine-3.20") {
                "https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.2-x86_64.tar.gz"
            } else {
                anyhow::bail!("Unsupported base image for automated download: {}", base_name);
            };

            crate::dlog!("OverlayEngine", "INFO", "Downloading base image: {} from {}", base_name, url);
            let status = Command::new("curl")
                .arg("-sLO")
                .arg(url)
                .current_dir("/tmp")
                .status()?;

            if !status.success() {
                std::fs::remove_dir_all(&base_dir).ok();
                anyhow::bail!("curl failed to download base image tarball (exit: {})", status);
            }

            let filename = url.split('/').last().unwrap();
            let tarball_path = format!("/tmp/{}", filename);

            crate::dlog!("OverlayEngine", "INFO", "Extracting {} to {}", tarball_path, base_dir);
            let extract_status = Command::new("tar")
                .arg("-xzf")
                .arg(&tarball_path)
                .arg("-C")
                .arg(&base_dir)
                .status()?;

            std::fs::remove_file(&tarball_path).ok();

            if !extract_status.success() {
                std::fs::remove_dir_all(&base_dir).ok();
                anyhow::bail!("tar failed to extract base image tarball (exit: {})", extract_status);
            }

            crate::dlog!("OverlayEngine", "INFO", "Base image '{}' extracted successfully to {}", base_name, base_dir);
            Ok(())
        })
        .await?;

        {
            let mut locks = BASE_IMAGE_LOCKS.lock().await;
            if let Some(n) = locks.remove(base_image) {
                n.notify_waiters();
            }
        }

        res?;
        Ok(())
    }

    pub async fn ensure_overlay(manifest: &AppManifest) -> Result<Option<String>> {
        let base_image = manifest.get_base_image();

        let packages_dir = match &manifest.packages {
            Some(pkgs) => pkgs,
            None => {
                crate::dlog!("OverlayEngine", "INFO", "No packages declared for '{}', skipping overlay build.", manifest.app_name);
                return Ok(None);
            }
        };

        let mut all_pkgs = Vec::new();
        let mut pkg_manager = "none";

        if let Some(apt_pkgs) = &packages_dir.apt {
            all_pkgs.extend(apt_pkgs.clone());
            pkg_manager = "apt";
        } else if let Some(apk_pkgs) = &packages_dir.apk {
            all_pkgs.extend(apk_pkgs.clone());
            pkg_manager = "apk";
        } else if let Some(dnf_pkgs) = &packages_dir.dnf {
            all_pkgs.extend(dnf_pkgs.clone());
            pkg_manager = "dnf";
        }

        if all_pkgs.is_empty() {
            crate::dlog!("OverlayEngine", "INFO", "Package list is empty for '{}', skipping overlay build.", manifest.app_name);
            return Ok(None);
        }

        let cache_layer = Self::get_overlay_path(manifest).unwrap();
        let hash = cache_layer.split('/').nth(6).unwrap().to_string();
        let _cache_dir = format!("/var/lib/openback/cache/overlays/{}", hash);

        if std::path::Path::new(&cache_layer).exists() {
            crate::dlog!("OverlayEngine", "INFO", "Package overlay cache HIT for hash {} (packages: {})", &hash[..12], all_pkgs.join(", "));
            return Ok(Some(cache_layer));
        }

        crate::dlog!("OverlayEngine", "INFO", "Package overlay cache MISS for hash {} — building ephemeral sandbox...", &hash[..12]);
        crate::dlog!("OverlayEngine", "INFO", "Packages to install via {}: {:?}", pkg_manager, all_pkgs);

        let notify = {
            let mut locks = OVERLAY_LOCKS.lock().await;
            if let Some(n) = locks.get(&hash) {
                crate::dlog!("OverlayEngine", "INFO", "Another task is already building overlay {}, waiting...", &hash[..12]);
                Some(n.clone())
            } else {
                let n = Arc::new(Notify::new());
                locks.insert(hash.clone(), n.clone());
                None
            }
        };

        if let Some(n) = notify {
            n.notified().await;
            if std::path::Path::new(&cache_layer).exists() {
                return Ok(Some(cache_layer));
            } else {
                anyhow::bail!("Overlay build failed in another task");
            }
        }

        let build_hash = hash.clone();
        let build_base = base_image.clone();
        let build_pkg_manager = pkg_manager.to_string();
        let build_pkgs = all_pkgs.clone();

        let res = task::spawn_blocking(move || {
            Self::build_ephemeral_sandbox(
                &build_base,
                &build_hash,
                &build_pkg_manager,
                &build_pkgs,
            )
        })
        .await?;

        {
            let mut locks = OVERLAY_LOCKS.lock().await;
            if let Some(n) = locks.remove(&hash) {
                n.notify_waiters();
            }
        }

        res?;
        crate::dlog!("OverlayEngine", "INFO", "Overlay build complete. Cache layer ready at: {}", cache_layer);
        Ok(Some(cache_layer))
    }

    fn build_ephemeral_sandbox(
        base_image: &str,
        hash: &str,
        pkg_manager: &str,
        pkgs: &[String],
    ) -> Result<()> {
        let base_dir = format!("/var/lib/openback/store/images/{}", base_image);
        if !std::path::Path::new(&base_dir).exists() {
            anyhow::bail!("[OverlayEngine] Base image directory missing: {}", base_dir);
        }

        let build_root = format!("/var/lib/openback/cache/build_{}", hash);
        let upper_dir = format!("{}/upper", build_root);
        let work_dir = format!("{}/work", build_root);
        let merged_dir = format!("{}/merged", build_root);

        crate::dlog!("OverlayEngine", "INFO", "Creating build sandbox at {}", build_root);
        std::fs::create_dir_all(&upper_dir)?;
        std::fs::create_dir_all(&work_dir)?;
        std::fs::create_dir_all(&merged_dir)?;

        crate::dlog!("OverlayEngine", "INFO", "Unsharing mount namespace for ephemeral build...");
        unshare(CloneFlags::CLONE_NEWNS)?;

        mount(
            None::<&str>,
            "/",
            None::<&str>,
            MsFlags::MS_PRIVATE | MsFlags::MS_REC,
            None::<&str>,
        )
        .ok();

        let mount_options = format!(
            "lowerdir={},upperdir={},workdir={}",
            base_dir, upper_dir, work_dir
        );

        crate::dlog!("OverlayEngine", "INFO", "Mounting build OverlayFS: lower={} upper={}", base_dir, upper_dir);
        mount(
            Some("overlay"),
            merged_dir.as_str(),
            Some("overlay"),
            MsFlags::MS_NODEV,
            Some(mount_options.as_str()),
        )
        .context("Failed to mount build overlayfs")?;

        let proc_dir = format!("{}/proc", merged_dir);
        let dev_dir = format!("{}/dev", merged_dir);
        let resolv_conf = format!("{}/etc/resolv.conf", merged_dir);

        std::fs::create_dir_all(&proc_dir).ok();
        std::fs::create_dir_all(&dev_dir).ok();
        std::fs::create_dir_all(format!("{}/etc", merged_dir)).ok();

        mount(
            Some("proc"),
            proc_dir.as_str(),
            Some("proc"),
            MsFlags::empty(),
            None::<&str>,
        )
        .ok();
        mount(
            Some("/dev"),
            dev_dir.as_str(),
            Some("bind"),
            MsFlags::MS_BIND | MsFlags::MS_REC,
            None::<&str>,
        )
        .ok();

        crate::dlog!("OverlayEngine", "INFO", "Copying host /etc/resolv.conf into chroot for DNS resolution...");
        std::fs::copy("/etc/resolv.conf", &resolv_conf).ok();

        // Build log file — kept for post-mortem debugging
        let build_log_path = format!("/tmp/openback_build_{}.log", &hash[..16]);
        let mut build_log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&build_log_path)
            .ok();
        crate::dlog!("OverlayEngine", "INFO", "Build output will also be captured to {}", build_log_path);

        let mut cmd;
        match pkg_manager {
            "apt" => {
                crate::dlog!("OverlayEngine", "BUILD", "Running apt-get to install: {}", pkgs.join(" "));
                cmd = Command::new("chroot");
                cmd.arg(&merged_dir);
                cmd.arg("sh").arg("-c").arg(format!(
                    "apt-get update && apt-get install -y --no-install-recommends {}",
                    pkgs.join(" ")
                ));
                cmd.env("DEBIAN_FRONTEND", "noninteractive");
                cmd.env("DEBCONF_NONINTERACTIVE_SEEN", "true");
            }
            "apk" => {
                crate::dlog!("OverlayEngine", "BUILD", "Running apk to install: {}", pkgs.join(" "));
                cmd = Command::new("chroot");
                cmd.arg(&merged_dir);
                cmd.arg("sh").arg("-c").arg(format!(
                    "sed -i 's/https/http/g' /etc/apk/repositories && apk add --no-cache {}",
                    pkgs.join(" ")
                ));
            }
            "dnf" => {
                crate::dlog!("OverlayEngine", "BUILD", "Running dnf to install: {}", pkgs.join(" "));
                cmd = Command::new("chroot");
                cmd.arg(&merged_dir);
                cmd.arg("sh").arg("-c").arg(format!(
                    "dnf install -y {}",
                    pkgs.join(" ")
                ));
            }
            _ => anyhow::bail!("Unsupported package manager: {}", pkg_manager),
        }

        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn chroot/{} command", pkg_manager))?;

        let child_stdout = child.stdout.take().unwrap();
        let child_stderr = child.stderr.take().unwrap();

        let mut out_reader = std::io::BufReader::new(child_stdout);
        let mut err_reader = std::io::BufReader::new(child_stderr);

        let mut out_line = String::new();
        loop {
            out_line.clear();
            match out_reader.read_line(&mut out_line) {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = out_line.trim_end();
                    crate::dlog!("OverlayEngine", "BUILD", "{}", trimmed);
                    if let Some(ref mut f) = build_log_file {
                        let _ = writeln!(f, "{}", trimmed);
                    }
                }
                Err(_) => break,
            }
        }

        let mut err_line = String::new();
        loop {
            err_line.clear();
            match err_reader.read_line(&mut err_line) {
                Ok(0) => break,
                Ok(_) => {
                    let trimmed = err_line.trim_end();
                    crate::dlog!("OverlayEngine", "BUILD", "{}", trimmed);
                    if let Some(ref mut f) = build_log_file {
                        let _ = writeln!(f, "{}", trimmed);
                    }
                }
                Err(_) => break,
            }
        }

        let status = child
            .wait()
            .with_context(|| format!("Failed to wait for chroot/{} command", pkg_manager))?;

        // Tear down mounts before checking status so we always clean up
        nix::mount::umount2(proc_dir.as_str(), nix::mount::MntFlags::MNT_DETACH).ok();
        nix::mount::umount2(dev_dir.as_str(), nix::mount::MntFlags::MNT_DETACH).ok();
        nix::mount::umount2(merged_dir.as_str(), nix::mount::MntFlags::MNT_DETACH).ok();

        if !status.success() {
            // Clean up the broken build root so the next apply triggers a fresh build
            crate::dlog!("OverlayEngine", "ERROR", "{} exited with {} — removing broken build root {}", pkg_manager, status, build_root);
            let _ = Command::new("rm").arg("-rf").arg(&build_root).status();
            anyhow::bail!("{} failed with exit status {} while installing {:?}", pkg_manager, status, pkgs);
        }

        crate::dlog!("OverlayEngine", "INFO", "Package installation succeeded. Persisting overlay layer...");

        let cache_dir = format!("/var/lib/openback/cache/overlays/{}", hash);
        let cache_layer = format!("{}/layer", cache_dir);
        std::fs::create_dir_all(&cache_dir)?;

        if let Err(e) = std::fs::rename(&upper_dir, &cache_layer) {
            crate::dlog!("OverlayEngine", "WARN", "rename failed ({}), falling back to cp -a...", e);
            let cp_status = Command::new("cp")
                .arg("-a")
                .arg(&upper_dir)
                .arg(&cache_layer)
                .status();
            if cp_status.map(|s| !s.success()).unwrap_or(true) {
                let _ = Command::new("rm").arg("-rf").arg(&build_root).status();
                anyhow::bail!("Failed to persist overlay layer to cache");
            }
        }

        crate::dlog!("OverlayEngine", "INFO", "Overlay layer persisted at: {}", cache_layer);
        let _ = Command::new("rm").arg("-rf").arg(&build_root).status();

        Ok(())
    }
}
