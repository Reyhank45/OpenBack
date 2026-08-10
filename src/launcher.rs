use anyhow::{Context, Result};
use nix::mount::{mount, umount2, MntFlags, MsFlags};
use nix::sched::{unshare, CloneFlags};
use nix::sys::wait::waitpid;
use nix::unistd::{chdir, fork, pivot_root, ForkResult};
use openback::manifest::AppManifest;
use std::os::unix::process::CommandExt;
use std::process::Command;

pub fn launch_container(manifest_json: String) -> Result<()> {
    let manifest: AppManifest = serde_json::from_str(&manifest_json)?;

    println!("[Launcher] Unsharing Mount, PID, and Network namespaces...");
    unshare(CloneFlags::CLONE_NEWNS | CloneFlags::CLONE_NEWPID | CloneFlags::CLONE_NEWNET)
        .context("Failed to unshare namespaces (are you running as root?)")?;

    match unsafe { fork() } {
        Ok(ForkResult::Parent { child, .. }) => {
            println!("[Launcher] Forked child process {}", child);
            waitpid(child, None).context("Failed to wait for child process")?;
            println!("[Launcher] Child process exited.");
            std::process::exit(0);
        }
        Ok(ForkResult::Child) => {
            unsafe {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            }

            if let Err(e) = setup_and_exec(manifest) {
                eprintln!("[Container PID 1] Setup failed: {:?}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            anyhow::bail!("Fork failed: {}", e);
        }
    }

    Ok(())
}

fn setup_and_exec(manifest: AppManifest) -> Result<()> {
    println!("[Container PID 1] Setting up GD Base Layer Root...");

    // Bring up the loopback interface in the new network namespace
    println!("[Container PID 1] Bringing up loopback interface...");
    let status = Command::new("ip")
        .args(["link", "set", "lo", "up"])
        .status();
    if let Err(e) = status {
        eprintln!(
            "[Container PID 1] Warning: Failed to bring up loopback interface using 'ip': {}",
            e
        );
    } else if let Ok(exit_status) = status {
        if !exit_status.success() {
            eprintln!(
                "[Container PID 1] Warning: 'ip link set lo up' failed with status: {}",
                exit_status
            );
        }
    }

    mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    )
    .context("Failed to make root private")?;

    let base_image = manifest.get_base_image();
    let base_dir = format!("/var/lib/openback/store/images/{}", base_image);

    let replica_id = &manifest.app_name;
    let replica_base = format!("/var/lib/openback/store/containers/{}", replica_id);
    let replica_upper = format!("{}/upper", replica_base);
    let replica_work = format!("{}/work", replica_base);
    let target_root = format!("{}/rootfs", replica_base);

    std::fs::create_dir_all(&replica_upper)?;
    std::fs::create_dir_all(&replica_work)?;
    std::fs::create_dir_all(&target_root)?;

    let mut lower_dirs = vec![base_dir.clone()];
    if let Some(overlay_path) =
        openback::engine::overlay::OverlayEngine::get_overlay_path(&manifest)
    {
        if std::path::Path::new(&overlay_path).exists() {
            lower_dirs.insert(0, overlay_path); // Package overlay is on top of base image
        }
    }

    let lower_dirs_str = lower_dirs.join(":");
    let mount_options = format!(
        "lowerdir={},upperdir={},workdir={}",
        lower_dirs_str, replica_upper, replica_work
    );

    mount(
        Some("overlay"),
        target_root.as_str(),
        Some("overlay"),
        MsFlags::MS_NODEV,
        Some(mount_options.as_str()),
    )
    .context("Failed to mount 3-tier OverlayFS")?;

    let app_rootfs = target_root;
    let app_workspace = replica_base;

    // Mount /run for IPC sockets
    let app_run_dir = format!("{}/run", app_workspace);
    let rootfs_run = format!("{}/run", app_rootfs);
    std::fs::create_dir_all(&app_run_dir)?;
    std::fs::create_dir_all(&rootfs_run)?;
    mount(
        Some(app_run_dir.as_str()),
        rootfs_run.as_str(),
        Some("bind"),
        MsFlags::MS_BIND,
        None::<&str>,
    )
    .context("Failed to bind mount run directory")?;

    // Mount /app for the application source code if provided
    let rootfs_app = format!("{}/app", app_rootfs);
    std::fs::create_dir_all(&rootfs_app)?;
    if let Some(app_source) = &manifest.app_source {
        let host_app_dir = app_source; // Path relative to launcher or absolute
        std::fs::create_dir_all(host_app_dir).ok();
        mount(
            Some(host_app_dir.as_str()),
            rootfs_app.as_str(),
            Some("bind"),
            MsFlags::MS_BIND | MsFlags::MS_REC,
            None::<&str>,
        )
        .context("Failed to bind mount app_source directory")?;
    }

    // Mount /proc for $ORIGIN dynamic linking resolution and other tools
    let rootfs_proc = format!("{}/proc", app_rootfs);
    std::fs::create_dir_all(&rootfs_proc)?;
    mount(
        Some("proc"),
        rootfs_proc.as_str(),
        Some("proc"),
        MsFlags::empty(),
        None::<&str>,
    )
    .context("Failed to mount /proc")?;

    // Mount /dev using a sterile tmpfs (scoped permissions bridge)
    let rootfs_dev = format!("{}/dev", app_rootfs);
    std::fs::create_dir_all(&rootfs_dev)?;
    mount(
        Some("tmpfs"),
        rootfs_dev.as_str(),
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    )
    .context("Failed to mount /dev as tmpfs")?;

    // Helper to bind-mount specific devices from the host
    let bind_mount_device = |dev_path: &str| -> Result<()> {
        let dest = format!("{}{}", app_rootfs, dev_path);
        if let Some(parent) = std::path::Path::new(&dest).parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::File::create(&dest).context(format!("Failed to touch {}", dest))?;
        mount(
            Some(dev_path),
            dest.as_str(),
            Some("bind"),
            MsFlags::MS_BIND,
            None::<&str>,
        )
        .context(format!("Failed to bind mount {}", dev_path))?;
        Ok(())
    };

    // Hardcode essential pseudo-devices
    bind_mount_device("/dev/null")?;
    bind_mount_device("/dev/zero")?;
    bind_mount_device("/dev/urandom")?;
    bind_mount_device("/dev/random")?;

    // Bind mount dynamic user-requested devices
    if let Some(perms) = &manifest.permissions {
        for dev in &perms.devices {
            println!(
                "[Container PID 1] Scoped Permission Bridge: Bridging device node {}",
                dev
            );
            bind_mount_device(dev)?;
        }
    }

    // Mount /tmp for temporary scratch space and writable HOME
    let rootfs_tmp = format!("{}/tmp", app_rootfs);
    std::fs::create_dir_all(&rootfs_tmp)?;
    mount(
        Some("tmpfs"),
        rootfs_tmp.as_str(),
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    )
    .context("Failed to mount /tmp")?;

    // Create oldroot for pivot_root before making rootfs read-only
    let old_root = format!("{}/oldroot", app_rootfs);
    std::fs::create_dir_all(&old_root)?;

    // The rootfs is already overlayfs, so we don't strictly need to remount it read-only
    // since upperdir gives us the writable tmp space we need, but we could if we wanted strict ro root.

    // 3. Pivot Root
    println!("[Container PID 1] Changing root to {}", app_rootfs);
    pivot_root(app_rootfs.as_str(), old_root.as_str()).context("pivot_root failed")?;

    // Change current directory to work_dir
    let work_dir = manifest
        .work_dir
        .clone()
        .unwrap_or_else(|| "/app".to_string());
    chdir(work_dir.as_str()).context(format!("Failed to chdir to {}", work_dir))?;

    // Unmount the old root
    umount2("/oldroot", MntFlags::MNT_DETACH).context("Failed to unmount old root")?;

    if manifest.entrypoint.is_empty() {
        anyhow::bail!("Entrypoint array is empty");
    }

    let cmd_name = &manifest.entrypoint[0];
    let cmd_args = &manifest.entrypoint[1..];

    println!("[Container PID 1] Executing: {} {:?}", cmd_name, cmd_args);

    let mut cmd = Command::new(cmd_name);
    cmd.args(cmd_args);

    // Inject custom environment variables
    for (k, v) in &manifest.env {
        cmd.env(k, v);
    }

    if let Ok(fifo_path) = std::env::var("OPENBACK_STDIN_FIFO") {
        if let Ok(fifo_file) = std::fs::OpenOptions::new()
            .read(true)
            .write(true) // O_RDWR prevents blocking on open for FIFOs
            .open(&fifo_path)
        {
            cmd.stdin(std::process::Stdio::from(fifo_file));
        }
    }

    if let Some(net) = manifest.networking {
        if !net.ipc_socket.is_empty() {
            cmd.env("OPENBACK_IPC_SOCKET", net.ipc_socket);
        }
    } else {
        cmd.env("OPENBACK_IPC_SOCKET", "/run/app.sock");
    }

    // Handle Linux Capabilities (Scoped Permission Bridge)
    let mut requested_caps = std::collections::HashSet::new();
    if let Some(perms) = &manifest.permissions {
        for cap_str in &perms.capabilities {
            match cap_str.parse::<caps::Capability>() {
                Ok(cap) => {
                    requested_caps.insert(cap);
                    println!(
                        "[Container PID 1] Scoped Permission Bridge: Retaining capability {:?}",
                        cap
                    );
                }
                Err(_) => {
                    eprintln!(
                        "[Container PID 1] Warning: Unknown capability requested: {}",
                        cap_str
                    );
                }
            }
        }
    }

    if let Ok(bounding) = caps::read(None, caps::CapSet::Bounding) {
        for cap in bounding {
            if !requested_caps.contains(&cap) {
                let _ = caps::drop(None, caps::CapSet::Bounding, cap);
            }
        }
    }
    if let Ok(inheritable) = caps::read(None, caps::CapSet::Inheritable) {
        for cap in inheritable {
            if !requested_caps.contains(&cap) {
                let _ = caps::drop(None, caps::CapSet::Inheritable, cap);
            }
        }
    }

    let err = cmd.exec();
    anyhow::bail!("Failed to exec {}: {}", cmd_name, err);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_launcher_compiles_and_links() {
        assert!(true);
    }
}
