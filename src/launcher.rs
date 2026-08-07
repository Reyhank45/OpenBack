use openback::manifest::AppManifest;
use anyhow::{Context, Result};
use nix::mount::{mount, umount2, MntFlags, MsFlags};
use nix::sched::{unshare, CloneFlags};
use nix::sys::wait::waitpid;
use nix::unistd::{chdir, fork, pivot_root, ForkResult};
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
    let status = Command::new("ip").args(["link", "set", "lo", "up"]).status();
    if let Err(e) = status {
        eprintln!("[Container PID 1] Warning: Failed to bring up loopback interface using 'ip': {}", e);
    } else if let Ok(exit_status) = status {
        if !exit_status.success() {
            eprintln!("[Container PID 1] Warning: 'ip link set lo up' failed with status: {}", exit_status);
        }
    }

    mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    ).context("Failed to make root private")?;

    let app_workspace = format!("{}/store/apps/{}", std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()), manifest.app_name);
    let app_rootfs = format!("{}/rootfs", app_workspace);
    if manifest.target_gd.is_some() {
        println!("[Container PID 1] [WARN] 'target_gd' is deprecated, please use 'base_image' in the manifest.");
    }

    let base_image = manifest.get_base_image();
    let gd_path = format!("{}/store/bases/{}", std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()), base_image);
    let deps_base = &format!("{}/store/deps", std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()));
    
    std::fs::create_dir_all(&app_rootfs)?;
    
    // 1. Bind-mount GD Base into rootfs
    mount(
        Some(gd_path.as_str()),
        app_rootfs.as_str(),
        Some("bind"),
        MsFlags::MS_BIND,
        None::<&str>,
    ).with_context(|| format!("Failed to bind mount GD Base layer from {}", gd_path))?;

    let deps_in_rootfs = format!("{}/deps", app_rootfs);
    std::fs::create_dir_all(&deps_in_rootfs)?;
    
    // Mount tmpfs over /deps in rootfs
    mount(
        Some("tmpfs"),
        deps_in_rootfs.as_str(),
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    ).context("Failed to mount tmpfs for dependencies")?;

    for dep_str in &manifest.dependencies {
        let parts: Vec<&str> = dep_str.split('@').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid dependency format: {}", dep_str);
        }
        let dep_name = parts[0];
        let dep_version = parts[1];

        let host_dep_path = format!("{}/{}/{}", deps_base, dep_name, dep_version);
        let stage_dep_path = format!("{}/{}/{}", deps_in_rootfs, dep_name, dep_version);
        
        println!("[Container PID 1] Mounting dependency: {}@{}", dep_name, dep_version);
        
        std::fs::create_dir_all(&stage_dep_path)?;
        
        mount(
            Some(host_dep_path.as_str()),
            stage_dep_path.as_str(),
            Some("bind"),
            MsFlags::MS_BIND,
            None::<&str>,
        ).with_context(|| format!("Failed to bind mount {}", host_dep_path))?;

        mount(
            Some("none"),
            stage_dep_path.as_str(),
            Some("bind"),
            MsFlags::MS_REMOUNT | MsFlags::MS_BIND | MsFlags::MS_RDONLY,
            None::<&str>,
        ).with_context(|| format!("Failed to remount {} as read-only", host_dep_path))?;
    }

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
    ).context("Failed to bind mount run directory")?;

    // Mount /app for the application source code
    let app_src_dir = format!("{}/src", app_workspace);
    let rootfs_app = format!("{}/app", app_rootfs);
    std::fs::create_dir_all(&app_src_dir)?;
    std::fs::create_dir_all(&rootfs_app)?;
    mount(
        Some(app_src_dir.as_str()),
        rootfs_app.as_str(),
        Some("bind"),
        MsFlags::MS_BIND,
        None::<&str>,
    ).context("Failed to bind mount app directory")?;

    // Mount /proc for $ORIGIN dynamic linking resolution and other tools
    let rootfs_proc = format!("{}/proc", app_rootfs);
    std::fs::create_dir_all(&rootfs_proc)?;
    mount(
        Some("proc"),
        rootfs_proc.as_str(),
        Some("proc"),
        MsFlags::empty(),
        None::<&str>,
    ).context("Failed to mount /proc")?;

    // Mount /dev using a sterile tmpfs (scoped permissions bridge)
    let rootfs_dev = format!("{}/dev", app_rootfs);
    std::fs::create_dir_all(&rootfs_dev)?;
    mount(
        Some("tmpfs"),
        rootfs_dev.as_str(),
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    ).context("Failed to mount /dev as tmpfs")?;

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
        ).context(format!("Failed to bind mount {}", dev_path))?;
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
            println!("[Container PID 1] Scoped Permission Bridge: Bridging device node {}", dev);
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
    ).context("Failed to mount /tmp")?;

    // Create oldroot for pivot_root before making rootfs read-only
    let old_root = format!("{}/oldroot", app_rootfs);
    std::fs::create_dir_all(&old_root)?;

    // Now remount the rootfs as read-only
    mount(
        Some("none"),
        app_rootfs.as_str(),
        Some("bind"),
        MsFlags::MS_REMOUNT | MsFlags::MS_BIND | MsFlags::MS_RDONLY,
        None::<&str>,
    ).context("Failed to remount rootfs read-only")?;

    // 3. Pivot Root
    println!("[Container PID 1] Changing root to {}", app_rootfs);
    pivot_root(app_rootfs.as_str(), old_root.as_str())
        .context("pivot_root failed")?;

    // Change current directory to new root
    chdir("/").context("Failed to chdir to /")?;

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
                    println!("[Container PID 1] Scoped Permission Bridge: Retaining capability {:?}", cap);
                },
                Err(_) => {
                    eprintln!("[Container PID 1] Warning: Unknown capability requested: {}", cap_str);
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
