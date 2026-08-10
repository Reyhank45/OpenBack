use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use openback::manifest::AppManifest;
use openback::rpc::{BaseInfo, BaseManifest, DepInfo, EngineRequest, EngineResponse};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::io::FromRawFd;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

pub struct ContainerState {
    pub pid: u32,
    pub is_building: bool,
    pub exit_code: Option<i32>,
    pub manifest: AppManifest,
    pub start_time: DateTime<Local>,
    pub log_file: String,
    pub stdin_fifo: Option<String>,
    pub proxy_tasks: Vec<tokio::task::JoinHandle<()>>,
}

fn get_dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut size = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                size += get_dir_size(&entry.path())?;
            } else {
                size += metadata.len();
            }
        }
    } else {
        size += path.metadata()?.len();
    }
    Ok(size)
}

pub async fn run_daemon(port: Option<u16>) -> Result<()> {
    let socket_path =
        &std::env::var("OPENBACK_SOCKET").unwrap_or_else(|_| "/tmp/openbackd.sock".to_string());
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("Failed to bind to {}", socket_path))?;

    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o777))
        .context("Failed to set socket permissions")?;

    let state: Arc<Mutex<HashMap<String, ContainerState>>> = Arc::new(Mutex::new(HashMap::new()));

    // Synchronous Recovery blocks the thread
    recover_containers(state.clone()).await;

    openback::dlog!(
        "Daemon",
        "INFO",
        "OpenBack Engine listening on {}",
        socket_path
    );

    let state_tcp = state.clone();

    if let Some(p) = port {
        openback::dlog!(
            "Daemon",
            "INFO",
            "OpenBack Engine listening on TCP 0.0.0.0:{}",
            p
        );
        let tcp_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", p)).await?;

        tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = tcp_listener.accept().await {
                    let state_clone = state_tcp.clone();
                    tokio::spawn(async move {
                        let (reader, mut writer) = stream.split();
                        let mut reader = tokio::io::BufReader::new(reader);
                        let mut line = String::new();
                        while let Ok(n) =
                            tokio::io::AsyncBufReadExt::read_line(&mut reader, &mut line).await
                        {
                            if n == 0 {
                                break;
                            }

                            if let Ok(envelope) =
                                serde_json::from_str::<openback::rpc::EngineEnvelope>(&line)
                            {
                                let response =
                                    handle_request(envelope.request, state_clone.clone()).await;
                                let mut response_str = serde_json::to_string(&response).unwrap();
                                response_str.push('\n');
                                let _ = tokio::io::AsyncWriteExt::write_all(
                                    &mut writer,
                                    response_str.as_bytes(),
                                )
                                .await;
                            } else if let Ok(request) =
                                serde_json::from_str::<openback::rpc::EngineRequest>(&line)
                            {
                                let response = handle_request(request, state_clone.clone()).await;
                                let mut response_str = serde_json::to_string(&response).unwrap();
                                response_str.push('\n');
                                let _ = tokio::io::AsyncWriteExt::write_all(
                                    &mut writer,
                                    response_str.as_bytes(),
                                )
                                .await;
                            }
                            line.clear();
                        }
                    });
                }
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.split();
                    let mut buf_reader = BufReader::new(reader);
                    let mut line = String::new();

                    if let Ok(bytes_read) = buf_reader.read_line(&mut line).await {
                        if bytes_read == 0 {
                            return;
                        }

                        let request_clone =
                            match serde_json::from_str::<openback::rpc::EngineEnvelope>(&line) {
                                Ok(env) => Some(env.request),
                                Err(_) => None,
                            };

                        let response = match request_clone.clone() {
                            Some(request) => handle_request(request, state.clone()).await,
                            None => EngineResponse::Error("Invalid RPC request".to_string()),
                        };

                        if let Ok(response_str) = serde_json::to_string(&response) {
                            let _ = writer
                                .write_all(format!("{}\n", response_str).as_bytes())
                                .await;
                        }

                        if matches!(response, EngineResponse::AttachStream) {
                            if let Some(EngineRequest::Attach { container_name }) = request_clone {
                                handle_attach_stream(container_name, state.clone(), buf_reader, writer)
                                    .await;
                            }
                        }
                    }
                });
            }
            Err(e) => openback::dlog!("Daemon", "ERROR", "Failed to accept connection: {}", e),
        }
    }
}

async fn handle_attach_stream(
    container_name: String,
    state: Arc<tokio::sync::Mutex<HashMap<String, ContainerState>>>,
    mut buf_reader: BufReader<tokio::net::unix::ReadHalf<'_>>,
    mut writer: tokio::net::unix::WriteHalf<'_>,
) {
    let (log_file, stdin_fifo) = {
        let map = state.lock().await;
        if let Some(inst) = map.get(&container_name) {
            if inst.exit_code.is_none() && !inst.is_building {
                (inst.log_file.clone(), inst.stdin_fifo.clone())
            } else {
                return;
            }
        } else {
            return;
        }
    };

    let log_file_path = log_file.clone();

    // Spawn task to read logs and write to socket
    let mut log_child = match tokio::process::Command::new("tail")
        .arg("-n")
        .arg("50")
        .arg("-f")
        .arg(&log_file_path)
        .stdout(std::process::Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return,
    };

    let mut log_stdout = log_child.stdout.take().unwrap();

    let to_socket = tokio::io::copy(&mut log_stdout, &mut writer);

    // Write to FIFO
    let from_socket = async {
        if let Some(fifo_path) = stdin_fifo {
            if let Ok(mut fifo) = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&fifo_path)
                .await
            {
                let _ = tokio::io::copy(&mut buf_reader, &mut fifo).await;
            } else {
                let mut discard = tokio::io::sink();
                let _ = tokio::io::copy(&mut buf_reader, &mut discard).await;
            }
        } else {
            let mut discard = tokio::io::sink();
            let _ = tokio::io::copy(&mut buf_reader, &mut discard).await;
        }
    };

    tokio::select! {
        _ = to_socket => (),
        _ = from_socket => (),
    }

    let _ = log_child.kill().await;
}

async fn recover_containers(
    state: Arc<tokio::sync::Mutex<std::collections::HashMap<String, ContainerState>>>,
) {
    let store_dir = std::env::var("OPENBACK_STORE_DIR")
        .unwrap_or_else(|_| "/var/lib/openback/store".to_string());
    let containers_dir = format!("{}/containers", store_dir);
    if let Ok(entries) = std::fs::read_dir(&containers_dir) {
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_dir() {
                    let replica_name = entry.file_name().to_string_lossy().to_string();
                    let pid_path = entry.path().join("pid");
                    let manifest_path = entry.path().join("manifest.json");
                    let start_time_path = entry.path().join("start_time");

                    if pid_path.exists() {
                        let pid_str = std::fs::read_to_string(&pid_path).unwrap_or_default();
                        if let Ok(pid) = pid_str.trim().parse::<u32>() {
                            let fd = unsafe {
                                libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0)
                            };
                            if fd >= 0 {
                                // Explicitly handle missing manifest
                                if !manifest_path.exists() {
                                    openback::dlog!("Daemon", "ERROR", "Recovery: manifest.json missing for container '{}'. Killing orphaned PID {}.", replica_name, pid);
                                    let _ = nix::sys::signal::kill(
                                        nix::unistd::Pid::from_raw(pid as i32),
                                        nix::sys::signal::Signal::SIGKILL,
                                    );
                                    unsafe { libc::close(fd as i32) };
                                    let _ = std::fs::remove_dir_all(entry.path());
                                    continue;
                                }

                                let manifest_json =
                                    std::fs::read_to_string(&manifest_path).unwrap_or_default();
                                let manifest: openback::manifest::AppManifest =
                                    match serde_json::from_str(&manifest_json) {
                                        Ok(m) => m,
                                        Err(e) => {
                                            openback::dlog!(
                                            "Daemon", "ERROR",
                                            "Recovery: manifest.json for container '{}' is corrupt or unreadable ({}). \
                                             Killing orphaned PID {} and removing stale directory to allow clean replacement.",
                                            replica_name, e, pid
                                        );
                                            let _ = nix::sys::signal::kill(
                                                nix::unistd::Pid::from_raw(pid as i32),
                                                nix::sys::signal::Signal::SIGKILL,
                                            );
                                            unsafe { libc::close(fd as i32) };
                                            let _ = std::fs::remove_dir_all(entry.path());
                                            continue;
                                        }
                                    };

                                openback::dlog!(
                                    "Daemon",
                                    "INFO",
                                    "Recovered running container: {} with PID {}",
                                    replica_name,
                                    pid
                                );

                                let start_time_str =
                                    std::fs::read_to_string(&start_time_path).unwrap_or_default();
                                let start_time =
                                    chrono::DateTime::parse_from_rfc3339(start_time_str.trim())
                                        .map(|dt| dt.with_timezone(&chrono::Local))
                                        .unwrap_or_else(|_| chrono::Local::now());



                                let log_dir = format!("{}/logs", store_dir);
                                let log_file = format!("{}/{}.log", log_dir, replica_name);
                                let fifo_path = format!("{}/{}.fifo", log_dir, replica_name);

                                let mut proxy_tasks = Vec::new();
                                if let Some(networking) = &manifest.networking {
                                    for port_mapping in &networking.ports {
                                        let host_port = port_mapping.host_port;
                                        let app_name_inner = replica_name.clone();
                                        let container_socket =
                                            port_mapping.container_socket.clone();

                                        match tokio::net::TcpListener::bind(format!(
                                            "0.0.0.0:{}",
                                            host_port
                                        ))
                                        .await
                                        {
                                            Ok(tcp_listener) => {
                                                let store_dir_clone = store_dir.clone();
                                                let proxy_task = tokio::spawn(async move {
                                                    let socket_path = container_socket.replace(
                                                        "/run",
                                                        &format!(
                                                            "{}/containers/{}/run",
                                                            store_dir_clone, app_name_inner
                                                        ),
                                                    );
                                                    while let Ok((mut tcp_stream, _)) =
                                                        tcp_listener.accept().await
                                                    {
                                                        if let Ok(mut unix_stream) =
                                                            tokio::net::UnixStream::connect(
                                                                &socket_path,
                                                            )
                                                            .await
                                                        {
                                                            tokio::spawn(async move {
                                                                let _ =
                                                                    tokio::io::copy_bidirectional(
                                                                        &mut tcp_stream,
                                                                        &mut unix_stream,
                                                                    )
                                                                    .await;
                                                            });
                                                        }
                                                    }
                                                });
                                                proxy_tasks.push(proxy_task);
                                            }
                                            Err(e) => {
                                                openback::dlog!(
                                                    "Daemon", "ERROR",
                                                    "Recovery: cannot re-bind host port {} for replica '{}': {} \
                                                     — port conflict detected; proxy for this replica will NOT be active.",
                                                    host_port, replica_name, e
                                                );
                                            }
                                        }
                                    }
                                }

                                let state_clone2 = state.clone();
                                let replica_name_clone = replica_name.clone();
                                let fifo_path_clone = fifo_path.clone();
                                tokio::spawn(async move {
                                    let owned_fd =
                                        unsafe { std::os::fd::OwnedFd::from_raw_fd(fd as i32) };
                                    if let Ok(async_fd) = tokio::io::unix::AsyncFd::new(owned_fd) {
                                        let mut guard = async_fd.readable().await.unwrap();
                                        guard.clear_ready();
                                    }
                                    openback::dlog!(
                                        "Daemon",
                                        "INFO",
                                        "Recovered container '{}' (PID {}) exited",
                                        replica_name_clone,
                                        pid
                                    );
                                    let mut map = state_clone2.lock().await;
                                    if let Some(inst) = map.get_mut(&replica_name_clone) {
                                        for task in inst.proxy_tasks.drain(..) {
                                            task.abort();
                                        }
                                        inst.exit_code = Some(-1);
                                        let _ = std::fs::remove_file(&fifo_path_clone);
                                    }
                                });

                                let mut map = state.lock().await;
                                map.insert(
                                    replica_name.clone(),
                                    ContainerState {
                                        pid,
                                        is_building: false,
                                        exit_code: None,
                                        manifest,
                                        start_time,
                                        log_file,
                                        stdin_fifo: Some(fifo_path),
                                        proxy_tasks,
                                    },
                                );
                            } else {
                                openback::dlog!("Daemon", "INFO", "Recovery: container '{}' (PID {}) is no longer running. Registering as stopped.", replica_name, pid);
                                if !manifest_path.exists() {
                                    let _ = std::fs::remove_dir_all(entry.path());
                                    continue;
                                }
                                let manifest_json =
                                    std::fs::read_to_string(&manifest_path).unwrap_or_default();
                                if let Ok(manifest) =
                                    serde_json::from_str::<openback::manifest::AppManifest>(
                                        &manifest_json,
                                    )
                                {
                                    let start_time_str = std::fs::read_to_string(&start_time_path)
                                        .unwrap_or_default();
                                    let start_time =
                                        chrono::DateTime::parse_from_rfc3339(start_time_str.trim())
                                            .map(|dt| dt.with_timezone(&chrono::Local))
                                            .unwrap_or_else(|_| chrono::Local::now());
                                    let mut map = state.lock().await;
                                    map.insert(
                                        replica_name.clone(),
                                        ContainerState {
                                            manifest,
                                            pid: 0,
                                            start_time,
                                            is_building: false,
                                            exit_code: Some(0),
                                            log_file: format!(
                                                "{}/logs/{}.log",
                                                store_dir, replica_name
                                            ),
                                            stdin_fifo: None,
                                            proxy_tasks: vec![],
                                        },
                                    );
                                } else {
                                    let _ = std::fs::remove_dir_all(entry.path());
                                }
                            }
                        } else {
                            openback::dlog!("Daemon", "WARN", "Recovery: container '{}' has an unreadable PID file; removing stale directory.", replica_name);
                            let _ = std::fs::remove_dir_all(entry.path());
                        }
                    }
                }
            }
        }
    }
}

async fn spawn_replica(
    manifest: openback::manifest::AppManifest,
    state: Arc<tokio::sync::Mutex<std::collections::HashMap<String, ContainerState>>>,
) -> Result<(), String> {
    let replica_name = manifest.app_name.clone();
    let replica_name = manifest.app_name.clone();
    
    {
        let mut map = state.lock().await;
        if let Some(inst) = map.get(&replica_name) {
            if inst.exit_code.is_none() {
                return Err("Already running".into());
            }
        }

        let log_dir = &format!(
            "{}/logs",
            std::env::var("OPENBACK_STORE_DIR")
                .unwrap_or_else(|_| "/var/lib/openback/store".to_string())
        );
        let log_file = format!("{}/{}.log", log_dir, replica_name);

        openback::dlog!(
            "Daemon",
            "INFO",
            "Registering container '{}' in state map (Building)",
            replica_name
        );
        map.insert(
            replica_name.clone(),
            ContainerState {
                pid: 0,
                is_building: true,
                exit_code: None,
                manifest: manifest.clone(),
                start_time: chrono::Local::now(),
                log_file,
                stdin_fifo: None,
                proxy_tasks: vec![],
            },
        );
    }

    let state_clone = state.clone();
    tokio::spawn(async move {
        openback::dlog!(
            "Daemon",
            "INFO",
            "[{}] Step 1: Ensuring base image '{}' is available...",
            replica_name,
            manifest.get_base_image()
        );
        if let Err(e) =
            openback::engine::overlay::OverlayEngine::ensure_base_image(&manifest.get_base_image())
                .await
        {
            openback::dlog!(
                "Daemon",
                "ERROR",
                "[{}] ERROR: Failed to ensure base image: {}",
                manifest.app_name,
                e
            );
            let mut map = state_clone.lock().await;
            map.remove(&replica_name);
            return;
        }

        openback::dlog!(
            "Daemon",
            "INFO",
            "[{}] Step 2: Ensuring package overlay layer is built and cached...",
            replica_name
        );
        if let Err(e) = openback::engine::overlay::OverlayEngine::ensure_overlay(&manifest).await {
            openback::dlog!(
                "Daemon",
                "ERROR",
                "[{}] ERROR: Failed to ensure overlay: {}",
                manifest.app_name,
                e
            );
            let mut map = state_clone.lock().await;
            map.remove(&replica_name);
            return;
        }

        let log_dir = &format!(
            "{}/logs",
            std::env::var("OPENBACK_STORE_DIR")
                .unwrap_or_else(|_| "/var/lib/openback/store".to_string())
        );
        let _ = std::fs::create_dir_all(log_dir);
        let log_file = format!("{}/{}.log", log_dir, manifest.app_name);

        let log_out = match std::fs::File::create(&log_file) {
            Ok(f) => f,
            Err(e) => {
                openback::dlog!(
                    "Daemon",
                    "ERROR",
                    "[{}] ERROR: Failed to create log file: {}",
                    replica_name,
                    e
                );
                return;
            }
        };
        let log_err = match log_out.try_clone() {
            Ok(f) => f,
            Err(_) => return,
        };

        let manifest_json = serde_json::to_string(&manifest).unwrap();
        let current_exe = std::env::current_exe().unwrap_or_else(|_| "openback".into());

        let mut proxy_tasks = Vec::new();
        if let Some(networking) = &manifest.networking {
            for port_mapping in &networking.ports {
                let host_port = port_mapping.host_port;
                let app_name_inner = replica_name.clone();
                let container_socket = port_mapping.container_socket.clone();

                let tcp_listener = match tokio::net::TcpListener::bind(format!(
                    "0.0.0.0:{}",
                    host_port
                ))
                .await
                {
                    Ok(l) => l,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::AddrInUse {
                            openback::dlog!(
                                    "Daemon", "ERROR",
                                    "[{}] Port conflict: host port {} is already bound by another process \
                                     (possible orphaned replica). Aborting spawn to avoid double-bind.",
                                    replica_name, host_port
                                );
                        } else {
                            openback::dlog!(
                                "Daemon",
                                "ERROR",
                                "[{}] ERROR: Failed to bind TCP port {}: {}",
                                replica_name,
                                host_port,
                                e
                            );
                        }
                        let mut map = state_clone.lock().await;
                        map.remove(&replica_name);
                        return;
                    }
                };

                let replica_name_clone = replica_name.clone();
                let proxy_task = tokio::spawn(async move {
                    let socket_path = container_socket.replace(
                        "/run",
                        &format!(
                            "{}/containers/{}/run",
                            std::env::var("OPENBACK_STORE_DIR")
                                .unwrap_or_else(|_| "/var/lib/openback/store".to_string()),
                            replica_name_clone
                        ),
                    );
                    while let Ok((mut tcp_stream, _)) = tcp_listener.accept().await {
                        if let Ok(mut unix_stream) =
                            tokio::net::UnixStream::connect(&socket_path).await
                        {
                            tokio::spawn(async move {
                                let _ = tokio::io::copy_bidirectional(
                                    &mut tcp_stream,
                                    &mut unix_stream,
                                )
                                .await;
                            });
                        }
                    }
                });
                proxy_tasks.push(proxy_task);
            }
        }

        let fifo_path = format!("{}/{}.fifo", log_dir, replica_name);
        let _ = std::fs::remove_file(&fifo_path);
        let _ = nix::unistd::mkfifo(
            fifo_path.as_str(),
            nix::sys::stat::Mode::from_bits_truncate(0o666),
        );

        match tokio::process::Command::new(current_exe)
            .arg("internal-launcher")
            .arg(&manifest_json)
            .env("OPENBACK_STDIN_FIFO", &fifo_path)
            .stdout(std::process::Stdio::from(log_out))
            .stderr(std::process::Stdio::from(log_err))
            .spawn()
        {
            Ok(mut child) => {
                let pid = child.id().unwrap_or(0);
                let replica_name_inner = replica_name.clone();

                let store_dir = std::env::var("OPENBACK_STORE_DIR")
                    .unwrap_or_else(|_| "/var/lib/openback/store".to_string());
                let containers_dir = format!("{}/containers/{}", store_dir, replica_name);
                let _ = std::fs::create_dir_all(&containers_dir);
                let _ = std::fs::write(format!("{}/pid", containers_dir), pid.to_string());
                let _ = std::fs::write(format!("{}/manifest.json", containers_dir), &manifest_json);
                let _ = std::fs::write(
                    format!("{}/start_time", containers_dir),
                    chrono::Local::now().to_rfc3339(),
                );

                {
                    let mut map = state_clone.lock().await;
                    if let Some(inst) = map.get_mut(&replica_name_inner) {
                        inst.pid = pid;
                        inst.is_building = false;
                        inst.proxy_tasks = proxy_tasks;
                        inst.stdin_fifo = Some(fifo_path.clone());
                    }
                }

                let state_clone2 = state_clone.clone();
                let replica_name_inner = replica_name.clone();
                let fifo_path_clone = fifo_path.clone();
                tokio::spawn(async move {
                    let exit_status = child.wait().await;
                    let code = exit_status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                    openback::dlog!(
                        "Daemon",
                        "INFO",
                        "[{}] Process exited with code {}",
                        replica_name_inner,
                        code
                    );
                    let mut map = state_clone2.lock().await;
                    if let Some(inst) = map.get_mut(&replica_name_inner) {
                        for task in inst.proxy_tasks.drain(..) {
                            task.abort();
                        }
                        inst.exit_code = Some(code);
                        let _ = std::fs::remove_file(&fifo_path_clone);
                    }
                });
            }
            Err(e) => {
                openback::dlog!(
                    "Daemon",
                    "ERROR",
                    "[{}] ERROR: Failed to spawn child process: {}",
                    replica_name,
                    e
                );
                let mut map = state_clone.lock().await;
                map.remove(&replica_name);
            }
        }
    });

    Ok(())
}

async fn handle_request(
    request: EngineRequest,
    state: Arc<Mutex<HashMap<String, ContainerState>>>,
) -> EngineResponse {
    match request {
        EngineRequest::Attach { container_name } => {
            let map = state.lock().await;
            if let Some(inst) = map.get(&container_name) {
                if inst.exit_code.is_none() && !inst.is_building {
                    EngineResponse::AttachStream
                } else {
                    EngineResponse::Error("Container is not running".to_string())
                }
            } else {
                EngineResponse::Error(format!("Container '{}' not found", container_name))
            }
        }
        EngineRequest::Run(manifest) => match spawn_replica(manifest, state.clone()).await {
            Ok(_) => EngineResponse::Ok("Dispatched successfully".to_string()),
            Err(e) => EngineResponse::Error(e),
        },
        EngineRequest::Start { container_name } => {
            let manifest = {
                let map = state.lock().await;
                map.get(&container_name).map(|inst| inst.manifest.clone())
            };
            if let Some(manifest) = manifest {
                match spawn_replica(manifest, state.clone()).await {
                    Ok(_) => EngineResponse::Ok("Container started successfully".to_string()),
                    Err(e) => EngineResponse::Error(e),
                }
            } else {
                EngineResponse::Error("Container not found".to_string())
            }
        }
        EngineRequest::Ps { all } => {
            let map = state.lock().await;
            let mut containers = Vec::new();
            for (name, inst) in map.iter() {
                if !all && inst.exit_code.is_some() {
                    continue;
                }
                containers.push(openback::rpc::ContainerInfo {
                    container_name: name.clone(),
                    pid: inst.pid,
                    status: if let Some(code) = inst.exit_code {
                        format!("Exited ({})", code)
                    } else if inst.is_building {
                        "Building".to_string()
                    } else {
                        "Running".to_string()
                    },
                    start_time: inst.start_time.format("%Y-%m-%d %H:%M:%S").to_string(),
                });
            }
            EngineResponse::ContainerList(containers)
        }
        EngineRequest::Rm { container_name } => {
            let mut map = state.lock().await;
            if let Some(inst) = map.get(&container_name) {
                if inst.pid > 0 && inst.exit_code.is_none() {
                    return EngineResponse::Error(
                        "Container is still running. Stop it first.".to_string(),
                    );
                }
                
                let store_dir = std::env::var("OPENBACK_STORE_DIR")
                    .unwrap_or_else(|_| "/var/lib/openback/store".to_string());
                let container_dir = format!("{}/containers/{}", store_dir, container_name);
                let _ = std::fs::remove_dir_all(container_dir);
                
                map.remove(&container_name);
                EngineResponse::Ok(format!("Removed container {}", container_name))
            } else {
                EngineResponse::Error(format!("Container {} not found", container_name))
            }
        }
        EngineRequest::Stop { container_name } => {
            let mut map = state.lock().await;
            if let Some(name) = container_name {
                if let Some(inst) = map.get_mut(&name) {
                    for task in inst.proxy_tasks.drain(..) {
                        task.abort();
                    }
                    if inst.exit_code.is_none() && inst.pid > 0 {
                        let _ = signal::kill(Pid::from_raw(inst.pid as i32), Signal::SIGKILL);
                    }
                    EngineResponse::Ok(format!("Container '{}' stopped", name))
                } else {
                    EngineResponse::Error(format!("Container '{}' not found", name))
                }
            } else {
                let mut stopped = 0;
                for inst in map.values_mut() {
                    for task in inst.proxy_tasks.drain(..) {
                        task.abort();
                    }
                    if inst.exit_code.is_none() && inst.pid > 0 {
                        let _ = signal::kill(Pid::from_raw(inst.pid as i32), Signal::SIGKILL);
                        stopped += 1;
                    }
                }
                EngineResponse::Ok(format!("Stopped {} containers", stopped))
            }
        }
        EngineRequest::DepsList => {
            let map = state.lock().await;
            let deps_path = &std::path::PathBuf::from("/var/lib/openback/cache/overlays");
            let mut deps = Vec::new();
            if deps_path.exists() {
                if let Ok(entries) = std::fs::read_dir(deps_path) {
                    for entry in entries.flatten() {
                        let hash_name = entry.file_name().into_string().unwrap_or_default();
                        if let Ok(meta) = entry.metadata() {
                            if meta.is_dir() {
                                let size_bytes = get_dir_size(&entry.path()).unwrap_or(0);
                                let mut dep_name = "overlay".to_string();
                                let mut consumers = Vec::new();
                                for (name, inst) in map.iter() {
                                    if let Some(overlay_path) = openback::engine::overlay::OverlayEngine::get_overlay_path(&inst.manifest) {
                                        if overlay_path.contains(&hash_name) {
                                            if !consumers.contains(name) {
                                                consumers.push(name.clone());
                                            }
                                            if dep_name == "overlay" {
                                                if let Some(pkgs) = &inst.manifest.packages {
                                                    let mut all_pkgs = Vec::new();
                                                    if let Some(apt) = &pkgs.apt { all_pkgs.extend(apt.clone()); }
                                                    if let Some(apk) = &pkgs.apk { all_pkgs.extend(apk.clone()); }
                                                    if let Some(dnf) = &pkgs.dnf { all_pkgs.extend(dnf.clone()); }
                                                    if !all_pkgs.is_empty() {
                                                        dep_name = all_pkgs.join(",");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                let created_time = meta.created().ok().map(|t| {
                                    let dt: chrono::DateTime<chrono::Local> = t.into();
                                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                                });

                                let mut target_os = Some("unknown".to_string());
                                let mut target_libc = Some("unknown".to_string());
                                let mut target_arch = Some("unknown".to_string());
                                let manifest_path = entry.path().join("overlay_manifest.json");
                                if let Ok(content) = std::fs::read_to_string(&manifest_path) {
                                    if let Ok(manifest) = serde_json::from_str::<BaseManifest>(&content) {
                                        target_os = Some(manifest.os);
                                        target_libc = Some(manifest.libc);
                                        target_arch = Some(manifest.architecture);
                                    }
                                }

                                deps.push(DepInfo {
                                    name: dep_name,
                                    version: hash_name,
                                    size_bytes,
                                    consumers,
                                    created_time,
                                    target_os,
                                    target_libc,
                                    target_arch,
                                });
                            }
                        }
                    }
                }
            }
            EngineResponse::DepsList(deps)
        }
        EngineRequest::DepsInspect(name) => {
            let map = state.lock().await;
            let parts: Vec<&str> = name.split('@').collect();
            if parts.len() != 2 {
                return EngineResponse::Error(format!(
                    "Invalid dependency format: {}. Use name@version",
                    name
                ));
            }
            let dep_path = format!(
                "{}/deps/{}/{}",
                std::env::var("OPENBACK_STORE_DIR")
                    .unwrap_or_else(|_| "/var/lib/openback/store".to_string()),
                parts[0],
                parts[1]
            );
            let path = std::path::Path::new(&dep_path);
            if !path.exists() {
                return EngineResponse::Error(format!("Dependency '{}' not found", name));
            }

            let size_bytes = get_dir_size(path).unwrap_or(0);
            let mut consumers = Vec::new();
            for (name_, inst) in map.iter() {
                if inst.manifest.dependencies.contains(&name) && !consumers.contains(name_) {
                    consumers.push(name_.clone());
                }
            }
            let created_time = path
                .metadata()
                .ok()
                .and_then(|m| m.created().ok())
                .map(|t| {
                    let dt: chrono::DateTime<chrono::Local> = t.into();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                });

            let mut target_os = Some("unknown".to_string());
            let mut target_libc = Some("unknown".to_string());
            let mut target_arch = Some("unknown".to_string());
            let manifest_path = path.join("overlay_manifest.json");
            if let Ok(content) = std::fs::read_to_string(&manifest_path) {
                if let Ok(manifest) = serde_json::from_str::<BaseManifest>(&content) {
                    target_os = Some(manifest.os);
                    target_libc = Some(manifest.libc);
                    target_arch = Some(manifest.architecture);
                }
            }

            EngineResponse::DepDetails(DepInfo {
                name: parts[0].to_string(),
                version: parts[1].to_string(),
                size_bytes,
                consumers,
                created_time,
                target_os,
                target_libc,
                target_arch,
            })
        }
        EngineRequest::DepsRemove { .. } => EngineResponse::Ok("Removed".to_string()),
        EngineRequest::DepsPrune => {
            let map = state.lock().await;
            let deps_path = &std::path::PathBuf::from(format!(
                "{}/deps",
                std::env::var("OPENBACK_STORE_DIR")
                    .unwrap_or_else(|_| "/var/lib/openback/store".to_string())
            ));
            let mut pruned = Vec::new();
            if deps_path.exists() {
                if let Ok(entries) = std::fs::read_dir(deps_path) {
                    for entry in entries.flatten() {
                        let dep_name = entry.file_name().into_string().unwrap_or_default();
                        if let Ok(meta) = entry.metadata() {
                            if meta.is_dir() {
                                if let Ok(sub_entries) = std::fs::read_dir(entry.path()) {
                                    for sub_entry in sub_entries.flatten() {
                                        let dep_version =
                                            sub_entry.file_name().into_string().unwrap_or_default();
                                        let full_dep_str = format!("{}@{}", dep_name, dep_version);

                                        let is_used = map.values().any(|inst| {
                                            inst.manifest.dependencies.contains(&full_dep_str)
                                        });

                                        if !is_used
                                            && std::fs::remove_dir_all(sub_entry.path()).is_ok()
                                        {
                                            pruned.push(full_dep_str);
                                        }
                                    }
                                }
                                let _ = std::fs::remove_dir(entry.path());
                            }
                        }
                    }
                }
            }
            EngineResponse::PruneResult(pruned)
        }
        EngineRequest::BaseList => {
            let map = state.lock().await;
            let bases_path = &std::path::PathBuf::from(format!(
                "{}/images",
                std::env::var("OPENBACK_STORE_DIR")
                    .unwrap_or_else(|_| "/var/lib/openback/store".to_string())
            ));
            let mut bases = Vec::new();
            if bases_path.exists() {
                if let Ok(entries) = std::fs::read_dir(bases_path) {
                    for entry in entries.flatten() {
                        let base_name = entry.file_name().into_string().unwrap_or_default();
                        if let Ok(meta) = entry.metadata() {
                            if meta.is_dir() {
                                let size_bytes = get_dir_size(&entry.path()).unwrap_or(0);
                                let mut consumers = Vec::new();
                                for (name, inst) in map.iter() {
                                    if inst.manifest.get_base_image() == base_name && !consumers.contains(name) {
                                        consumers.push(name.clone());
                                    }
                                }
                                let created_time = meta.created().ok().map(|t| {
                                    let dt: chrono::DateTime<chrono::Local> = t.into();
                                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                                });

                                let manifest_path = entry.path().join("base_manifest.json");
                                let base_manifest = if manifest_path.exists() {
                                    if let Ok(content) = std::fs::read_to_string(&manifest_path) {
                                        serde_json::from_str::<BaseManifest>(&content).ok()
                                    } else {
                                        None
                                    }
                                } else {
                                    let detected = openback::engine::inspector::inspect_base_image(&entry.path());
                                    if let Ok(json) = serde_json::to_string_pretty(&detected) {
                                        let _ = std::fs::write(&manifest_path, json);
                                    }
                                    Some(detected)
                                };

                                bases.push(BaseInfo {
                                    name: base_name,
                                    size_bytes,
                                    consumers,
                                    created_time,
                                    manifest: base_manifest,
                                });
                            }
                        }
                    }
                }
            }
            EngineResponse::BaseList(bases)
        }
        EngineRequest::BaseInspect(name) => {
            let map = state.lock().await;
            let base_path = format!(
                "{}/images/{}",
                std::env::var("OPENBACK_STORE_DIR")
                    .unwrap_or_else(|_| "/var/lib/openback/store".to_string()),
                name
            );
            let path = std::path::Path::new(&base_path);
            if !path.exists() {
                return EngineResponse::Error(format!("Base image '{}' not found", name));
            }

            let size_bytes = get_dir_size(path).unwrap_or(0);
            let mut consumers = Vec::new();
            for (name_, inst) in map.iter() {
                if inst.manifest.get_base_image() == name && !consumers.contains(name_) {
                    consumers.push(name_.clone());
                }
            }
            let created_time = path
                .metadata()
                .ok()
                .and_then(|m| m.created().ok())
                .map(|t| {
                    let dt: chrono::DateTime<chrono::Local> = t.into();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                });

            let manifest_path = path.join("base_manifest.json");
            let base_manifest = if manifest_path.exists() {
                if let Ok(content) = std::fs::read_to_string(&manifest_path) {
                    serde_json::from_str::<BaseManifest>(&content).ok()
                } else {
                    None
                }
            } else {
                let detected = openback::engine::inspector::inspect_base_image(&path);
                if let Ok(json) = serde_json::to_string_pretty(&detected) {
                    let _ = std::fs::write(&manifest_path, json);
                }
                Some(detected)
            };

            EngineResponse::BaseDetails(BaseInfo {
                name,
                size_bytes,
                consumers,
                created_time,
                manifest: base_manifest,
            })
        }
        EngineRequest::BasePrune => {
            let map = state.lock().await;
            let bases_path = &std::path::PathBuf::from(format!(
                "{}/store/bases",
                std::env::var("OPENBACK_STORE_DIR")
                    .unwrap_or_else(|_| "/var/lib/openback/store".to_string())
            ));
            let mut pruned = Vec::new();
            if bases_path.exists() {
                if let Ok(entries) = std::fs::read_dir(bases_path) {
                    for entry in entries.flatten() {
                        let base_name = entry.file_name().into_string().unwrap_or_default();
                        if let Ok(meta) = entry.metadata() {
                            if meta.is_dir() {
                                let is_used = map.values().any(|inst| {
                                    inst.manifest.get_base_image() == base_name
                                });

                                if !is_used && std::fs::remove_dir_all(entry.path()).is_ok() {
                                    pruned.push(base_name);
                                }
                            }
                        }
                    }
                }
            }
            EngineResponse::PruneResult(pruned)
        }
        EngineRequest::Logs {
            container_name,
            tail,
        } => {
            let log_file = {
                let map = state.lock().await;
                map.get(&container_name).map(|inst| inst.log_file.clone())
            };

            if let Some(path) = log_file {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    let lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
                    let selected = if let Some(t) = tail {
                        lines.into_iter().rev().take(t).rev().collect()
                    } else {
                        lines
                    };
                    EngineResponse::LogLines(selected)
                } else {
                    EngineResponse::Error(format!("Failed to read logs for container '{}'", container_name))
                }
            } else {
                EngineResponse::Error(format!("Container '{}' not found", container_name))
            }
        }
    }
}
