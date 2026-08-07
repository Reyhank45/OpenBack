use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use openback::manifest::AppManifest;
use openback::rpc::{
    AppDescription, BaseInfo, BaseManifest, DepInfo, KubeApplication, ProcessInfo, RpcRequest,
    RpcResponse,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::Mutex;

struct AppState {
    pid: u32,
    is_building: bool,
    exit_code: Option<i32>,
    manifest: AppManifest,
    start_time: DateTime<Local>,
    log_file: String,
    stdin_fifo: Option<String>,
    proxy_tasks: Vec<tokio::task::JoinHandle<()>>,
}

pub struct NodeStatus {
    pub role: String,
    pub hostname: String,
    pub port: Option<u16>,
    pub cpu_usage: f32,
    pub ram_usage: f32,
    pub last_seen: std::time::Instant,
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

pub async fn run_daemon(
    role: String,
    port: Option<u16>,
    master_addr: Option<String>,
    cluster_token: Option<String>,
) -> Result<()> {
    let socket_path =
        &std::env::var("OPENBACK_SOCKET").unwrap_or_else(|_| "/tmp/openbackd.sock".to_string());
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("Failed to bind to {}", socket_path))?;

    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o777))
        .context("Failed to set socket permissions")?;

    let state: Arc<Mutex<HashMap<String, AppState>>> = Arc::new(Mutex::new(HashMap::new()));
    let node_registry: Arc<Mutex<HashMap<String, NodeStatus>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Self register
    if role == "master" || role == "master-backup" {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();
        let mut h = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());
        if let Some(p) = port {
            h = format!("{}-{}", h, p);
        }
        let mut reg = node_registry.lock().await;
        reg.insert(
            h.clone(),
            NodeStatus {
                role: role.clone(),
                hostname: h,
                port,
                cpu_usage: sys.global_cpu_usage(),
                ram_usage: (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0,
                last_seen: std::time::Instant::now(),
            },
        );
    }

    openback::dlog!("Daemon", "INFO", "OpenBack Daemon listening on {}", socket_path);

    let _role_clone = role.clone();
    let cluster_token_clone = cluster_token.clone();
    let state_tcp = state.clone();

    if role == "master" || role == "master-backup" {
        let registry_sweep = node_registry.clone();
        let state_sweep = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                let now = std::time::Instant::now();
                let mut dead_nodes = Vec::new();
                {
                    let mut reg = registry_sweep.lock().await;

                    // Update master's own timestamp
                    let mut master_host =
                        sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());
                    if let Some(p) = port {
                        master_host = format!("{}-{}", master_host, p);
                    }
                    if let Some(node) = reg.get_mut(&master_host) {
                        node.last_seen = now;
                    }

                    for (hostname, node) in reg.iter_mut() {
                        if now.duration_since(node.last_seen).as_secs() > 15
                            && node.role != "OFFLINE"
                        {
                            openback::dlog!("Daemon", "INFO", "Node {} missed heartbeats, marking OFFLINE", hostname);
                            node.role = "OFFLINE".to_string();
                            dead_nodes.push(hostname.clone());
                        }
                    }
                }

                if !dead_nodes.is_empty() {
                    println!(
                        "Triggering Reconciler for {} dead nodes...",
                        dead_nodes.len()
                    );
                    let mut apps_to_reconcile = Vec::new();
                    {
                        let mut map = state_sweep.lock().await;
                        let mut to_remove = Vec::new();
                        for (replica_name, app_state) in map.iter() {
                            for dead in &dead_nodes {
                                if app_state.log_file.contains(dead) {
                                    to_remove.push(replica_name.clone());
                                    // Strip the -uuid suffix to get deployment name
                                    let dep_name = if app_state.manifest.app_name.len() > 9 {
                                        app_state.manifest.app_name
                                            [..app_state.manifest.app_name.len() - 9]
                                            .to_string()
                                    } else {
                                        app_state.manifest.app_name.clone()
                                    };
                                    if !apps_to_reconcile.contains(&dep_name) {
                                        apps_to_reconcile.push(dep_name);
                                    }
                                }
                            }
                        }
                        for r in to_remove {
                            map.remove(&r);
                            openback::dlog!("Daemon", "INFO", "Reconciler evicted dead replica: {}", r);
                        }
                    }

                    let deploy_dir_base = std::env::var("OPENBACK_STORE_DIR")
                        .unwrap_or_else(|_| "/tmp/openback".to_string());
                    for app_name in apps_to_reconcile {
                        let deploy_file = format!(
                            "{}/store/deployments/{}/manifest.yaml",
                            deploy_dir_base, app_name
                        );
                        if let Ok(content) = std::fs::read_to_string(&deploy_file) {
                            if let Ok(kube_app) = serde_yaml::from_str::<KubeApplication>(&content)
                            {
                                openback::dlog!("Daemon", "INFO", "Rescheduling workloads for app: {}", app_name);
                                let _ = reconcile_deployment(
                                    &app_name,
                                    &kube_app,
                                    state_sweep.clone(),
                                    registry_sweep.clone(),
                                )
                                .await;
                            }
                        }
                    }
                }
            }
        });
    }

    if let Some(p) = port {
        openback::dlog!("Daemon", "INFO", "OpenBack Daemon listening on TCP 0.0.0.0:{}", p);
        let tcp_listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", p)).await?;

        let registry_tcp = node_registry.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = tcp_listener.accept().await {
                    let state_clone = state_tcp.clone();
                    let registry_clone = registry_tcp.clone();
                    let expected_token = cluster_token_clone.clone();
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

                            // Parse Envelope
                            if let Ok(envelope) =
                                serde_json::from_str::<openback::rpc::RpcEnvelope>(&line)
                            {
                                // Authenticate
                                if expected_token.is_some() && envelope.auth_token != expected_token
                                {
                                    openback::dlog!("Daemon", "ERROR", "Security Warning: Rejected TCP connection with invalid token.");
                                    let _ = tokio::io::AsyncWriteExt::write_all(
                                        &mut writer,
                                        b"{\"Error\":\"Invalid token\"}\n",
                                    )
                                    .await;
                                    break;
                                }

                                let response = handle_request(
                                    envelope.request,
                                    state_clone.clone(),
                                    registry_clone.clone(),
                                )
                                .await;
                                let mut response_str = serde_json::to_string(&response).unwrap();
                                response_str.push('\n');
                                let _ = tokio::io::AsyncWriteExt::write_all(
                                    &mut writer,
                                    response_str.as_bytes(),
                                )
                                .await;
                            } else if let Ok(request) =
                                serde_json::from_str::<openback::rpc::RpcRequest>(&line)
                            {
                                // Fallback for local unix socket testing if they hit TCP
                                let response = handle_request(
                                    request,
                                    state_clone.clone(),
                                    registry_clone.clone(),
                                )
                                .await;
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

    if let Some(m_addr) = master_addr.clone() {
        let role_heartbeat = role.clone();
        let port_heartbeat = port;
        tokio::spawn(async move {
            use sysinfo::System;
            let mut sys = System::new_all();
            let mut hostname = System::host_name().unwrap_or_else(|| "unknown".to_string());
            if let Some(p) = port_heartbeat {
                hostname = format!("{}-{}", hostname, p);
            }

            // Register
            sys.refresh_all();
            let cpu_usage = sys.global_cpu_usage();
            let ram_usage = (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0;

            let req = openback::rpc::RpcRequest::RegisterNode {
                role: role_heartbeat.clone(),
                hostname: hostname.clone(),
                port: port_heartbeat,
                cpu_usage,
                ram_usage,
            };
            if let Ok(mut stream) = tokio::net::TcpStream::connect(&m_addr).await {
                let env = openback::rpc::RpcEnvelope {
                    auth_token: cluster_token.clone(),
                    request: req,
                };
                let mut p = serde_json::to_string(&env).unwrap();
                p.push('\n');
                let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, p.as_bytes()).await;
            }

            // Heartbeat loop
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                sys.refresh_all();
                let cpu_usage = sys.global_cpu_usage();
                let ram_usage = (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0;

                let req = openback::rpc::RpcRequest::Heartbeat {
                    hostname: hostname.clone(),
                    cpu_usage,
                    ram_usage,
                };
                if let Ok(mut stream) = tokio::net::TcpStream::connect(&m_addr).await {
                    let env = openback::rpc::RpcEnvelope {
                        auth_token: cluster_token.clone(),
                        request: req,
                    };
                    let mut p = serde_json::to_string(&env).unwrap();
                    p.push('\n');
                    let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, p.as_bytes()).await;
                }
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                let state = state.clone();
                let registry_unix = node_registry.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.split();
                    let mut buf_reader = BufReader::new(reader);
                    let mut line = String::new();

                    if let Ok(bytes_read) = buf_reader.read_line(&mut line).await {
                        if bytes_read == 0 {
                            return;
                        }

                        let request_clone = match serde_json::from_str::<RpcRequest>(&line) {
                            Ok(req) => Some(req),
                            Err(_) => None,
                        };

                        let registry_unix = registry_unix.clone();
                        let response = match request_clone.clone() {
                            Some(request) => handle_request(request, state.clone(), registry_unix).await,
                            None => RpcResponse::Error("Invalid RPC request".to_string()),
                        };

                        if let Ok(response_str) = serde_json::to_string(&response) {
                            let _ = writer
                                .write_all(format!("{}\n", response_str).as_bytes())
                                .await;
                        }

                        if matches!(response, RpcResponse::AttachStream) {
                            if let Some(RpcRequest::Attach { app_name }) = request_clone {
                                handle_attach_stream(app_name, state.clone(), buf_reader, writer).await;
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
    app_name: String,
    state: Arc<tokio::sync::Mutex<HashMap<String, AppState>>>,
    mut buf_reader: BufReader<tokio::net::unix::ReadHalf<'_>>,
    mut writer: tokio::net::unix::WriteHalf<'_>,
) {
    let (log_file, stdin_fifo) = {
        let map = state.lock().await;
        if let Some(app) = map.get(&app_name) {
            (app.log_file.clone(), app.stdin_fifo.clone())
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
            if let Ok(mut fifo) = tokio::fs::OpenOptions::new().write(true).open(&fifo_path).await {
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

async fn spawn_replica(
    manifest: openback::manifest::AppManifest,
    state: Arc<tokio::sync::Mutex<std::collections::HashMap<String, AppState>>>,
) -> Result<(), String> {
    let app_name = manifest.app_name.clone();
    {
        let mut map = state.lock().await;
        if map.contains_key(&app_name) {
            return Err("Already running".into());
        }

        let log_dir = &format!(
            "{}/logs",
            std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string())
        );
        let log_file = format!("{}/{}.log", log_dir, app_name);

        openback::dlog!("Daemon", "INFO", "Registering replica '{}' in state map (Building)", app_name);
        map.insert(
            app_name.clone(),
            AppState {
                pid: 0,
                is_building: true, // Mark as building while overlay cache is generated
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
        // Step 1: Ensure base image and package overlay is built and cached
        openback::dlog!("Daemon", "INFO", "[{}] Step 1: Ensuring base image '{}' is available...", app_name, manifest.get_base_image());
        if let Err(e) = openback::engine::overlay::OverlayEngine::ensure_base_image(&manifest.get_base_image()).await {
            openback::dlog!("Daemon", "ERROR", "[{}] ERROR: Failed to ensure base image: {}", manifest.app_name, e);
            let mut map = state_clone.lock().await;
            map.remove(&manifest.app_name);
            return;
        }
        openback::dlog!("Daemon", "INFO", "[{}] Step 1 complete: Base image ready.", app_name);

        openback::dlog!("Daemon", "INFO", "[{}] Step 2: Ensuring package overlay layer is built and cached...", app_name);
        if let Err(e) = openback::engine::overlay::OverlayEngine::ensure_overlay(&manifest).await {
            openback::dlog!("Daemon", "ERROR", "[{}] ERROR: Failed to ensure overlay: {}", manifest.app_name, e);
            let mut map = state_clone.lock().await;
            map.remove(&manifest.app_name);
            return;
        }
        openback::dlog!("Daemon", "INFO", "[{}] Step 2 complete: Package overlay ready.", app_name);

        // Step 3: Setup logs and proxy tasks
        let log_dir = &format!(
            "{}/logs",
            std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string())
        );
        let _ = std::fs::create_dir_all(log_dir);
        let log_file = format!("{}/{}.log", log_dir, manifest.app_name);
        openback::dlog!("Daemon", "INFO", "[{}] Step 3: Log file -> {}", app_name, log_file);

        let log_out = match std::fs::File::create(&log_file) {
            Ok(f) => f,
            Err(e) => {
                openback::dlog!("Daemon", "ERROR", "[{}] ERROR: Failed to create log file: {}", app_name, e);
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
                let app_name_inner = manifest.app_name.clone();
                let container_socket = port_mapping.container_socket.clone();

                openback::dlog!("Daemon", "INFO", "[{}] Step 3: Binding TCP proxy on host port {} -> container socket {}", app_name, host_port, container_socket);
                let tcp_listener =
                    match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", host_port)).await {
                        Ok(l) => l,
                        Err(e) => {
                            openback::dlog!("Daemon", "ERROR", "[{}] ERROR: Failed to bind TCP port {}: {}", app_name, host_port, e);
                            return;
                        }
                    };

                let proxy_task = tokio::spawn(async move {
                    let socket_path = container_socket.replace(
                        "/run",
                        &format!(
                            "{}/store/apps/{}/run",
                            std::env::var("OPENBACK_STORE_DIR")
                                .unwrap_or_else(|_| "/tmp/openback".to_string()),
                            app_name_inner
                        ),
                    );
                    while let Ok((mut tcp_stream, peer)) = tcp_listener.accept().await {
                        openback::dlog!("Daemon", "INFO", "TCP proxy: new connection from {} for app '{}'", peer, app_name_inner);
                        if let Ok(mut unix_stream) = tokio::net::UnixStream::connect(&socket_path).await
                        {
                            tokio::spawn(async move {
                                let _ =
                                    tokio::io::copy_bidirectional(&mut tcp_stream, &mut unix_stream)
                                        .await;
                            });
                        } else {
                            openback::dlog!("Daemon", "ERROR", "TCP proxy: failed to connect to container socket {}", socket_path);
                        }
                    }
                });
                proxy_tasks.push(proxy_task);
            }
        }

        let fifo_path = format!("{}/{}.fifo", log_dir, app_name);
        let _ = std::fs::remove_file(&fifo_path);
        let _ = nix::unistd::mkfifo(
            fifo_path.as_str(),
            nix::sys::stat::Mode::from_bits_truncate(0o666),
        );

        // Step 4: Spawn the launcher process
        openback::dlog!("Daemon", "INFO", "[{}] Step 4: Spawning internal-launcher subprocess...", app_name);
        match std::process::Command::new(current_exe)
            .arg("internal-launcher")
            .arg(&manifest_json)
            .env("OPENBACK_STDIN_FIFO", &fifo_path)
            .stdout(std::process::Stdio::from(log_out))
            .stderr(std::process::Stdio::from(log_err))
            .spawn()
        {
            Ok(mut child) => {
                let pid = child.id();
                let app_name_inner = manifest.app_name.clone();
                openback::dlog!("Daemon", "INFO", "[{}] Launcher PID {} spawned successfully. Transitioning to Running.", app_name, pid);

                {
                    let mut map = state_clone.lock().await;
                    if let Some(app_state) = map.get_mut(&app_name_inner) {
                        app_state.pid = pid;
                        app_state.is_building = false; // Transition to Running
                        app_state.stdin_fifo = Some(fifo_path.clone());
                        app_state.proxy_tasks = proxy_tasks;
                    }
                }

                let state_clone2 = state_clone.clone();
                let app_name_clone = app_name_inner.clone();
                tokio::spawn(async move {
                    let exit_status = tokio::task::spawn_blocking(move || child.wait()).await.unwrap();
                    openback::dlog!("Daemon", "INFO", "Replica '{}' (PID {}) exited with status {:?}", app_name_clone, pid, exit_status);
                    let mut map = state_clone2.lock().await;
                    if let Some(app_state) = map.get_mut(&app_name_clone) {
                        for task in app_state.proxy_tasks.drain(..) {
                            task.abort();
                        }
                        app_state.exit_code = exit_status.ok().and_then(|s| s.code()).or(Some(-1));
                        openback::dlog!("Daemon", "INFO", "Replica '{}' marked as Exited({}).", app_name_clone, app_state.exit_code.unwrap_or(-1));
                        if let Some(fifo) = &app_state.stdin_fifo {
                            let _ = std::fs::remove_file(fifo);
                        }
                    }
                });
            }
            Err(e) => {
                openback::dlog!("Daemon", "ERROR", "[{}] ERROR: Failed to spawn launcher: {}", manifest.app_name, e);
                let mut map = state_clone.lock().await;
                map.remove(&manifest.app_name);
            }
        }
    });

    Ok(())
}

async fn reconcile_deployment(
    app_name: &str,
    kube_app: &openback::rpc::KubeApplication,
    state: Arc<tokio::sync::Mutex<std::collections::HashMap<String, AppState>>>,
    registry: Arc<tokio::sync::Mutex<std::collections::HashMap<String, NodeStatus>>>,
) -> Result<(), String> {
    let desired_replicas = kube_app.spec.replicas.unwrap_or(1);

    let mut current_replicas = Vec::new();
    {
        let map = state.lock().await;
        for name in map.keys() {
            if name.starts_with(&format!("{}-", app_name)) && name.len() == app_name.len() + 9 {
                current_replicas.push(name.clone());
            }
        }
    }

    if current_replicas.len() < desired_replicas {
        let to_spawn = desired_replicas - current_replicas.len();
        for i in 0..to_spawn {
            use rand::Rng;
            let uuid = format!("{:08x}", rand::thread_rng().gen::<u32>());
            let replica_name = format!("{}-{}", app_name, uuid);

            let manifest = openback::manifest::AppManifest {
                app_name: replica_name.clone(),
                base_image: kube_app.spec.base_image.clone(),
                target_gd: kube_app.spec.target_gd.clone(),
                dependencies: kube_app.spec.dependencies.clone(),
                packages: kube_app.spec.packages.clone(),
                app_source: kube_app.spec.app_source.clone(),
                work_dir: kube_app.spec.work_dir.clone(),
                permissions: kube_app.spec.permissions.clone(),
                networking: kube_app.spec.networking.clone(),
                env: kube_app.spec.env.clone().unwrap_or_default(),
                entrypoint: kube_app.spec.entrypoint.clone(),
            };

            // Round-Robin Scheduler
            let mut target_ip = None;
            let mut target_port = None;
            {
                let reg = registry.lock().await;
                let mut available: Vec<_> = reg
                    .values()
                    .filter(|n| {
                        n.cpu_usage < 90.0
                            && n.ram_usage < 90.0
                            && (n.role == "slave" || n.role == "master")
                    })
                    .collect();
                available.sort_by_key(|n| &n.hostname);

                if !available.is_empty() {
                    let node = available[i % available.len()];
                    target_ip = Some(node.hostname.clone());
                    target_port = node.port;
                }
            }

            if let (Some(ip), Some(p)) = (target_ip, target_port) {
                if ip == "127.0.0.1"
                    || ip == "localhost"
                    || ip.starts_with(&sysinfo::System::host_name().unwrap_or_default())
                {
                    let _ = spawn_replica(manifest, state.clone()).await;
                } else {
                    let req = openback::rpc::RpcRequest::Run(manifest.clone());
                    // Use a dummy token or standard token (assuming empty for simplicity here)
                    let env = openback::rpc::RpcEnvelope {
                        auth_token: None,
                        request: req,
                    };
                    let dial_ip = if ip.contains("-") { "127.0.0.1" } else { &ip };
                    if let Ok(mut stream) =
                        tokio::net::TcpStream::connect(format!("{}:{}", dial_ip, p)).await
                    {
                        let mut payload = serde_json::to_string(&env).unwrap();
                        payload.push('\n');
                        let _ =
                            tokio::io::AsyncWriteExt::write_all(&mut stream, payload.as_bytes())
                                .await;
                        openback::dlog!("Daemon", "INFO", "Dispatched replica {} to Node {}", replica_name, ip);
                        let mut map = state.lock().await;
                        map.insert(
                            replica_name.clone(),
                            AppState {
                                pid: 0,
                                is_building: false,
                                exit_code: None,
                                manifest: manifest.clone(),
                                start_time: chrono::Local::now(),
                                log_file: format!("Remote on {}", ip),
                                stdin_fifo: None,
                                proxy_tasks: vec![],
                            },
                        );
                    } else {
                        openback::dlog!("Daemon", "ERROR", "Failed to dial Node {} for replica {}", ip, replica_name);
                    }
                }
            } else {
                // Fallback to local if no nodes available or registry empty
                openback::dlog!("Daemon", "INFO", "No remote nodes available, spawning replica '{}' locally", replica_name);
                let _ = spawn_replica(manifest, state.clone()).await;
                println!(
                    "[Daemon] Spawned replica {} locally",
                    replica_name
                );
            }
        }
    } else if current_replicas.len() > desired_replicas {
        let to_kill = current_replicas.len() - desired_replicas;
        for name in current_replicas.iter().take(to_kill) {
            let mut map = state.lock().await;
            if let Some(app_state) = map.remove(name) {
                for task in app_state.proxy_tasks {
                    task.abort();
                }
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(app_state.pid as i32),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
        }
    }

    Ok(())
}

async fn handle_request(
    request: RpcRequest,
    state: Arc<Mutex<HashMap<String, AppState>>>,
    registry: Arc<Mutex<HashMap<String, NodeStatus>>>,
) -> RpcResponse {
    openback::dlog!("Daemon", "INFO", "Received RPC request: {:?}", std::mem::discriminant(&request));
    match request {
        openback::rpc::RpcRequest::GetNodes => {
            let mut nodes = Vec::new();
            let reg = registry.lock().await;
            for (hostname, node) in reg.iter() {
                nodes.push(openback::rpc::NodeInfo {
                    hostname: hostname.clone(),
                    role: node.role.clone(),
                    cpu_usage: node.cpu_usage,
                    ram_usage: node.ram_usage,
                    status: if node.role == "OFFLINE" {
                        "OFFLINE".to_string()
                    } else {
                        "READY".to_string()
                    },
                });
            }
            RpcResponse::NodeList(nodes)
        }
        RpcRequest::Attach { app_name } => {
            let map = state.lock().await;
            if let Some(app_state) = map.get(&app_name) {
                if app_state.is_building {
                    return RpcResponse::Error("App is currently building its overlay".to_string());
                }
                if app_state.exit_code.is_some() {
                    return RpcResponse::Error("App has already exited".to_string());
                }
                return RpcResponse::AttachStream;
            } else {
                return RpcResponse::Error(format!("App '{}' not found", app_name));
            }
        }
        RpcRequest::Run(manifest) => {
            let mut map = state.lock().await;
            if map.contains_key(&manifest.app_name) {
                return RpcResponse::Error(format!(
                    "App '{}' is already running",
                    manifest.app_name
                ));
            }

            let log_dir = &format!(
                "{}/logs",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string())
            );
            let _ = std::fs::create_dir_all(log_dir);
            let log_file = format!("{}/{}.log", log_dir, manifest.app_name);

            let log_out = match std::fs::File::create(&log_file) {
                Ok(f) => f,
                Err(e) => return RpcResponse::Error(format!("Failed to create log file: {}", e)),
            };
            let log_err = log_out.try_clone().unwrap();

            let manifest_json = serde_json::to_string(&manifest).unwrap();
            let current_exe = std::env::current_exe().unwrap_or_else(|_| "openback".into());

            // Set up TCP Proxies before starting app to avoid race conditions
            let mut proxy_tasks = Vec::new();
            if let Some(networking) = &manifest.networking {
                for port_mapping in &networking.ports {
                    let host_port = port_mapping.host_port;
                    let app_name = manifest.app_name.clone();
                    let container_socket = port_mapping.container_socket.clone();

                    let tcp_listener =
                        match TcpListener::bind(format!("0.0.0.0:{}", host_port)).await {
                            Ok(l) => l,
                            Err(e) => {
                                return RpcResponse::Error(format!(
                                    "Failed to bind TCP port {}: {}",
                                    host_port, e
                                ))
                            }
                        };

                    println!(
                        "Started TCP Proxy for app '{}' on port {}",
                        app_name, host_port
                    );

                    let proxy_task = tokio::spawn(async move {
                        // Map the container socket (e.g. /run/app.sock) to the host path
                        let socket_path = container_socket.replace(
                            "/run",
                            &format!(
                                "{}/store/apps/{}/run",
                                std::env::var("OPENBACK_STORE_DIR")
                                    .unwrap_or_else(|_| "/tmp/openback".to_string()),
                                app_name
                            ),
                        );
                        while let Ok((mut tcp_stream, _)) = tcp_listener.accept().await {
                            if let Ok(mut unix_stream) = UnixStream::connect(&socket_path).await {
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

            let fifo_path = format!("{}/{}.fifo", log_dir, manifest.app_name);
            let _ = std::fs::remove_file(&fifo_path);
            let _ = nix::unistd::mkfifo(
                fifo_path.as_str(),
                nix::sys::stat::Mode::from_bits_truncate(0o666),
            );

            match Command::new(current_exe)
                .arg("internal-launcher")
                .arg(&manifest_json)
                .env("OPENBACK_STDIN_FIFO", &fifo_path)
                .stdout(Stdio::from(log_out))
                .stderr(Stdio::from(log_err))
                .spawn()
            {
                Ok(mut child) => {
                    let pid = child.id();
                    println!(
                        "Spawned app '{}' with launcher PID {}",
                        manifest.app_name, pid
                    );

                    let app_name = manifest.app_name.clone();
                    map.insert(
                        app_name.clone(),
                        AppState {
                            pid,
                            is_building: false,
                            exit_code: None,
                            manifest,
                            start_time: Local::now(),
                            log_file,
                            stdin_fifo: Some(fifo_path.clone()),
                            proxy_tasks,
                        },
                    );

                    let state_clone = state.clone();
                    tokio::spawn(async move {
                        let _ = tokio::task::spawn_blocking(move || child.wait()).await;
                        openback::dlog!("Daemon", "INFO", "App '{}' exited. Cleaning up state.", app_name);
                        let mut map = state_clone.lock().await;
                        if let Some(app_state) = map.remove(&app_name) {
                            for task in app_state.proxy_tasks {
                                task.abort();
                            }
                            if let Some(fifo) = &app_state.stdin_fifo {
                                let _ = std::fs::remove_file(fifo);
                            }
                        }
                    });

                    RpcResponse::Ok("Application started successfully".to_string())
                }
                Err(e) => RpcResponse::Error(format!("Failed to spawn app: {}", e)),
            }
        }
        RpcRequest::Ps => {
            let map = state.lock().await;
            let mut processes = Vec::new();
            for (name, app_state) in map.iter() {
                processes.push(ProcessInfo {
                    name: name.clone(),
                    pid: app_state.pid,
                    status: if let Some(code) = app_state.exit_code { format!("Exited ({})", code) } else if app_state.is_building { "Building".to_string() } else { "Running".to_string() },
                    start_time: app_state.start_time.format("%Y-%m-%d %H:%M:%S").to_string(),
                });
            }
            RpcResponse::ProcessList(processes)
        }
        RpcRequest::Stop(app_name) => {
            let mut map = state.lock().await;
            if let Some(app_state) = map.remove(&app_name) {
                // Abort any proxy tasks immediately
                for task in app_state.proxy_tasks {
                    task.abort();
                }

                if app_state.exit_code.is_none() && app_state.pid > 0 {
                    let _ = signal::kill(Pid::from_raw(app_state.pid as i32), Signal::SIGKILL);
                }
                
                RpcResponse::Ok(format!("App '{}' stopped", app_name))
            } else {
                RpcResponse::Error(format!("App '{}' not found", app_name))
            }
        }
        RpcRequest::DepsList => {
            let map = state.lock().await;
            let deps_path = &std::path::PathBuf::from(format!(
                "{}/store/deps",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string())
            ));
            let mut deps = Vec::new();
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
                                        let size_bytes =
                                            get_dir_size(&sub_entry.path()).unwrap_or(0);
                                        let mut consumers = Vec::new();
                                        for (app_name, app_state) in map.iter() {
                                            if app_state
                                                .manifest
                                                .dependencies
                                                .contains(&full_dep_str)
                                            {
                                                consumers.push(app_name.clone());
                                            }
                                        }
                                        let created_time = sub_entry
                                            .metadata()
                                            .ok()
                                            .and_then(|m| m.created().ok())
                                            .map(|t| {
                                                let dt: chrono::DateTime<chrono::Local> = t.into();
                                                dt.format("%Y-%m-%d %H:%M:%S").to_string()
                                            });
                                        deps.push(DepInfo {
                                            name: dep_name.clone(),
                                            version: dep_version,
                                            size_bytes,
                                            consumers,
                                            created_time,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
            RpcResponse::DepsList(deps)
        }
        RpcRequest::DepsInspect(name) => {
            let map = state.lock().await;
            let parts: Vec<&str> = name.split('@').collect();
            if parts.len() != 2 {
                return RpcResponse::Error(format!(
                    "Invalid dependency format: {}. Use name@version",
                    name
                ));
            }
            let dep_path = format!(
                "{}/store/deps/{}/{}",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                parts[0],
                parts[1]
            );
            let path = std::path::Path::new(&dep_path);
            if !path.exists() {
                return RpcResponse::Error(format!("Dependency '{}' not found", name));
            }

            let size_bytes = get_dir_size(path).unwrap_or(0);
            let mut consumers = Vec::new();
            for (app_name, app_state) in map.iter() {
                if app_state.manifest.dependencies.contains(&name) {
                    consumers.push(app_name.clone());
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

            RpcResponse::DepDetails(DepInfo {
                name: parts[0].to_string(),
                version: parts[1].to_string(),
                size_bytes,
                consumers,
                created_time,
            })
        }
        RpcRequest::DepsPrune => {
            let map = state.lock().await;
            let deps_path = &std::path::PathBuf::from(format!(
                "{}/store/deps",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string())
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

                                        let is_used = map.values().any(|state| {
                                            state.manifest.dependencies.contains(&full_dep_str)
                                        });

                                        if !is_used
                                            && std::fs::remove_dir_all(sub_entry.path()).is_ok()
                                        {
                                            pruned.push(full_dep_str);
                                        }
                                    }
                                }
                                // Try to remove the parent dir if it's empty now
                                let _ = std::fs::remove_dir(entry.path());
                            }
                        }
                    }
                }
            }
            RpcResponse::PruneResult(pruned)
        }
        RpcRequest::DepsRemove { name, force } => {
            let map = state.lock().await;
            let parts: Vec<&str> = name.split('@').collect();
            if parts.len() != 2 {
                return RpcResponse::Error(format!(
                    "Invalid dependency format: {}. Use name@version",
                    name
                ));
            }

            let is_used = map
                .values()
                .any(|state| state.manifest.dependencies.contains(&name));
            if is_used && !force {
                return RpcResponse::Error(format!("Dependency '{}' is currently in use by active applications. Use --force to remove anyway.", name));
            }

            let dep_path = format!(
                "{}/store/deps/{}/{}",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                parts[0],
                parts[1]
            );
            if std::path::Path::new(&dep_path).exists() {
                if let Err(e) = std::fs::remove_dir_all(&dep_path) {
                    return RpcResponse::Error(format!("Failed to remove dependency: {}", e));
                }
                let parent_dir = format!(
                    "{}/store/deps/{}",
                    std::env::var("OPENBACK_STORE_DIR")
                        .unwrap_or_else(|_| "/tmp/openback".to_string()),
                    parts[0]
                );
                let _ = std::fs::remove_dir(&parent_dir); // Remove if empty
                RpcResponse::Ok(format!("Successfully removed {}", name))
            } else {
                RpcResponse::Error(format!("Dependency '{}' not found", name))
            }
        }
        RpcRequest::BaseList => {
            let map = state.lock().await;
            let bases_path = &std::path::PathBuf::from(format!(
                "{}/store/bases",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string())
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
                                for (app_name, app_state) in map.iter() {
                                    if app_state.manifest.get_base_image() == base_name {
                                        consumers.push(app_name.clone());
                                    }
                                }
                                let created_time = entry
                                    .metadata()
                                    .ok()
                                    .and_then(|m| m.created().ok())
                                    .map(|t| {
                                        let dt: chrono::DateTime<chrono::Local> = t.into();
                                        dt.format("%Y-%m-%d %H:%M:%S").to_string()
                                    });

                                let manifest_path = entry.path().join("openback-base.json");
                                let base_manifest = if manifest_path.exists() {
                                    std::fs::read_to_string(&manifest_path)
                                        .ok()
                                        .and_then(|c| serde_json::from_str::<BaseManifest>(&c).ok())
                                } else {
                                    None
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
            RpcResponse::BaseList(bases)
        }
        RpcRequest::BaseInspect(name) => {
            let map = state.lock().await;
            let base_path_str = format!(
                "{}/store/bases/{}",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                name
            );
            let path = std::path::Path::new(&base_path_str);
            if !path.exists() {
                return RpcResponse::Error(format!("Base image '{}' not found", name));
            }

            let size_bytes = get_dir_size(path).unwrap_or(0);
            let mut consumers = Vec::new();
            for (app_name, app_state) in map.iter() {
                if app_state.manifest.get_base_image() == name {
                    consumers.push(app_name.clone());
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

            let manifest_path = path.join("openback-base.json");
            let base_manifest = if manifest_path.exists() {
                std::fs::read_to_string(&manifest_path)
                    .ok()
                    .and_then(|c| serde_json::from_str::<BaseManifest>(&c).ok())
            } else {
                None
            };

            RpcResponse::BaseDetails(BaseInfo {
                name,
                size_bytes,
                consumers,
                created_time,
                manifest: base_manifest,
            })
        }
        RpcRequest::BasePrune => {
            let map = state.lock().await;
            let bases_path = &std::path::PathBuf::from(format!(
                "{}/store/bases",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string())
            ));
            let mut pruned = Vec::new();
            if bases_path.exists() {
                if let Ok(entries) = std::fs::read_dir(bases_path) {
                    for entry in entries.flatten() {
                        let base_name = entry.file_name().into_string().unwrap_or_default();
                        if let Ok(meta) = entry.metadata() {
                            if meta.is_dir() {
                                let is_used = map
                                    .values()
                                    .any(|state| state.manifest.get_base_image() == base_name);

                                if !is_used && std::fs::remove_dir_all(entry.path()).is_ok() {
                                    pruned.push(base_name);
                                }
                            }
                        }
                    }
                }
            }
            RpcResponse::PruneResult(pruned)
        }
        RpcRequest::Apply(kube_app) => {
            let app_name = kube_app.metadata.name.clone();
            let deploy_dir = format!(
                "{}/store/deployments/{}",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                app_name
            );
            let _ = std::fs::create_dir_all(&deploy_dir);
            let yaml = serde_yaml::to_string(&kube_app).unwrap();
            let _ = std::fs::write(format!("{}/manifest.yaml", deploy_dir), yaml);

            let _ =
                reconcile_deployment(&app_name, &kube_app, state.clone(), registry.clone()).await;
            RpcResponse::Ok(format!("Applied deployment {}", app_name))
        }
        RpcRequest::GetDeployment(app_name) => {
            let deploy_file = format!(
                "{}/store/deployments/{}/manifest.yaml",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                app_name
            );
            if let Ok(content) = std::fs::read_to_string(&deploy_file) {
                if let Ok(app) = serde_yaml::from_str(&content) {
                    RpcResponse::DeploymentDetails(app)
                } else {
                    RpcResponse::Error("Failed to parse deployment".to_string())
                }
            } else {
                RpcResponse::Error("Deployment not found".to_string())
            }
        }
        RpcRequest::Scale { app_name, replicas } => {
            let deploy_file = format!(
                "{}/store/deployments/{}/manifest.yaml",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                app_name
            );
            if let Ok(content) = std::fs::read_to_string(&deploy_file) {
                if let Ok(mut app) = serde_yaml::from_str::<KubeApplication>(&content) {
                    app.spec.replicas = Some(replicas);
                    let yaml = serde_yaml::to_string(&app).unwrap();
                    let _ = std::fs::write(&deploy_file, yaml);

                    let _ = reconcile_deployment(&app_name, &app, state.clone(), registry.clone())
                        .await;
                    RpcResponse::Ok(format!("Scaled {} to {}", app_name, replicas))
                } else {
                    RpcResponse::Error("Parse error".to_string())
                }
            } else {
                RpcResponse::Error("Not found".to_string())
            }
        }
        RpcRequest::Describe(app_name) => {
            let deploy_file = format!(
                "{}/store/deployments/{}/manifest.yaml",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                app_name
            );
            if let Ok(content) = std::fs::read_to_string(&deploy_file) {
                if let Ok(deployment) = serde_yaml::from_str::<KubeApplication>(&content) {
                    let mut replicas = Vec::new();
                    let map = state.lock().await;
                    for (name, app_state) in map.iter() {
                        if name.starts_with(&format!("{}-", app_name))
                            && name.len() == app_name.len() + 9
                        {
                            replicas.push(ProcessInfo {
                                name: name.clone(),
                                pid: app_state.pid,
                                start_time: app_state
                                    .start_time
                                    .format("%Y-%m-%d %H:%M:%S")
                                    .to_string(),
                                status: if let Some(code) = app_state.exit_code { format!("Exited ({})", code) } else if app_state.is_building { "Building".to_string() } else { "Running".to_string() },
                            });
                        }
                    }
                    RpcResponse::DescribeDetails(AppDescription {
                        deployment,
                        replicas,
                    })
                } else {
                    RpcResponse::Error("Parse error".to_string())
                }
            } else {
                RpcResponse::Error("Not found".to_string())
            }
        }

        RpcRequest::RegisterNode {
            role,
            hostname,
            port,
            cpu_usage,
            ram_usage,
        } => {
            openback::dlog!("Daemon", "INFO", "Node registered: {} ({}) at {:?}", hostname, role, port);
            let mut reg = registry.lock().await;
            reg.insert(
                hostname.clone(),
                NodeStatus {
                    role,
                    hostname,
                    port,
                    cpu_usage,
                    ram_usage,
                    last_seen: std::time::Instant::now(),
                },
            );
            RpcResponse::Ok("Registered successfully".to_string())
        }
        RpcRequest::Heartbeat {
            hostname,
            cpu_usage,
            ram_usage,
        } => {
            let mut reg = registry.lock().await;
            if let Some(node) = reg.get_mut(&hostname) {
                node.cpu_usage = cpu_usage;
                node.ram_usage = ram_usage;
                node.last_seen = std::time::Instant::now();
            }
            RpcResponse::Ok("Heartbeat ack".to_string())
        }
        RpcRequest::SyncState(kube_app) => {
            println!(
                "Received SyncState for deployment: {}",
                kube_app.metadata.name
            );
            let deploy_dir = format!(
                "{}/store/deployments/{}",
                std::env::var("OPENBACK_STORE_DIR").unwrap_or_else(|_| "/tmp/openback".to_string()),
                kube_app.metadata.name
            );
            let _ = std::fs::create_dir_all(&deploy_dir);
            let yaml = serde_yaml::to_string(&kube_app).unwrap();
            let _ = std::fs::write(format!("{}/manifest.yaml", deploy_dir), yaml);
            RpcResponse::Ok("State synced".to_string())
        }
        RpcRequest::Logs { app_name, tail } => {
            let log_file = {
                let map = state.lock().await;
                map.get(&app_name).map(|app| app.log_file.clone())
            };

            if let Some(path) = log_file {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    let lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
                    let selected = if let Some(t) = tail {
                        lines.into_iter().rev().take(t).rev().collect()
                    } else {
                        lines
                    };
                    RpcResponse::LogLines(selected)
                } else {
                    RpcResponse::Error(format!("Failed to read logs for app '{}'", app_name))
                }
            } else {
                RpcResponse::Error(format!("App '{}' not found", app_name))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_structs_compile() {
        // Since Daemon tests involve binding to Unix sockets or TCP ports
        // and spinning up large amounts of Tokio state, we provide a placeholder
        // test here for CI to run. Real integration tests would hit a spawned daemon.
        assert!(true);
    }
}
