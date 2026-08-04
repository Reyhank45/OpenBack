use crate::manifest::AppManifest;
use crate::rpc::{ProcessInfo, RpcRequest, RpcResponse};
use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::sync::Mutex;

struct AppState {
    pid: u32,
    manifest: AppManifest,
    start_time: DateTime<Local>,
    log_file: String,
    proxy_tasks: Vec<tokio::task::JoinHandle<()>>,
}

pub async fn run_daemon() -> Result<()> {
    let socket_path = "/tmp/openbackd.sock";
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("Failed to bind to {}", socket_path))?;
    
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o777))
        .context("Failed to set socket permissions")?;

    let state: Arc<Mutex<HashMap<String, AppState>>> = Arc::new(Mutex::new(HashMap::new()));

    println!("OpenBack Daemon listening on {}", socket_path);

    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.split();
                    let mut buf_reader = BufReader::new(reader);
                    let mut line = String::new();
                    
                    if let Ok(bytes_read) = buf_reader.read_line(&mut line).await {
                        if bytes_read == 0 { return; }
                        
                        let response = match serde_json::from_str::<RpcRequest>(&line) {
                            Ok(request) => handle_request(request, state).await,
                            Err(e) => RpcResponse::Error(format!("Invalid RPC request: {}", e)),
                        };

                        if let Ok(response_str) = serde_json::to_string(&response) {
                            let _ = writer.write_all(format!("{}\n", response_str).as_bytes()).await;
                        }
                    }
                });
            }
            Err(e) => eprintln!("Failed to accept connection: {}", e),
        }
    }
}

async fn handle_request(request: RpcRequest, state: Arc<Mutex<HashMap<String, AppState>>>) -> RpcResponse {
    match request {
        RpcRequest::Run(manifest) => {
            let mut map = state.lock().await;
            if map.contains_key(&manifest.app_name) {
                return RpcResponse::Error(format!("App '{}' is already running", manifest.app_name));
            }

            let log_dir = "/tmp/openback/logs";
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
                    
                    let tcp_listener = match TcpListener::bind(format!("0.0.0.0:{}", host_port)).await {
                        Ok(l) => l,
                        Err(e) => return RpcResponse::Error(format!("Failed to bind TCP port {}: {}", host_port, e)),
                    };
                    
                    println!("Started TCP Proxy for app '{}' on port {}", app_name, host_port);
                    
                    let proxy_task = tokio::spawn(async move {
                        // Map the container socket (e.g. /run/app.sock) to the host path
                        let socket_path = container_socket.replace("/run", &format!("/tmp/openback/store/apps/{}/run", app_name));
                        while let Ok((mut tcp_stream, _)) = tcp_listener.accept().await {
                            if let Ok(mut unix_stream) = UnixStream::connect(&socket_path).await {
                                tokio::spawn(async move {
                                    let _ = tokio::io::copy_bidirectional(&mut tcp_stream, &mut unix_stream).await;
                                });
                            }
                        }
                    });
                    proxy_tasks.push(proxy_task);
                }
            }

            match Command::new(current_exe)
                .arg("internal-launcher")
                .arg(&manifest_json)
                .stdout(Stdio::from(log_out))
                .stderr(Stdio::from(log_err))
                .spawn() {
                Ok(mut child) => {
                    let pid = child.id();
                    println!("Spawned app '{}' with launcher PID {}", manifest.app_name, pid);
                    
                    let app_name = manifest.app_name.clone();
                    map.insert(app_name.clone(), AppState {
                        pid,
                        manifest,
                        start_time: Local::now(),
                        log_file,
                        proxy_tasks,
                    });

                    let state_clone = state.clone();
                    tokio::spawn(async move {
                        let _ = tokio::task::spawn_blocking(move || { child.wait() }).await;
                        println!("App '{}' exited. Cleaning up state.", app_name);
                        let mut map = state_clone.lock().await;
                        if let Some(app_state) = map.remove(&app_name) {
                            for task in app_state.proxy_tasks {
                                task.abort();
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
                    status: "Running".to_string(),
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
                
                if let Err(e) = signal::kill(Pid::from_raw(app_state.pid as i32), Signal::SIGKILL) {
                    RpcResponse::Error(format!("Failed to kill process {}: {}", app_state.pid, e))
                } else {
                    RpcResponse::Ok(format!("App '{}' stopped", app_name))
                }
            } else {
                RpcResponse::Error(format!("App '{}' not found", app_name))
            }
        }
        RpcRequest::Logs(app_name) => {
            let log_file = {
                let map = state.lock().await;
                map.get(&app_name).map(|app| app.log_file.clone())
            };
            
            if let Some(path) = log_file {
                match std::fs::read_to_string(&path) {
                    Ok(content) => RpcResponse::Ok(content),
                    Err(e) => RpcResponse::Error(format!("Failed to read logs: {}", e)),
                }
            } else {
                let path = format!("/tmp/openback/logs/{}.log", app_name);
                match std::fs::read_to_string(&path) {
                    Ok(content) => RpcResponse::Ok(content),
                    Err(_) => RpcResponse::Error(format!("App '{}' not found or has no logs", app_name)),
                }
            }
        }
    }
}

