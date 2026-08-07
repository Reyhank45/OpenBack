use anyhow::{Context, Result};
use openback::manifest::AppManifest;
use openback::rpc::{RpcRequest, RpcResponse};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

async fn send_rpc_request(request: RpcRequest) -> Result<RpcResponse> {
    let socket_path =
        &std::env::var("OPENBACK_SOCKET").unwrap_or_else(|_| "/tmp/openbackd.sock".to_string());
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Failed to connect to daemon at {}", socket_path))?;

    let mut payload = serde_json::to_string(&request)?;
    payload.push('\n');

    stream.write_all(payload.as_bytes()).await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    if line.is_empty() {
        anyhow::bail!("Daemon closed connection unexpectedly");
    }

    let response: RpcResponse = serde_json::from_str(&line)?;
    Ok(response)
}

pub async fn run_app(manifest_path: PathBuf) -> Result<()> {
    let content = tokio::fs::read_to_string(&manifest_path)
        .await
        .with_context(|| format!("Failed to read manifest file: {:?}", manifest_path))?;

    let manifest: AppManifest =
        serde_json::from_str(&content).context("Failed to parse manifest JSON")?;

    println!("Sending Run request for app: {}", manifest.app_name);

    match send_rpc_request(RpcRequest::Run(manifest)).await? {
        RpcResponse::Ok(msg) => println!("Success: {}", msg),
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }

    Ok(())
}

pub async fn ps() -> Result<()> {
    match send_rpc_request(RpcRequest::Ps).await? {
        RpcResponse::ProcessList(processes) => {
            println!(
                "{:<20} | {:<10} | {:<10} | {:<25}",
                "APP NAME", "PID", "STATUS", "START TIME"
            );
            println!("{:-<20}-+-{:-<10}-+-{:-<10}-+-{:-<25}", "", "", "", "");
            for p in processes {
                println!(
                    "{:<20} | {:<10} | {:<10} | {:<25}",
                    p.name, p.pid, p.status, p.start_time
                );
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn stop(app_name: String) -> Result<()> {
    match send_rpc_request(RpcRequest::Stop(app_name)).await? {
        RpcResponse::Ok(msg) => println!("Success: {}", msg),
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn logs(app_name: String) -> Result<()> {
    match send_rpc_request(RpcRequest::Logs {
        app_name,
        tail: None,
    })
    .await?
    {
        RpcResponse::LogLines(lines) => {
            for line in lines {
                println!("{}", line);
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_list() -> Result<()> {
    match send_rpc_request(RpcRequest::DepsList).await? {
        RpcResponse::DepsList(deps) => {
            println!(
                "{:<30} | {:<15} | {:<10} | {:<30}",
                "DEPENDENCY", "VERSION", "SIZE (MB)", "ACTIVE CONSUMERS"
            );
            println!("{:-<30}-+-{:-<15}-+-{:-<10}-+-{:-<30}", "", "", "", "");
            for d in deps {
                let size_mb = d.size_bytes as f64 / 1_048_576.0;
                let consumers_str = if d.consumers.is_empty() {
                    "None".to_string()
                } else {
                    format!("{} ({})", d.consumers.len(), d.consumers.join(", "))
                };
                println!(
                    "{:<30} | {:<15} | {:<10.2} | {:<30}",
                    d.name, d.version, size_mb, consumers_str
                );
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_inspect(name: String) -> Result<()> {
    match send_rpc_request(RpcRequest::DepsInspect(name)).await? {
        RpcResponse::DepDetails(details) => {
            let json = serde_json::to_string_pretty(&details)?;
            println!("{}", json);
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_prune() -> Result<()> {
    match send_rpc_request(RpcRequest::DepsPrune).await? {
        RpcResponse::PruneResult(pruned) => {
            if pruned.is_empty() {
                println!("No unused dependencies found. Nothing to prune.");
            } else {
                println!("Successfully pruned unused dependencies:");
                for p in pruned {
                    println!("  - {}", p);
                }
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_remove(name: String, force: bool) -> Result<()> {
    match send_rpc_request(RpcRequest::DepsRemove { name, force }).await? {
        RpcResponse::Ok(msg) => println!("Success: {}", msg),
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn base_list() -> Result<()> {
    match send_rpc_request(RpcRequest::BaseList).await? {
        RpcResponse::BaseList(bases) => {
            println!(
                "{:<25} | {:<10} | {:<10} | {:<10} | {:<15} | {:<25}",
                "BASE NAME", "OS", "LIBC", "ARCH", "SIZE (MB)", "ACTIVE CONSUMERS"
            );
            println!(
                "{:-<25}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<15}-+-{:-<25}",
                "", "", "", "", "", ""
            );
            for b in bases {
                let size_mb = b.size_bytes as f64 / 1_048_576.0;
                let consumers_str = if b.consumers.is_empty() {
                    "None".to_string()
                } else {
                    format!("{} ({})", b.consumers.len(), b.consumers.join(", "))
                };

                let (os, libc, arch) = if let Some(m) = b.manifest {
                    (m.os, m.libc, m.architecture)
                } else {
                    (
                        "unknown".to_string(),
                        "unknown".to_string(),
                        "unknown".to_string(),
                    )
                };

                println!(
                    "{:<25} | {:<10} | {:<10} | {:<10} | {:<15.2} | {:<25}",
                    b.name, os, libc, arch, size_mb, consumers_str
                );
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn base_inspect(name: String) -> Result<()> {
    match send_rpc_request(RpcRequest::BaseInspect(name)).await? {
        RpcResponse::BaseDetails(details) => {
            let json = serde_json::to_string_pretty(&details)?;
            println!("{}", json);
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn base_prune() -> Result<()> {
    match send_rpc_request(RpcRequest::BasePrune).await? {
        RpcResponse::PruneResult(pruned) => {
            if pruned.is_empty() {
                println!("No unused base images found. Nothing to prune.");
            } else {
                println!("Successfully pruned unused base images:");
                for p in pruned {
                    println!("  - {}", p);
                }
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn test_ps_client_parsing() {
        let socket_path = "/tmp/openbackd_test_ps.sock";
        let _ = std::fs::remove_file(socket_path);
        std::env::set_var("OPENBACK_SOCKET", socket_path);

        let listener = UnixListener::bind(socket_path).unwrap();

        // Spawn mock server
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();

            // Send back a mock RpcResponse::ProcessList
            let mock_response =
                openback::rpc::RpcResponse::ProcessList(vec![openback::rpc::ProcessInfo {
                    name: "test-app".to_string(),
                    pid: 1234,
                    status: "running".to_string(),
                    start_time: "2026-08-07T00:00:00Z".to_string(),
                }]);
            let mut response_json = serde_json::to_string(&mock_response).unwrap();
            response_json.push('\n');
            stream.write_all(response_json.as_bytes()).await.unwrap();
        });

        // Run the client function, which will hit the mock server.
        // ps() prints to stdout, so we just check it doesn't error.
        let res = ps().await;
        assert!(res.is_ok());
        let _ = std::fs::remove_file(socket_path);
    }
}
