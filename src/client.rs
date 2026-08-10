use anyhow::{Context, Result};
use openback::manifest::AppManifest;
use openback::rpc::{EngineEnvelope, EngineRequest, EngineResponse};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

async fn send_rpc_request(request: EngineRequest) -> Result<EngineResponse> {
    let socket_path =
        &std::env::var("OPENBACK_SOCKET").unwrap_or_else(|_| "/tmp/openbackd.sock".to_string());
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Failed to connect to daemon at {}", socket_path))?;

    let envelope = EngineEnvelope {
        auth_token: None,
        request,
    };
    let mut payload = serde_json::to_string(&envelope)?;
    payload.push('\n');

    stream.write_all(payload.as_bytes()).await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    if line.is_empty() {
        anyhow::bail!("Daemon closed connection unexpectedly");
    }

    let response: EngineResponse = serde_json::from_str(&line)?;
    Ok(response)
}

pub async fn run_app(manifest_path: PathBuf) -> Result<()> {
    let content = tokio::fs::read_to_string(&manifest_path)
        .await
        .with_context(|| format!("Failed to read manifest file: {:?}", manifest_path))?;

    let manifest: AppManifest =
        serde_json::from_str(&content).context("Failed to parse manifest JSON")?;

    println!("Sending Run request for app: {}", manifest.app_name);

    match send_rpc_request(EngineRequest::Run(manifest)).await? {
        EngineResponse::Ok(msg) => println!("Success: {}", msg),
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }

    Ok(())
}

pub async fn ps(all: bool) -> Result<()> {
    match send_rpc_request(EngineRequest::Ps { all }).await? {
        EngineResponse::ContainerList(containers) => {
            println!(
                "{:<40} | {:<10} | {:<10} | {:<25}",
                "APP NAME", "PID", "STATUS", "START TIME"
            );
            println!("{:-<40}-+-{:-<10}-+-{:-<10}-+-{:-<25}", "", "", "", "");
            for c in containers {
                println!(
                    "{:<40} | {:<10} | {:<10} | {:<25}",
                    c.container_name, c.pid, c.status, c.start_time
                );
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn stop(container_names: Vec<String>) -> Result<()> {
    for container_name in container_names {
        match send_rpc_request(EngineRequest::Stop {
            container_name: Some(container_name),
        })
        .await?
        {
            EngineResponse::Ok(msg) => println!("Success: {}", msg),
            EngineResponse::Error(err) => eprintln!("Error: {}", err),
            _ => eprintln!("Unexpected response from daemon"),
        }
    }
    Ok(())
}

pub async fn start(container_names: Vec<String>) -> Result<()> {
    for container_name in container_names {
        match send_rpc_request(EngineRequest::Start {
            container_name,
        })
        .await?
        {
            EngineResponse::Ok(msg) => println!("Success: {}", msg),
            EngineResponse::Error(err) => eprintln!("Error: {}", err),
            _ => eprintln!("Unexpected response from daemon"),
        }
    }
    Ok(())
}

pub async fn rm(container_names: Vec<String>) -> Result<()> {
    for container_name in container_names {
        match send_rpc_request(EngineRequest::Rm {
            container_name,
        })
        .await?
        {
            EngineResponse::Ok(msg) => println!("Success: {}", msg),
            EngineResponse::Error(err) => eprintln!("Error: {}", err),
            _ => eprintln!("Unexpected response from daemon"),
        }
    }
    Ok(())
}

pub async fn logs(container_name: String) -> Result<()> {
    match send_rpc_request(EngineRequest::Logs {
        container_name,
        tail: None,
    })
    .await?
    {
        EngineResponse::LogLines(lines) => {
            for line in lines {
                println!("{}", line);
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_list() -> Result<()> {
    match send_rpc_request(EngineRequest::DepsList).await? {
        EngineResponse::DepsList(deps) => {
            println!(
                "{:<30} | {:<15} | {:<25} | {:<10} | {:<30}",
                "DEPENDENCY", "VERSION", "TARGET", "SIZE (MB)", "ACTIVE CONSUMERS"
            );
            println!("{:-<30}-+-{:-<15}-+-{:-<25}-+-{:-<10}-+-{:-<30}", "", "", "", "", "");
            for d in deps {
                let size_mb = d.size_bytes as f64 / 1_048_576.0;
                let consumers_str = if d.consumers.is_empty() {
                    "None".to_string()
                } else {
                    format!("{} ({})", d.consumers.len(), d.consumers.join(", "))
                };
                let target = format!("{}/{}/{}", 
                    d.target_os.unwrap_or_else(|| "unknown".to_string()),
                    d.target_libc.unwrap_or_else(|| "unknown".to_string()),
                    d.target_arch.unwrap_or_else(|| "unknown".to_string())
                );
                println!(
                    "{:<30} | {:<15} | {:<25} | {:<10.2} | {:<30}",
                    d.name, &d.version[..std::cmp::min(12, d.version.len())], target, size_mb, consumers_str
                );
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_inspect(name: String) -> Result<()> {
    match send_rpc_request(EngineRequest::DepsInspect(name)).await? {
        EngineResponse::DepDetails(details) => {
            let json = serde_json::to_string_pretty(&details)?;
            println!("{}", json);
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_prune() -> Result<()> {
    match send_rpc_request(EngineRequest::DepsPrune).await? {
        EngineResponse::PruneResult(pruned) => {
            if pruned.is_empty() {
                println!("No unused dependencies found. Nothing to prune.");
            } else {
                println!("Successfully pruned unused dependencies:");
                for p in pruned {
                    println!("  - {}", p);
                }
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn deps_remove(name: String, force: bool) -> Result<()> {
    match send_rpc_request(EngineRequest::DepsRemove { name, force }).await? {
        EngineResponse::Ok(msg) => println!("Success: {}", msg),
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn base_list() -> Result<()> {
    match send_rpc_request(EngineRequest::BaseList).await? {
        EngineResponse::BaseList(bases) => {
            println!(
                "{:<30} | {:<10} | {:<10} | {:<10} | {:<15} | {:<30}",
                "BASE NAME", "OS", "LIBC", "ARCH", "SIZE (MB)", "ACTIVE CONSUMERS"
            );
            println!(
                "{:-<30}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<15}-+-{:-<30}",
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
                    "{:<30} | {:<10} | {:<10} | {:<10} | {:<15.2} | {:<30}",
                    b.name, os, libc, arch, size_mb, consumers_str
                );
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn base_inspect(name: String) -> Result<()> {
    match send_rpc_request(EngineRequest::BaseInspect(name)).await? {
        EngineResponse::BaseDetails(details) => {
            let json = serde_json::to_string_pretty(&details)?;
            println!("{}", json);
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}

pub async fn base_prune() -> Result<()> {
    match send_rpc_request(EngineRequest::BasePrune).await? {
        EngineResponse::PruneResult(pruned) => {
            if pruned.is_empty() {
                println!("No unused base images found. Nothing to prune.");
            } else {
                println!("Successfully pruned unused base images:");
                for p in pruned {
                    println!("  - {}", p);
                }
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
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

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();

            let mock_response = EngineResponse::AppList(vec![openback::rpc::AppInfo {
                app_name: "test-app".to_string(),
                instances: vec![openback::rpc::InstanceInfo {
                    instance_id: "test-hash".to_string(),
                    pid: 1234,
                    status: "running".to_string(),
                    start_time: "2026-08-07T00:00:00Z".to_string(),
                }],
            }]);
            let mut response_json = serde_json::to_string(&mock_response).unwrap();
            response_json.push('\n');
            stream.write_all(response_json.as_bytes()).await.unwrap();
        });

        let res = ps().await;
        assert!(res.is_ok());
        let _ = std::fs::remove_file(socket_path);
    }
}
