use crate::manifest::AppManifest;
use crate::rpc::{RpcRequest, RpcResponse};
use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

async fn send_rpc_request(request: RpcRequest) -> Result<RpcResponse> {
    let socket_path = "/tmp/openbackd.sock";
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

    let manifest: AppManifest = serde_json::from_str(&content)
        .context("Failed to parse manifest JSON")?;

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
            println!("{:<20} | {:<10} | {:<10} | {:<25}", "APP NAME", "PID", "STATUS", "START TIME");
            println!("{:-<20}-+-{:-<10}-+-{:-<10}-+-{:-<25}", "", "", "", "");
            for p in processes {
                println!("{:<20} | {:<10} | {:<10} | {:<25}", p.name, p.pid, p.status, p.start_time);
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
    match send_rpc_request(RpcRequest::Logs(app_name)).await? {
        RpcResponse::Ok(logs) => print!("{}", logs),
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response from daemon"),
    }
    Ok(())
}
