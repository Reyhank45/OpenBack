use clap::{Parser, Subcommand};
use openback::manifest::{AppManifest, Networking, Permissions};
use openback::rpc::{RpcRequest, RpcResponse, ProcessInfo, KubeApplication};
use std::process::Command as StdCommand;
use anyhow::{Context, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;





#[derive(Parser, Debug)]
#[command(name = "backcli", about = "OpenBack Orchestrator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Apply a declarative YAML manifest
    Apply {
        #[arg(short, long)]
        file: PathBuf,
    },
    /// Get resources (e.g. apps)
    Get {
        resource: String,
    },
    /// Delete resources (e.g. app <name>)
    Delete {
        resource: String,
        name: String,

    },
    /// Describe an application
    Describe {
        resource: String,
        name: String,
    },
    /// View logs for an application
    Logs {
        name: String,
        #[arg(short, long)]
        tail: Option<usize>,
    },
    /// Scale an application
    Scale {
        resource: String,
        name: String,
        #[arg(long)]
        replicas: usize,
    },
    /// Edit an application in $EDITOR
    Edit {
        resource: String,
        name: String,
    },
}


async fn send_rpc_request(request: RpcRequest) -> Result<RpcResponse> {
    let socket_path = &std::env::var("OPENBACK_SOCKET").unwrap_or_else(|_| "/tmp/openbackd.sock".to_string());
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




async fn apply(file_path: PathBuf) -> Result<()> {
    let content = tokio::fs::read_to_string(&file_path)
        .await
        .context("Failed to read YAML file")?;
        
    let kube_app: KubeApplication = serde_yaml::from_str(&content)
        .context("Failed to parse YAML manifest")?;
        
    if kube_app.api_version != "openback.io/v1" || kube_app.kind != "Application" {
        anyhow::bail!("Invalid apiVersion or kind");
    }

    match send_rpc_request(RpcRequest::Apply(kube_app.clone())).await? {
        RpcResponse::Ok(msg) => println!("Success: {}", msg),
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    
    Ok(())
}



async fn get_apps() -> Result<()> {
    match send_rpc_request(RpcRequest::Ps).await? {
        RpcResponse::ProcessList(processes) => {
            let mut apps: HashMap<String, usize> = HashMap::new();
            for p in processes {
                // Heuristic: strip the -<8char_hex> suffix
                let parts: Vec<&str> = p.name.split('-').collect();
                if parts.len() >= 2 {
                    let suffix = parts.last().unwrap();
                    if suffix.len() == 8 && u32::from_str_radix(suffix, 16).is_ok() {
                        let base_name = p.name[0..p.name.len() - 9].to_string();
                        *apps.entry(base_name).or_insert(0) += 1;
                        continue;
                    }
                }
                *apps.entry(p.name.clone()).or_insert(0) += 1;
            }

            println!("{:<20} | {:<10} | {:<15}", "APPLICATION", "REPLICAS", "STATUS");
            println!("{:-<20}-+-{:-<10}-+-{:-<15}", "", "", "");
            for (name, count) in apps {
                println!("{:<20} | {:<10} | {:<15}", name, count, "Running");
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn delete_app(app_name: String) -> Result<()> {
    match send_rpc_request(RpcRequest::Ps).await? {
        RpcResponse::ProcessList(processes) => {
            let mut deleted = 0;
            for p in processes {
                let is_match = p.name == app_name || (p.name.starts_with(&format!("{}-", app_name)) && p.name.len() == app_name.len() + 9);
                if is_match {
                    println!("Stopping replica: {}", p.name);
                    match send_rpc_request(RpcRequest::Stop(p.name.clone())).await? {
                        RpcResponse::Ok(msg) => {
                            println!("Success: {}", msg);
                            deleted += 1;
                        }
                        RpcResponse::Error(err) => eprintln!("Error: {}", err),
                        _ => (),
                    }
                }
            }
            if deleted == 0 {
                println!("No replicas found for application '{}'", app_name);
            } else {
                println!("Deleted {} replicas for '{}'", deleted, app_name);
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}


async fn describe_app(app_name: String) -> Result<()> {
    match send_rpc_request(RpcRequest::Describe(app_name)).await? {
        RpcResponse::DescribeDetails(details) => {
            println!("{}", serde_yaml::to_string(&details)?);
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn logs_app(app_name: String, tail: Option<usize>) -> Result<()> {
    match send_rpc_request(RpcRequest::Logs { app_name, tail }).await? {
        RpcResponse::LogLines(lines) => {
            for line in lines {
                println!("{}", line);
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn scale_app(app_name: String, replicas: usize) -> Result<()> {
    match send_rpc_request(RpcRequest::Scale { app_name, replicas }).await? {
        RpcResponse::Ok(msg) => println!("Success: {}", msg),
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn edit_app(app_name: String) -> Result<()> {
    match send_rpc_request(RpcRequest::GetDeployment(app_name.clone())).await? {
        RpcResponse::DeploymentDetails(app) => {
            let yaml = serde_yaml::to_string(&app)?;
            let temp_path = format!("/tmp/openback-edit-{}.yaml", app_name);
            std::fs::write(&temp_path, yaml)?;
            
            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "nano".to_string());
            StdCommand::new(editor).arg(&temp_path).status()?;
            
            let new_content = std::fs::read_to_string(&temp_path)?;
            let new_app: KubeApplication = serde_yaml::from_str(&new_content)?;
            
            match send_rpc_request(RpcRequest::Apply(new_app)).await? {
                RpcResponse::Ok(msg) => println!("Success: {}", msg),
                RpcResponse::Error(err) => eprintln!("Error: {}", err),
                _ => eprintln!("Unexpected response"),
            }
            let _ = std::fs::remove_file(temp_path);
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn get_nodes() -> Result<()> {
    match send_rpc_request(RpcRequest::GetNodes).await? {
        RpcResponse::NodeList(nodes) => {
            println!("{:<20} | {:<15} | {:<10} | {:<10} | {:<10}", "HOSTNAME", "ROLE", "CPU %", "RAM %", "STATUS");
            println!("{:-<20}-+-{:-<15}-+-{:-<10}-+-{:-<10}-+-{:-<10}", "", "", "", "", "");
            for node in nodes {
                println!("{:<20} | {:<15} | {:<10.2} | {:<10.2} | {:<10}", node.hostname, node.role, node.cpu_usage, node.ram_usage, node.status);
            }
        }
        RpcResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Apply { file } => apply(file).await?,
        Commands::Get { resource } => {
            if resource == "apps" {
                get_apps().await?;
            } else if resource == "nodes" {
                get_nodes().await?;
            } else {
                println!("Unknown resource: {}", resource);
            }
        }
        Commands::Delete { resource, name } => {
            if resource == "app" {
                delete_app(name).await?;
            }
        }
        Commands::Describe { resource, name } => {
            if resource == "app" {
                describe_app(name).await?;
            }
        }
        Commands::Logs { name, tail } => logs_app(name, tail).await?,
        Commands::Scale { resource, name, replicas } => {
            if resource == "app" {
                scale_app(name, replicas).await?;
            }
        }
        Commands::Edit { resource, name } => {
            if resource == "app" {
                edit_app(name).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        let args = vec!["backcli", "get", "apps"];
        let cli = Cli::parse_from(args);
        match cli.command {
            Commands::Get { resource } => assert_eq!(resource, "apps"),
            _ => panic!("Expected Get command"),
        }

        let args2 = vec!["backcli", "scale", "app", "test-app", "--replicas", "3"];
        let cli2 = Cli::parse_from(args2);
        match cli2.command {
            Commands::Scale { resource, name, replicas } => {
                assert_eq!(resource, "app");
                assert_eq!(name, "test-app");
                assert_eq!(replicas, 3);
            },
            _ => panic!("Expected Scale command"),
        }
    }
}
