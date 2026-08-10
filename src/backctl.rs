use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use openback::rpc::{
    ClusterEnvelope, ClusterRequest, ClusterResponse, EngineEnvelope, EngineRequest,
    EngineResponse, KubeApplication,
};
use std::path::PathBuf;
use std::process::Command as StdCommand;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Parser, Debug)]
#[command(name = "backctl", about = "OpenBack Orchestrator CLI")]
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
    /// Get resources (e.g. apps, nodes, deps, base)
    Get { resource: String },
    /// Delete resources (e.g. app <name> or -f <manifest>)
    Delete {
        resource: Option<String>,
        name: Option<String>,
        /// Delete by manifest file
        #[arg(short, long)]
        file: Option<PathBuf>,
    },
    /// Describe an application
    Describe { resource: String, name: String },
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
    Edit { resource: String, name: String },
    /// Attach to a running replica for live logs and interactive terminal
    Attach { name: String },
    /// List active applications and instances
    Ps,
    /// Remove a stopped container from the engine
    Rm {
        app_name: String,
        instance_id: String,
    },
}

async fn send_engine_request(request: EngineRequest) -> Result<EngineResponse> {
    let socket_path =
        &std::env::var("OPENBACK_SOCKET").unwrap_or_else(|_| "/tmp/openbackd.sock".to_string());
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Failed to connect to engine at {}", socket_path))?;

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
        anyhow::bail!("Engine closed connection unexpectedly");
    }

    let response: EngineResponse = serde_json::from_str(&line)?;
    Ok(response)
}

async fn send_cluster_request(request: ClusterRequest) -> Result<ClusterResponse> {
    let socket_path =
        &std::env::var("BACKLET_SOCKET").unwrap_or_else(|_| "/tmp/backlet.sock".to_string());
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Failed to connect to backlet at {}", socket_path))?;

    let envelope = ClusterEnvelope {
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
        anyhow::bail!("Backlet closed connection unexpectedly");
    }

    let response: ClusterResponse = serde_json::from_str(&line)?;
    Ok(response)
}

async fn apply(file_path: PathBuf) -> Result<()> {
    let content = tokio::fs::read_to_string(&file_path)
        .await
        .context("Failed to read YAML file")?;

    let kube_app: KubeApplication =
        serde_yaml::from_str(&content).context("Failed to parse YAML manifest")?;

    if kube_app.api_version != "openback.io/v1" || kube_app.kind != "Application" {
        anyhow::bail!("Invalid apiVersion or kind");
    }

    match send_cluster_request(ClusterRequest::Apply(kube_app.clone())).await? {
        ClusterResponse::Ok(msg) => println!("Success: {}", msg),
        ClusterResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }

    Ok(())
}

async fn get_apps(all: bool) -> Result<()> {
    match send_engine_request(EngineRequest::Ps { all }).await? {
        EngineResponse::AppList(apps) => {
            println!(
                "{:<25} | {:<15} | {:<15} | {:<10}",
                "APPLICATION", "INSTANCE ID", "STATUS", "PID"
            );
            println!("{:-<25}-+-{:-<15}-+-{:-<15}-+-{:-<10}", "", "", "", "");
            for app in apps {
                if app.instances.is_empty() {
                    println!(
                        "{:<25} | {:<15} | {:<15} | {:<10}",
                        app.app_name, "<none>", "-", "-"
                    );
                } else {
                    for inst in app.instances {
                        println!(
                            "{:<25} | {:<15} | {:<15} | {:<10}",
                            app.app_name, inst.instance_id, inst.status, inst.pid
                        );
                    }
                }
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn delete_app(app_name: String) -> Result<()> {
    match send_cluster_request(ClusterRequest::DeleteDeployment(app_name.clone())).await? {
        ClusterResponse::Ok(msg) => println!("Success: {}", msg),
        ClusterResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn rm_container(app_name: String, instance_id: String) -> Result<()> {
    match send_engine_request(EngineRequest::Rm {
        app_name,
        instance_id,
    })
    .await?
    {
        EngineResponse::Ok(msg) => println!("Success: {}", msg),
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn get_deps() -> Result<()> {
    match send_engine_request(EngineRequest::DepsList).await? {
        EngineResponse::DepsList(deps) => {
            println!(
                "{:<30} | {:<15} | {:<10} | {:<30}",
                "DEPENDENCY", "VERSION", "SIZE (MB)", "ACTIVE CONSUMERS"
            );
            println!("{:-<30}-+-{:-<15}-+-{:-<10}-+-{:-<30}", "", "", "", "");
            if deps.is_empty() {
                println!("No dependencies installed.");
            }
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
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn get_base() -> Result<()> {
    match send_engine_request(EngineRequest::BaseList).await? {
        EngineResponse::BaseList(bases) => {
            println!(
                "{:<25} | {:<10} | {:<10} | {:<10} | {:<15} | {:<25}",
                "BASE NAME", "OS", "LIBC", "ARCH", "SIZE (MB)", "ACTIVE CONSUMERS"
            );
            println!(
                "{:-<25}-+-{:-<10}-+-{:-<10}-+-{:-<10}-+-{:-<15}-+-{:-<25}",
                "", "", "", "", "", ""
            );
            if bases.is_empty() {
                println!("No base images installed.");
            }
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
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn describe_app(app_name: String) -> Result<()> {
    match send_cluster_request(ClusterRequest::Describe(app_name)).await? {
        ClusterResponse::DescribeDetails(details) => {
            println!("{}", serde_yaml::to_string(&details)?);
        }
        ClusterResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn logs_app(app_name: String, tail: Option<usize>) -> Result<()> {
    match send_engine_request(EngineRequest::Logs {
        app_name,
        instance_id: None,
        tail,
    })
    .await?
    {
        EngineResponse::LogLines(lines) => {
            for line in lines {
                println!("{}", line);
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn scale_app(app_name: String, replicas: usize) -> Result<()> {
    match send_cluster_request(ClusterRequest::Scale { app_name, replicas }).await? {
        ClusterResponse::Ok(msg) => println!("Success: {}", msg),
        ClusterResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn edit_app(app_name: String) -> Result<()> {
    match send_cluster_request(ClusterRequest::GetDeployment(app_name.clone())).await? {
        ClusterResponse::DeploymentDetails(app) => {
            let yaml = serde_yaml::to_string(&app)?;
            let temp_path = format!("/tmp/openback-edit-{}.yaml", app_name);
            std::fs::write(&temp_path, yaml)?;

            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "nano".to_string());
            StdCommand::new(editor).arg(&temp_path).status()?;

            let new_content = std::fs::read_to_string(&temp_path)?;
            let new_app: KubeApplication = serde_yaml::from_str(&new_content)?;

            match send_cluster_request(ClusterRequest::Apply(new_app)).await? {
                ClusterResponse::Ok(msg) => println!("Success: {}", msg),
                ClusterResponse::Error(err) => eprintln!("Error: {}", err),
                _ => eprintln!("Unexpected response"),
            }
            let _ = std::fs::remove_file(temp_path);
        }
        ClusterResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn get_nodes() -> Result<()> {
    match send_cluster_request(ClusterRequest::GetNodes).await? {
        ClusterResponse::NodeList(nodes) => {
            println!(
                "{:<20} | {:<15} | {:<10} | {:<10} | {:<10}",
                "HOSTNAME", "ROLE", "CPU %", "RAM %", "STATUS"
            );
            println!(
                "{:-<20}-+-{:-<15}-+-{:-<10}-+-{:-<10}-+-{:-<10}",
                "", "", "", "", ""
            );
            for node in nodes {
                println!(
                    "{:<20} | {:<15} | {:<10.2} | {:<10.2} | {:<10}",
                    node.hostname, node.role, node.cpu_usage, node.ram_usage, node.status
                );
            }
        }
        ClusterResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

async fn attach_app(app_name: String) -> Result<()> {
    let socket_path =
        &std::env::var("OPENBACK_SOCKET").unwrap_or_else(|_| "/tmp/openbackd.sock".to_string());
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("Failed to connect to daemon at {}", socket_path))?;

    let request = EngineRequest::Attach { app_name };
    let envelope = EngineEnvelope {
        auth_token: None,
        request,
    };
    let mut payload = serde_json::to_string(&envelope)?;
    payload.push('\n');

    stream.write_all(payload.as_bytes()).await?;

    let mut buf_reader = BufReader::new(&mut stream);
    let mut line = String::new();
    buf_reader.read_line(&mut line).await?;

    if line.is_empty() {
        anyhow::bail!("Daemon closed connection unexpectedly");
    }

    let response: EngineResponse = serde_json::from_str(&line)?;
    match response {
        EngineResponse::AttachStream => {
            println!("Attached to stream. Press Ctrl-C to detach.");
            let stream = buf_reader.into_inner();

            // Set terminal to raw mode if possible (for sending inputs directly)
            // But for simplicity, we just pipe stdin and stdout
            let (mut reader, mut writer) = tokio::io::split(stream);
            let mut stdin = tokio::io::stdin();
            let mut stdout = tokio::io::stdout();

            let to_stdout = tokio::io::copy(&mut reader, &mut stdout);
            let from_stdin = tokio::io::copy(&mut stdin, &mut writer);

            tokio::select! {
                _ = to_stdout => (),
                _ = from_stdin => (),
            }
        }
        EngineResponse::Error(err) => eprintln!("Error: {}", err),
        _ => eprintln!("Unexpected response"),
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Apply { file } => apply(file).await?,
        Commands::Ps => {
            get_apps(false).await?;
        }
        Commands::Get { resource } => {
            if resource == "apps" {
                get_apps(true).await?;
            } else if resource == "nodes" {
                get_nodes().await?;
            } else if resource == "deps" || resource == "dependencies" {
                get_deps().await?;
            } else if resource == "base" || resource == "images" {
                get_base().await?;
            } else {
                eprintln!(
                    "Unknown resource: '{}'. Valid resources: apps, nodes, deps, base",
                    resource
                );
                std::process::exit(1);
            }
        }
        Commands::Delete {
            resource,
            name,
            file,
        } => {
            // Support: delete -f manifest.yaml  OR  delete app <name>
            if let Some(file_path) = file {
                let content = tokio::fs::read_to_string(&file_path)
                    .await
                    .with_context(|| format!("Failed to read {:?}", file_path))?;
                let kube_app: openback::rpc::KubeApplication =
                    serde_yaml::from_str(&content).context("Failed to parse YAML manifest")?;
                delete_app(kube_app.metadata.name).await?;
            } else if let (Some(res), Some(n)) = (resource, name) {
                if res == "app" {
                    delete_app(n).await?;
                } else {
                    eprintln!(
                        "Unknown resource: '{}'. Use 'delete app <name>' or 'delete -f <manifest>'",
                        res
                    );
                    std::process::exit(1);
                }
            } else {
                eprintln!(
                    "Usage: backcli delete app <name>  OR  backcli delete -f <manifest.yaml>"
                );
                std::process::exit(1);
            }
        }
        Commands::Describe { resource, name } => {
            if resource == "app" {
                describe_app(name).await?;
            }
        }
        Commands::Logs { name, tail } => logs_app(name, tail).await?,
        Commands::Scale {
            resource,
            name,
            replicas,
        } => {
            if resource == "app" {
                scale_app(name, replicas).await?;
            }
        }
        Commands::Edit { resource, name } => {
            if resource == "app" {
                edit_app(name).await?;
            }
        }
        Commands::Attach { name } => {
            attach_app(name).await?;
        }
        Commands::Rm {
            app_name,
            instance_id,
        } => {
            rm_container(app_name, instance_id).await?;
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
            Commands::Scale {
                resource,
                name,
                replicas,
            } => {
                assert_eq!(resource, "app");
                assert_eq!(name, "test-app");
                assert_eq!(replicas, 3);
            }
            _ => panic!("Expected Scale command"),
        }
    }
}
