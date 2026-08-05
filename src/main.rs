mod client;
mod daemon;
mod launcher;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "openback", about = "OpenBack Process Orchestrator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the OpenBack daemon
    Daemon {
        #[arg(long, default_value = "standalone")]
        role: String,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        master_addr: Option<String>,
        #[arg(long)]
        cluster_token: Option<String>,
    },
    /// Run an application from a manifest file
    Run {
        /// Path to the openback.json manifest
        manifest_path: PathBuf,
    },
    /// List all running applications
    Ps,
    /// Stop a running application by name
    Stop {
        /// Name of the application to stop
        app_name: String,
    },
    /// Get logs for a running application
    Logs {
        /// Name of the application
        app_name: String,
    },
    /// Internal command to launch the container namespaces
    #[command(hide = true)]
    InternalLauncher {
        /// JSON serialized manifest string
        manifest_json: String,
    },
    /// Manage dependency payloads in the OpenBack store
    Deps {
        #[command(subcommand)]
        action: DepsCommand,
    },
    /// Manage Layer 1 Base Images in the OpenBack store
    Base {
        #[command(subcommand)]
        action: BaseCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum BaseCommand {
    /// List all installed base images and their active consumers
    List,
    /// Inspect metadata for a specific base image
    Inspect {
        /// Format: <base_name>
        name: String,
    },
    /// Safely remove base images with 0 active app attachments
    Prune,
}

#[derive(Subcommand, Debug)]
pub enum DepsCommand {
    /// List all installed dependencies and their active consumers
    List,
    /// Inspect details of a specific dependency
    Inspect {
        /// Format: <name>@<version>
        name: String,
    },
    /// Safely remove unused dependencies
    Prune,
    /// Manually remove a specific dependency
    Remove {
        /// Format: <name>@<version>
        name: String,
        /// Force removal even if actively used
        #[arg(short, long)]
        force: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon { role, port, master_addr, cluster_token } => {
            daemon::run_daemon(role, port, master_addr, cluster_token).await?;
        }
        Commands::Run { manifest_path } => {
            client::run_app(manifest_path).await?;
        }
        Commands::Ps => {
            client::ps().await?;
        }
        Commands::Stop { app_name } => {
            client::stop(app_name).await?;
        }
        Commands::Logs { app_name } => {
            client::logs(app_name).await?;
        }
        Commands::InternalLauncher { manifest_json } => {
            launcher::launch_container(manifest_json)?;
        }
        Commands::Deps { action } => {
            match action {
                DepsCommand::List => client::deps_list().await?,
                DepsCommand::Inspect { name } => client::deps_inspect(name).await?,
                DepsCommand::Prune => client::deps_prune().await?,
                DepsCommand::Remove { name, force } => client::deps_remove(name, force).await?,
            }
        }
        Commands::Base { action } => {
            match action {
                BaseCommand::List => client::base_list().await?,
                BaseCommand::Inspect { name } => client::base_inspect(name).await?,
                BaseCommand::Prune => client::base_prune().await?,
            }
        }
    }

    Ok(())
}
