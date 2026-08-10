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
        #[arg(long)]
        port: Option<u16>,
    },
    /// Run an application from a manifest file
    Run {
        /// Path to the openback.json manifest
        manifest_path: PathBuf,
    },
    /// List all running applications
    Ps {
        #[arg(short, long)]
        all: bool,
    },
    /// Stop a running container by name
    Stop {
        /// Name of the container to stop
        container_name: String,
    },
    /// Start a stopped container by name
    Start {
        /// Name of the container to start
        container_name: String,
    },
    /// Remove a stopped container
    Rm {
        /// Name of the container to remove
        container_name: String,
    },
    /// Get logs for a container
    Logs {
        /// Name of the container
        container_name: String,
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
        Commands::Daemon { port } => {
            daemon::run_daemon(port).await?;
        }
        Commands::Run { manifest_path } => {
            client::run_app(manifest_path).await?;
        }
        Commands::Ps { all } => {
            client::ps(all).await?;
        }
        Commands::Stop { container_name } => {
            client::stop(container_name).await?;
        }
        Commands::Start { container_name } => {
            client::start(container_name).await?;
        }
        Commands::Rm { container_name } => {
            client::rm(container_name).await?;
        }
        Commands::Logs { container_name } => {
            client::logs(container_name).await?;
        }
        Commands::InternalLauncher { manifest_json } => {
            launcher::launch_container(manifest_json)?;
        }
        Commands::Deps { action } => match action {
            DepsCommand::List => client::deps_list().await?,
            DepsCommand::Inspect { name } => client::deps_inspect(name).await?,
            DepsCommand::Prune => client::deps_prune().await?,
            DepsCommand::Remove { name, force } => client::deps_remove(name, force).await?,
        },
        Commands::Base { action } => match action {
            BaseCommand::List => client::base_list().await?,
            BaseCommand::Inspect { name } => client::base_inspect(name).await?,
            BaseCommand::Prune => client::base_prune().await?,
        },
    }

    Ok(())
}
