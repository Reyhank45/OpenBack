mod client;
mod daemon;
mod launcher;
mod manifest;
mod rpc;

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
    Daemon,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon => {
            daemon::run_daemon().await?;
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
    }

    Ok(())
}
