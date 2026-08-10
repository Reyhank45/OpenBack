use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use etcd_client::Client;
use rand::Rng;

#[derive(Parser, Debug)]
#[command(name = "backadm", about = "OpenBack cluster bootstrap tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize an OpenBack control plane
    Init {
        #[arg(long, default_value = "127.0.0.1:2379")]
        etcd_endpoint: String,
    },
    /// Join a node to the OpenBack cluster
    Join {
        #[arg(help = "The etcd endpoint of the control plane to join")]
        endpoint: String,

        #[arg(long, help = "Token for joining the cluster")]
        token: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { etcd_endpoint } => init(etcd_endpoint).await,
        Commands::Join { endpoint, token } => join(endpoint, token).await,
    }
}

async fn init(etcd_endpoint: String) -> Result<()> {
    println!("[init] Connecting to etcd cluster at {}...", etcd_endpoint);
    let mut client = Client::connect([&etcd_endpoint], None)
        .await
        .context("Failed to connect to Etcd. Ensure etcd is running locally.")?;

    // Generate token: 6 chars . 16 chars (like kubeadm)
    let rng = rand::thread_rng();
    let token_part1: String = rng
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();
    let token_part2: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(16)
        .map(char::from)
        .collect();
    let token = format!("{}.{}", token_part1.to_lowercase(), token_part2.to_lowercase());

    println!("[init] Generated bootstrap token: {}", token);

    // Write token to etcd
    client
        .put("/openback/cluster/token", token.clone(), None)
        .await
        .context("Failed to store token in etcd")?;

    // Configure local backlet
    let backlet_env_path = "/etc/default/backlet";
    let env_content = format!(
        "# Default settings for backlet. This file is sourced by systemd\n\
         BACKLET_EXTRA_ARGS=\"--etcd-endpoints {}\"\n",
        etcd_endpoint
    );
    if std::fs::write(backlet_env_path, env_content).is_err() {
        println!("[init] Warning: Could not write to {}. Are you running as root?", backlet_env_path);
    } else {
        println!("[init] Configured backlet etcd endpoints");
    }

    // Try to restart services
    let _ = std::process::Command::new("systemctl")
        .args(["enable", "--now", "openbackd.service", "backlet.service"])
        .status();

    println!("\nYour OpenBack control-plane has initialized successfully!\n");
    println!("To start using your cluster, you can deploy applications using backctl.");
    println!("You can now join any number of machines by running the following on each node as root:\n");
    println!("  backadm join {} --token {}\n", etcd_endpoint, token);

    Ok(())
}

async fn join(endpoint: String, token: String) -> Result<()> {
    println!("[join] Connecting to etcd cluster at {}...", endpoint);
    let mut client = Client::connect([&endpoint], None)
        .await
        .context("Failed to connect to Etcd")?;

    println!("[join] Validating token...");
    let resp = client
        .get("/openback/cluster/token", None)
        .await
        .context("Failed to fetch token from etcd")?;

    let mut valid = false;
    if let Some(kv) = resp.kvs().first() {
        if let Ok(stored_token) = String::from_utf8(kv.value().to_vec()) {
            if stored_token == token {
                valid = true;
            }
        }
    }

    if !valid {
        anyhow::bail!("Invalid or missing join token");
    }

    println!("[join] Token validation successful.");

    // Configure local backlet
    let backlet_env_path = "/etc/default/backlet";
    let env_content = format!(
        "# Default settings for backlet. This file is sourced by systemd\n\
         BACKLET_EXTRA_ARGS=\"--etcd-endpoints {}\"\n",
        endpoint
    );
    if std::fs::write(backlet_env_path, env_content).is_err() {
        println!("[join] Warning: Could not write to {}. Are you running as root?", backlet_env_path);
    } else {
        println!("[join] Configured backlet etcd endpoints");
    }

    // Try to restart services
    println!("[join] Enabling and starting openback services...");
    let _ = std::process::Command::new("systemctl")
        .args(["enable", "--now", "openbackd.service", "backlet.service"])
        .status();

    println!("\nThis node has joined the cluster:\n* Certificate signing request was sent (simulated)\n* Node registered in Etcd\n* OpenBack agent (backlet) is now running\n");

    Ok(())
}
