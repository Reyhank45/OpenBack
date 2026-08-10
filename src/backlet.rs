use anyhow::{Context, Result};
use clap::Parser;
use etcd_client::{Client, GetOptions, PutOptions};
use openback::rpc::{
    ClusterEnvelope, ClusterRequest, ClusterResponse, EngineEnvelope, EngineRequest,
    EngineResponse, KubeApplication,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

#[derive(Parser, Debug)]
#[command(name = "backlet", about = "OpenBack Cluster Agent")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:2379")]
    etcd_endpoints: String,
    #[arg(long)]
    port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeInfo {
    cpu_usage: f32,
    ram_usage: f32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());

    let eps: Vec<String> = cli
        .etcd_endpoints
        .split(',')
        .map(|s| s.to_string())
        .collect();
    let client = Client::connect(eps, None)
        .await
        .context("Failed to connect to Etcd")?;

    openback::dlog!("Backlet", "INFO", "Started backlet on node {}", hostname);

    let socket_path = "/tmp/backlet.sock";
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o777))?;

    let client_clone = client.clone();
    tokio::spawn(async move {
        run_cluster_rpc_server(listener, client_clone).await;
    });

    let client_agent = client.clone();
    let host_agent = hostname.clone();
    tokio::spawn(async move {
        run_node_agent(client_agent, host_agent).await;
    });

    let client_leader = client.clone();
    tokio::spawn(async move {
        run_leader_election(client_leader).await;
    });

    // Block forever
    tokio::signal::ctrl_c().await?;
    openback::dlog!("Backlet", "INFO", "Shutting down");
    Ok(())
}

async fn run_cluster_rpc_server(listener: UnixListener, etcd: Client) {
    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut etcd_clone = etcd.clone();
            tokio::spawn(async move {
                let (reader, mut writer) = stream.split();
                let mut buf_reader = BufReader::new(reader);
                let mut line = String::new();

                while let Ok(n) = buf_reader.read_line(&mut line).await {
                    if n == 0 {
                        break;
                    }
                    if let Ok(env) = serde_json::from_str::<ClusterEnvelope>(&line) {
                        let res = handle_cluster_request(env.request, &mut etcd_clone).await;
                        let mut res_str = serde_json::to_string(&res).unwrap();
                        res_str.push('\n');
                        let _ = writer.write_all(res_str.as_bytes()).await;
                    }
                    line.clear();
                }
            });
        }
    }
}

async fn handle_cluster_request(req: ClusterRequest, etcd: &mut Client) -> ClusterResponse {
    match req {
        ClusterRequest::Apply(app) => {
            let key = format!("/openback/applications/{}", app.metadata.name);
            let val = serde_json::to_string(&app).unwrap();
            if let Err(e) = etcd.put(key, val, None).await {
                return ClusterResponse::Error(format!("etcd error: {}", e));
            }
            ClusterResponse::Ok(format!("Applied deployment {}", app.metadata.name))
        }
        ClusterRequest::DeleteDeployment(name) => {
            let key = format!("/openback/applications/{}", name);
            if let Err(e) = etcd.delete(key, None).await {
                return ClusterResponse::Error(format!("etcd error: {}", e));
            }
            ClusterResponse::Ok(format!("Deleted deployment {}", name))
        }
        ClusterRequest::Scale { app_name, replicas } => {
            let key = format!("/openback/applications/{}", app_name);
            match etcd.get(key.clone(), None).await {
                Ok(resp) => {
                    if let Some(kv) = resp.kvs().first() {
                        let mut app: KubeApplication = serde_json::from_slice(kv.value()).unwrap();
                        app.spec.replicas = Some(replicas);
                        let val = serde_json::to_string(&app).unwrap();
                        let _ = etcd.put(key, val, None).await;
                        ClusterResponse::Ok(format!("Scaled {} to {}", app_name, replicas))
                    } else {
                        ClusterResponse::Error("Not found".to_string())
                    }
                }
                Err(e) => ClusterResponse::Error(format!("etcd error: {}", e)),
            }
        }
        ClusterRequest::GetDeployment(name) => {
            let key = format!("/openback/applications/{}", name);
            match etcd.get(key, None).await {
                Ok(resp) => {
                    if let Some(kv) = resp.kvs().first() {
                        let app: KubeApplication = serde_json::from_slice(kv.value()).unwrap();
                        ClusterResponse::DeploymentDetails(app)
                    } else {
                        ClusterResponse::Error("Not found".to_string())
                    }
                }
                Err(e) => ClusterResponse::Error(format!("etcd error: {}", e)),
            }
        }
        ClusterRequest::Describe(name) => {
            let key = format!("/openback/applications/{}", name);
            let app = match etcd.get(key, None).await {
                Ok(resp) => {
                    if let Some(kv) = resp.kvs().first() {
                        serde_json::from_slice::<KubeApplication>(kv.value()).unwrap()
                    } else {
                        return ClusterResponse::Error("Not found".to_string());
                    }
                }
                Err(_) => return ClusterResponse::Error("etcd error".to_string()),
            };

            // Get instances for this app from all nodes
            let mut instances = Vec::new();
            let opts = GetOptions::new().with_prefix();
            if let Ok(resp) = etcd.get("/openback/assignments/", Some(opts)).await {
                for kv in resp.kvs() {
                    let k = kv.key_str().unwrap_or("");
                    // /openback/assignments/{node}/{replica_name}
                    let parts: Vec<&str> = k.split('/').collect();
                    if parts.len() == 5 {
                        let replica_name = parts[4];
                        if replica_name.starts_with(&name) {
                            instances.push(openback::rpc::InstanceInfo {
                                instance_id: replica_name.to_string(),
                                pid: 0,
                                status: "Assigned".to_string(),
                                start_time: "-".to_string(),
                            });
                        }
                    }
                }
            }

            ClusterResponse::DescribeDetails(openback::rpc::AppDescription {
                deployment: app,
                replicas: instances,
            })
        }
        ClusterRequest::GetNodes => {
            let opts = GetOptions::new().with_prefix();
            let mut nodes = Vec::new();
            if let Ok(resp) = etcd.get("/openback/nodes/", Some(opts)).await {
                for kv in resp.kvs() {
                    let k = kv.key_str().unwrap_or("");
                    if k.ends_with("/status") {
                        let parts: Vec<&str> = k.split('/').collect();
                        if parts.len() == 5 {
                            let hostname = parts[3].to_string();
                            let status: NodeInfo =
                                serde_json::from_slice(kv.value()).unwrap_or(NodeInfo {
                                    cpu_usage: 0.0,
                                    ram_usage: 0.0,
                                });
                            nodes.push(openback::rpc::NodeInfo {
                                hostname,
                                role: "worker".to_string(),
                                cpu_usage: status.cpu_usage,
                                ram_usage: status.ram_usage,
                                status: "READY".to_string(),
                            });
                        }
                    }
                }
            }
            ClusterResponse::NodeList(nodes)
        }
        _ => ClusterResponse::Error("Unsupported operation".to_string()),
    }
}

async fn send_engine_request(req: EngineRequest) -> Result<EngineResponse> {
    let mut stream = UnixStream::connect("/tmp/openbackd.sock").await?;
    let env = EngineEnvelope {
        auth_token: None,
        request: req,
    };
    let mut p = serde_json::to_string(&env)?;
    p.push('\n');
    stream.write_all(p.as_bytes()).await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(&line)?)
}

async fn run_node_agent(mut etcd: Client, hostname: String) {
    let mut sys = sysinfo::System::new_all();
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // 1. Heartbeat
        sys.refresh_all();
        let cpu_usage = sys.global_cpu_usage();
        let ram_usage = (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0;
        let info = NodeInfo {
            cpu_usage,
            ram_usage,
        };

        let lease = etcd.lease_grant(15, None).await;
        if let Ok(l) = lease {
            let opts = PutOptions::new().with_lease(l.id());
            let key = format!("/openback/nodes/{}/status", hostname);
            let val = serde_json::to_string(&info).unwrap();
            let _ = etcd.put(key, val, Some(opts)).await;
        }

        // 2. Fetch Assignments
        let mut expected_replicas = Vec::new();
        let assign_prefix = format!("/openback/assignments/{}/", hostname);
        let opts = GetOptions::new().with_prefix();
        if let Ok(resp) = etcd.get(assign_prefix, Some(opts)).await {
            for kv in resp.kvs() {
                let replica_name = kv
                    .key_str()
                    .unwrap()
                    .split('/')
                    .next_back()
                    .unwrap()
                    .to_string();
                if let Ok(app) = serde_json::from_slice::<KubeApplication>(kv.value()) {
                    expected_replicas.push((replica_name, app));
                }
            }
        }

        // 3. Fetch Actual running instances from Engine
        let mut actual_replicas = HashMap::new();
        if let Ok(EngineResponse::AppList(apps)) =
            send_engine_request(EngineRequest::Ps { all: true }).await
        {
            for app in apps {
                for inst in app.instances {
                    if inst.status != "Exited" && !inst.status.starts_with("Exited") {
                        let full_name = format!("{}-{}", app.app_name, inst.instance_id);
                        actual_replicas.insert(full_name, inst);
                    }
                }
            }
        }

        // 4. Reconcile expected vs actual
        for (replica_name, app) in expected_replicas.iter() {
            if !actual_replicas.contains_key(replica_name) {
                // Needs to be started!
                openback::dlog!(
                    "Backlet",
                    "INFO",
                    "Agent starting assigned replica: {}",
                    replica_name
                );
                let manifest = openback::manifest::AppManifest {
                    app_name: replica_name.clone(),
                    target_gd: app.spec.target_gd.clone(),
                    base_image: app.spec.base_image.clone(),
                    dependencies: app.spec.dependencies.clone(),
                    packages: app.spec.packages.clone(),
                    app_source: app.spec.app_source.clone(),
                    work_dir: app.spec.work_dir.clone(),
                    entrypoint: app.spec.entrypoint.clone(),
                    env: app.spec.env.clone().unwrap_or_default(),
                    networking: app.spec.networking.clone(),
                    permissions: app.spec.permissions.clone(),
                };
                let _ = send_engine_request(EngineRequest::Run(manifest)).await;
            }
        }

        for (replica_name, _inst) in actual_replicas.iter() {
            if !expected_replicas.iter().any(|(r, _)| r == replica_name) {
                // Running but not assigned! Kill it.
                openback::dlog!(
                    "Backlet",
                    "INFO",
                    "Agent stopping unassigned replica: {}",
                    replica_name
                );

                // Parse app_name and instance_id
                let (app_name, instance_id) = match replica_name.rfind('-') {
                    Some(pos) => (
                        replica_name[..pos].to_string(),
                        Some(replica_name[pos + 1..].to_string()),
                    ),
                    None => (replica_name.clone(), None),
                };

                let _ = send_engine_request(EngineRequest::Stop {
                    app_name: app_name.clone(),
                    instance_id: instance_id.clone(),
                })
                .await;

                if let Some(iid) = instance_id {
                    let _ = send_engine_request(EngineRequest::Rm {
                        app_name,
                        instance_id: iid,
                    })
                    .await;
                }
            }
        }
    }
}

async fn run_leader_election(mut etcd: Client) {
    loop {
        if let Ok(lease) = etcd.lease_grant(10, None).await {
            let opts = PutOptions::new().with_lease(lease.id());
            let hostname = sysinfo::System::host_name().unwrap_or_else(|| "unknown".to_string());

            // Wait, we need a transaction to only put if key doesn't exist
            let txn = etcd_client::Txn::new()
                .when(vec![etcd_client::Compare::create_revision(
                    "/openback/leader",
                    etcd_client::CompareOp::Equal,
                    0,
                )])
                .and_then(vec![etcd_client::TxnOp::put(
                    "/openback/leader",
                    hostname.clone(),
                    Some(opts),
                )]);

            if let Ok(resp) = etcd.txn(txn).await {
                if resp.succeeded() {
                    openback::dlog!(
                        "Backlet",
                        "INFO",
                        "Acquired leader lease! Running leader loop..."
                    );

                    let mut leader_etcd = etcd.clone();
                    // Leader loop
                    let (mut keeper, mut stream) = etcd.lease_keep_alive(lease.id()).await.unwrap();
                    loop {
                        let _ = keeper.keep_alive().await;
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

                        run_leader_reconciliation(&mut leader_etcd).await;

                        if let Ok(Some(_)) = stream.message().await {
                            // Still alive
                        } else {
                            openback::dlog!("Backlet", "WARN", "Lost leader lease keep-alive");
                            break;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

async fn run_leader_reconciliation(etcd: &mut Client) {
    // 1. Get Live Nodes
    let mut live_nodes = Vec::new();
    let opts = GetOptions::new().with_prefix();
    if let Ok(resp) = etcd.get("/openback/nodes/", Some(opts)).await {
        for kv in resp.kvs() {
            let k = kv.key_str().unwrap_or("");
            if k.ends_with("/status") {
                let parts: Vec<&str> = k.split('/').collect();
                if parts.len() == 5 {
                    live_nodes.push(parts[3].to_string());
                }
            }
        }
    }

    if live_nodes.is_empty() {
        return;
    }

    // 2. Get Desired Apps
    let mut apps = HashMap::new();
    let opts = GetOptions::new().with_prefix();
    if let Ok(resp) = etcd.get("/openback/applications/", Some(opts)).await {
        for kv in resp.kvs() {
            if let Ok(app) = serde_json::from_slice::<KubeApplication>(kv.value()) {
                apps.insert(app.metadata.name.clone(), app);
            }
        }
    }

    // 3. Get Actual Assignments
    let mut assignments_by_app: HashMap<String, Vec<(String, String)>> = HashMap::new(); // app_name -> [(node, replica_name)]
    let opts = GetOptions::new().with_prefix();
    if let Ok(resp) = etcd.get("/openback/assignments/", Some(opts)).await {
        for kv in resp.kvs() {
            let k = kv.key_str().unwrap_or("");
            let parts: Vec<&str> = k.split('/').collect();
            if parts.len() == 5 {
                let node = parts[3].to_string();
                let replica_name = parts[4].to_string();

                // If node is dead, delete assignment immediately
                if !live_nodes.contains(&node) {
                    openback::dlog!(
                        "Backlet",
                        "INFO",
                        "Node {} is dead, deleting assignment {}",
                        node,
                        replica_name
                    );
                    let _ = etcd.delete(k, None).await;
                    continue;
                }

                let app_name = match replica_name.rfind('-') {
                    Some(pos) => replica_name[..pos].to_string(),
                    None => replica_name.clone(),
                };

                assignments_by_app
                    .entry(app_name)
                    .or_default()
                    .push((node, replica_name));
            }
        }
    }

    // 4. Reconcile

    for (app_name, app) in &apps {
        let desired = app.spec.replicas.unwrap_or(1);
        let current_assignments = assignments_by_app
            .get(app_name)
            .cloned()
            .unwrap_or_default();
        let actual = current_assignments.len();

        if actual < desired {
            let to_add = desired - actual;
            openback::dlog!(
                "Backlet",
                "INFO",
                "Scaling up app {}: {} -> {}",
                app_name,
                actual,
                desired
            );

            for _ in 0..to_add {
                let target_node = live_nodes[rand::random::<usize>() % live_nodes.len()].clone();

                let hash = format!("{:08x}", rand::random::<u32>());
                let replica_name = format!("{}-{}", app_name, hash);
                let key = format!("/openback/assignments/{}/{}", target_node, replica_name);

                let val = serde_json::to_string(&app).unwrap();
                let _ = etcd.put(key, val, None).await;
            }
        } else if actual > desired {
            let to_remove = actual - desired;
            openback::dlog!(
                "Backlet",
                "INFO",
                "Scaling down app {}: {} -> {}",
                app_name,
                actual,
                desired
            );

            for i in 0..to_remove {
                let (node, replica_name) = &current_assignments[i];
                let key = format!("/openback/assignments/{}/{}", node, replica_name);
                let _ = etcd.delete(key, None).await;
            }
        }
    }

    // Clean up assignments for apps that no longer exist
    for (app_name, assignments) in assignments_by_app {
        if !apps.contains_key(&app_name) {
            openback::dlog!(
                "Backlet",
                "INFO",
                "App {} deleted, removing assignments",
                app_name
            );
            for (node, replica_name) in assignments {
                let key = format!("/openback/assignments/{}/{}", node, replica_name);
                let _ = etcd.delete(key, None).await;
            }
        }
    }
}
