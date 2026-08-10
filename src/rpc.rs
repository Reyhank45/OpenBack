use crate::manifest::AppManifest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EngineEnvelope {
    pub auth_token: Option<String>,
    pub request: EngineRequest,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum EngineRequest {
    Run(AppManifest),
    Ps {
        all: bool,
    },
    Start {
        app_name: String,
        instance_id: String,
    },
    Stop {
        app_name: String,
        instance_id: Option<String>,
    },
    Rm {
        app_name: String,
        instance_id: String,
    },
    DepsList,
    DepsInspect(String),
    DepsPrune,
    DepsRemove {
        name: String,
        force: bool,
    },
    BaseList,
    BaseInspect(String),
    BasePrune,
    Logs {
        app_name: String,
        instance_id: Option<String>,
        tail: Option<usize>,
    },
    /// Attach to a running replica — stream log output and forward stdin.
    Attach {
        app_name: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterEnvelope {
    pub auth_token: Option<String>,
    pub request: ClusterRequest,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClusterRequest {
    Apply(KubeApplication),
    DeleteDeployment(String),
    GetDeployment(String),
    Scale {
        app_name: String,
        replicas: usize,
    },
    Describe(String),
    GetNodes,
    RegisterNode {
        role: String,
        hostname: String,
        port: Option<u16>,
        cpu_usage: f32,
        ram_usage: f32,
    },
    Heartbeat {
        hostname: String,
        cpu_usage: f32,
        ram_usage: f32,
    },
    SyncState(KubeApplication),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct KubeApplication {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Spec {
    #[serde(default)]
    pub base_image: Option<String>,
    #[serde(default)]
    pub target_gd: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub packages: Option<crate::manifest::PackageDirectives>,
    #[serde(default)]
    pub app_source: Option<String>,
    #[serde(default)]
    pub work_dir: Option<String>,
    #[serde(default)]
    pub replicas: Option<usize>,
    #[serde(default)]
    pub entrypoint: Vec<String>,
    #[serde(default)]
    pub env: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub networking: Option<crate::manifest::Networking>,
    #[serde(default)]
    pub permissions: Option<crate::manifest::Permissions>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeInfo {
    pub hostname: String,
    pub role: String,
    pub cpu_usage: f32,
    pub ram_usage: f32,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppDescription {
    pub deployment: KubeApplication,
    pub replicas: Vec<InstanceInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BaseManifest {
    pub os: String,
    pub libc: String,
    pub architecture: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BaseInfo {
    pub name: String,
    pub size_bytes: u64,
    pub consumers: Vec<String>,
    pub created_time: Option<String>,
    pub manifest: Option<BaseManifest>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstanceInfo {
    pub instance_id: String,
    pub pid: u32,
    pub status: String,
    pub start_time: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppInfo {
    pub app_name: String,
    pub instances: Vec<InstanceInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DepInfo {
    pub name: String,
    pub version: String,
    pub size_bytes: u64,
    pub consumers: Vec<String>,
    pub created_time: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EngineResponse {
    Ok(String),
    Error(String),
    AppList(Vec<AppInfo>),
    DepsList(Vec<DepInfo>),
    DepDetails(DepInfo),
    PruneResult(Vec<String>),
    BaseList(Vec<BaseInfo>),
    BaseDetails(BaseInfo),
    LogLines(Vec<String>),
    /// Sentinel: daemon is switching to raw attach streaming mode.
    AttachStream,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterResponse {
    Ok(String),
    Error(String),
    DeploymentDetails(KubeApplication),
    DescribeDetails(AppDescription),
    NodeList(Vec<NodeInfo>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_request_serialization() {
        let req = EngineRequest::Ps { all: true };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, "{\"Ps\":{\"all\":true}}");

        let req = EngineRequest::Stop {
            app_name: "app1".to_string(),
            instance_id: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(
            json,
            "{\"Stop\":{\"app_name\":\"app1\",\"instance_id\":null}}"
        );
    }

    #[test]
    fn test_kube_application_deserialization() {
        let json = r#"{
            "apiVersion": "v1",
            "kind": "Application",
            "metadata": { "name": "test-app" },
            "spec": {
                "baseImage": "alpine",
                "replicas": 3
            }
        }"#;
        let app: KubeApplication = serde_json::from_str(json).unwrap();
        assert_eq!(app.api_version, "v1");
        assert_eq!(app.kind, "Application");
        assert_eq!(app.metadata.name, "test-app");
        assert_eq!(app.spec.base_image, Some("alpine".to_string()));
        assert_eq!(app.spec.replicas, Some(3));
    }
}
