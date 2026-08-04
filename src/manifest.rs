use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PortMapping {
    pub host_port: u16,
    pub container_socket: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Networking {
    pub ipc_socket: String,
    pub ports: Vec<PortMapping>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Permissions {
    #[serde(default)]
    pub devices: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppManifest {
    pub app_name: String,
    pub target_gd: String,
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub permissions: Option<Permissions>,
    #[serde(default)]
    pub networking: Option<Networking>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub entrypoint: Vec<String>,
}
