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
    #[serde(default)]
    pub base_image: Option<String>,
    #[serde(default)]
    pub target_gd: Option<String>,
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub permissions: Option<Permissions>,
    #[serde(default)]
    pub networking: Option<Networking>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    pub entrypoint: Vec<String>,
}

impl AppManifest {
    pub fn get_base_image(&self) -> String {
        if let Some(base) = &self.base_image {
            return base.clone();
        }
        if let Some(gd) = &self.target_gd {
            return gd.clone();
        }
        "openback-gd-v1".to_string()
    }
}
