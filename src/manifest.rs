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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_manifest_deserialization() {
        let data = json!({
            "app_name": "test-app",
            "base_image": "test-base",
            "dependencies": ["lib1", "lib2"],
            "entrypoint": ["/bin/sh"]
        });

        let manifest: AppManifest = serde_json::from_value(data).unwrap();
        assert_eq!(manifest.app_name, "test-app");
        assert_eq!(manifest.base_image, Some("test-base".to_string()));
        assert_eq!(manifest.dependencies, vec!["lib1", "lib2"]);
        assert_eq!(manifest.entrypoint, vec!["/bin/sh"]);
        assert!(manifest.permissions.is_none());
        assert!(manifest.networking.is_none());
    }

    #[test]
    fn test_get_base_image() {
        let manifest_with_base = AppManifest {
            app_name: "test".to_string(),
            base_image: Some("custom-base".to_string()),
            target_gd: Some("ignored-gd".to_string()),
            dependencies: vec![],
            permissions: None,
            networking: None,
            env: HashMap::new(),
            entrypoint: vec![],
        };
        assert_eq!(manifest_with_base.get_base_image(), "custom-base");

        let manifest_with_gd = AppManifest {
            app_name: "test".to_string(),
            base_image: None,
            target_gd: Some("gd-image".to_string()),
            dependencies: vec![],
            permissions: None,
            networking: None,
            env: HashMap::new(),
            entrypoint: vec![],
        };
        assert_eq!(manifest_with_gd.get_base_image(), "gd-image");

        let manifest_default = AppManifest {
            app_name: "test".to_string(),
            base_image: None,
            target_gd: None,
            dependencies: vec![],
            permissions: None,
            networking: None,
            env: HashMap::new(),
            entrypoint: vec![],
        };
        assert_eq!(manifest_default.get_base_image(), "openback-gd-v1");
    }
}
