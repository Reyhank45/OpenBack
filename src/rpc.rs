use crate::manifest::AppManifest;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcRequest {
    Run(AppManifest),
    Ps,
    Stop(String),
    Logs(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub name: String,
    pub pid: u32,
    pub status: String,
    pub start_time: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
    Ok(String),
    Error(String),
    ProcessList(Vec<ProcessInfo>),
}
