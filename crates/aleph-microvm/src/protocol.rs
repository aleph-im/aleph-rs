use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::config::{Encoding, Interface, LocalVmConfig};

/// Sent over `CONNECT 52` (length-prefixed) to configure the program.
/// Serialized as a msgpack map (named fields), matching init1.py's dataclass.
#[derive(Debug, Serialize)]
pub struct ConfigurationPayload {
    pub input_data: Option<ByteBuf>,
    pub interface: String,
    pub vm_hash: String,
    pub code: ByteBuf,
    pub encoding: String,
    pub entrypoint: String,
    pub ip: Option<String>,
    pub ipv6: Option<String>,
    pub route: Option<String>,
    pub ipv6_gateway: Option<String>,
    pub dns_servers: Vec<String>,
    pub volumes: Vec<ConfigVolume>,
    pub variables: Option<std::collections::BTreeMap<String, String>>,
    pub authorized_keys: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct ConfigVolume {
    pub mount: String,
    pub device: String,
    pub read_only: bool,
}

impl ConfigurationPayload {
    /// Build the payload. `code` is the zip bytes for zip encoding, or empty for squashfs
    /// (the code is then a mounted /opt/code volume).
    pub fn from_config(cfg: &LocalVmConfig, code: Vec<u8>) -> Self {
        ConfigurationPayload {
            input_data: None,
            interface: cfg.interface.as_str().to_string(),
            vm_hash: cfg.vm_hash.clone(),
            code: ByteBuf::from(code),
            encoding: cfg.encoding.as_str().to_string(),
            entrypoint: cfg.entrypoint.clone(),
            ip: None,
            ipv6: None,
            route: None,
            ipv6_gateway: None,
            dns_servers: vec![],
            volumes: cfg
                .volumes
                .iter()
                .map(|(_, v)| ConfigVolume {
                    mount: v.mount.clone(),
                    device: v.device.clone(),
                    read_only: v.read_only,
                })
                .collect(),
            variables: if cfg.variables.is_empty() {
                None
            } else {
                Some(cfg.variables.iter().cloned().collect())
            },
            authorized_keys: None,
        }
    }

    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec_named(self)
    }
}

// silence unused import in M1 (Encoding/Interface used via as_str on cfg)
const _: fn() = || {
    let _ = Encoding::Zip;
    let _ = Interface::Asgi;
};

#[derive(Debug, Deserialize)]
pub struct ConfigurationResponse {
    pub success: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub traceback: Option<String>,
}

/// ASGI scope sent inside a RunCodePayload.
#[derive(Debug, Serialize)]
pub struct AsgiScope {
    #[serde(rename = "type")]
    pub kind: String,
    pub method: String,
    pub path: String,
    pub query_string: ByteBuf,
    pub headers: Vec<(ByteBuf, ByteBuf)>,
    pub body: ByteBuf,
}

impl AsgiScope {
    pub fn http(
        method: &str,
        path: &str,
        query_string: Vec<u8>,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
        body: Vec<u8>,
    ) -> Self {
        AsgiScope {
            kind: "http".into(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: ByteBuf::from(query_string),
            headers: headers
                .into_iter()
                .map(|(k, v)| (ByteBuf::from(k), ByteBuf::from(v)))
                .collect(),
            body: ByteBuf::from(body),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RunCodePayload {
    pub scope: AsgiScope,
}

impl RunCodePayload {
    pub fn to_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec_named(self)
    }
}

/// Raw run reply: success and error variants share the wire as a map.
#[derive(Debug, Deserialize)]
pub struct RunResponse {
    #[serde(default)]
    headers: Option<RunHeaders>,
    #[serde(default)]
    body: Option<RunBody>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    traceback: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RunHeaders {
    status: u16,
    headers: Vec<(ByteBuf, ByteBuf)>,
}

#[derive(Debug, Deserialize)]
struct RunBody {
    body: ByteBuf,
}

pub struct RunSuccess {
    pub status: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

impl RunResponse {
    /// Convert to success, or Err(traceback/error) when the VM reported a failure.
    pub fn into_success(self) -> Result<RunSuccess, String> {
        if let Some(tb) = self.traceback.or(self.error) {
            return Err(tb);
        }
        let h = self.headers.ok_or_else(|| "missing headers in VM response".to_string())?;
        let b = self.body.ok_or_else(|| "missing body in VM response".to_string())?;
        Ok(RunSuccess {
            status: h.status,
            headers: h
                .headers
                .into_iter()
                .map(|(k, v)| (k.into_vec(), v.into_vec()))
                .collect(),
            body: b.body.into_vec(),
        })
    }
}
