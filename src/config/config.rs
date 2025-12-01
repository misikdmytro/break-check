use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub default_policy: PolicyDefinition,
    pub policies: Vec<PolicyRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub address: String,
    pub redis_url: String,
}

#[derive(Debug, Copy, Deserialize, Clone)]
pub struct PolicyDefinition {
    pub max_tokens: u32,
    pub window_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyRule {
    pub pattern: String,

    #[serde(rename = "type")]
    pub pattern_type: PatternType,

    #[serde(flatten)]
    pub policy: PolicyDefinition,

    #[serde(default = "default_priority")]
    pub priority: u32,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum PatternType {
    Exact,
    Prefix,
}

fn default_priority() -> u32 {
    0
}

#[derive(Debug, Error)]
pub enum ConfigErr {
    #[error("IO error: {0}")]
    Io(std::io::Error),

    #[error("Parse error: {0}")]
    Parse(toml::de::Error),
}

pub fn load_config(path: impl Into<PathBuf>) -> Result<Config, ConfigErr> {
    let content = std::fs::read_to_string(path.into()).map_err(ConfigErr::Io)?;
    let config: Config = toml::from_str(&content).map_err(ConfigErr::Parse)?;
    Ok(config)
}
