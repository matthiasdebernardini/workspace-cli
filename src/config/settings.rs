use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub api: ApiConfig,
    /// Custom config directory override. When set, all paths resolve relative
    /// to this directory instead of the default `~/.config/workspace-cli/`.
    #[serde(skip)]
    pub config_dir_override: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Path to OAuth2 client credentials JSON
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// Path to service account key JSON (for headless mode)
    #[serde(default)]
    pub service_account_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Default output format: json, jsonl, csv
    #[serde(default = "default_format")]
    pub format: String,
    /// Whether to use compact JSON (no pretty printing)
    #[serde(default)]
    pub compact: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    /// Maximum retries on failure
    #[serde(default = "default_retries")]
    pub max_retries: u32,
}

fn default_format() -> String {
    "json".to_string()
}

fn default_timeout() -> u64 {
    30
}

fn default_retries() -> u32 {
    3
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auth: AuthConfig::default(),
            output: OutputConfig::default(),
            api: ApiConfig::default(),
            config_dir_override: None,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            credentials_path: None,
            service_account_path: None,
        }
    }
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            format: default_format(),
            compact: false,
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            timeout_seconds: default_timeout(),
            max_retries: default_retries(),
        }
    }
}

impl Config {
    /// Load config from file, falling back to defaults
    pub fn load() -> Self {
        Self::default_config_path()
            .and_then(|path| std::fs::read_to_string(path).ok())
            .and_then(|content| toml::from_str(&content).ok())
            .unwrap_or_default()
    }

    /// Load config from a custom directory.
    pub fn load_from_dir(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let path = dir.join("config.toml");
        let mut config = std::fs::read_to_string(path)
            .ok()
            .and_then(|content| toml::from_str::<Self>(&content).ok())
            .unwrap_or_default();
        config.config_dir_override = Some(dir);
        config
    }

    /// Set a custom config directory, returning `self` for chaining.
    pub fn with_config_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config_dir_override = Some(dir.into());
        self
    }

    /// Get the config file path (instance method, respects override).
    pub fn config_path(&self) -> Option<PathBuf> {
        self.config_dir().map(|d| d.join("config.toml"))
    }

    /// Get the config directory path (instance method, respects override).
    pub fn config_dir(&self) -> Option<PathBuf> {
        if let Some(ref dir) = self.config_dir_override {
            Some(dir.clone())
        } else {
            Self::default_config_dir()
        }
    }

    /// Default config directory (static, no override).
    pub fn default_config_dir() -> Option<PathBuf> {
        dirs::config_dir().map(|p| p.join("workspace-cli"))
    }

    /// Default config file path (static, no override).
    pub fn default_config_path() -> Option<PathBuf> {
        Self::default_config_dir().map(|d| d.join("config.toml"))
    }

    /// Save config to file
    pub fn save(&self) -> std::io::Result<()> {
        if let Some(dir) = self.config_dir() {
            std::fs::create_dir_all(&dir)?;
            let path = dir.join("config.toml");
            let content = toml::to_string_pretty(self)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            std::fs::write(path, content)?;
        }
        Ok(())
    }

    /// Override with environment variables
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(path) = std::env::var("WORKSPACE_CREDENTIALS_PATH") {
            self.auth.credentials_path = Some(PathBuf::from(path));
        }
        if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            self.auth.service_account_path = Some(PathBuf::from(path));
        }
        if let Ok(format) = std::env::var("WORKSPACE_OUTPUT_FORMAT") {
            self.output.format = format;
        }
        if let Ok(compact) = std::env::var("WORKSPACE_OUTPUT_COMPACT") {
            self.output.compact = compact.eq_ignore_ascii_case("true") || compact == "1";
        }
        if let Ok(timeout) = std::env::var("WORKSPACE_API_TIMEOUT") {
            if let Ok(seconds) = timeout.parse::<u64>() {
                self.api.timeout_seconds = seconds;
            }
        }
        if let Ok(retries) = std::env::var("WORKSPACE_API_MAX_RETRIES") {
            if let Ok(max) = retries.parse::<u32>() {
                self.api.max_retries = max;
            }
        }
        self
    }
}
