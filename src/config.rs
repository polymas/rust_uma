use std::{env, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use thiserror::Error;
use url::Url;

pub const DEFAULT_ORACLE: &str = "0xCB1822859cEF82Cd2Eb4E6276C7916e692995130";

#[derive(Clone)]
pub struct Config {
    pub api_addr: SocketAddr,
    pub polygon_wss_url: String,
    pub polygon_rpc_url: String,
    pub contract_addresses: Vec<String>,
    pub contract_address_bytes: Vec<[u8; 20]>,
    pub data_dir: PathBuf,
    pub start_block: Option<u64>,
    pub backfill_batch_blocks: u64,
    pub live_buffer: usize,
    pub event_ring_capacity: usize,
    pub frame_ring_capacity: usize,
    pub batch_max_events: usize,
    pub batch_max_bytes: usize,
    pub zstd_threshold: usize,
    pub max_decompressed_bytes: usize,
    pub gamma_base_url: String,
    pub gamma_bootstrap: bool,
    pub gamma_refresh_interval: Duration,
    pub gamma_refresh_pages: usize,
    pub require_market_id: bool,
    pub ws_write_timeout: Duration,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("POLYGON_WSS_URL is required")]
    MissingWss,
    #[error("invalid {key}: {value}")]
    Invalid { key: &'static str, value: String },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let polygon_wss_url = required("POLYGON_WSS_URL")?;
        let polygon_rpc_url = match nonempty("POLYGON_RPC_URL") {
            Some(value) => value,
            None => derive_http_url(&polygon_wss_url)?,
        };
        let contract_addresses = nonempty("UMA_CONTRACT_ADDRESSES")
            .unwrap_or_else(|| DEFAULT_ORACLE.to_owned())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let contract_address_bytes = contract_addresses
            .iter()
            .map(|value| parse_fixed_hex::<20>("UMA_CONTRACT_ADDRESSES", value))
            .collect::<Result<Vec<_>, _>>()?;
        if contract_addresses.is_empty() {
            return Err(ConfigError::Invalid {
                key: "UMA_CONTRACT_ADDRESSES",
                value: String::new(),
            });
        }

        Ok(Self {
            api_addr: parse("API_ADDR", "127.0.0.1:8011")?,
            polygon_wss_url,
            polygon_rpc_url,
            contract_addresses,
            contract_address_bytes,
            data_dir: PathBuf::from(nonempty("DATA_DIR").unwrap_or_else(|| "./data".into())),
            start_block: optional_parse("START_BLOCK")?,
            backfill_batch_blocks: parse("RPC_BACKFILL_BATCH_BLOCKS", "2000")?,
            live_buffer: parse("RPC_LIVE_BUFFER", "8192")?,
            event_ring_capacity: parse("EVENT_RING_CAPACITY", "10000")?,
            frame_ring_capacity: parse("FRAME_RING_CAPACITY", "2048")?,
            batch_max_events: parse("BATCH_MAX_EVENTS", "64")?,
            batch_max_bytes: parse("BATCH_MAX_BYTES", "32768")?,
            zstd_threshold: parse("ZSTD_THRESHOLD", "4096")?,
            max_decompressed_bytes: parse("MAX_DECOMPRESSED_BYTES", "262144")?,
            gamma_base_url: nonempty("GAMMA_BASE_URL")
                .unwrap_or_else(|| "https://gamma-api.polymarket.com".into())
                .trim_end_matches('/')
                .to_owned(),
            gamma_bootstrap: parse_bool("GAMMA_BOOTSTRAP", true)?,
            gamma_refresh_interval: Duration::from_secs(parse(
                "GAMMA_REFRESH_INTERVAL_SECONDS",
                "60",
            )?),
            gamma_refresh_pages: parse("GAMMA_REFRESH_PAGES", "5")?,
            require_market_id: parse_bool("REQUIRE_MARKET_ID", true)?,
            ws_write_timeout: Duration::from_millis(parse("WS_WRITE_TIMEOUT_MS", "5000")?),
        })
    }
}

fn required(key: &'static str) -> Result<String, ConfigError> {
    nonempty(key).ok_or(ConfigError::MissingWss)
}

fn nonempty(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|v| !v.is_empty())
}

fn parse<T>(key: &'static str, default: &str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    let value = nonempty(key).unwrap_or_else(|| default.to_owned());
    value
        .parse()
        .map_err(|_| ConfigError::Invalid { key, value })
}

fn optional_parse<T>(key: &'static str) -> Result<Option<T>, ConfigError>
where
    T: FromStr,
{
    nonempty(key)
        .map(|value| {
            value
                .parse()
                .map_err(|_| ConfigError::Invalid { key, value })
        })
        .transpose()
}

fn parse_bool(key: &'static str, default: bool) -> Result<bool, ConfigError> {
    let Some(value) = nonempty(key) else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::Invalid { key, value }),
    }
}

fn derive_http_url(wss: &str) -> Result<String, ConfigError> {
    let mut url = Url::parse(wss).map_err(|_| ConfigError::Invalid {
        key: "POLYGON_WSS_URL",
        value: "<redacted>".into(),
    })?;
    let scheme = match url.scheme() {
        "wss" => "https",
        "ws" => "http",
        _ => {
            return Err(ConfigError::Invalid {
                key: "POLYGON_WSS_URL",
                value: "<redacted>".into(),
            });
        }
    };
    url.set_scheme(scheme).map_err(|_| ConfigError::Invalid {
        key: "POLYGON_WSS_URL",
        value: "<redacted>".into(),
    })?;
    Ok(url.to_string())
}

fn parse_fixed_hex<const N: usize>(key: &'static str, value: &str) -> Result<[u8; N], ConfigError> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    let decoded = hex::decode(raw).map_err(|_| ConfigError::Invalid {
        key,
        value: value.to_owned(),
    })?;
    decoded.try_into().map_err(|_| ConfigError::Invalid {
        key,
        value: value.to_owned(),
    })
}
