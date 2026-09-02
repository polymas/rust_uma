use std::{env, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use thiserror::Error;
use url::Url;

pub const DEFAULT_ORACLES: &str =
    "0xCB1822859cEF82Cd2Eb4E6276C7916e692995130,0xeE3Afe347D5C74317041E2618C49534dAf887c24";

#[derive(Clone)]
pub struct Config {
    pub api_addr: SocketAddr,
    pub polygon_wss_url: String,
    /// Polygon WSS endpoints raced against each other for live subscription.
    /// Always non-empty; degenerates to a single connection (`polygon_wss_url`)
    /// when only one endpoint is configured.
    pub wss_rpc_urls: Vec<String>,
    pub polygon_rpc_url: String,
    pub contract_addresses: Vec<String>,
    pub contract_address_bytes: Vec<[u8; 20]>,
    pub data_dir: PathBuf,
    pub start_block: Option<u64>,
    pub initial_backfill_days: u64,
    pub backfill_batch_blocks: u64,
    pub live_buffer: usize,
    pub event_ring_capacity: usize,
    pub frame_ring_capacity: usize,
    pub batch_max_events: usize,
    pub batch_max_bytes: usize,
    pub zstd_threshold: usize,
    pub max_decompressed_bytes: usize,
    pub gamma_base_url: String,
    pub gamma_refresh_interval: Duration,
    /// How many days back to also cache recently-*closed* Gamma markets, in
    /// addition to the always-cached active (closed=false) set. ProposePrice
    /// mostly fires right as a market closes, so an active-only cache misses
    /// most real events; but caching every closed market ever would grow
    /// unbounded for no benefit once past UMA's dispute window. This bounds
    /// intake to a rolling recent window instead of full history.
    pub closed_market_lookback_days: u64,
    /// How often to re-walk the entire Gamma active + recently-closed set
    /// from scratch (ignoring the incremental cursor) and merge any missing
    /// markets into the catalog. Exists because Gamma's keyset pagination has
    /// been observed to silently drop entries — most reliably reproduced with
    /// a batch of Neg Risk sibling markets sharing a near-identical
    /// `updatedAt` — and once the incremental cursor advances past a missed
    /// market's timestamp, the fast path can never pick it up again. Runs as
    /// an independent background task so it never delays the incremental
    /// sync or the hot path.
    pub catalog_reconcile_interval: Duration,
    pub require_market_id: bool,
    pub ws_write_timeout: Duration,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("WSS_RPC is required")]
    MissingWss,
    #[error("invalid {key}: {value}")]
    Invalid { key: &'static str, value: String },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let extra_wss_urls: Vec<String> = nonempty("WSS_RPC_LIST")
            .map(|raw| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let polygon_wss_url = match extra_wss_urls.first() {
            Some(first) => first.clone(),
            None => required_any("WSS_RPC", "POLYGON_WSS_URL")?,
        };
        // De-duplicate while preserving order, so a WSS_RPC_LIST entry that also
        // matches WSS_RPC/POLYGON_WSS_URL doesn't open a redundant connection.
        let mut wss_rpc_urls = Vec::with_capacity(extra_wss_urls.len().max(1));
        for url in extra_wss_urls
            .into_iter()
            .chain(nonempty("WSS_RPC").or_else(|| nonempty("POLYGON_WSS_URL")))
        {
            if !wss_rpc_urls.contains(&url) {
                wss_rpc_urls.push(url);
            }
        }
        if wss_rpc_urls.is_empty() {
            wss_rpc_urls.push(polygon_wss_url.clone());
        }
        let polygon_rpc_url = match nonempty("HTTP_RPC").or_else(|| nonempty("POLYGON_RPC_URL")) {
            Some(value) => value,
            None => derive_http_url(&polygon_wss_url)?,
        };
        let contract_addresses = nonempty("UMA_CONTRACT_ADDRESSES")
            .unwrap_or_else(|| DEFAULT_ORACLES.to_owned())
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
            wss_rpc_urls,
            polygon_rpc_url,
            contract_addresses,
            contract_address_bytes,
            data_dir: PathBuf::from(nonempty("DATA_DIR").unwrap_or_else(|| "./.cache".into())),
            start_block: optional_parse("START_BLOCK")?,
            initial_backfill_days: parse("INITIAL_BACKFILL_DAYS", "7")?,
            backfill_batch_blocks: parse("RPC_BACKFILL_BATCH_BLOCKS", "1000")?,
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
            gamma_refresh_interval: Duration::from_secs(parse(
                "GAMMA_REFRESH_INTERVAL_SECONDS",
                "60",
            )?),
            closed_market_lookback_days: parse("CLOSED_MARKET_LOOKBACK_DAYS", "3")?,
            catalog_reconcile_interval: Duration::from_secs(
                parse::<u64>("CATALOG_RECONCILE_INTERVAL_HOURS", "6")? * 3600,
            ),
            require_market_id: parse_bool("REQUIRE_MARKET_ID", true)?,
            ws_write_timeout: Duration::from_millis(parse("WS_WRITE_TIMEOUT_MS", "5000")?),
        })
    }
}

fn required_any(primary: &'static str, fallback: &'static str) -> Result<String, ConfigError> {
    nonempty(primary)
        .or_else(|| nonempty(fallback))
        .ok_or(ConfigError::MissingWss)
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
        key: "WSS_RPC",
        value: "<redacted>".into(),
    })?;
    let scheme = match url.scheme() {
        "wss" => "https",
        "ws" => "http",
        _ => {
            return Err(ConfigError::Invalid {
                key: "WSS_RPC",
                value: "<redacted>".into(),
            });
        }
    };
    url.set_scheme(scheme).map_err(|_| ConfigError::Invalid {
        key: "WSS_RPC",
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
