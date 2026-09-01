use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use reqwest::Client;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::{config::Config, model::MarketEnrichment, stats::Stats, storage::Storage};

pub struct Catalog {
    markets: RwLock<HashMap<u64, Arc<MarketEnrichment>>>,
}

impl Catalog {
    pub fn new(markets: Vec<MarketEnrichment>) -> Self {
        Self {
            markets: RwLock::new(
                markets
                    .into_iter()
                    .map(|market| (market.market_id, Arc::new(market)))
                    .collect(),
            ),
        }
    }

    pub fn get(&self, market_id: u64) -> Option<Arc<MarketEnrichment>> {
        self.markets
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&market_id)
            .cloned()
    }

    pub fn upsert(&self, market: MarketEnrichment) -> bool {
        let mut markets = self.markets.write().unwrap_or_else(|e| e.into_inner());
        if markets
            .get(&market.market_id)
            .is_some_and(|existing| existing.as_ref() == &market)
        {
            return false;
        }
        markets.insert(market.market_id, Arc::new(market));
        true
    }

    pub fn len(&self) -> usize {
        self.markets.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn snapshot(&self) -> Vec<MarketEnrichment> {
        self.markets
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|market| market.as_ref().clone())
            .collect()
    }
}

#[derive(Clone)]
pub struct RepairHandle {
    tx: mpsc::Sender<u64>,
    pending: Arc<Mutex<HashSet<u64>>>,
}

impl RepairHandle {
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<u64>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                tx,
                pending: Arc::new(Mutex::new(HashSet::new())),
            },
            rx,
        )
    }

    pub fn enqueue(&self, market_id: u64) {
        if market_id == 0 {
            return;
        }
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if !pending.insert(market_id) {
            return;
        }
        if self.tx.try_send(market_id).is_err() {
            pending.remove(&market_id);
        }
    }

    fn complete(&self, market_id: u64) {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&market_id);
    }
}

#[derive(Clone)]
pub struct PersistHandle(mpsc::Sender<()>);

impl PersistHandle {
    pub fn channel() -> (Self, mpsc::Receiver<()>) {
        let (tx, rx) = mpsc::channel(1);
        (Self(tx), rx)
    }

    pub fn request(&self) {
        let _ = self.0.try_send(());
    }
}

#[derive(Clone)]
pub struct GammaClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Error)]
pub enum EnrichmentError {
    #[error("Gamma request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Gamma returned HTTP {0}")]
    Status(reqwest::StatusCode),
    #[error("Gamma response exceeds 16 MiB")]
    Oversized,
    #[error("Gamma JSON decode failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid market identity: {0}")]
    Identity(&'static str),
}

impl GammaClient {
    pub fn new(base_url: String) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .user_agent("rust_uma/0.1")
                .build()?,
            base_url,
        })
    }

    async fn exact(&self, market_id: u64) -> Result<GammaMarket, EnrichmentError> {
        self.get_json(
            self.client
                .get(format!("{}/markets/{market_id}", self.base_url))
                .query(&[("include_tag", "true")]),
        )
        .await
    }

    async fn keyset(
        &self,
        cursor: Option<&str>,
        newest_first: bool,
    ) -> Result<GammaPage, EnrichmentError> {
        let mut request = self
            .client
            .get(format!("{}/markets/keyset", self.base_url))
            .query(&[
                ("limit", "100"),
                ("closed", "false"),
                ("include_tag", "true"),
            ]);
        if newest_first {
            request = request.query(&[("order", "updatedAt"), ("ascending", "false")]);
        }
        if let Some(cursor) = cursor {
            request = request.query(&[("after_cursor", cursor)]);
        }
        self.get_json(request).await
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, EnrichmentError> {
        let response = request.send().await?;
        if !response.status().is_success() {
            return Err(EnrichmentError::Status(response.status()));
        }
        let body = response.bytes().await?;
        if body.len() > 16 << 20 {
            return Err(EnrichmentError::Oversized);
        }
        Ok(serde_json::from_slice(&body)?)
    }
}

pub async fn run_repair_worker(
    gamma: GammaClient,
    catalog: Arc<Catalog>,
    repair: RepairHandle,
    mut rx: mpsc::Receiver<u64>,
    persist: PersistHandle,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            market_id = rx.recv() => {
                let Some(market_id) = market_id else { break; };
                match gamma.exact(market_id).await.and_then(compact_market) {
                    Ok(market) => {
                        if catalog.upsert(market) {
                            stats.catalog_markets.store(catalog.len() as u64, std::sync::atomic::Ordering::Relaxed);
                            persist.request();
                        }
                    }
                    Err(error) => warn!(market_id, %error, "Gamma exact enrichment repair failed"),
                }
                repair.complete(market_id);
            }
        }
    }
}

pub async fn run_catalog_sync(
    config: Arc<Config>,
    gamma: GammaClient,
    catalog: Arc<Catalog>,
    persist: PersistHandle,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    if config.gamma_bootstrap && catalog.is_empty() {
        if let Err(error) = full_bootstrap(&gamma, &catalog).await {
            warn!(%error, "Gamma catalog bootstrap failed; exact miss repair remains active");
        } else {
            stats
                .catalog_markets
                .store(catalog.len() as u64, std::sync::atomic::Ordering::Relaxed);
            persist.request();
            info!(
                markets = catalog.len(),
                "Gamma active catalog bootstrap complete"
            );
        }
    }

    let mut interval = tokio::time::interval(config.gamma_refresh_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            _ = interval.tick() => {
                match refresh_recent(&gamma, &catalog, config.gamma_refresh_pages).await {
                    Ok(changed) if changed > 0 => {
                        stats.catalog_markets.store(catalog.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        persist.request();
                        info!(changed, markets=catalog.len(), "Gamma recent catalog refresh complete");
                    }
                    Ok(_) => {}
                    Err(error) => warn!(%error, "Gamma recent catalog refresh failed"),
                }
            }
        }
    }
}

pub async fn run_catalog_persister(
    storage: Storage,
    catalog: Arc<Catalog>,
    mut rx: mpsc::Receiver<()>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            requested = rx.recv() => {
                if requested.is_none() { break; }
                tokio::time::sleep(Duration::from_secs(1)).await;
                while rx.try_recv().is_ok() {}
                let snapshot = catalog.snapshot();
                let storage = storage.clone();
                if let Err(error) = tokio::task::spawn_blocking(move || storage.save_catalog(&snapshot)).await {
                    warn!(%error, "catalog persistence task failed");
                }
            }
        }
    }
}

async fn full_bootstrap(gamma: &GammaClient, catalog: &Catalog) -> Result<(), EnrichmentError> {
    let mut cursor: Option<String> = None;
    loop {
        let page = gamma.keyset(cursor.as_deref(), false).await?;
        if page.markets.is_empty() {
            break;
        }
        for raw in page.markets {
            if let Ok(market) = compact_market(raw) {
                catalog.upsert(market);
            }
        }
        if page.next_cursor.is_empty()
            || page.next_cursor == "LTE="
            || cursor.as_deref() == Some(&page.next_cursor)
        {
            break;
        }
        cursor = Some(page.next_cursor);
    }
    Ok(())
}

async fn refresh_recent(
    gamma: &GammaClient,
    catalog: &Catalog,
    max_pages: usize,
) -> Result<usize, EnrichmentError> {
    let mut cursor: Option<String> = None;
    let mut changed = 0;
    for _ in 0..max_pages.max(1) {
        let page = gamma.keyset(cursor.as_deref(), true).await?;
        if page.markets.is_empty() {
            break;
        }
        for raw in page.markets {
            if let Ok(market) = compact_market(raw)
                && catalog.upsert(market)
            {
                changed += 1;
            }
        }
        if page.next_cursor.is_empty()
            || page.next_cursor == "LTE="
            || cursor.as_deref() == Some(&page.next_cursor)
        {
            break;
        }
        cursor = Some(page.next_cursor);
    }
    Ok(changed)
}

#[derive(Debug, Deserialize)]
struct GammaPage {
    #[serde(default)]
    markets: Vec<GammaMarket>,
    #[serde(default)]
    next_cursor: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    id: String,
    condition_id: String,
    #[serde(default, deserialize_with = "string_list")]
    clob_token_ids: Vec<String>,
    #[serde(default)]
    tags: Vec<GammaTag>,
    #[serde(default)]
    events: Vec<GammaEvent>,
}

#[derive(Debug, Deserialize)]
struct GammaTag {
    id: String,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    #[serde(default)]
    tags: Vec<GammaTag>,
}

fn string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        List(Vec<String>),
        Encoded(String),
    }
    match Option::<Value>::deserialize(deserializer)? {
        Some(Value::List(values)) => Ok(values),
        Some(Value::Encoded(value)) if value.trim().is_empty() => Ok(Vec::new()),
        Some(Value::Encoded(value)) => {
            serde_json::from_str(&value).map_err(serde::de::Error::custom)
        }
        None => Ok(Vec::new()),
    }
}

fn compact_market(raw: GammaMarket) -> Result<MarketEnrichment, EnrichmentError> {
    let market_id = raw
        .id
        .parse()
        .map_err(|_| EnrichmentError::Identity("market_id"))?;
    let condition_id = parse_hex_32(&raw.condition_id)?;
    let token_ids = raw
        .clob_token_ids
        .iter()
        .map(|value| parse_uint256(value))
        .collect::<Result<Vec<_>, _>>()?;
    let mut tag_ids = raw
        .tags
        .iter()
        .chain(raw.events.iter().flat_map(|event| event.tags.iter()))
        .filter_map(|tag| tag.id.parse::<u32>().ok())
        .collect::<Vec<_>>();
    tag_ids.sort_unstable();
    tag_ids.dedup();
    Ok(MarketEnrichment {
        market_id,
        condition_id,
        token_ids,
        tag_ids,
    })
}

fn parse_hex_32(value: &str) -> Result<[u8; 32], EnrichmentError> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    let decoded = hex::decode(raw).map_err(|_| EnrichmentError::Identity("condition_id"))?;
    decoded
        .try_into()
        .map_err(|_| EnrichmentError::Identity("condition_id"))
}

pub fn parse_uint256(value: &str) -> Result<[u8; 32], EnrichmentError> {
    if value.starts_with("0x") || value.starts_with("0X") {
        return parse_hex_32(value).map_err(|_| EnrichmentError::Identity("token_id"));
    }
    if value.is_empty() {
        return Err(EnrichmentError::Identity("token_id"));
    }
    let mut bytes = [0_u8; 32];
    for digit in value.bytes() {
        if !digit.is_ascii_digit() {
            return Err(EnrichmentError::Identity("token_id"));
        }
        let mut carry = (digit - b'0') as u16;
        for byte in bytes.iter_mut().rev() {
            let next = (*byte as u16) * 10 + carry;
            *byte = next as u8;
            carry = next >> 8;
        }
        if carry != 0 {
            return Err(EnrichmentError::Identity("token_id overflow"));
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint256_decimal_round_trip_for_small_value() {
        let value = parse_uint256("258").unwrap();
        assert_eq!(&value[30..], &[1, 2]);
    }

    #[test]
    fn compacts_and_deduplicates_market_and_event_tags() {
        let raw: GammaMarket = serde_json::from_str(&format!(
            r#"{{"id":"42","conditionId":"0x{}","clobTokenIds":"[\"1\",\"2\"]","tags":[{{"id":"1"}}],"events":[{{"tags":[{{"id":"1"}},{{"id":"64"}}]}}]}}"#,
            "11".repeat(32)
        ))
        .unwrap();
        let market = compact_market(raw).unwrap();
        assert_eq!(market.market_id, 42);
        assert_eq!(market.tag_ids, vec![1, 64]);
        assert_eq!(market.token_ids.len(), 2);
    }
}
