use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::{config::Config, model::MarketEnrichment, stats::Stats, storage::Storage};

#[derive(Default)]
struct CatalogIndexes {
    by_condition: HashMap<[u8; 32], Arc<MarketEnrichment>>,
    market_to_condition: HashMap<u64, [u8; 32]>,
}

pub struct Catalog {
    indexes: RwLock<CatalogIndexes>,
}

impl Catalog {
    pub fn new(markets: Vec<MarketEnrichment>) -> Self {
        let catalog = Self {
            indexes: RwLock::new(CatalogIndexes::default()),
        };
        for market in markets {
            catalog.upsert(market);
        }
        catalog
    }

    pub fn get(&self, condition_id: &[u8; 32]) -> Option<Arc<MarketEnrichment>> {
        self.indexes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .by_condition
            .get(condition_id)
            .cloned()
    }

    pub fn get_by_market_id(&self, market_id: u64) -> Option<Arc<MarketEnrichment>> {
        let indexes = self.indexes.read().unwrap_or_else(|e| e.into_inner());
        indexes
            .market_to_condition
            .get(&market_id)
            .and_then(|condition_id| indexes.by_condition.get(condition_id))
            .cloned()
    }

    /// Resolves enrichment for a decoded UMA event, preferring `market_id`.
    ///
    /// Gamma's `condition_id` is authoritative for every Polymarket adapter
    /// (standard binary UmaCtfAdapter and Neg Risk alike), so a market_id hit
    /// is always correct and requires no on-chain call. `derived_condition_id`
    /// (the `keccak256(adapter, question_id, 2)` formula) only holds for
    /// standard binary adapters, so it is used purely as a fallback for the
    /// rare event whose ancillary data omits market_id.
    pub fn resolve(
        &self,
        market_id: Option<u64>,
        derived_condition_id: &[u8; 32],
    ) -> Option<Arc<MarketEnrichment>> {
        market_id
            .and_then(|id| self.get_by_market_id(id))
            .or_else(|| self.get(derived_condition_id))
    }

    pub fn upsert(&self, market: MarketEnrichment) -> bool {
        let mut indexes = self.indexes.write().unwrap_or_else(|e| e.into_inner());
        if indexes
            .by_condition
            .get(&market.condition_id)
            .is_some_and(|existing| existing.as_ref() == &market)
        {
            return false;
        }
        if let Some(previous) = indexes
            .market_to_condition
            .insert(market.market_id, market.condition_id)
            && previous != market.condition_id
        {
            indexes.by_condition.remove(&previous);
        }
        indexes
            .by_condition
            .insert(market.condition_id, Arc::new(market));
        true
    }

    pub fn len(&self) -> usize {
        self.indexes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .by_condition
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn snapshot(&self) -> Vec<MarketEnrichment> {
        self.indexes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .by_condition
            .values()
            .map(|market| market.as_ref().clone())
            .collect()
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
    #[error("catalog storage failed: {0}")]
    Storage(#[from] crate::storage::StorageError),
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

    async fn keyset(
        &self,
        cursor: Option<&str>,
        newest_first: bool,
        closed: bool,
    ) -> Result<GammaPage, EnrichmentError> {
        let mut request = self
            .client
            .get(format!("{}/markets/keyset", self.base_url))
            .query(&[
                ("limit", "100"),
                ("closed", if closed { "true" } else { "false" }),
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

    /// Fetches Polymarket's full tag taxonomy (id -> human label), e.g.
    /// `{"id":"1512","label":"caitlin clark",...}`. `limit=1000` comfortably
    /// covers the full set (~150 tags as of this writing) in one page — Gamma
    /// gives no pagination cursor on this endpoint to page through anyway.
    /// Called once at startup only (see `load_tag_labels`), never on the hot
    /// path.
    async fn tags(&self) -> Result<Vec<GammaTagFull>, EnrichmentError> {
        let request = self
            .client
            .get(format!("{}/tags", self.base_url))
            .query(&[("limit", "1000")]);
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct EnrichmentCursor {
    updated_at: String,
    market_id: u64,
}

impl EnrichmentCursor {
    fn is_older(&self, updated_at: &str) -> bool {
        updated_at < self.updated_at.as_str()
    }

    fn advance(&mut self, updated_at: &str, market_id: u64) {
        if (updated_at, market_id) > (self.updated_at.as_str(), self.market_id) {
            self.updated_at = updated_at.to_owned();
            self.market_id = market_id;
        }
    }
}

/// Loads/persists the two independent cursor streams (active markets, and
/// recently-closed markets) under one shared name so the call sites in
/// `sync_catalog_before_uma` / `run_catalog_sync` don't repeat themselves.
struct CursorPair {
    active: Option<EnrichmentCursor>,
    closed: Option<EnrichmentCursor>,
}

impl CursorPair {
    fn load(storage: &Storage) -> Result<Self, EnrichmentError> {
        Ok(Self {
            active: storage
                .load_enrichment_cursor()?
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            closed: storage
                .load_closed_market_cursor()?
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
        })
    }
}

/// One combined sync pass: the always-cached active (closed=false) set, plus
/// Gamma markets that closed within `closed_lookback_days` — bounded so the
/// catalog doesn't grow to hold every market that has ever closed, while
/// still covering the window ProposePrice/DisputePrice realistically lands
/// in (see the `closed_market_lookback_days` doc comment on Config).
async fn sync_both(
    gamma: &GammaClient,
    catalog: &Catalog,
    cursors: &CursorPair,
    closed_lookback_days: u64,
) -> Result<(usize, Option<EnrichmentCursor>, Option<EnrichmentCursor>), EnrichmentError> {
    let (changed_active, next_active) = sync_incremental(
        gamma,
        catalog,
        cursors.active.as_ref(),
        false,
        None,
        usize::MAX,
    )
    .await?;
    let boundary = lookback_boundary_date(closed_lookback_days);
    let (changed_closed, next_closed) = sync_incremental(
        gamma,
        catalog,
        cursors.closed.as_ref(),
        true,
        Some(&boundary),
        usize::MAX,
    )
    .await?;
    Ok((changed_active + changed_closed, next_active, next_closed))
}

pub async fn sync_catalog_before_uma(
    gamma: &GammaClient,
    catalog: &Catalog,
    storage: &Storage,
    closed_lookback_days: u64,
) -> Result<usize, EnrichmentError> {
    let cursors = CursorPair::load(storage)?;
    let (changed, next_active, next_closed) =
        sync_both(gamma, catalog, &cursors, closed_lookback_days).await?;

    // The catalog must become durable before either cursor advances.
    storage.save_catalog(&catalog.snapshot())?;
    if let Some(next) = next_active {
        storage.save_enrichment_cursor(&serde_json::to_string(&next)?)?;
    }
    if let Some(next) = next_closed {
        storage.save_closed_market_cursor(&serde_json::to_string(&next)?)?;
    }
    Ok(changed)
}

pub async fn run_catalog_sync(
    config: Arc<Config>,
    gamma: GammaClient,
    catalog: Arc<Catalog>,
    storage: Storage,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(config.gamma_refresh_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            _ = interval.tick() => {
                let cursors = match CursorPair::load(&storage) {
                    Ok(cursors) => cursors,
                    Err(error) => { warn!(%error, "loading Gamma cursors"); continue; }
                };
                match sync_both(&gamma, &catalog, &cursors, config.closed_market_lookback_days).await {
                    Ok((changed, next_active, next_closed))
                        if changed > 0 || next_active != cursors.active || next_closed != cursors.closed =>
                    {
                        let snapshot = catalog.snapshot();
                        let persisted = storage.save_catalog(&snapshot).and_then(|_| {
                            if let Some(value) = &next_active {
                                storage.save_enrichment_cursor(&serde_json::to_string(value).map_err(|_| {
                                    crate::storage::StorageError::Format("enrichment cursor")
                                })?)?;
                            }
                            if let Some(value) = &next_closed {
                                storage.save_closed_market_cursor(&serde_json::to_string(value).map_err(|_| {
                                    crate::storage::StorageError::Format("closed market cursor")
                                })?)?;
                            }
                            Ok(())
                        });
                        if let Err(error) = persisted {
                            warn!(%error, "Gamma incremental catalog persistence failed");
                            continue;
                        }
                        stats.catalog_markets.store(catalog.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        info!(changed, markets=catalog.len(), "Gamma incremental catalog sync complete");
                    }
                    Ok(_) => {}
                    Err(error) => warn!(%error, "Gamma recent catalog refresh failed"),
                }
            }
        }
    }
}

/// Independent background task: every `config.catalog_reconcile_interval`,
/// re-walk the entire active (closed=false) and recently-closed Gamma set
/// from scratch — ignoring the incremental cursors — and merge any missing
/// markets into the catalog.
///
/// This exists because Gamma's keyset pagination has been observed in
/// production to silently drop entries (most reliably reproduced with a
/// batch of Neg Risk sibling markets sharing a near-identical `updatedAt`,
/// which likely straddles a page boundary on a tied sort key). Once the
/// incremental cursor in `run_catalog_sync` advances past a missed market's
/// timestamp, that fast path can never recover it — only a from-scratch walk
/// can. Deliberately does not touch the persisted incremental cursors (it
/// discards the `next` cursor `sync_incremental` would have returned), so it
/// can never regress or race the fast path's own progress; it purely adds
/// whatever the fast path missed. Runs as its own task (not a branch in
/// `run_catalog_sync`'s select loop) so a multi-minute full walk never
/// delays the 60s-scale incremental refresh.
pub async fn run_catalog_reconcile(
    config: Arc<Config>,
    gamma: GammaClient,
    catalog: Arc<Catalog>,
    storage: Storage,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(config.catalog_reconcile_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // consume the immediate first tick; reconcile only after a full interval
    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            _ = interval.tick() => {
                match reconcile_full(&gamma, &catalog, config.closed_market_lookback_days).await {
                    Ok(changed) if changed > 0 => {
                        if let Err(error) = storage.save_catalog(&catalog.snapshot()) {
                            warn!(%error, "persist catalog after reconciliation");
                        }
                        stats.catalog_markets.store(catalog.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        stats.catalog_reconcile_gaps_closed.fetch_add(changed as u64, std::sync::atomic::Ordering::Relaxed);
                        info!(changed, markets = catalog.len(), "Gamma full catalog reconciliation closed a coverage gap");
                    }
                    Ok(_) => info!("Gamma full catalog reconciliation found no gap"),
                    Err(error) => warn!(%error, "Gamma full catalog reconciliation failed"),
                }
            }
        }
    }
}

async fn reconcile_full(
    gamma: &GammaClient,
    catalog: &Catalog,
    closed_lookback_days: u64,
) -> Result<usize, EnrichmentError> {
    let (changed_active, _) =
        sync_incremental(gamma, catalog, None, false, None, usize::MAX).await?;
    let boundary = lookback_boundary_date(closed_lookback_days);
    let (changed_closed, _) =
        sync_incremental(gamma, catalog, None, true, Some(&boundary), usize::MAX).await?;
    Ok(changed_active + changed_closed)
}

/// `YYYY-MM-DD` for `days_ago` days before now (UTC), used as a lexicographic
/// lower bound against Gamma's `updatedAt` (also UTC, ISO 8601) — no need to
/// pull in a date/time crate for a day-granularity cutoff.
fn lookback_boundary_date(days_ago: u64) -> String {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let boundary_days = now_secs / 86_400 - days_ago.min(now_secs / 86_400);
    let (y, m, d) = civil_from_days(boundary_days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Howard Hinnant's `civil_from_days`: days-since-1970-01-01 -> (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

async fn sync_incremental(
    gamma: &GammaClient,
    catalog: &Catalog,
    previous: Option<&EnrichmentCursor>,
    closed: bool,
    lookback_boundary: Option<&str>,
    max_pages: usize,
) -> Result<(usize, Option<EnrichmentCursor>), EnrichmentError> {
    let mut page_cursor: Option<String> = None;
    let mut changed = 0;
    let mut newest = previous.cloned();
    for _ in 0..max_pages.max(1) {
        let page = gamma.keyset(page_cursor.as_deref(), true, closed).await?;
        if page.markets.is_empty() {
            break;
        }
        let mut reached_previous = false;
        for raw in page.markets {
            let market_id = raw.id.parse::<u64>().ok();
            if previous.is_some_and(|cursor| cursor.is_older(&raw.updated_at)) {
                reached_previous = true;
                continue;
            }
            if lookback_boundary.is_some_and(|boundary| raw.updated_at.as_str() < boundary) {
                reached_previous = true;
                continue;
            }
            let updated_at = raw.updated_at.clone();
            let Ok(market) = compact_market(raw) else {
                continue;
            };
            if let Some(market_id) = market_id {
                match &mut newest {
                    Some(cursor) => cursor.advance(&updated_at, market_id),
                    None => {
                        newest = Some(EnrichmentCursor {
                            updated_at,
                            market_id,
                        })
                    }
                }
            }
            if catalog.upsert(market) {
                changed += 1;
            }
        }
        if reached_previous {
            break;
        }
        if page.next_cursor.is_empty()
            || page.next_cursor == "LTE="
            || page_cursor.as_deref() == Some(&page.next_cursor)
        {
            break;
        }
        page_cursor = Some(page.next_cursor);
    }
    Ok((changed, newest))
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
    #[serde(default)]
    updated_at: String,
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

/// One row of `GET /tags` — the id/label pair, ignoring the slug/timestamp
/// fields we have no use for.
#[derive(Debug, Deserialize)]
struct GammaTagFull {
    id: String,
    label: String,
}

/// Fetches the id -> label map once at startup for display purposes only
/// (dashboard event table). Never fatal: a failure here must not block
/// booting, so it logs and falls back to an empty map — UI then shows raw
/// numeric ids, exactly like before this existed.
pub async fn load_tag_labels(gamma: &GammaClient) -> HashMap<u32, String> {
    match gamma.tags().await {
        Ok(tags) => {
            let map: HashMap<u32, String> = tags
                .into_iter()
                .filter_map(|tag| tag.id.parse::<u32>().ok().map(|id| (id, tag.label)))
                .collect();
            info!(tags = map.len(), "Gamma tag labels loaded");
            map
        }
        Err(error) => {
            warn!(%error, "Gamma tag label fetch failed; dashboard will show raw tag ids");
            HashMap::new()
        }
    }
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
    use std::sync::{Arc, RwLock};

    use axum::{Json, Router, extract::State, routing::get};
    use serde_json::{Value, json};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn uint256_decimal_round_trip_for_small_value() {
        let value = parse_uint256("258").unwrap();
        assert_eq!(&value[30..], &[1, 2]);
    }

    #[test]
    fn civil_from_days_matches_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_698), (2026, 9, 2));
        assert_eq!(civil_from_days(20_695), (2026, 8, 30));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29)); // leap day
    }

    #[test]
    fn resolves_neg_risk_market_via_market_id_not_binary_formula() {
        // Same real market as uma::events::tests::
        // neg_risk_event_binary_formula_yields_wrong_condition_id: Gamma id
        // 907474, a Neg Risk market whose on-chain binary-adapter condition_id
        // formula gives the wrong answer. Gamma's real condition_id, fetched
        // from https://gamma-api.polymarket.com/markets/907474 in the same
        // session that added this test:
        let gamma_condition_id = crate::uma::events::decode_fixed::<32>(
            "0xa50547851bf565603ad7e866d9d2aa2c6c2ee77b2d390e581bf2e8a53b466902",
            "condition_id",
        )
        .unwrap();
        let catalog = Catalog::new(vec![MarketEnrichment {
            market_id: 907_474,
            condition_id: gamma_condition_id,
            token_ids: vec![[0x11; 32]],
            tag_ids: vec![7],
        }]);

        // Stand-in for whatever the (wrong, for this market) binary formula
        // would have produced — resolve() must not use it when market_id hits.
        let wrong_derived_condition_id = [0xAB; 32];

        let resolved = catalog
            .resolve(Some(907_474), &wrong_derived_condition_id)
            .expect("market_id lookup must hit");
        assert_eq!(resolved.condition_id, gamma_condition_id);

        // No market_id: falls back to the derived value, which is the only
        // thing available for standard binary markets missing market_id.
        assert!(catalog.resolve(None, &wrong_derived_condition_id).is_none());
        assert!(catalog.resolve(None, &gamma_condition_id).is_some());
    }

    #[tokio::test]
    async fn tag_labels_load_from_gamma_and_ignore_unparseable_ids() {
        async fn list_tags() -> Json<Value> {
            Json(json!([
                {"id": "1512", "label": "caitlin clark"},
                {"id": "100", "label": "politics"},
                {"id": "not-a-number", "label": "should be skipped"},
            ]))
        }
        let app = Router::new().route("/tags", get(list_tags));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let gamma = GammaClient::new(format!("http://{address}")).unwrap();
        let labels = load_tag_labels(&gamma).await;
        assert_eq!(labels.get(&1512).map(String::as_str), Some("caitlin clark"));
        assert_eq!(labels.get(&100).map(String::as_str), Some("politics"));
        assert_eq!(labels.len(), 2);
        server.abort();
    }

    #[tokio::test]
    async fn tag_labels_degrade_to_empty_map_on_fetch_failure() {
        // No server listening at all — connection refused, not a panic.
        let gamma = GammaClient::new("http://127.0.0.1:1".into()).unwrap();
        assert!(load_tag_labels(&gamma).await.is_empty());
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

    #[tokio::test]
    async fn startup_sync_persists_cursor_then_only_applies_increment() {
        type Markets = Arc<RwLock<Vec<Value>>>;

        async fn list_markets(State(markets): State<Markets>) -> Json<Value> {
            Json(json!({
                "markets": markets.read().unwrap().clone(),
                "next_cursor": ""
            }))
        }

        fn market(id: u64, updated_at: &str) -> Value {
            json!({
                "id": id.to_string(),
                "updatedAt": updated_at,
                "conditionId": format!("0x{}", format!("{id:02x}").repeat(32)),
                "clobTokenIds": [id.to_string()],
                "tags": [{"id": "2"}],
                "events": [{"tags": [{"id": "1"}, {"id": "2"}]}]
            })
        }

        let markets = Arc::new(RwLock::new(vec![
            market(2, "2026-09-01T00:00:02Z"),
            market(1, "2026-09-01T00:00:01Z"),
        ]));
        let app = Router::new()
            .route("/markets/keyset", get(list_markets))
            .with_state(markets.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let catalog = Catalog::new(Vec::new());
        let gamma = GammaClient::new(format!("http://{address}")).unwrap();

        assert_eq!(
            sync_catalog_before_uma(&gamma, &catalog, &storage, 7)
                .await
                .unwrap(),
            2
        );
        assert_eq!(catalog.len(), 2);
        let first: EnrichmentCursor = serde_json::from_str(
            storage
                .load_enrichment_cursor()
                .unwrap()
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first.market_id, 2);

        *markets.write().unwrap() = vec![
            market(3, "2026-09-01T00:00:03Z"),
            market(2, "2026-09-01T00:00:02Z"),
            market(1, "2026-09-01T00:00:01Z"),
        ];
        assert_eq!(
            sync_catalog_before_uma(&gamma, &catalog, &storage, 7)
                .await
                .unwrap(),
            1
        );
        assert_eq!(catalog.len(), 3);
        let second: EnrichmentCursor = serde_json::from_str(
            storage
                .load_enrichment_cursor()
                .unwrap()
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second.market_id, 3);
        server.abort();
    }

    /// Regression test for the miss-rate bug: ProposePrice/DisputePrice fire
    /// right as a market closes, so an active-only (closed=false) catalog
    /// systematically misses almost every real event. This proves the
    /// closed-market sync (a) picks up a market that just closed, within the
    /// lookback window, and (b) still excludes one that closed long before
    /// it — the catalog isn't supposed to hold every market that has ever
    /// closed, only a bounded recent window.
    #[tokio::test]
    async fn recently_closed_markets_are_cached_but_old_ones_are_not() {
        use axum::extract::Query;
        use std::collections::HashMap;

        #[derive(Clone, Default)]
        struct MarketsByStatus {
            active: Arc<RwLock<Vec<Value>>>,
            closed: Arc<RwLock<Vec<Value>>>,
        }

        async fn list_markets(
            State(markets): State<MarketsByStatus>,
            Query(query): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            let source = if query.get("closed").map(String::as_str) == Some("true") {
                &markets.closed
            } else {
                &markets.active
            };
            Json(json!({ "markets": source.read().unwrap().clone(), "next_cursor": "" }))
        }

        fn market(id: u64, updated_at: &str) -> Value {
            json!({
                "id": id.to_string(),
                "updatedAt": updated_at,
                "conditionId": format!("0x{}", format!("{id:02x}").repeat(32)),
                "clobTokenIds": [id.to_string()],
                "tags": [],
                "events": []
            })
        }

        let now = lookback_boundary_date(0); // today, YYYY-MM-DD
        let today_ts = format!("{now}T12:00:00Z");
        let state = MarketsByStatus {
            active: Arc::new(RwLock::new(vec![market(10, &today_ts)])),
            closed: Arc::new(RwLock::new(vec![
                market(20, &today_ts),              // just closed: must be cached
                market(30, "2020-01-01T00:00:00Z"), // closed ages ago: must NOT be cached
            ])),
        };
        let app = Router::new()
            .route("/markets/keyset", get(list_markets))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let catalog = Catalog::new(Vec::new());
        let gamma = GammaClient::new(format!("http://{address}")).unwrap();

        sync_catalog_before_uma(&gamma, &catalog, &storage, 3)
            .await
            .unwrap();

        assert!(
            catalog.get_by_market_id(10).is_some(),
            "active market must be cached"
        );
        assert!(
            catalog.get_by_market_id(20).is_some(),
            "market closed today must be cached — this is the ProposePrice miss-rate fix"
        );
        assert!(
            catalog.get_by_market_id(30).is_none(),
            "market closed in 2020 must stay outside the bounded lookback window"
        );
        server.abort();
    }

    /// Regression test for the second, distinct real-world gap: Gamma's
    /// keyset pagination can silently drop a market (production evidence:
    /// several batches of Neg Risk siblings, all created within the same
    /// second, were entirely absent from the catalog days later). Once the
    /// incremental cursor has advanced past a missed market's `updatedAt`,
    /// `sync_incremental`'s own `is_older` check makes that market
    /// permanently unreachable via the fast path — this proves
    /// `reconcile_full` (which ignores the cursor) recovers it anyway, and
    /// that doing so never disturbs the persisted incremental cursor.
    #[tokio::test]
    async fn full_reconciliation_recovers_a_market_the_incremental_cursor_has_passed_by() {
        fn market(id: u64, updated_at: &str) -> Value {
            json!({
                "id": id.to_string(),
                "updatedAt": updated_at,
                "conditionId": format!("0x{}", format!("{id:02x}").repeat(32)),
                "clobTokenIds": [id.to_string()],
                "tags": [],
                "events": []
            })
        }

        async fn list_markets(State(markets): State<Arc<RwLock<Vec<Value>>>>) -> Json<Value> {
            Json(json!({ "markets": markets.read().unwrap().clone(), "next_cursor": "" }))
        }

        // A single market that Gamma's pagination "lost": present in the
        // active listing, but the sync will run with a cursor already past it.
        let markets = Arc::new(RwLock::new(vec![market(10, "2026-01-01T00:00:00Z")]));
        let app = Router::new()
            .route("/markets/keyset", get(list_markets))
            .with_state(markets.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        let catalog = Catalog::new(Vec::new());
        let gamma = GammaClient::new(format!("http://{address}")).unwrap();

        // Simulate "the fast path already moved past this point in time" —
        // exactly what happens after a pagination gap once real, newer
        // markets keep advancing the cursor.
        let stale_cursor = EnrichmentCursor {
            updated_at: "2026-06-01T00:00:00Z".into(),
            market_id: 999,
        };
        storage
            .save_enrichment_cursor(&serde_json::to_string(&stale_cursor).unwrap())
            .unwrap();

        let cursors = CursorPair::load(&storage).unwrap();
        sync_both(&gamma, &catalog, &cursors, 3).await.unwrap();
        assert!(
            catalog.get_by_market_id(10).is_none(),
            "sanity check: the incremental path must indeed miss it, matching the real bug"
        );

        let changed = reconcile_full(&gamma, &catalog, 3).await.unwrap();
        assert_eq!(changed, 1);
        assert!(
            catalog.get_by_market_id(10).is_some(),
            "reconciliation must recover the market the incremental cursor skipped"
        );
        assert_eq!(
            storage.load_enrichment_cursor().unwrap().unwrap(),
            serde_json::to_string(&stale_cursor).unwrap(),
            "reconciliation must never touch the persisted incremental cursor"
        );

        // Idempotent: reconciling again with the market already present
        // finds nothing new to merge.
        assert_eq!(reconcile_full(&gamma, &catalog, 3).await.unwrap(), 0);
        server.abort();
    }
}
