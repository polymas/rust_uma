//! 电竞弃权排除名单（forfeit blocklist）。
//!
//! 数据源是 forfeit-feed（`tools/forfeit-feed`，协议 `proto/polyuma/forfeit/v1`）：
//! PandaScore 判定某场比赛任意一局弃权后，它推一条 `ForfeitEvent`，带这场比赛在
//! Polymarket 上对应事件的**全部**子市场 condition_id。命中名单的 UMA
//! propose/dispute 不广播给下游（同富化 miss 的处理方式：照样进去重环和 WAL），
//! 避免下游在一场结果可能因弃权争议被反转的比赛上下单。
//!
//! 名单怎么维护：
//! - **唯一来源是 forfeit-feed 的事件日志**（只增不改，带 sequence）。本地名单是它
//!   的派生物：每次连上都带 `backfill` 把整段历史重放一遍，按 condition_id 取并集，
//!   重复推送是幂等的；forfeit-feed 事后补全名单（Polymarket 晚开盘、首次查询失败）
//!   会用同一个 sequence 再推，这里同样只做并集。
//! - **落盘**（`DATA_DIR/forfeit_blocklist.json`）：重启后在连上 forfeit-feed 之前
//!   就生效，forfeit-feed 挂掉也不影响已有名单。
//! - **过期**：condition_id 不会被复用，留着无害；只是没必要无限增长——一场比赛的
//!   UMA propose/dispute 都在几天内发生，`FORFEIT_BLOCK_TTL_DAYS`（默认 30）天后
//!   整场移出名单。
//! - **失败方向是放行**：forfeit-feed 不可用时不拦任何新比赛，热路径不会因为它等待
//!   或报错；面板上能看到订阅是否在线。
//!
//! 热路径（`pipeline::Processor::process`）只做一次读锁 + `HashMap` 查询；订阅、
//! 解码、落盘全部在后台任务里。

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::{
    config::Config,
    model::{EventKind, EventRecord, hex_prefixed},
    wire::now_us,
};

// prost 生成的 oneof：Hello 很小、ForfeitEvent 很大；一个连接一秒不到一帧，不值得为此装箱。
#[allow(clippy::large_enum_variant)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/polyuma.forfeit.v1.rs"));
}

const SNAPSHOT_FILE: &str = "forfeit_blocklist.json";
const SNAPSHOT_VERSION: u32 = 1;
/// 面板上保留的最近被拦事件条数（也随名单一起落盘）。
const RECENT_BLOCKED: usize = 200;
/// forfeit-feed 每 25s 发 ping；超过这个时长一帧都没收到就当连接已死、重连。
const FEED_READ_TIMEOUT: Duration = Duration::from_secs(90);
const FEED_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PRUNE_INTERVAL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedMarket {
    pub condition_id: String,
    pub sports_market_type: String,
    /// 0 = 整场盘。
    pub game_number: u32,
    pub question: String,
    pub is_forfeited_market: bool,
}

/// 一场被判弃权的比赛（PandaScore match_id 维度），名单里的 condition_id 都挂在它下面。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedMatch {
    pub pandascore_match_id: u64,
    pub game: String,
    pub league: String,
    pub match_name: String,
    pub team_a: String,
    pub team_b: String,
    pub winner: String,
    /// 被判弃权的局号，0 = 整场。
    pub forfeited_games: Vec<u32>,
    pub pm_event_slug: String,
    /// forfeit-feed 最近一次给出的关联结果：matched / not_found / failed。
    pub lookup_status: String,
    pub markets: Vec<BlockedMarket>,
    pub detect_at_us: u64,
    /// rust-uma 第一次得知这场比赛的时刻；过期按它算。
    pub added_at_us: u64,
    pub updated_at_us: u64,
    pub feed_sequences: Vec<u64>,
    /// 因这场比赛被拦下、没有广播的 UMA 事件数。
    pub blocked_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedEvent {
    pub sequence: u64,
    pub kind: String,
    pub transaction_hash: String,
    pub condition_id: String,
    pub market_id: u64,
    pub pandascore_match_id: u64,
    pub match_name: String,
    pub blocked_at_us: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    last_feed_sequence: u64,
    blocked_events_total: u64,
    matches: Vec<BlockedMatch>,
    recent_blocked: Vec<BlockedEvent>,
}

#[derive(Default)]
struct State {
    matches: BTreeMap<u64, BlockedMatch>,
    recent_blocked: VecDeque<BlockedEvent>,
    last_feed_sequence: u64,
    blocked_events_total: u64,
}

#[derive(Clone, Copy)]
struct Entry {
    match_id: u64,
    added_at_us: u64,
}

/// 订阅 forfeit-feed 的连接状态，只给面板/健康检查看。
#[derive(Default)]
pub struct FeedStatus {
    pub connected: AtomicBool,
    pub connects: AtomicU64,
    pub frames: AtomicU64,
    pub last_frame_at_us: AtomicU64,
    pub server_last_sequence: AtomicU64,
    last_error: Mutex<Option<(u64, String)>>,
}

pub struct ForfeitBlocklist {
    enabled: bool,
    path: Option<PathBuf>,
    ttl_us: u64,
    /// 热路径唯一读的结构：condition_id -> 所属比赛。写入只发生在名单变化时（一天几次）。
    index: RwLock<HashMap<[u8; 32], Entry>>,
    state: Mutex<State>,
    dirty: Notify,
    pub feed: FeedStatus,
}

impl ForfeitBlocklist {
    /// 未配置 `FORFEIT_FEED_URL`：空名单，什么都不拦。
    pub fn disabled() -> Self {
        Self::empty(false, None, 0)
    }

    fn empty(enabled: bool, path: Option<PathBuf>, ttl_us: u64) -> Self {
        Self {
            enabled,
            path,
            ttl_us,
            index: RwLock::new(HashMap::new()),
            state: Mutex::new(State::default()),
            dirty: Notify::new(),
            feed: FeedStatus::default(),
        }
    }

    /// 按配置构造：配了 `FORFEIT_FEED_URL` 就从 `DATA_DIR` 读回上次的名单（读不了
    /// 就从空名单开始、等 forfeit-feed 重放——名单本来就是派生数据，不能因为一个坏
    /// 文件让服务起不来）。
    pub fn from_config(config: &Config) -> Self {
        if config.forfeit_feed_url.is_none() {
            return Self::disabled();
        }
        let ttl_us = config.forfeit_block_ttl.as_micros() as u64;
        let list = Self::empty(true, Some(config.data_dir.join(SNAPSHOT_FILE)), ttl_us);
        list.load();
        list
    }

    #[cfg(test)]
    pub(crate) fn enabled_for_test() -> Self {
        Self::empty(true, None, 0)
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// 热路径：condition_id 在名单里就返回所属比赛的 PandaScore match_id。
    pub fn lookup(&self, condition_id: &[u8; 32]) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        self.index
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(condition_id)
            .map(|entry| entry.match_id)
    }

    /// 重启时判断 WAL 里的一条事件当初是否被拦下（用来算 `FrameHub` 的续传下限）：
    /// 在名单里、且名单项早于事件到达。
    pub fn blocked_before(&self, condition_id: &[u8; 32], at_us: u64) -> bool {
        self.index
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(condition_id)
            .is_some_and(|entry| entry.added_at_us <= at_us)
    }

    /// 记一条被拦下的 UMA 事件（热路径调用：只动内存，落盘交给后台任务）。
    pub fn record_blocked(&self, record: &EventRecord, match_id: u64) {
        let now = now_us();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.blocked_events_total += 1;
        let match_name = match state.matches.get_mut(&match_id) {
            Some(blocked) => {
                blocked.blocked_events += 1;
                format!("{} vs {}", blocked.team_a, blocked.team_b)
            }
            None => String::new(),
        };
        state.recent_blocked.push_front(BlockedEvent {
            sequence: record.sequence,
            kind: match record.event.kind() {
                EventKind::Propose => "propose".into(),
                EventKind::Dispute => "dispute".into(),
            },
            transaction_hash: hex_prefixed(&record.event.chain().transaction_hash),
            condition_id: hex_prefixed(&record.resolved_condition_id()),
            market_id: record.enrichment.as_ref().map_or(0, |e| e.market_id),
            pandascore_match_id: match_id,
            match_name,
            blocked_at_us: now,
        });
        state.recent_blocked.truncate(RECENT_BLOCKED);
        drop(state);
        self.dirty.notify_one();
    }

    /// 合并 forfeit-feed 的一条事件；名单有变化返回 true。
    pub fn apply(&self, event: &pb::ForfeitEvent) -> bool {
        let now = now_us();
        let match_id = event.pandascore_match_id;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.last_feed_sequence = state.last_feed_sequence.max(event.sequence);
        let status = match pb::LookupStatus::try_from(event.lookup_status) {
            Ok(pb::LookupStatus::Matched) => "matched",
            Ok(pb::LookupStatus::NotFound) => "not_found",
            Ok(pb::LookupStatus::Failed) => "failed",
            _ => "unspecified",
        };
        let is_new = !state.matches.contains_key(&match_id);
        let blocked = state
            .matches
            .entry(match_id)
            .or_insert_with(|| BlockedMatch {
                pandascore_match_id: match_id,
                game: event.game.clone(),
                league: event.league.clone(),
                match_name: event.match_name.clone(),
                team_a: event.team_a.clone(),
                team_b: event.team_b.clone(),
                winner: event.winner.clone(),
                forfeited_games: Vec::new(),
                pm_event_slug: String::new(),
                lookup_status: status.into(),
                markets: Vec::new(),
                detect_at_us: event.detect_at_us,
                added_at_us: now,
                updated_at_us: now,
                feed_sequences: Vec::new(),
                blocked_events: 0,
            });
        let mut changed = is_new;
        if !blocked
            .forfeited_games
            .contains(&event.forfeited_game_number)
        {
            blocked.forfeited_games.push(event.forfeited_game_number);
            blocked.forfeited_games.sort_unstable();
            changed = true;
        }
        if !blocked.feed_sequences.contains(&event.sequence) {
            blocked.feed_sequences.push(event.sequence);
            blocked.feed_sequences.sort_unstable();
            changed = true;
        }
        // 关联状态只往"更好"的方向改：已经 matched 的比赛，不会因为一次查询失败的
        // 重放被降级（名单本身只增不减，状态文字跟着名单走）。
        if blocked.lookup_status != "matched" && blocked.lookup_status != status {
            blocked.lookup_status = status.into();
            changed = true;
        }
        if !event.pm_event_slug.is_empty() && blocked.pm_event_slug != event.pm_event_slug {
            blocked.pm_event_slug = event.pm_event_slug.clone();
            changed = true;
        }
        let known: BTreeSet<String> = blocked
            .markets
            .iter()
            .map(|m| m.condition_id.clone())
            .collect();
        let mut new_ids = Vec::new();
        for market in &event.markets {
            let Ok(id) = <[u8; 32]>::try_from(market.condition_id.as_slice()) else {
                continue;
            };
            let hex = hex_prefixed(&id);
            if known.contains(&hex) {
                continue;
            }
            blocked.markets.push(BlockedMarket {
                condition_id: hex,
                sports_market_type: market.sports_market_type.clone(),
                game_number: market.game_number,
                question: market.question.clone(),
                is_forfeited_market: market.is_forfeited_market,
            });
            new_ids.push(id);
        }
        // `condition_ids` 是协议里的权威名单；`markets` 只是明细。万一两者不一致
        // （比如以后只发 condition_ids），以 condition_ids 为准补齐。
        for raw in &event.condition_ids {
            let Ok(id) = <[u8; 32]>::try_from(raw.as_slice()) else {
                continue;
            };
            let hex = hex_prefixed(&id);
            if known.contains(&hex) || new_ids.contains(&id) {
                continue;
            }
            blocked.markets.push(BlockedMarket {
                condition_id: hex,
                sports_market_type: String::new(),
                game_number: 0,
                question: String::new(),
                is_forfeited_market: false,
            });
            new_ids.push(id);
        }
        if !new_ids.is_empty() {
            blocked.markets.sort_by(|a, b| {
                (a.game_number, &a.sports_market_type).cmp(&(b.game_number, &b.sports_market_type))
            });
            changed = true;
        }
        if changed {
            blocked.updated_at_us = now;
        }
        let added_at_us = blocked.added_at_us;
        drop(state);
        if !new_ids.is_empty() {
            let mut index = self
                .index
                .write()
                .unwrap_or_else(|error| error.into_inner());
            for id in new_ids {
                index.entry(id).or_insert(Entry {
                    match_id,
                    added_at_us,
                });
            }
        }
        if changed {
            self.dirty.notify_one();
        }
        changed
    }

    /// 移除超过 TTL 的比赛。
    fn prune(&self, now: u64) -> usize {
        if self.ttl_us == 0 {
            return 0;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let before = state.matches.len();
        state
            .matches
            .retain(|_, m| m.added_at_us.saturating_add(self.ttl_us) > now);
        let removed = before - state.matches.len();
        if removed > 0 {
            let index = build_index(&state.matches);
            drop(state);
            *self
                .index
                .write()
                .unwrap_or_else(|error| error.into_inner()) = index;
            self.dirty.notify_one();
        }
        removed
    }

    fn load(&self) {
        let Some(path) = &self.path else { return };
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => {
                warn!(%error, "forfeit blocklist unreadable; starting empty");
                return;
            }
        };
        let snapshot: Snapshot = match serde_json::from_slice(&bytes) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(%error, "forfeit blocklist malformed; starting empty");
                return;
            }
        };
        if snapshot.version != SNAPSHOT_VERSION {
            warn!(
                version = snapshot.version,
                "forfeit blocklist version unknown; starting empty"
            );
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.last_feed_sequence = snapshot.last_feed_sequence;
        state.blocked_events_total = snapshot.blocked_events_total;
        state.recent_blocked = snapshot.recent_blocked.into();
        state.matches = snapshot
            .matches
            .into_iter()
            .map(|m| (m.pandascore_match_id, m))
            .collect();
        let index = build_index(&state.matches);
        info!(
            matches = state.matches.len(),
            condition_ids = index.len(),
            "forfeit blocklist loaded"
        );
        drop(state);
        *self
            .index
            .write()
            .unwrap_or_else(|error| error.into_inner()) = index;
        self.prune(now_us());
    }

    fn snapshot_bytes(&self) -> Option<Vec<u8>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            last_feed_sequence: state.last_feed_sequence,
            blocked_events_total: state.blocked_events_total,
            matches: state.matches.values().cloned().collect(),
            recent_blocked: state.recent_blocked.iter().cloned().collect(),
        };
        drop(state);
        serde_json::to_vec(&snapshot).ok()
    }

    /// 面板用的只读视图。
    pub fn view(&self) -> ForfeitView {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut matches: Vec<BlockedMatch> = state.matches.values().cloned().collect();
        matches.sort_by_key(|m| std::cmp::Reverse(m.detect_at_us));
        let last_error = self
            .feed
            .last_error
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        ForfeitView {
            enabled: self.enabled,
            feed_connected: self.feed.connected.load(Ordering::Relaxed),
            feed_connects_total: self.feed.connects.load(Ordering::Relaxed),
            feed_frames_total: self.feed.frames.load(Ordering::Relaxed),
            feed_last_frame_at_us: self.feed.last_frame_at_us.load(Ordering::Relaxed),
            feed_server_last_sequence: self.feed.server_last_sequence.load(Ordering::Relaxed),
            feed_last_error: last_error.as_ref().map(|(_, e)| e.clone()),
            feed_last_error_at_us: last_error.map(|(at, _)| at).unwrap_or_default(),
            last_feed_sequence: state.last_feed_sequence,
            blocked_condition_ids: self
                .index
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .len(),
            blocked_events_total: state.blocked_events_total,
            ttl_days: self.ttl_us / 86_400_000_000,
            matches,
            recent_blocked: state.recent_blocked.iter().cloned().collect(),
        }
    }

    pub fn blocked_condition_ids(&self) -> usize {
        self.index
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    pub fn blocked_events_total(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .blocked_events_total
    }

    fn set_error(&self, error: String) {
        *self
            .feed
            .last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some((now_us(), error));
    }
}

fn build_index(matches: &BTreeMap<u64, BlockedMatch>) -> HashMap<[u8; 32], Entry> {
    let mut index = HashMap::new();
    for m in matches.values() {
        for market in &m.markets {
            let raw = market.condition_id.trim_start_matches("0x");
            let Ok(bytes) = hex::decode(raw) else {
                continue;
            };
            let Ok(id) = <[u8; 32]>::try_from(bytes.as_slice()) else {
                continue;
            };
            index.entry(id).or_insert(Entry {
                match_id: m.pandascore_match_id,
                added_at_us: m.added_at_us,
            });
        }
    }
    index
}

#[derive(Serialize)]
pub struct ForfeitView {
    pub enabled: bool,
    pub feed_connected: bool,
    pub feed_connects_total: u64,
    pub feed_frames_total: u64,
    pub feed_last_frame_at_us: u64,
    pub feed_server_last_sequence: u64,
    pub feed_last_error: Option<String>,
    pub feed_last_error_at_us: u64,
    pub last_feed_sequence: u64,
    pub blocked_condition_ids: usize,
    pub blocked_events_total: u64,
    pub ttl_days: u64,
    pub matches: Vec<BlockedMatch>,
    pub recent_blocked: Vec<BlockedEvent>,
}

/// 后台任务：订阅 forfeit-feed、维护名单、落盘、定期过期清理。
pub async fn run_forfeit_feed(
    config: Arc<Config>,
    list: Arc<ForfeitBlocklist>,
    shutdown: watch::Receiver<bool>,
) {
    let Some(base) = config.forfeit_feed_url.clone() else {
        return;
    };
    tokio::spawn(run_saver(list.clone(), shutdown.clone()));
    let separator = if base.contains('?') { '&' } else { '?' };
    let mut url = format!("{base}{separator}backfill={}", config.forfeit_feed_backfill);
    if let Some(token) = &config.forfeit_feed_token {
        url.push_str("&token=");
        url.push_str(token);
    }
    let mut backoff = Duration::from_secs(1);
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let started = tokio::time::Instant::now();
        match feed_session(&url, &list, &mut shutdown).await {
            Ok(()) => return, // shutdown
            Err(error) => {
                list.feed.connected.store(false, Ordering::Relaxed);
                // 不打印 url：里面带 token。
                warn!(%error, "forfeit feed disconnected");
                list.set_error(error);
            }
        }
        if started.elapsed() > Duration::from_secs(30) {
            backoff = Duration::from_secs(1);
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn feed_session(
    url: &str,
    list: &ForfeitBlocklist,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    let (mut socket, _) = tokio::time::timeout(FEED_CONNECT_TIMEOUT, connect_async(url))
        .await
        .map_err(|_| "connect timeout".to_owned())?
        .map_err(|error| format!("connect: {error}"))?;
    list.feed.connected.store(true, Ordering::Relaxed);
    list.feed.connects.fetch_add(1, Ordering::Relaxed);
    info!("forfeit feed connected");
    let mut changed_in_session = 0_u64;
    loop {
        let message = tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { return Ok(()) } else { continue },
            message = tokio::time::timeout(FEED_READ_TIMEOUT, socket.next()) => message,
        };
        let message = match message {
            Err(_) => return Err("read timeout".into()),
            Ok(None) => return Err("closed by server".into()),
            Ok(Some(Err(error))) => return Err(format!("read: {error}")),
            Ok(Some(Ok(message))) => message,
        };
        list.feed
            .last_frame_at_us
            .store(now_us(), Ordering::Relaxed);
        match message {
            Message::Binary(bytes) => {
                list.feed.frames.fetch_add(1, Ordering::Relaxed);
                let frame = match pb::ForfeitFrame::decode(bytes.as_ref()) {
                    Ok(frame) => frame,
                    Err(error) => {
                        warn!(%error, "undecodable forfeit frame");
                        continue;
                    }
                };
                match frame.body {
                    Some(pb::forfeit_frame::Body::Hello(hello)) => {
                        list.feed
                            .server_last_sequence
                            .store(hello.last_sequence, Ordering::Relaxed);
                    }
                    Some(pb::forfeit_frame::Body::Event(event)) => {
                        list.feed
                            .server_last_sequence
                            .fetch_max(event.sequence, Ordering::Relaxed);
                        if list.apply(&event) {
                            changed_in_session += 1;
                            if !event.backfill {
                                info!(
                                    sequence = event.sequence,
                                    match_name = %event.match_name,
                                    condition_ids = event.condition_ids.len(),
                                    "forfeit blocklist updated"
                                );
                            }
                        }
                    }
                    None => {}
                }
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| format!("pong: {error}"))?;
            }
            Message::Close(_) => return Err("server sent close".into()),
            _ => {}
        }
        // 重放完成后打一条汇总（之后每次变化仍有上面的逐条日志）。
        if changed_in_session > 0
            && list.feed.server_last_sequence.load(Ordering::Relaxed)
                <= list.state.lock().map(|s| s.last_feed_sequence).unwrap_or(0)
        {
            info!(
                changed = changed_in_session,
                condition_ids = list.blocked_condition_ids(),
                "forfeit blocklist in sync with feed"
            );
            changed_in_session = 0;
        }
    }
}

/// 名单有变化就落盘（合并 1s 内的连续变化），每小时做一次过期清理。
async fn run_saver(list: Arc<ForfeitBlocklist>, mut shutdown: watch::Receiver<bool>) {
    let mut prune = tokio::time::interval(PRUNE_INTERVAL);
    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break; },
            _ = prune.tick() => {
                let removed = list.prune(now_us());
                if removed > 0 {
                    info!(removed, "forfeit blocklist: expired matches pruned");
                }
                continue;
            }
            _ = list.dirty.notified() => {}
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        save(&list).await;
    }
    save(&list).await;
}

async fn save(list: &Arc<ForfeitBlocklist>) {
    let (Some(path), Some(bytes)) = (list.path.clone(), list.snapshot_bytes()) else {
        return;
    };
    let result =
        tokio::task::spawn_blocking(move || crate::storage::atomic_write(&path, &bytes)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "forfeit blocklist save failed"),
        Err(error) => warn!(%error, "forfeit blocklist save task failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    fn event(sequence: u64, match_id: u64, game: u32, ids: &[u8]) -> pb::ForfeitEvent {
        pb::ForfeitEvent {
            sequence,
            pandascore_match_id: match_id,
            forfeited_game_number: game,
            team_a: "A".into(),
            team_b: "B".into(),
            lookup_status: if ids.is_empty() {
                pb::LookupStatus::NotFound as i32
            } else {
                pb::LookupStatus::Matched as i32
            },
            condition_ids: ids.iter().map(|b| cid(*b)).collect(),
            markets: ids
                .iter()
                .map(|b| pb::PmMarket {
                    condition_id: cid(*b),
                    sports_market_type: "child_moneyline".into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn enabled(ttl_us: u64) -> ForfeitBlocklist {
        ForfeitBlocklist::empty(true, None, ttl_us)
    }

    #[test]
    fn replay_is_idempotent_and_unions_condition_ids() {
        let list = enabled(0);
        assert!(list.apply(&event(1, 7, 1, &[1, 2])));
        // 同一 sequence 重放：不算变化
        assert!(!list.apply(&event(1, 7, 1, &[1, 2])));
        // 同一 sequence 补推了更全的名单：并集
        assert!(list.apply(&event(1, 7, 1, &[1, 2, 3])));
        // 同场比赛另一局也弃权
        assert!(list.apply(&event(2, 7, 2, &[2, 3])));
        assert_eq!(list.blocked_condition_ids(), 3);
        assert_eq!(list.lookup(&[3; 32]), Some(7));
        assert_eq!(list.lookup(&[9; 32]), None);
        let view = list.view();
        assert_eq!(view.matches[0].forfeited_games, vec![1, 2]);
        assert_eq!(view.matches[0].feed_sequences, vec![1, 2]);
    }

    #[test]
    fn not_found_then_matched_upgrades_status_and_never_downgrades() {
        let list = enabled(0);
        list.apply(&event(1, 7, 1, &[]));
        assert_eq!(list.blocked_condition_ids(), 0);
        list.apply(&event(1, 7, 1, &[4]));
        assert_eq!(list.view().matches[0].lookup_status, "matched");
        // 重放时 Polymarket 查询失败：状态和名单都不退化
        let mut failed = event(1, 7, 1, &[]);
        failed.lookup_status = pb::LookupStatus::Failed as i32;
        list.apply(&failed);
        assert_eq!(list.view().matches[0].lookup_status, "matched");
        assert_eq!(list.lookup(&[4; 32]), Some(7));
    }

    #[test]
    fn disabled_list_never_blocks() {
        let list = ForfeitBlocklist::disabled();
        list.apply(&event(1, 7, 1, &[1]));
        assert_eq!(list.lookup(&[1; 32]), None);
    }

    #[test]
    fn expired_matches_are_pruned_from_the_hot_index() {
        let list = enabled(1_000);
        list.apply(&event(1, 7, 1, &[1]));
        let added = list.view().matches[0].added_at_us;
        assert_eq!(list.prune(added + 10), 0);
        assert_eq!(list.prune(added + 2_000), 1);
        assert_eq!(list.lookup(&[1; 32]), None);
    }

    #[test]
    fn snapshot_round_trips_and_rebuilds_the_index() {
        let dir = std::env::temp_dir().join(format!("forfeit-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(SNAPSHOT_FILE);
        let list = ForfeitBlocklist::empty(true, Some(path.clone()), 0);
        list.apply(&event(5, 7, 0, &[1, 2]));
        std::fs::write(&path, list.snapshot_bytes().unwrap()).unwrap();

        let reloaded = ForfeitBlocklist::empty(true, Some(path), 0);
        reloaded.load();
        assert_eq!(reloaded.lookup(&[2; 32]), Some(7));
        assert_eq!(reloaded.view().last_feed_sequence, 5);
        std::fs::remove_dir_all(dir).ok();
    }

    /// 真实 WebSocket 往返：本地起一个假 forfeit-feed，发 hello + 一条事件（二进制
    /// protobuf 帧），确认订阅任务解码、合并进名单，并在连接断开后自动重连。
    #[tokio::test]
    async fn feed_task_decodes_frames_and_reconnects() {
        use tokio_tungstenite::accept_async;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicU64::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                counter.fetch_add(1, Ordering::Relaxed);
                let mut ws = accept_async(stream).await.unwrap();
                let hello = pb::ForfeitFrame {
                    body: Some(pb::forfeit_frame::Body::Hello(pb::Hello {
                        last_sequence: 3,
                        ..Default::default()
                    })),
                };
                ws.send(Message::Binary(hello.encode_to_vec().into()))
                    .await
                    .unwrap();
                let frame = pb::ForfeitFrame {
                    body: Some(pb::forfeit_frame::Body::Event(event(3, 9, 2, &[5, 6]))),
                };
                ws.send(Message::Binary(frame.encode_to_vec().into()))
                    .await
                    .unwrap();
                ws.close(None).await.ok();
            }
        });
        let mut config = crate::config::test_config();
        config.forfeit_feed_url = Some(format!("ws://{addr}/ws"));
        config.forfeit_feed_backfill = 10;
        let list = Arc::new(enabled(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_forfeit_feed(
            Arc::new(config),
            list.clone(),
            shutdown_rx,
        ));
        for _ in 0..100 {
            if accepted.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(list.lookup(&[6; 32]), Some(9));
        assert_eq!(list.feed.server_last_sequence.load(Ordering::Relaxed), 3);
        assert!(accepted.load(Ordering::Relaxed) >= 2, "should reconnect");
        shutdown_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }

    #[test]
    fn blocked_before_respects_when_the_match_was_learned() {
        let list = enabled(0);
        list.apply(&event(1, 7, 1, &[1]));
        let added = list.view().matches[0].added_at_us;
        assert!(list.blocked_before(&[1; 32], added + 1));
        assert!(!list.blocked_before(&[1; 32], added - 1));
    }
}
