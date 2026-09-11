//! 告警：定时对 tinyuma 状态、edge 心跳、面板转发做规则检查。条件要持续到
//! 阈值才触发，消失一段时间才算恢复；同一轮新触发、到期提醒、已恢复的合并
//! 成一条消息推到飞书群机器人。没配 webhook 时只写日志。
//!
//! 数据全部来自 console 已经在拉的东西（upstream 快照、心跳注册表、feed 状态），
//! 不额外请求 tinyuma/edge，也不在任何热路径上。

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    path::Path,
    sync::{Mutex, MutexGuard, atomic::Ordering},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use super::{
    SharedState, now_ms,
    registry::{NodeView, Status},
    store::load_json,
    upstream::UpstreamSnapshot,
};

const MINUTE_MS: u64 = 60_000;
const HOUR_MS: u64 = 60 * MINUTE_MS;
/// 计数器样本最多留这么久（最长窗口 1h + 余量）。
const SERIES_KEEP_MS: u64 = HOUR_MS + 5 * MINUTE_MS;
/// 推送失败后至少隔这么久再试，别对着一个坏掉的 webhook 每轮都打。
const RETRY_AFTER_FAILURE_MS: u64 = 60_000;
/// 恢复消息积压上限（webhook 长期失败时防止无限增长）。
const MAX_PENDING_RESOLVED: usize = 200;

/// 阈值。缺的字段用默认值，放在 `<CONSOLE_DATA_DIR>/alerts.json` 里改完重启。
/// 默认值按 2026-09-11 生产基线定：RPC 一天断 4 次、解码错误 ~1/h、未命中
/// ~1/h、正常 edge 每小时新接入 ~230 个连接。
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertRules {
    pub upstream_unreachable_s: u64,
    pub rpc_disconnected_s: u64,
    pub rpc_sources_degraded_s: u64,
    pub rpc_reconnects_per_30m: u64,
    pub upstream_silent_min: u64,
    pub enrichment_recent_min_pct: f64,
    pub enrichment_misses_per_1h: u64,
    pub decode_errors_per_1h: u64,
    pub slow_clients_per_10m: u64,
    pub cursor_rejected_per_10m: u64,
    pub storage_dropped_per_10m: u64,
    pub catalog_min_markets: u64,
    pub min_serving_nodes: u64,
    pub edge_stale_s: u64,
    pub edge_upstream_down_s: u64,
    pub edge_sequence_lag: u64,
    pub edge_sequence_lag_s: u64,
    pub edge_link_lag_ms: u64,
    pub edge_link_lag_s: u64,
    pub edge_accepted_per_1h: u64,
    pub edge_slow_clients_per_10m: u64,
    pub edge_bad_frames_per_10m: u64,
    pub edge_full_pct: u64,
    pub edge_full_s: u64,
    pub panel_feed_down_s: u64,
    /// 条件消失多久才算恢复，避免抖动时来回推。
    pub resolve_after_s: u64,
    /// 仍未恢复的告警隔多久再提醒一次。
    pub repeat_after_min: u64,
}

impl Default for AlertRules {
    fn default() -> Self {
        Self {
            upstream_unreachable_s: 60,
            rpc_disconnected_s: 30,
            rpc_sources_degraded_s: 300,
            rpc_reconnects_per_30m: 6,
            upstream_silent_min: 20,
            enrichment_recent_min_pct: 95.0,
            enrichment_misses_per_1h: 30,
            decode_errors_per_1h: 30,
            slow_clients_per_10m: 3,
            cursor_rejected_per_10m: 10,
            storage_dropped_per_10m: 1,
            catalog_min_markets: 200_000,
            min_serving_nodes: 1,
            edge_stale_s: 30,
            edge_upstream_down_s: 60,
            edge_sequence_lag: 20,
            edge_sequence_lag_s: 60,
            edge_link_lag_ms: 1000,
            edge_link_lag_s: 60,
            edge_accepted_per_1h: 800,
            edge_slow_clients_per_10m: 20,
            edge_bad_frames_per_10m: 1,
            edge_full_pct: 95,
            edge_full_s: 300,
            panel_feed_down_s: 300,
            resolve_after_s: 60,
            repeat_after_min: 60,
        }
    }
}

/// 坏文件不拦启动：告警阈值不值得让控制台起不来，退回默认值并打日志。
pub fn load_rules(path: &Path) -> AlertRules {
    match load_json::<AlertRules>(path) {
        Ok(Some(rules)) => rules,
        Ok(None) => AlertRules::default(),
        Err(error) => {
            warn!(%error, path = %path.display(), "alerts.json unreadable; using default thresholds");
            AlertRules::default()
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Critical,
    Warning,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Severity::Critical => "🔴 严重",
            Severity::Warning => "🟠 警告",
        }
    }
}

/// 一次检查里命中的一条规则（`subject` 是节点 ID，全局规则为空）。
struct Finding {
    rule: &'static str,
    title: &'static str,
    severity: Severity,
    subject: String,
    detail: String,
    hold_ms: u64,
}

/// 单调计数器的样本，用来算"窗口内增长了多少"。
#[derive(Default)]
struct Series(VecDeque<(u64, u64)>);

impl Series {
    fn push(&mut self, now: u64, value: u64) {
        self.0.push_back((now, value));
        while self
            .0
            .front()
            .is_some_and(|(at, _)| now.saturating_sub(*at) > SERIES_KEEP_MS)
        {
            self.0.pop_front();
        }
    }

    /// 窗口内的累计增长。计数器变小视为对端重启、从 0 重新累计。窗口开始前
    /// 最后一个样本作为基线；样本还不够覆盖整个窗口时只会少算，不会误报。
    fn increase(&self, now: u64, window_ms: u64) -> u64 {
        let start = now.saturating_sub(window_ms);
        let mut total = 0;
        let mut prev: Option<u64> = None;
        for &(at, value) in &self.0 {
            if at >= start
                && let Some(prev) = prev
            {
                total += if value >= prev { value - prev } else { value };
            }
            prev = Some(value);
        }
        total
    }

    fn last_at(&self) -> u64 {
        self.0.back().map(|(at, _)| *at).unwrap_or(0)
    }
}

struct Condition {
    rule: &'static str,
    title: &'static str,
    severity: Severity,
    subject: String,
    detail: String,
    pending_since: u64,
    firing: bool,
    notified_ms: Option<u64>,
    absent_since: Option<u64>,
}

struct Resolved {
    title: &'static str,
    subject: String,
    lasted_ms: u64,
}

/// 面板上展示的一条告警（触发中或观察中）。
#[derive(Clone, Serialize)]
pub struct AlertView {
    pub rule: &'static str,
    pub title: &'static str,
    pub severity: Severity,
    pub subject: String,
    pub detail: String,
    pub since_ms: u64,
    pub firing: bool,
}

pub struct Inputs {
    pub upstream: UpstreamSnapshot,
    pub nodes: Vec<NodeView>,
    pub feed_connected: bool,
}

pub struct Notification {
    pub text: String,
    keys: Vec<String>,
    resolved: usize,
}

pub struct Engine {
    rules: AlertRules,
    series: HashMap<String, Series>,
    conditions: BTreeMap<String, Condition>,
    resolved: Vec<Resolved>,
}

impl Engine {
    pub fn new(rules: AlertRules) -> Self {
        Self {
            rules,
            series: HashMap::new(),
            conditions: BTreeMap::new(),
            resolved: Vec::new(),
        }
    }

    pub fn observe(&mut self, now: u64, input: &Inputs) {
        let (findings, evaluated) = self.evaluate(now, input);
        let mut present = HashSet::new();
        for finding in findings {
            let key = format!("{}:{}", finding.rule, finding.subject);
            present.insert(key.clone());
            let condition = self.conditions.entry(key).or_insert_with(|| Condition {
                rule: finding.rule,
                title: finding.title,
                severity: finding.severity,
                subject: finding.subject.clone(),
                detail: String::new(),
                pending_since: now,
                firing: false,
                notified_ms: None,
                absent_since: None,
            });
            condition.detail = finding.detail;
            condition.absent_since = None;
            if !condition.firing && now.saturating_sub(condition.pending_since) >= finding.hold_ms {
                condition.firing = true;
            }
        }

        let resolve_ms = self.rules.resolve_after_s * 1000;
        let mut gone = Vec::new();
        for (key, condition) in &mut self.conditions {
            // 这一轮没检查到的规则（比如 tinyuma 抓不到时的上游规则）保持原状，
            // 不能因为"没数据"就当成恢复。
            if present.contains(key) || !evaluated.contains(condition.rule) {
                continue;
            }
            if !condition.firing {
                gone.push(key.clone());
                continue;
            }
            let since = *condition.absent_since.get_or_insert(now);
            if now.saturating_sub(since) >= resolve_ms {
                gone.push(key.clone());
            }
        }
        for key in gone {
            let Some(condition) = self.conditions.remove(&key) else {
                continue;
            };
            // 从没推出去过的触发，恢复也不用推，免得群里凭空冒出一条"已恢复"。
            if condition.firing && condition.notified_ms.is_some() {
                let ended = condition.absent_since.unwrap_or(now);
                self.resolved.push(Resolved {
                    title: condition.title,
                    subject: condition.subject,
                    lasted_ms: ended.saturating_sub(condition.pending_since),
                });
            }
        }
        if self.resolved.len() > MAX_PENDING_RESOLVED {
            let excess = self.resolved.len() - MAX_PENDING_RESOLVED;
            self.resolved.drain(..excess);
        }
        self.series
            .retain(|_, series| now.saturating_sub(series.last_at()) <= SERIES_KEEP_MS);
    }

    fn counter(
        &mut self,
        key: String,
        value: Option<u64>,
        now: u64,
        window_ms: u64,
    ) -> Option<u64> {
        let value = value?;
        let series = self.series.entry(key).or_default();
        series.push(now, value);
        Some(series.increase(now, window_ms))
    }

    fn evaluate(&mut self, now: u64, input: &Inputs) -> (Vec<Finding>, HashSet<&'static str>) {
        let r = self.rules.clone();
        let mut out = Vec::new();
        let mut evaluated = HashSet::new();
        let push = |out: &mut Vec<Finding>,
                    rule: &'static str,
                    title: &'static str,
                    severity: Severity,
                    subject: &str,
                    detail: String,
                    hold_s: u64| {
            out.push(Finding {
                rule,
                title,
                severity,
                subject: subject.to_owned(),
                detail,
                hold_ms: hold_s * 1000,
            });
        };

        // ---------- tinyuma ----------
        let up = &input.upstream;
        let fresh = up.ok && up.fetched_at_ms > 0;
        evaluated.insert("tinyuma_unreachable");
        if !fresh {
            let detail = up
                .error
                .clone()
                .unwrap_or_else(|| "console 还没成功抓取过 /healthz".to_owned());
            push(
                &mut out,
                "tinyuma_unreachable",
                "console 抓不到 tinyuma",
                Severity::Critical,
                "",
                detail,
                r.upstream_unreachable_s,
            );
        }
        if fresh && let Some(data) = up.dashboard.as_ref().or(up.healthz.as_ref()) {
            evaluated.extend([
                "rpc_disconnected",
                "rpc_sources_degraded",
                "rpc_reconnects",
                "upstream_silent",
                "enrichment_recent_low",
                "enrichment_misses",
                "decode_errors",
                "slow_clients",
                "cursor_rejected",
                "storage_dropped",
                "catalog_small",
            ]);
            if data.get("rpc_connected").and_then(Value::as_bool) == Some(false) {
                push(
                    &mut out,
                    "rpc_disconnected",
                    "tinyuma 的 Polygon RPC 订阅全部断开",
                    Severity::Critical,
                    "",
                    "rpc_connected=false".to_owned(),
                    r.rpc_disconnected_s,
                );
            }
            if let (Some(connected), Some(configured)) = (
                num(data, "rpc_sources_connected"),
                num(data, "rpc_sources_configured"),
            ) && connected > 0
                && connected < configured
            {
                push(
                    &mut out,
                    "rpc_sources_degraded",
                    "多路 RPC 有掉线",
                    Severity::Warning,
                    "",
                    format!("在线 {connected}/{configured} 路"),
                    r.rpc_sources_degraded_s,
                );
            }
            if let Some(n) = self.counter(
                "up:rpc_reconnects_total".into(),
                num(data, "rpc_reconnects_total"),
                now,
                30 * MINUTE_MS,
            ) && n >= r.rpc_reconnects_per_30m
            {
                push(
                    &mut out,
                    "rpc_reconnects",
                    "RPC 订阅频繁重连",
                    Severity::Warning,
                    "",
                    format!("近 30min 重连 {n} 次（阈值 {}）", r.rpc_reconnects_per_30m),
                    0,
                );
            }
            if let Some(last_us) = num(data, "last_upstream_received_at_us").filter(|v| *v > 0) {
                let idle_ms = (now * 1000).saturating_sub(last_us) / 1000;
                if idle_ms >= r.upstream_silent_min * MINUTE_MS {
                    push(
                        &mut out,
                        "upstream_silent",
                        "长时间没收到链上 UMA 事件",
                        Severity::Critical,
                        "",
                        format!(
                            "最近一次在 {} 前（阈值 {}min），订阅可能假死",
                            fmt_duration(idle_ms),
                            r.upstream_silent_min
                        ),
                        0,
                    );
                }
            }
            if let (Some(hits), Some(total)) = (
                num(data, "enrichment_recent_hits"),
                num(data, "enrichment_recent_total"),
            ) && total >= 200
            {
                let pct = hits as f64 * 100.0 / total as f64;
                if pct < r.enrichment_recent_min_pct {
                    push(
                        &mut out,
                        "enrichment_recent_low",
                        "富化命中率低（未命中的事件不会广播）",
                        Severity::Warning,
                        "",
                        format!(
                            "近 {total} 条命中 {pct:.1}%（阈值 {}%）",
                            r.enrichment_recent_min_pct
                        ),
                        0,
                    );
                }
            }
            if let Some(n) = self.counter(
                "up:enrichment_misses_total".into(),
                num(data, "enrichment_misses_total"),
                now,
                HOUR_MS,
            ) && n >= r.enrichment_misses_per_1h
            {
                push(
                    &mut out,
                    "enrichment_misses",
                    "富化未命中激增",
                    Severity::Warning,
                    "",
                    format!("近 1h 未命中 {n} 条（阈值 {}）", r.enrichment_misses_per_1h),
                    0,
                );
            }
            if let Some(n) = self.counter(
                "up:decode_errors_total".into(),
                num(data, "decode_errors_total"),
                now,
                HOUR_MS,
            ) && n >= r.decode_errors_per_1h
            {
                push(
                    &mut out,
                    "decode_errors",
                    "链上日志解码失败激增",
                    Severity::Warning,
                    "",
                    format!(
                        "近 1h 解码失败 {n} 条（阈值 {}），看 journal 里的 discarding undecodable RPC log",
                        r.decode_errors_per_1h
                    ),
                    0,
                );
            }
            if let Some(n) = self.counter(
                "up:slow_clients_dropped_total".into(),
                num(data, "slow_clients_dropped_total"),
                now,
                10 * MINUTE_MS,
            ) && n >= r.slow_clients_per_10m
            {
                push(
                    &mut out,
                    "slow_clients",
                    "tinyuma 下游写超时断开",
                    Severity::Warning,
                    "",
                    format!(
                        "近 10min {n} 次（阈值 {}），法兰克福→香港链路可能拥塞",
                        r.slow_clients_per_10m
                    ),
                    0,
                );
            }
            if let Some(n) = self.counter(
                "up:ws_cursor_rejected_total".into(),
                num(data, "ws_cursor_rejected_total"),
                now,
                10 * MINUTE_MS,
            ) && n >= r.cursor_rejected_per_10m
            {
                push(
                    &mut out,
                    "cursor_rejected",
                    "下游续传游标频繁过期（1013，可能丢帧）",
                    Severity::Warning,
                    "",
                    format!("近 10min {n} 次（阈值 {}）", r.cursor_rejected_per_10m),
                    0,
                );
            }
            if let Some(n) = self.counter(
                "up:storage_queue_dropped_total".into(),
                num(data, "storage_queue_dropped_total"),
                now,
                10 * MINUTE_MS,
            ) && n >= r.storage_dropped_per_10m
            {
                push(
                    &mut out,
                    "storage_dropped",
                    "tinyuma 本地 WAL 写入队列丢弃",
                    Severity::Critical,
                    "",
                    format!("近 10min 丢弃 {n} 条"),
                    0,
                );
            }
            if let Some(markets) = num(data, "catalog_markets")
                && markets < r.catalog_min_markets
            {
                push(
                    &mut out,
                    "catalog_small",
                    "富化缓存市场数异常偏少",
                    Severity::Warning,
                    "",
                    format!("缓存 {markets} 个市场（阈值 {}）", r.catalog_min_markets),
                    300,
                );
            }
        }

        // ---------- edge ----------
        evaluated.extend([
            "edge_stale",
            "edge_upstream_down",
            "edge_sequence_lag",
            "edge_link_lag",
            "edge_churn",
            "edge_slow_clients",
            "edge_bad_frames",
            "edge_near_full",
            "cluster_serving_low",
        ]);
        let latest = if fresh { up.latest_sequence } else { 0 };
        let mut serving = 0_u64;
        for node in &input.nodes {
            let hb = &node.heartbeat;
            let id = hb.node_id.as_str();
            match node.status {
                Status::Disabled => continue,
                Status::Stale => {
                    push(
                        &mut out,
                        "edge_stale",
                        "edge 失联（心跳中断）",
                        Severity::Critical,
                        id,
                        format!(
                            "心跳 {} 没到",
                            fmt_duration(now.saturating_sub(node.last_seen_ms))
                        ),
                        r.edge_stale_s,
                    );
                    continue;
                }
                Status::Serving => serving += 1,
                _ => {}
            }
            if !hb.upstream_connected {
                push(
                    &mut out,
                    "edge_upstream_down",
                    "edge 与 tinyuma 断开",
                    Severity::Critical,
                    id,
                    format!("累计重连 {} 次", hb.upstream_reconnects_total),
                    r.edge_upstream_down_s,
                );
            }
            if latest > 0 && hb.last_event_sequence > 0 {
                let lag = latest.saturating_sub(hb.last_event_sequence);
                if lag >= r.edge_sequence_lag {
                    push(
                        &mut out,
                        "edge_sequence_lag",
                        "edge 事件序号落后",
                        Severity::Warning,
                        id,
                        format!("落后 {lag} 条（阈值 {}）", r.edge_sequence_lag),
                        r.edge_sequence_lag_s,
                    );
                }
            }
            if hb.upstream_lag_last_us >= r.edge_link_lag_ms * 1000
                && now.saturating_sub(hb.last_frame_at_ms) <= 2 * MINUTE_MS
            {
                push(
                    &mut out,
                    "edge_link_lag",
                    "tinyuma→edge 传输延迟高",
                    Severity::Warning,
                    id,
                    format!(
                        "最近一帧 {}（阈值 {}ms）",
                        fmt_duration(hb.upstream_lag_last_us / 1000),
                        r.edge_link_lag_ms
                    ),
                    r.edge_link_lag_s,
                );
            }
            if let Some(n) = self.counter(
                format!("edge:{id}:clients_accepted_total"),
                Some(hb.clients_accepted_total),
                now,
                HOUR_MS,
            ) && n >= r.edge_accepted_per_1h
            {
                push(
                    &mut out,
                    "edge_churn",
                    "edge 客户端反复断线重连",
                    Severity::Warning,
                    id,
                    format!(
                        "近 1h 新接入 {n} 个连接，当前在线 {}（阈值 {}）",
                        hb.clients, r.edge_accepted_per_1h
                    ),
                    0,
                );
            }
            if let Some(n) = self.counter(
                format!("edge:{id}:slow_clients_disconnected_total"),
                Some(hb.slow_clients_disconnected_total),
                now,
                10 * MINUTE_MS,
            ) && n >= r.edge_slow_clients_per_10m
            {
                push(
                    &mut out,
                    "edge_slow_clients",
                    "edge 慢客户端被踢",
                    Severity::Warning,
                    id,
                    format!("近 10min {n} 个（阈值 {}）", r.edge_slow_clients_per_10m),
                    0,
                );
            }
            if let Some(n) = self.counter(
                format!("edge:{id}:bad_frames_total"),
                Some(hb.bad_frames_total),
                now,
                10 * MINUTE_MS,
            ) && n >= r.edge_bad_frames_per_10m
            {
                push(
                    &mut out,
                    "edge_bad_frames",
                    "edge 收到解析失败的帧",
                    Severity::Warning,
                    id,
                    format!("近 10min {n} 帧"),
                    0,
                );
            }
            if hb.max_clients > 0 && hb.clients * 100 >= hb.max_clients * r.edge_full_pct {
                push(
                    &mut out,
                    "edge_near_full",
                    "edge 接近满员",
                    Severity::Warning,
                    id,
                    format!(
                        "{}/{}（阈值 {}%）",
                        hb.clients, hb.max_clients, r.edge_full_pct
                    ),
                    r.edge_full_s,
                );
            }
        }
        if serving < r.min_serving_nodes {
            push(
                &mut out,
                "cluster_serving_low",
                "服务中的 edge 节点太少",
                Severity::Critical,
                "",
                format!("服务中 {serving} 台（阈值 {}）", r.min_serving_nodes),
                60,
            );
        }

        // ---------- console ----------
        evaluated.insert("panel_feed_down");
        if !input.feed_connected {
            push(
                &mut out,
                "panel_feed_down",
                "面板实时转发断开",
                Severity::Warning,
                "",
                "console → tinyuma 的 ws 未连上（只影响面板，不影响业务下游）".to_owned(),
                r.panel_feed_down_s,
            );
        }

        (out, evaluated)
    }

    /// 需要推送时拼好一条消息；推送成功后调 `mark_sent`，失败不调、下一轮重拼。
    pub fn pending_message(&self, now: u64, panel_url: Option<&str>) -> Option<Notification> {
        let repeat_ms = self.rules.repeat_after_min.max(1) * MINUTE_MS;
        let mut fresh = Vec::new();
        let mut repeat = Vec::new();
        for (key, condition) in &self.conditions {
            if !condition.firing {
                continue;
            }
            match condition.notified_ms {
                None => fresh.push((key, condition)),
                Some(at) if now.saturating_sub(at) >= repeat_ms => repeat.push((key, condition)),
                Some(_) => {}
            }
        }
        if fresh.is_empty() && repeat.is_empty() && self.resolved.is_empty() {
            return None;
        }

        let mut text = format!(
            "【uma 告警】新触发 {} · 仍未恢复 {} · 已恢复 {}",
            count_groups(&fresh),
            count_groups(&repeat),
            self.resolved.len()
        );
        append_section(&mut text, "新触发", &fresh, now);
        append_section(&mut text, "仍未恢复（到期提醒）", &repeat, now);
        if !self.resolved.is_empty() {
            text.push_str("\n\n—— 已恢复 ——");
            for resolved in &self.resolved {
                let subject = if resolved.subject.is_empty() {
                    String::new()
                } else {
                    format!("：{}", resolved.subject)
                };
                text.push_str(&format!(
                    "\n✅ {}{}（持续 {}）",
                    resolved.title,
                    subject,
                    fmt_duration(resolved.lasted_ms)
                ));
            }
        }
        if let Some(url) = panel_url {
            text.push_str(&format!("\n\n面板：{url}"));
        }
        Some(Notification {
            text,
            keys: fresh
                .iter()
                .chain(repeat.iter())
                .map(|(key, _)| (*key).clone())
                .collect(),
            resolved: self.resolved.len(),
        })
    }

    pub fn mark_sent(&mut self, notification: &Notification, now: u64) {
        for key in &notification.keys {
            if let Some(condition) = self.conditions.get_mut(key) {
                condition.notified_ms = Some(now);
            }
        }
        let n = notification.resolved.min(self.resolved.len());
        self.resolved.drain(..n);
    }

    pub fn views(&self) -> Vec<AlertView> {
        let mut views: Vec<AlertView> = self
            .conditions
            .values()
            .map(|c| AlertView {
                rule: c.rule,
                title: c.title,
                severity: c.severity,
                subject: c.subject.clone(),
                detail: c.detail.clone(),
                since_ms: c.pending_since,
                firing: c.firing,
            })
            .collect();
        views.sort_by(|a, b| {
            b.firing
                .cmp(&a.firing)
                .then(a.severity.cmp(&b.severity))
                .then(a.rule.cmp(b.rule))
                .then(a.subject.cmp(&b.subject))
        });
        views
    }
}

/// 按 (严重程度, 规则) 分组的组数——同一规则多个节点算一项。
fn count_groups(items: &[(&String, &Condition)]) -> usize {
    items
        .iter()
        .map(|(_, c)| (c.severity, c.rule))
        .collect::<HashSet<_>>()
        .len()
}

fn append_section(text: &mut String, name: &str, items: &[(&String, &Condition)], now: u64) {
    if items.is_empty() {
        return;
    }
    let mut groups: BTreeMap<(Severity, &'static str), Vec<&Condition>> = BTreeMap::new();
    for (_, condition) in items {
        groups
            .entry((condition.severity, condition.rule))
            .or_default()
            .push(condition);
    }
    text.push_str(&format!("\n\n—— {name} ——"));
    for ((severity, _), conditions) in groups {
        text.push_str(&format!("\n{}｜{}", severity.label(), conditions[0].title));
        for condition in conditions {
            let lasted = fmt_duration(now.saturating_sub(condition.pending_since));
            if condition.subject.is_empty() {
                text.push_str(&format!("\n    {}（已持续 {lasted}）", condition.detail));
            } else {
                text.push_str(&format!(
                    "\n    {}：{}（已持续 {lasted}）",
                    condition.subject, condition.detail
                ));
            }
        }
    }
}

fn num(data: &Value, key: &str) -> Option<u64> {
    data.get(key)?.as_u64()
}

fn fmt_duration(ms: u64) -> String {
    let s = ms / 1000;
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{}s", s / 60, s % 60)
    } else {
        format!("{}h{}m", s / 3600, s % 3600 / 60)
    }
}

#[derive(Clone, Default, Serialize)]
pub struct Delivery {
    pub webhook_configured: bool,
    pub sent_total: u64,
    pub last_sent_ms: u64,
    pub last_error: Option<String>,
    pub last_error_ms: u64,
}

#[derive(Serialize)]
pub struct AlertsStatus {
    #[serde(flatten)]
    pub delivery: Delivery,
    pub interval_s: u64,
    pub alerts: Vec<AlertView>,
    pub rules: AlertRules,
}

pub struct Alerts {
    engine: Mutex<Engine>,
    rules: AlertRules,
    delivery: Mutex<Delivery>,
}

impl Alerts {
    pub fn new(rules: AlertRules, webhook_configured: bool) -> Self {
        Self {
            engine: Mutex::new(Engine::new(rules.clone())),
            rules,
            delivery: Mutex::new(Delivery {
                webhook_configured,
                ..Delivery::default()
            }),
        }
    }

    fn engine(&self) -> MutexGuard<'_, Engine> {
        self.engine.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn delivery(&self) -> MutexGuard<'_, Delivery> {
        self.delivery.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn status(&self, interval: Duration) -> AlertsStatus {
        AlertsStatus {
            delivery: self.delivery().clone(),
            interval_s: interval.as_secs(),
            alerts: self.engine().views(),
            rules: self.rules.clone(),
        }
    }
}

pub async fn run_alerts(state: SharedState) {
    let mut ticker = tokio::time::interval(state.config.alert_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now = now_ms();
        let input = Inputs {
            upstream: state.upstream.snapshot(),
            nodes: state.registry.admin_list(),
            feed_connected: state.feed.connected.load(Ordering::Relaxed),
        };
        let retry_blocked = {
            let delivery = state.alerts.delivery();
            delivery.last_error_ms > delivery.last_sent_ms
                && now.saturating_sub(delivery.last_error_ms) < RETRY_AFTER_FAILURE_MS
        };
        let notification = {
            let mut engine = state.alerts.engine();
            engine.observe(now, &input);
            if retry_blocked {
                None
            } else {
                engine.pending_message(now, state.config.alert_panel_url.as_deref())
            }
        };
        let Some(notification) = notification else {
            continue;
        };
        let result = match &state.config.alert_webhook {
            Some(url) => send_feishu(&state.http, url, &notification.text).await,
            None => {
                warn!(text = %notification.text, "alert (CONSOLE_ALERT_WEBHOOK not set, log only)");
                Ok(())
            }
        };
        match result {
            Ok(()) => {
                state.alerts.engine().mark_sent(&notification, now);
                if state.config.alert_webhook.is_some() {
                    info!(text = %notification.text, "alert pushed");
                    let mut delivery = state.alerts.delivery();
                    delivery.sent_total += 1;
                    delivery.last_sent_ms = now;
                }
            }
            Err(error) => {
                warn!(%error, "alert push failed; will retry");
                let mut delivery = state.alerts.delivery();
                delivery.last_error = Some(error);
                delivery.last_error_ms = now;
            }
        }
    }
}

/// 飞书群自定义机器人（open.feishu.cn / open.larksuite.com 的 bot/v2/hook）。
/// 成功是 HTTP 200 且 `code == 0`；错误信息里不带 URL（URL 本身就是凭据）。
pub async fn send_feishu(http: &reqwest::Client, url: &str, text: &str) -> Result<(), String> {
    let body = serde_json::json!({"msg_type": "text", "content": {"text": text}});
    let response = http
        .post(url)
        .timeout(Duration::from_secs(10))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("send: {}", e.without_url()))?;
    let status = response.status();
    let reply: Value = response.json().await.unwrap_or(Value::Null);
    let code = reply
        .get("code")
        .or_else(|| reply.get("StatusCode"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if !status.is_success() || code != 0 {
        let msg = reply
            .get("msg")
            .or_else(|| reply.get("StatusMessage"))
            .and_then(Value::as_str)
            .unwrap_or("");
        return Err(format!("HTTP {status} code {code} {msg}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::console::registry::Heartbeat;

    const T0: u64 = 1_800_000_000_000;

    fn upstream(data: Value, at: u64) -> UpstreamSnapshot {
        UpstreamSnapshot {
            base_url: String::new(),
            fetched_at_ms: at,
            ok: true,
            error: None,
            healthz: Some(data.clone()),
            dashboard: Some(data),
            latest_sequence: 1000,
        }
    }

    fn healthy(at: u64) -> Value {
        json!({
            "rpc_connected": true,
            "rpc_sources_connected": 1,
            "rpc_sources_configured": 1,
            "rpc_reconnects_total": 0,
            "last_upstream_received_at_us": at * 1000,
            "catalog_markets": 500_000,
        })
    }

    fn node(id: &str, status: Status, accepted: u64, at: u64) -> NodeView {
        NodeView {
            heartbeat: Heartbeat {
                node_id: id.into(),
                upstream_connected: true,
                last_event_sequence: 1000,
                clients: 300,
                max_clients: 1000,
                clients_accepted_total: accepted,
                ..Heartbeat::default()
            },
            remote_ip: String::new(),
            first_seen_ms: at,
            last_seen_ms: at,
            disabled: false,
            desired_drain: false,
            note: String::new(),
            status,
            url: String::new(),
            pending_release: 0,
            released_total: 0,
            last_release_ms: 0,
            balance_excess: 0,
        }
    }

    fn inputs(data: Value, nodes: Vec<NodeView>, at: u64) -> Inputs {
        Inputs {
            upstream: upstream(data, at),
            nodes,
            feed_connected: true,
        }
    }

    fn quiet(at: u64) -> Inputs {
        inputs(healthy(at), vec![node("a", Status::Serving, 0, at)], at)
    }

    #[test]
    fn counter_increase_survives_a_restart() {
        let mut series = Series::default();
        series.push(T0, 100);
        series.push(T0 + 1000, 150);
        series.push(T0 + 2000, 20); // 对端重启
        series.push(T0 + 3000, 30);
        assert_eq!(series.increase(T0 + 3000, 10_000), 50 + 20 + 10);
        // 窗口只盖住最后两个样本：基线是窗口前最后一个样本。
        assert_eq!(series.increase(T0 + 3000, 1500), 20 + 10);
    }

    #[test]
    fn sustained_condition_fires_once_then_resolves() {
        let mut engine = Engine::new(AlertRules::default());
        let mut down = healthy(T0);
        down["rpc_connected"] = json!(false);

        engine.observe(
            T0,
            &inputs(down.clone(), vec![node("a", Status::Serving, 0, T0)], T0),
        );
        assert!(
            engine.pending_message(T0, None).is_none(),
            "30s hold not reached"
        );

        let t = T0 + 30_000;
        engine.observe(
            t,
            &inputs(down.clone(), vec![node("a", Status::Serving, 0, t)], t),
        );
        let n = engine
            .pending_message(t, Some("https://panel"))
            .expect("fires");
        assert!(n.text.contains("RPC 订阅全部断开"), "{}", n.text);
        assert!(n.text.contains("面板：https://panel"));
        engine.mark_sent(&n, t);
        assert!(
            engine.pending_message(t + 15_000, None).is_none(),
            "no repeat before repeat_after_min"
        );

        // 恢复要等条件消失满 resolve_after_s。
        let t = T0 + 60_000;
        engine.observe(t, &quiet(t));
        assert!(engine.pending_message(t, None).is_none());
        let t = T0 + 120_000;
        engine.observe(t, &quiet(t));
        let n = engine.pending_message(t, None).expect("recovery");
        assert!(
            n.text.contains("✅ tinyuma 的 Polygon RPC 订阅全部断开"),
            "{}",
            n.text
        );
        engine.mark_sent(&n, t);
        assert!(engine.views().is_empty());
    }

    #[test]
    fn churn_on_several_nodes_is_one_grouped_line() {
        let mut engine = Engine::new(AlertRules::default());
        let mut t = T0;
        engine.observe(
            t,
            &inputs(
                healthy(t),
                vec![
                    node("uma-slave1", Status::Serving, 1000, t),
                    node("uma-slave2", Status::Serving, 5000, t),
                    node("ubuntu3", Status::Serving, 100, t),
                ],
                t,
            ),
        );
        t += 30 * MINUTE_MS;
        engine.observe(
            t,
            &inputs(
                healthy(t),
                vec![
                    node("uma-slave1", Status::Serving, 1900, t),
                    node("uma-slave2", Status::Serving, 5850, t),
                    node("ubuntu3", Status::Serving, 215, t),
                ],
                t,
            ),
        );
        let n = engine.pending_message(t, None).expect("fires");
        assert_eq!(
            n.text.matches("edge 客户端反复断线重连").count(),
            1,
            "{}",
            n.text
        );
        assert!(
            n.text.contains("uma-slave1：近 1h 新接入 900"),
            "{}",
            n.text
        );
        assert!(
            n.text.contains("uma-slave2：近 1h 新接入 850"),
            "{}",
            n.text
        );
        assert!(!n.text.contains("ubuntu3"), "{}", n.text);
        assert!(n.text.starts_with("【uma 告警】新触发 1 "), "{}", n.text);
    }

    #[test]
    fn unreachable_tinyuma_does_not_fake_a_recovery() {
        let mut engine = Engine::new(AlertRules::default());
        let mut down = healthy(T0);
        down["rpc_connected"] = json!(false);
        for t in [T0, T0 + 30_000] {
            engine.observe(
                t,
                &inputs(down.clone(), vec![node("a", Status::Serving, 0, t)], t),
            );
        }
        let n = engine.pending_message(T0 + 30_000, None).unwrap();
        engine.mark_sent(&n, T0 + 30_000);

        let mut unreachable = quiet(T0);
        unreachable.upstream.ok = false;
        unreachable.upstream.error = Some("timeout".into());
        for step in 1..=10 {
            let t = T0 + 30_000 + step * 15_000;
            unreachable.nodes = vec![node("a", Status::Serving, 0, t)];
            engine.observe(t, &unreachable);
        }
        let t = T0 + 30_000 + 150_000;
        let n = engine.pending_message(t, None).expect("unreachable fires");
        assert!(n.text.contains("console 抓不到 tinyuma"), "{}", n.text);
        assert!(
            !n.text.contains("✅"),
            "rpc alert must not resolve without data: {}",
            n.text
        );
    }

    #[test]
    fn failed_push_is_retried_and_repeats_after_interval() {
        let rules = AlertRules {
            repeat_after_min: 10,
            ..AlertRules::default()
        };
        let mut engine = Engine::new(rules);
        let stale = |t| inputs(healthy(t), vec![node("a", Status::Stale, 0, t - 60_000)], t);
        engine.observe(T0, &stale(T0));
        engine.observe(T0 + 30_000, &stale(T0 + 30_000));
        assert!(engine.pending_message(T0 + 30_000, None).is_some());
        // 没 mark_sent（推送失败）：下一轮仍然算"新触发"。
        engine.observe(T0 + 45_000, &stale(T0 + 45_000));
        let n = engine.pending_message(T0 + 45_000, None).unwrap();
        assert!(n.text.contains("新触发 1"), "{}", n.text);
        engine.mark_sent(&n, T0 + 45_000);
        assert!(engine.pending_message(T0 + 60_000, None).is_none());
        let t = T0 + 45_000 + 10 * MINUTE_MS;
        engine.observe(t, &stale(t));
        let n = engine.pending_message(t, None).unwrap();
        assert!(n.text.contains("仍未恢复（到期提醒）"), "{}", n.text);
        assert!(n.text.contains("a：心跳"), "{}", n.text);
    }

    #[test]
    fn rules_file_missing_fields_fall_back_to_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.json");
        std::fs::write(&path, r#"{"edge_accepted_per_1h": 1200}"#).unwrap();
        let rules = load_rules(&path);
        assert_eq!(rules.edge_accepted_per_1h, 1200);
        assert_eq!(
            rules.rpc_disconnected_s,
            AlertRules::default().rpc_disconnected_s
        );
        std::fs::write(&path, "not json").unwrap();
        assert_eq!(load_rules(&path).edge_accepted_per_1h, 800);
    }
}
