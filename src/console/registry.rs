//! 节点注册表：edge 心跳进来、状态判定、公开列表、管理位。
//! `disabled`/`note` 落盘，`desired_drain` 不落盘（一次性动作）。

use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::PathBuf,
    sync::Mutex,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::now_ms;
use super::store::{load_json, save_json};
use super::tokens::TokenGrant;

/// edge → console. Also the body of the edge's own `/edge/healthz`.
/// `serde(default)` so an older/newer edge never gets rejected for a field.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Heartbeat {
    pub node_id: String,
    pub version: String,
    pub commit: String,
    pub started_at_ms: u64,
    pub sent_at_ms: u64,
    pub advertise_host: String,
    pub port: u16,
    pub tls: bool,
    pub path: String,
    pub subprotocol: String,
    pub ready: bool,
    pub draining: bool,
    pub upstream_url: String,
    pub upstream_connected: bool,
    pub upstream_reconnects_total: u64,
    pub last_frame_at_ms: u64,
    pub frames_total: u64,
    pub bytes_total: u64,
    pub bad_frames_total: u64,
    pub last_event_sequence: u64,
    pub ring_frames: u64,
    pub ring_capacity: u64,
    pub upstream_lag_last_us: u64,
    pub upstream_lag_max_us: u64,
    pub clients: u64,
    pub max_clients: u64,
    pub clients_accepted_total: u64,
    pub clients_rejected_total: u64,
    pub rejected_token_total: u64,
    pub rejected_draining_total: u64,
    pub rejected_full_total: u64,
    pub rejected_subprotocol_total: u64,
    pub rejected_replay_total: u64,
    pub slow_clients_disconnected_total: u64,
    pub deliveries_total: u64,
    pub fanout_last_us: u64,
    pub fanout_max_us: u64,
    pub tokens_version: u64,
    pub clients_by_token: BTreeMap<String, u64>,
}

/// console → edge, heartbeat response.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Directive {
    pub drain: bool,
    pub drain_batch: u32,
    pub drain_interval_ms: u64,
    pub heartbeat_seconds: u64,
    /// 本次心跳要以 1012 释放多少个客户端（手动释放或自动均衡），0 表示不动。
    #[serde(default)]
    pub release: u32,
    pub tokens_version: u64,
    /// Only present when the edge's reported `tokens_version` is behind.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<Vec<TokenGrant>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Stale,
    Disabled,
    Draining,
    NotReady,
    Full,
    Serving,
}

/// Persisted per-node management bits.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct NodeAdmin {
    pub disabled: bool,
    pub note: String,
}

#[derive(Default, Serialize, Deserialize)]
struct NodesFile {
    nodes: BTreeMap<String, NodeAdmin>,
    #[serde(default)]
    settings: Settings,
}

/// 缺字段一律回落到 `Default`：旧 `nodes.json` 里是 `balance_threshold_pct`
/// （按均值判超载的老规则），换成 `balance_spread_pct` 后不能让控制台起不来。
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    /// 自动均衡：最忙的节点每次心跳释放一小批连接，让它们重连到最空的节点。
    pub auto_balance: bool,
    /// 每次心跳最多释放多少个（手动与自动共用）。
    pub release_batch: u32,
    /// 收敛目标：最忙节点的连接数不超过最空节点的 (100 + 这个值)%。
    pub balance_spread_pct: u32,
    /// 超出目标线的绝对数量低于这个值不动，避免为几个连接来回搬。
    pub balance_min_excess: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            auto_balance: true,
            release_batch: 20,
            balance_spread_pct: 5,
            balance_min_excess: 5,
        }
    }
}

#[derive(Clone)]
pub struct Node {
    pub heartbeat: Heartbeat,
    pub remote_ip: String,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub desired_drain: bool,
    /// 人工要求释放但还没下发完的数量。
    pub pending_release: u64,
    pub released_total: u64,
    pub last_release_ms: u64,
}

#[derive(Serialize)]
pub struct NodeView {
    pub heartbeat: Heartbeat,
    pub remote_ip: String,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub disabled: bool,
    pub desired_drain: bool,
    pub note: String,
    pub status: Status,
    pub url: String,
    pub pending_release: u64,
    pub released_total: u64,
    pub last_release_ms: u64,
    /// 自动均衡眼里的超出量（正数=超载），仅供面板展示。
    pub balance_excess: i64,
}

#[derive(Serialize)]
pub struct PublicNode {
    pub node_id: String,
    pub url: String,
    pub weight: u64,
    pub clients: u64,
}

#[derive(Serialize)]
pub struct NodeList {
    pub nodes: Vec<PublicNode>,
    pub subprotocol: String,
    pub path: String,
    pub ttl_s: u64,
    pub generated_at_ms: u64,
}

pub struct Registry {
    path: PathBuf,
    stale_after: Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    nodes: HashMap<String, Node>,
    admin: BTreeMap<String, NodeAdmin>,
    settings: Settings,
}

pub struct HeartbeatOutcome {
    pub first_seen: bool,
    pub restarted: bool,
    pub desired_drain: bool,
    /// 这次心跳让节点释放多少个（手动优先，其次自动均衡）。
    pub release: u32,
    pub release_reason: &'static str,
}

impl Registry {
    pub fn open(path: PathBuf, stale_after: Duration) -> io::Result<Self> {
        let file = load_json::<NodesFile>(&path)?.unwrap_or_default();
        Ok(Self {
            path,
            stale_after,
            inner: Mutex::new(Inner {
                nodes: HashMap::new(),
                admin: file.nodes,
                settings: file.settings,
            }),
        })
    }

    pub fn heartbeat(&self, hb: Heartbeat, remote_ip: String) -> HeartbeatOutcome {
        let now = now_ms();
        let mut inner = self.lock();
        let mut outcome = HeartbeatOutcome {
            first_seen: false,
            restarted: false,
            desired_drain: false,
            release: 0,
            release_reason: "",
        };
        let node = inner.nodes.entry(hb.node_id.clone()).or_insert_with(|| {
            outcome.first_seen = true;
            Node {
                heartbeat: Heartbeat::default(),
                remote_ip: remote_ip.clone(),
                first_seen_ms: now,
                last_seen_ms: now,
                desired_drain: false,
                pending_release: 0,
                released_total: 0,
                last_release_ms: 0,
            }
        });
        // 重启清 drain：同一 node_id 换了 started_at_ms 就是新进程，
        // 否则它会一起来就被再次摘流。
        if !outcome.first_seen
            && node.heartbeat.started_at_ms != 0
            && node.heartbeat.started_at_ms != hb.started_at_ms
        {
            outcome.restarted = true;
            node.desired_drain = false;
        }
        node.heartbeat = hb;
        node.remote_ip = remote_ip;
        node.last_seen_ms = now;
        outcome.desired_drain = node.desired_drain;
        let node_id = node.heartbeat.node_id.clone();
        let (release, reason) = self.decide_release(&mut inner, &node_id, now);
        outcome.release = release;
        outcome.release_reason = reason;
        outcome
    }

    /// 手动释放优先；没有手动任务时看自动均衡：只有当前最忙的那个服务中节点会
    /// 释放，且要超出"最空节点 × (1 + spread%)"这条线 ≥ min_excess。一次只动一个
    /// 节点是有意的——释放出去的连接会一起落到最空的那台，让每个节点各放一批很
    /// 容易把最空的一台直接顶成最忙的，来回搬。
    fn decide_release(&self, inner: &mut Inner, node_id: &str, now: u64) -> (u32, &'static str) {
        let batch = inner.settings.release_batch.max(1);
        let Some(node) = inner.nodes.get(node_id) else {
            return (0, "");
        };
        if node.pending_release > 0 {
            let n = node.pending_release.min(batch as u64) as u32;
            let node = inner.nodes.get_mut(node_id).expect("checked above");
            node.pending_release -= n as u64;
            node.released_total += n as u64;
            node.last_release_ms = now;
            return (n, "manual");
        }
        if !inner.settings.auto_balance || node.desired_drain {
            return (0, "");
        }
        if self.busiest_serving(inner, now).as_deref() != Some(node_id) {
            return (0, "");
        }
        let excess = self.balance_excess(inner, node_id, now);
        if excess < inner.settings.balance_min_excess as i64 {
            return (0, "");
        }
        let n = (excess as u64).min(batch as u64) as u32;
        let node = inner.nodes.get_mut(node_id).expect("checked above");
        node.released_total += n as u64;
        node.last_release_ms = now;
        (n, "auto-balance")
    }

    /// 该节点相对"最空的服务中节点 × (1 + spread%)"的超出量；不在服务、只有一个
    /// 节点、或别的节点没有余量时为 0（搬过去也接不住）。
    ///
    /// 跟最空的节点比而不是跟平均值比：9 台里有 1 台空着，均值只被拉低 1/9，
    /// 按"均值 +10%"算谁都不超载，那台空节点永远填不回来（2026-09-09 生产上
    /// 就是这样，一台重启后卡在 20 个连接不动）。
    fn balance_excess(&self, inner: &Inner, node_id: &str, now: u64) -> i64 {
        let serving = self.serving(inner, now);
        let Some(me) = serving.iter().find(|n| n.heartbeat.node_id == node_id) else {
            return 0;
        };
        if serving.len() < 2 {
            return 0;
        }
        let others_spare: u64 = serving
            .iter()
            .filter(|n| n.heartbeat.node_id != node_id)
            .map(|n| n.heartbeat.max_clients.saturating_sub(n.heartbeat.clients))
            .sum();
        if others_spare == 0 {
            return 0;
        }
        let emptiest = serving
            .iter()
            .map(|n| n.heartbeat.clients)
            .min()
            .unwrap_or(0);
        let ceiling = emptiest as f64 * (1.0 + inner.settings.balance_spread_pct as f64 / 100.0);
        let excess = (me.heartbeat.clients as f64 - ceiling).floor() as i64;
        // 一次最多搬掉差距的一半：全放到最空那台会让它反过来变成最忙的。
        let half_gap = (me.heartbeat.clients.saturating_sub(emptiest) / 2) as i64;
        excess.min(half_gap).min(others_spare as i64)
    }

    fn serving<'a>(&self, inner: &'a Inner, now: u64) -> Vec<&'a Node> {
        inner
            .nodes
            .values()
            .filter(|n| {
                self.status_of(n, admin_of(inner, &n.heartbeat.node_id), now) == Status::Serving
            })
            .collect()
    }

    /// 连接数最多的服务中节点（并列时取 node_id 小的，保证同一次心跳里只有一个
    /// 节点被选中）。
    fn busiest_serving(&self, inner: &Inner, now: u64) -> Option<String> {
        self.serving(inner, now)
            .into_iter()
            .max_by(|a, b| {
                a.heartbeat
                    .clients
                    .cmp(&b.heartbeat.clients)
                    .then_with(|| b.heartbeat.node_id.cmp(&a.heartbeat.node_id))
            })
            .map(|n| n.heartbeat.node_id.clone())
    }

    pub fn settings(&self) -> Settings {
        self.lock().settings.clone()
    }

    pub fn set_auto_balance(&self, enabled: bool) -> io::Result<()> {
        let mut inner = self.lock();
        inner.settings.auto_balance = enabled;
        self.persist(&inner)
    }

    /// 人工要求某节点释放 count 个连接，按 release_batch 分批经心跳下发。
    pub fn request_release(&self, id: &str, count: u64) -> bool {
        let mut inner = self.lock();
        match inner.nodes.get_mut(id) {
            Some(node) => {
                node.pending_release = count;
                true
            }
            None => false,
        }
    }

    pub fn status_of(&self, node: &Node, admin: &NodeAdmin, now: u64) -> Status {
        let hb = &node.heartbeat;
        if now.saturating_sub(node.last_seen_ms) > self.stale_after.as_millis() as u64 {
            Status::Stale
        } else if admin.disabled {
            Status::Disabled
        } else if node.desired_drain || hb.draining {
            Status::Draining
        } else if !hb.ready {
            Status::NotReady
        } else if hb.clients >= hb.max_clients {
            Status::Full
        } else {
            Status::Serving
        }
    }

    pub fn public_list(&self, ttl: Duration) -> NodeList {
        let now = now_ms();
        let inner = self.lock();
        let mut nodes: Vec<PublicNode> = inner
            .nodes
            .values()
            .filter(|n| {
                self.status_of(n, admin_of(&inner, &n.heartbeat.node_id), now) == Status::Serving
            })
            .map(|n| PublicNode {
                node_id: n.heartbeat.node_id.clone(),
                url: node_url(n),
                weight: n
                    .heartbeat
                    .max_clients
                    .saturating_sub(n.heartbeat.clients)
                    .max(1),
                clients: n.heartbeat.clients,
            })
            .collect();
        // 连接数最少的排最前：契约让下游取 nodes[0]，新连接就落到最空的节点。
        // `weight`（剩余容量）留着给按权重挑的下游，两者在各节点 max_clients
        // 相同时是同一个顺序。
        nodes.sort_by(|a, b| {
            a.clients
                .cmp(&b.clients)
                .then_with(|| b.weight.cmp(&a.weight))
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
        let (subprotocol, path) = inner
            .nodes
            .values()
            .next()
            .map(|n| {
                (
                    or_default(&n.heartbeat.subprotocol, "uma.pb.v1"),
                    or_default(&n.heartbeat.path, "/uma/v1/ws"),
                )
            })
            .unwrap_or_else(|| ("uma.pb.v1".to_owned(), "/uma/v1/ws".to_owned()));
        NodeList {
            nodes,
            subprotocol,
            path,
            ttl_s: ttl.as_secs(),
            generated_at_ms: now,
        }
    }

    pub fn admin_list(&self) -> Vec<NodeView> {
        let now = now_ms();
        let inner = self.lock();
        let mut views: Vec<NodeView> = inner
            .nodes
            .values()
            .map(|n| {
                let admin = admin_of(&inner, &n.heartbeat.node_id);
                NodeView {
                    heartbeat: n.heartbeat.clone(),
                    remote_ip: n.remote_ip.clone(),
                    first_seen_ms: n.first_seen_ms,
                    last_seen_ms: n.last_seen_ms,
                    disabled: admin.disabled,
                    desired_drain: n.desired_drain,
                    note: admin.note.clone(),
                    status: self.status_of(n, admin, now),
                    url: node_url(n),
                    pending_release: n.pending_release,
                    released_total: n.released_total,
                    last_release_ms: n.last_release_ms,
                    balance_excess: self.balance_excess(&inner, &n.heartbeat.node_id, now),
                }
            })
            .collect();
        views.sort_by(|a, b| a.heartbeat.node_id.cmp(&b.heartbeat.node_id));
        views
    }

    pub fn counts(&self) -> (usize, usize) {
        let now = now_ms();
        let inner = self.lock();
        let serving = inner
            .nodes
            .values()
            .filter(|n| {
                self.status_of(n, admin_of(&inner, &n.heartbeat.node_id), now) == Status::Serving
            })
            .count();
        (serving, inner.nodes.len())
    }

    pub fn set_drain(&self, id: &str, drain: bool) -> bool {
        let mut inner = self.lock();
        match inner.nodes.get_mut(id) {
            Some(node) => {
                node.desired_drain = drain;
                true
            }
            None => false,
        }
    }

    pub fn set_disabled(&self, id: &str, disabled: bool) -> io::Result<bool> {
        self.mutate_admin(id, |a| a.disabled = disabled)
    }

    pub fn set_note(&self, id: &str, note: &str) -> io::Result<bool> {
        self.mutate_admin(id, |a| a.note = note.trim().to_owned())
    }

    /// Drops the node from memory and its persisted bits. It reappears on
    /// its next heartbeat as a fresh, enabled node.
    pub fn forget(&self, id: &str) -> io::Result<bool> {
        let mut inner = self.lock();
        let known = inner.nodes.remove(id).is_some() | inner.admin.remove(id).is_some();
        if known {
            self.persist(&inner)?;
        }
        Ok(known)
    }

    fn mutate_admin(&self, id: &str, f: impl FnOnce(&mut NodeAdmin)) -> io::Result<bool> {
        let mut inner = self.lock();
        if !inner.nodes.contains_key(id) && !inner.admin.contains_key(id) {
            return Ok(false);
        }
        f(inner.admin.entry(id.to_owned()).or_default());
        self.persist(&inner)?;
        Ok(true)
    }

    fn persist(&self, inner: &Inner) -> io::Result<()> {
        save_json(
            &self.path,
            &NodesFile {
                nodes: inner.admin.clone(),
                settings: inner.settings.clone(),
            },
        )
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn admin_of<'a>(inner: &'a Inner, id: &str) -> &'a NodeAdmin {
    static EMPTY: NodeAdmin = NodeAdmin {
        disabled: false,
        note: String::new(),
    };
    inner.admin.get(id).unwrap_or(&EMPTY)
}

fn or_default(value: &str, default: &str) -> String {
    if value.is_empty() {
        default.to_owned()
    } else {
        value.to_owned()
    }
}

pub fn node_url(node: &Node) -> String {
    let hb = &node.heartbeat;
    let scheme = if hb.tls { "wss" } else { "ws" };
    let host = if hb.advertise_host.is_empty() {
        node.remote_ip.as_str()
    } else {
        hb.advertise_host.as_str()
    };
    let port = if hb.port == 0 { 8012 } else { hb.port };
    let path = or_default(&hb.path, "/uma/v1/ws");
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    };
    format!("{scheme}://{host}:{port}{path}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hb(id: &str, clients: u64) -> Heartbeat {
        Heartbeat {
            node_id: id.to_owned(),
            started_at_ms: 1,
            ready: true,
            clients,
            max_clients: 100,
            port: 8012,
            ..Default::default()
        }
    }

    fn registry() -> (tempfile::TempDir, Registry) {
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::open(dir.path().join("nodes.json"), Duration::from_secs(15)).unwrap();
        (dir, reg)
    }

    #[test]
    fn list_filters_and_sorts_by_spare_capacity() {
        let (_dir, reg) = registry();
        reg.heartbeat(hb("busy", 90), "10.0.0.1".into());
        reg.heartbeat(hb("idle", 10), "10.0.0.2".into());
        reg.heartbeat(hb("full", 100), "10.0.0.3".into());
        reg.heartbeat(
            Heartbeat {
                ready: false,
                ..hb("notready", 0)
            },
            "10.0.0.4".into(),
        );
        reg.heartbeat(
            Heartbeat {
                draining: true,
                ..hb("draining", 0)
            },
            "10.0.0.5".into(),
        );
        let list = reg.public_list(Duration::from_secs(30));
        let ids: Vec<&str> = list.nodes.iter().map(|n| n.node_id.as_str()).collect();
        assert_eq!(ids, ["idle", "busy"]);
        assert_eq!(list.nodes[0].weight, 90);
        assert_eq!(list.nodes[0].url, "ws://10.0.0.2:8012/uma/v1/ws");
        assert_eq!(list.ttl_s, 30);
    }

    #[test]
    fn status_priority_and_url() {
        let (_dir, reg) = registry();
        reg.heartbeat(hb("a", 0), "1.1.1.1".into());
        reg.set_disabled("a", true).unwrap();
        reg.set_drain("a", true);
        let view = &reg.admin_list()[0];
        assert_eq!(view.status, Status::Disabled, "disabled outranks draining");
        reg.set_disabled("a", false).unwrap();
        assert_eq!(reg.admin_list()[0].status, Status::Draining);
        assert!(reg.public_list(Duration::from_secs(1)).nodes.is_empty());

        reg.heartbeat(
            Heartbeat {
                advertise_host: "edge.example.com".into(),
                tls: true,
                port: 443,
                path: "/custom".into(),
                ..hb("a", 0)
            },
            "1.1.1.1".into(),
        );
        assert_eq!(reg.admin_list()[0].url, "wss://edge.example.com:443/custom");
    }

    #[test]
    fn stale_never_listed() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::open(dir.path().join("n.json"), Duration::from_millis(0)).unwrap();
        reg.heartbeat(hb("a", 0), "1.1.1.1".into());
        std::thread::sleep(Duration::from_millis(2));
        assert!(reg.public_list(Duration::from_secs(1)).nodes.is_empty());
        assert_eq!(reg.admin_list()[0].status, Status::Stale);
    }

    #[test]
    fn restart_clears_desired_drain() {
        let (_dir, reg) = registry();
        reg.heartbeat(hb("a", 0), "1.1.1.1".into());
        reg.set_drain("a", true);
        let same = reg.heartbeat(hb("a", 0), "1.1.1.1".into());
        assert!(same.desired_drain && !same.restarted);
        let restarted = reg.heartbeat(
            Heartbeat {
                started_at_ms: 2,
                ..hb("a", 0)
            },
            "1.1.1.1".into(),
        );
        assert!(restarted.restarted);
        assert!(!restarted.desired_drain);
        assert_eq!(reg.admin_list()[0].status, Status::Serving);
    }

    #[test]
    fn manual_release_is_batched_through_heartbeats() {
        let hb = |id: &str, clients: u64| Heartbeat {
            max_clients: 1000,
            ..hb(id, clients)
        };
        let (_dir, reg) = registry();
        reg.heartbeat(hb("a", 500), "1.1.1.1".into());
        assert!(reg.request_release("a", 45));
        assert!(!reg.request_release("ghost", 1));
        let first = reg.heartbeat(hb("a", 500), "1.1.1.1".into());
        assert_eq!((first.release, first.release_reason), (20, "manual"));
        let second = reg.heartbeat(hb("a", 480), "1.1.1.1".into());
        assert_eq!(second.release, 20);
        let third = reg.heartbeat(hb("a", 460), "1.1.1.1".into());
        assert_eq!(third.release, 5);
        assert_eq!(reg.admin_list()[0].released_total, 45);
        assert_eq!(reg.admin_list()[0].pending_release, 0);
    }

    #[test]
    fn auto_balance_releases_only_from_overloaded_nodes_with_room_elsewhere() {
        let hb = |id: &str, clients: u64| Heartbeat {
            max_clients: 1000,
            ..hb(id, clients)
        };
        let (_dir, reg) = registry();
        // busy 900 / idle 0 → 目标线 0×1.05=0，busy 超出 900（一次最多搬一半）
        // → 每次心跳释放 release_batch 个
        reg.heartbeat(hb("idle", 0), "1.1.1.2".into());
        let o = reg.heartbeat(hb("busy", 900), "1.1.1.1".into());
        assert_eq!((o.release, o.release_reason), (20, "auto-balance"));
        // 收敛到最空节点的 5% 以内就停
        reg.heartbeat(hb("idle", 450), "1.1.1.2".into());
        let o = reg.heartbeat(hb("busy", 470), "1.1.1.1".into());
        assert_eq!(o.release, 0, "within 5% spread: nothing to move");
        // 小差距不搬：超出 < min_excess
        reg.heartbeat(hb("idle", 100), "1.1.1.2".into());
        let o = reg.heartbeat(hb("busy", 108), "1.1.1.1".into());
        assert_eq!(o.release, 0);
        // 只有最忙的那台放；第二忙的即使超出目标线也不动
        reg.heartbeat(hb("idle", 0), "1.1.1.2".into());
        reg.heartbeat(hb("mid", 400), "1.1.1.3".into());
        let o = reg.heartbeat(hb("busy", 900), "1.1.1.1".into());
        assert_eq!(o.release, 20);
        let o = reg.heartbeat(hb("mid", 400), "1.1.1.3".into());
        assert_eq!(o.release, 0, "only the busiest node sheds per heartbeat");
        reg.heartbeat(
            Heartbeat {
                ready: false,
                ..hb("mid", 400)
            },
            "1.1.1.3".into(),
        );
        // 别的节点没余量就不搬
        reg.heartbeat(
            Heartbeat {
                max_clients: 100,
                ..hb("idle", 100)
            },
            "1.1.1.2".into(),
        );
        let o = reg.heartbeat(hb("busy", 900), "1.1.1.1".into());
        assert_eq!(o.release, 0);
        // 关掉自动均衡后不搬；开关持久化
        reg.set_auto_balance(false).unwrap();
        reg.heartbeat(hb("idle", 0), "1.1.1.2".into());
        let o = reg.heartbeat(hb("busy", 900), "1.1.1.1".into());
        assert_eq!(o.release, 0);
        assert!(!reg.settings().auto_balance);
        // 手动释放不受开关影响
        reg.request_release("busy", 3);
        assert_eq!(reg.heartbeat(hb("busy", 900), "1.1.1.1".into()).release, 3);
        // 单节点集群永不自动搬
        let (_d2, solo) = registry();
        solo.heartbeat(hb("only", 900), "1.1.1.1".into());
        assert_eq!(solo.heartbeat(hb("only", 900), "1.1.1.1".into()).release, 0);
    }

    /// 生产场景回归：9 台节点，1 台刚重启是空的，自动均衡必须把它填回来，
    /// 直到最忙/最空的差距收敛到 5% 以内。老规则（跟均值比 +10%、超出 ≥20）
    /// 在这个分布下一次都不会触发，这个测试就是照着那次故障写的。
    #[test]
    fn auto_balance_converges_a_freshly_restarted_node_to_within_the_spread() {
        let hb = |id: &str, clients: u64| Heartbeat {
            max_clients: 1000,
            ..hb(id, clients)
        };
        let (_dir, reg) = registry();
        let mut clients: BTreeMap<String, u64> = (0..8)
            .map(|i| (format!("node{i}"), 380))
            .chain([("fresh".to_owned(), 20)])
            .collect();

        let spread = |c: &BTreeMap<String, u64>| {
            let max = *c.values().max().unwrap();
            let min = *c.values().min().unwrap();
            (max, min)
        };
        let (max, min) = spread(&clients);
        assert!(max as f64 > min as f64 * 1.05, "starts out of balance");

        let mut rounds = 0;
        loop {
            for (id, n) in clients.clone() {
                let out = reg.heartbeat(hb(&id, n), "1.1.1.1".into());
                if out.release > 0 {
                    assert_eq!(out.release_reason, "auto-balance");
                    // 下游收到 1012 后重拉列表，落到当时最空的那台。
                    let emptiest = clients
                        .iter()
                        .min_by_key(|(id, n)| (**n, (*id).clone()))
                        .map(|(id, _)| id.clone())
                        .unwrap();
                    *clients.get_mut(&id).unwrap() -= out.release as u64;
                    *clients.get_mut(&emptiest).unwrap() += out.release as u64;
                }
            }
            let (max, min) = spread(&clients);
            if max as f64 <= min as f64 * 1.05 {
                break;
            }
            rounds += 1;
            assert!(rounds < 200, "never converged: {clients:?}");
        }
        assert_eq!(
            clients.values().sum::<u64>(),
            8 * 380 + 20,
            "rebalancing must not create or drop connections"
        );
        assert!(clients["fresh"] >= 300, "fresh node filled up");
    }

    #[test]
    fn admin_bits_persist_but_drain_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nodes.json");
        {
            let reg = Registry::open(path.clone(), Duration::from_secs(15)).unwrap();
            reg.heartbeat(hb("a", 0), "1.1.1.1".into());
            reg.set_disabled("a", true).unwrap();
            reg.set_note("a", " hk-1 ").unwrap();
            reg.set_drain("a", true);
            assert!(!reg.set_disabled("ghost", true).unwrap());
        }
        let reg = Registry::open(path.clone(), Duration::from_secs(15)).unwrap();
        reg.heartbeat(hb("a", 0), "1.1.1.1".into());
        let view = &reg.admin_list()[0];
        assert!(view.disabled);
        assert_eq!(view.note, "hk-1");
        assert!(!view.desired_drain);

        assert!(reg.forget("a").unwrap());
        assert!(reg.admin_list().is_empty());
        let reg = Registry::open(path, Duration::from_secs(15)).unwrap();
        reg.heartbeat(hb("a", 0), "1.1.1.1".into());
        assert!(
            !reg.admin_list()[0].disabled,
            "forget drops persisted bits too"
        );
    }
}
