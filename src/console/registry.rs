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
}

#[derive(Clone)]
pub struct Node {
    pub heartbeat: Heartbeat,
    pub remote_ip: String,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub desired_drain: bool,
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
}

pub struct HeartbeatOutcome {
    pub first_seen: bool,
    pub restarted: bool,
    pub desired_drain: bool,
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
        };
        let node = inner.nodes.entry(hb.node_id.clone()).or_insert_with(|| {
            outcome.first_seen = true;
            Node {
                heartbeat: Heartbeat::default(),
                remote_ip: remote_ip.clone(),
                first_seen_ms: now,
                last_seen_ms: now,
                desired_drain: false,
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
        outcome
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
        nodes.sort_by(|a, b| {
            b.weight
                .cmp(&a.weight)
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
