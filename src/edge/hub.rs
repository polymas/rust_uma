//! 订阅者表 + 非阻塞扇出。这是 edge 的热路径核心：
//! `publish` 只做 ring.push、Arc clone、try_send，绝不 await 客户端；
//! 队列满即摘除并标 1008。`subscribe` 在同一把锁内取回放快照并注册，保证
//! 回放与实时之间不重不漏。

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
    time::Instant,
};

use serde::Serialize;
use tokio::sync::mpsc;

use super::{
    EdgeStats,
    ring::{Frame, Ring, RingStats},
};

pub const CLOSE_NORMAL: u16 = 1000;
pub const CLOSE_POLICY: u16 = 1008;
pub const CLOSE_RESTART: u16 = 1012;

#[derive(Clone, Serialize)]
pub struct ClientInfo {
    pub id: u64,
    pub ip: String,
    pub port: u16,
    pub token_id: Option<String>,
    pub connected_at_ms: u64,
}

struct Subscriber {
    info: ClientInfo,
    tx: mpsc::Sender<Arc<Frame>>,
    close_code: Arc<AtomicU16>,
}

pub struct Subscription {
    pub id: u64,
    pub rx: mpsc::Receiver<Arc<Frame>>,
    pub replay: Vec<Arc<Frame>>,
    /// Read after `rx` yields `None`: why the hub closed the channel.
    pub close_code: Arc<AtomicU16>,
}

struct Inner {
    ring: Ring,
    subs: HashMap<u64, Subscriber>,
    next_id: u64,
}

pub struct Hub {
    inner: Mutex<Inner>,
    queue: usize,
    stats: Arc<EdgeStats>,
}

impl Hub {
    pub fn new(ring_capacity: usize, queue: usize, stats: Arc<EdgeStats>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                ring: Ring::new(ring_capacity),
                subs: HashMap::new(),
                next_id: 1,
            }),
            queue: queue.max(1),
            stats,
        }
    }

    /// Hot path. Returns the frame as stored (with its ring offset).
    pub fn publish(&self, frame: Frame) -> Arc<Frame> {
        let started = Instant::now();
        let mut inner = self.lock();
        let frame = inner.ring.push(frame);
        let mut evicted: Vec<u64> = Vec::new();
        let mut delivered = 0u64;
        let mut slow = 0u64;
        for (id, sub) in inner.subs.iter() {
            match sub.tx.try_send(frame.clone()) {
                Ok(()) => delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.close_code.store(CLOSE_POLICY, Ordering::Relaxed);
                    slow += 1;
                    evicted.push(*id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => evicted.push(*id),
            }
        }
        for id in evicted {
            inner.subs.remove(&id);
        }
        drop(inner);
        self.stats
            .deliveries
            .fetch_add(delivered, Ordering::Relaxed);
        if slow > 0 {
            self.stats
                .slow_clients_disconnected
                .fetch_add(slow, Ordering::Relaxed);
            tracing::warn!(slow, "evicted slow clients (queue full)");
        }
        self.stats
            .record_fanout(started.elapsed().as_micros() as u64);
        frame
    }

    /// Snapshot + register under one lock: a frame published after this call
    /// is either in `replay` or will arrive on `rx`, never both, never neither.
    pub fn subscribe(
        &self,
        replay: usize,
        ip: String,
        port: u16,
        token_id: Option<String>,
    ) -> Subscription {
        let (tx, rx) = mpsc::channel(self.queue);
        let close_code = Arc::new(AtomicU16::new(CLOSE_NORMAL));
        let mut inner = self.lock();
        let id = inner.next_id;
        inner.next_id += 1;
        let replay = inner.ring.tail(replay);
        inner.subs.insert(
            id,
            Subscriber {
                info: ClientInfo {
                    id,
                    ip,
                    port,
                    token_id,
                    connected_at_ms: super::now_ms(),
                },
                tx,
                close_code: close_code.clone(),
            },
        );
        Subscription {
            id,
            rx,
            replay,
            close_code,
        }
    }

    pub fn unsubscribe(&self, id: u64) {
        self.lock().subs.remove(&id);
    }

    /// Drops up to `n` subscribers with `code`; their writers see `None` after
    /// draining what is already queued, then send the close frame.
    pub fn release(&self, n: usize, code: u16) -> usize {
        let mut inner = self.lock();
        let ids: Vec<u64> = inner.subs.keys().take(n).copied().collect();
        for id in &ids {
            if let Some(sub) = inner.subs.remove(id) {
                sub.close_code.store(code, Ordering::Relaxed);
            }
        }
        ids.len()
    }

    /// Kicks every subscriber whose token id fails `keep` (token disabled or
    /// deleted on the console). Subscribers without a token id (static env
    /// tokens) are left alone.
    pub fn kick_tokens(&self, keep: impl Fn(&str) -> bool) -> usize {
        let mut inner = self.lock();
        let ids: Vec<u64> = inner
            .subs
            .iter()
            .filter(|(_, s)| s.info.token_id.as_deref().is_some_and(|t| !keep(t)))
            .map(|(id, _)| *id)
            .collect();
        for id in &ids {
            if let Some(sub) = inner.subs.remove(id) {
                sub.close_code.store(CLOSE_POLICY, Ordering::Relaxed);
            }
        }
        ids.len()
    }

    pub fn clients(&self) -> usize {
        self.lock().subs.len()
    }

    pub fn client_list(&self) -> Vec<ClientInfo> {
        let inner = self.lock();
        let mut list: Vec<ClientInfo> = inner.subs.values().map(|s| s.info.clone()).collect();
        list.sort_by_key(|c| c.id);
        list
    }

    pub fn clients_by_token(&self) -> BTreeMap<String, u64> {
        let inner = self.lock();
        let mut map = BTreeMap::new();
        for sub in inner.subs.values() {
            if let Some(token) = &sub.info.token_id {
                *map.entry(token.clone()).or_insert(0) += 1;
            }
        }
        map
    }

    pub fn ring_stats(&self) -> (RingStats, usize) {
        let inner = self.lock();
        (inner.ring.stats(), inner.ring.capacity())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::ring::test_frame;

    fn hub(queue: usize) -> (Hub, Arc<EdgeStats>) {
        let stats = Arc::new(EdgeStats::default());
        (Hub::new(16, queue, stats.clone()), stats)
    }

    #[test]
    fn replay_then_live_is_gapless() {
        let (hub, _) = hub(8);
        for seq in 1..=10 {
            hub.publish(test_frame(seq));
        }
        let mut sub = hub.subscribe(5, "1.1.1.1".into(), 1, None);
        let replay: Vec<u64> = sub.replay.iter().map(|f| f.event_sequence).collect();
        assert_eq!(replay, [6, 7, 8, 9, 10]);
        hub.publish(test_frame(11));
        let live = sub.rx.try_recv().unwrap();
        assert_eq!(live.event_sequence, 11);
        assert_eq!(live.offset, sub.replay.last().unwrap().offset + 1);
    }

    #[test]
    fn slow_client_is_evicted_with_1008_and_others_unaffected() {
        let (hub, stats) = hub(2);
        let mut slow = hub.subscribe(0, "1.1.1.1".into(), 1, Some("t".into()));
        let mut fast = hub.subscribe(0, "1.1.1.2".into(), 2, None);
        // fast keeps up (drains after every publish); slow never reads.
        hub.publish(test_frame(1));
        fast.rx.try_recv().unwrap();
        hub.publish(test_frame(2));
        fast.rx.try_recv().unwrap();
        assert_eq!(hub.clients(), 2);
        hub.publish(test_frame(3));
        assert_eq!(fast.rx.try_recv().unwrap().event_sequence, 3);
        assert_eq!(hub.clients(), 1);
        assert_eq!(stats.slow_clients_disconnected.load(Ordering::Relaxed), 1);
        assert_eq!(stats.deliveries.load(Ordering::Relaxed), 5);
        // slow: two queued frames, then the channel closes with 1008
        assert!(slow.rx.try_recv().is_ok());
        assert!(slow.rx.try_recv().is_ok());
        assert!(slow.rx.try_recv().is_err());
        assert_eq!(slow.close_code.load(Ordering::Relaxed), CLOSE_POLICY);
        assert_eq!(fast.close_code.load(Ordering::Relaxed), CLOSE_NORMAL);
        assert!(hub.clients_by_token().is_empty());
    }

    #[test]
    fn release_uses_1012_and_kick_tokens_uses_1008() {
        let (hub, _) = hub(4);
        let a = hub.subscribe(0, "1.1.1.1".into(), 1, Some("keep".into()));
        let b = hub.subscribe(0, "1.1.1.2".into(), 2, Some("gone".into()));
        let c = hub.subscribe(0, "1.1.1.3".into(), 3, None);
        assert_eq!(hub.clients_by_token().get("keep"), Some(&1));
        assert_eq!(hub.kick_tokens(|t| t == "keep"), 1);
        assert_eq!(b.close_code.load(Ordering::Relaxed), CLOSE_POLICY);
        assert_eq!(hub.clients(), 2);
        assert_eq!(hub.release(10, CLOSE_RESTART), 2);
        assert_eq!(a.close_code.load(Ordering::Relaxed), CLOSE_RESTART);
        assert_eq!(c.close_code.load(Ordering::Relaxed), CLOSE_RESTART);
        assert_eq!(hub.clients(), 0);
        assert_eq!(hub.client_list().len(), 0);
    }
}
