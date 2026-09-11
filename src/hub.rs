use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, RwLock},
};

use thiserror::Error;
use tokio::sync::watch;

use crate::{
    model::{EventKey, EventRecord},
    wire::WireFrame,
};

struct EventState {
    events: VecDeque<Arc<EventRecord>>,
    keys: HashSet<EventKey>,
}

pub struct EventHub {
    capacity: usize,
    state: RwLock<EventState>,
    latest_tx: watch::Sender<u64>,
}

impl EventHub {
    pub fn new(capacity: usize) -> Self {
        let (latest_tx, _) = watch::channel(0);
        Self {
            capacity: capacity.max(1),
            state: RwLock::new(EventState {
                events: VecDeque::with_capacity(capacity.min(65_536)),
                keys: HashSet::with_capacity(capacity.min(65_536)),
            }),
            latest_tx,
        }
    }

    pub fn contains(&self, key: &EventKey) -> bool {
        self.state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys
            .contains(key)
    }

    pub fn insert(&self, event: Arc<EventRecord>) -> bool {
        let key = event.key();
        let sequence = event.sequence;
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        if !state.keys.insert(key) {
            return false;
        }
        state.events.push_back(event);
        while state.events.len() > self.capacity {
            if let Some(removed) = state.events.pop_front() {
                state.keys.remove(&removed.key());
            }
        }
        drop(state);
        self.latest_tx.send_replace(sequence);
        true
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.latest_tx.subscribe()
    }

    pub fn latest_sequence(&self) -> u64 {
        *self.latest_tx.borrow()
    }

    pub fn bounds(&self) -> (u64, u64) {
        let state = self.state.read().unwrap_or_else(|e| e.into_inner());
        (
            state
                .events
                .front()
                .map(|event| event.sequence)
                .unwrap_or_default(),
            state
                .events
                .back()
                .map(|event| event.sequence)
                .unwrap_or_default(),
        )
    }

    pub fn snapshot(&self) -> Vec<Arc<EventRecord>> {
        self.state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .events
            .iter()
            .cloned()
            .collect()
    }
}

#[derive(Debug, Error)]
pub enum FrameReadError {
    #[error("requested sequence is older than the frame ring")]
    Lagged,
}

struct FrameRing {
    frames: VecDeque<Arc<WireFrame>>,
    /// Highest event sequence that was broadcast but can no longer be
    /// replayed: the last frame evicted from the ring, or — right after a
    /// restart — the last broadcast sequence recovered from the WAL (those
    /// frames belonged to the previous process). A cursor below this has
    /// genuinely missed frames.
    ///
    /// Why not "cursor older than the oldest frame's `first_sequence`": event
    /// sequences also advance for enrichment misses, which never produce a
    /// frame, and for everything before a restart. The oldest frame's
    /// `first_sequence` can sit well past a cursor that missed nothing, and
    /// that check turned such cursors into 1013 — e.g. the console's panel
    /// feed was rejected ~50 times in a row after the 2026-09-10 restart, and
    /// an edge that got 1013 redials *without* its cursor and skips frames.
    replay_floor: u64,
}

pub struct FrameHub {
    capacity: usize,
    ring: RwLock<FrameRing>,
    latest_tx: watch::Sender<u64>,
}

impl FrameHub {
    pub fn new(capacity: usize) -> Self {
        Self::resuming_after(capacity, 0)
    }

    /// `last_broadcast_sequence`: the newest sequence the previous process
    /// broadcast (the newest *enriched* event recovered from the WAL). Cursors
    /// at or past it can resume here; older ones get `Lagged`.
    pub fn resuming_after(capacity: usize, last_broadcast_sequence: u64) -> Self {
        let (latest_tx, _) = watch::channel(0);
        Self {
            capacity: capacity.max(1),
            ring: RwLock::new(FrameRing {
                frames: VecDeque::with_capacity(capacity.min(8192)),
                replay_floor: last_broadcast_sequence,
            }),
            latest_tx,
        }
    }

    pub fn publish(&self, frame: Arc<WireFrame>) {
        let latest = frame.last_sequence;
        let mut ring = self.ring.write().unwrap_or_else(|e| e.into_inner());
        ring.frames.push_back(frame);
        while ring.frames.len() > self.capacity {
            if let Some(evicted) = ring.frames.pop_front() {
                ring.replay_floor = ring.replay_floor.max(evicted.last_sequence);
            }
        }
        drop(ring);
        self.latest_tx.send_replace(latest);
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.latest_tx.subscribe()
    }

    pub fn latest_sequence(&self) -> u64 {
        *self.latest_tx.borrow()
    }

    /// Frames with any event after `sequence`. `0` means "everything the ring
    /// has" (edges warm up with it).
    pub fn after(&self, sequence: u64) -> Result<Vec<Arc<WireFrame>>, FrameReadError> {
        let ring = self.ring.read().unwrap_or_else(|e| e.into_inner());
        if sequence > 0 && sequence < ring.replay_floor {
            return Err(FrameReadError::Lagged);
        }
        Ok(ring
            .frames
            .iter()
            .filter(|frame| frame.last_sequence > sequence)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PriceOutcome, test_uma_event};

    fn record(sequence: u64, tx: u8) -> Arc<EventRecord> {
        Arc::new(EventRecord {
            sequence,
            event: test_uma_event(tx, 1),
            enrichment: None,
            price_outcome: PriceOutcome::Unspecified,
        })
    }

    fn frame(first: u64, last: u64) -> Arc<WireFrame> {
        Arc::new(WireFrame {
            batch_sequence: first,
            first_sequence: first,
            last_sequence: last,
            bytes: bytes::Bytes::new(),
        })
    }

    fn sequences(frames: &[Arc<WireFrame>]) -> Vec<u64> {
        frames.iter().map(|f| f.first_sequence).collect()
    }

    /// Restart shape seen on 2026-09-10: the previous process broadcast up to
    /// #100, then #101 was an enrichment miss (sequence consumed, no frame),
    /// so the first frame after the restart starts at #102. A client resuming
    /// from #100 missed nothing and must not get 1013.
    #[test]
    fn cursor_in_a_miss_gap_after_restart_resumes() {
        let hub = FrameHub::resuming_after(8, 100);
        hub.publish(frame(102, 102));
        hub.publish(frame(103, 104));
        assert_eq!(sequences(&hub.after(100).unwrap()), vec![102, 103]);
        assert_eq!(sequences(&hub.after(101).unwrap()), vec![102, 103]);
        assert_eq!(sequences(&hub.after(102).unwrap()), vec![103]);
    }

    #[test]
    fn cursor_before_the_previous_process_broadcast_is_lagged() {
        let hub = FrameHub::resuming_after(8, 100);
        hub.publish(frame(102, 102));
        assert!(matches!(hub.after(99), Err(FrameReadError::Lagged)));
        // 0 = warm-up from whatever the ring holds, never lagged.
        assert_eq!(sequences(&hub.after(0).unwrap()), vec![102]);
    }

    #[test]
    fn eviction_raises_the_replay_floor() {
        let hub = FrameHub::new(2);
        hub.publish(frame(1, 2));
        hub.publish(frame(4, 4)); // #3 was a miss
        hub.publish(frame(5, 6)); // evicts 1..=2
        assert!(matches!(hub.after(1), Err(FrameReadError::Lagged)));
        assert_eq!(sequences(&hub.after(2).unwrap()), vec![4, 5]);
        assert_eq!(sequences(&hub.after(3).unwrap()), vec![4, 5]);
        hub.publish(frame(7, 7)); // evicts #4
        assert!(matches!(hub.after(3), Err(FrameReadError::Lagged)));
        assert_eq!(sequences(&hub.after(4).unwrap()), vec![5, 7]);
    }

    #[test]
    fn event_ring_deduplicates_and_evicts() {
        let hub = EventHub::new(2);
        assert!(hub.insert(record(1, 1)));
        assert!(!hub.insert(record(2, 1)));
        assert!(hub.insert(record(2, 2)));
        assert!(hub.insert(record(3, 3)));
        assert_eq!(hub.bounds(), (2, 3));
    }
}
