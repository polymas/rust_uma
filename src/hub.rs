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

pub struct FrameHub {
    capacity: usize,
    frames: RwLock<VecDeque<Arc<WireFrame>>>,
    latest_tx: watch::Sender<u64>,
}

impl FrameHub {
    pub fn new(capacity: usize) -> Self {
        let (latest_tx, _) = watch::channel(0);
        Self {
            capacity: capacity.max(1),
            frames: RwLock::new(VecDeque::with_capacity(capacity.min(8192))),
            latest_tx,
        }
    }

    pub fn publish(&self, frame: Arc<WireFrame>) {
        let latest = frame.last_sequence;
        let mut frames = self.frames.write().unwrap_or_else(|e| e.into_inner());
        frames.push_back(frame);
        while frames.len() > self.capacity {
            frames.pop_front();
        }
        drop(frames);
        self.latest_tx.send_replace(latest);
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.latest_tx.subscribe()
    }

    pub fn latest_sequence(&self) -> u64 {
        *self.latest_tx.borrow()
    }

    pub fn after(&self, sequence: u64) -> Result<Vec<Arc<WireFrame>>, FrameReadError> {
        let frames = self.frames.read().unwrap_or_else(|e| e.into_inner());
        if let Some(oldest) = frames.front()
            && sequence > 0
            && sequence.saturating_add(1) < oldest.first_sequence
        {
            return Err(FrameReadError::Lagged);
        }
        Ok(frames
            .iter()
            .filter(|frame| frame.last_sequence > sequence)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::test_uma_event;

    fn record(sequence: u64, tx: u8) -> Arc<EventRecord> {
        Arc::new(EventRecord {
            sequence,
            event: test_uma_event(tx, 1),
            enrichment: None,
        })
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
