//! 固定容量帧环。`offset` 单调自增（跨帧连续），`event_sequence` 对压缩解析
//! 失败的帧继承上一帧，这样 `after_sequence` 续连的游标总是有意义。

use std::{collections::VecDeque, sync::Arc};

use bytes::Bytes;

pub struct Frame {
    pub offset: u64,
    pub batch_sequence: u64,
    pub event_sequence: u64,
    pub bytes: Bytes,
    pub recv_at_us: u64,
}

pub struct Ring {
    frames: VecDeque<Arc<Frame>>,
    capacity: usize,
    next_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RingStats {
    pub count: usize,
    pub oldest_offset: u64,
    pub latest_offset: u64,
}

impl Ring {
    pub fn new(capacity: usize) -> Self {
        Self {
            frames: VecDeque::with_capacity(capacity.max(1)),
            capacity: capacity.max(1),
            next_offset: 1,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn push(&mut self, mut frame: Frame) -> Arc<Frame> {
        frame.offset = self.next_offset;
        self.next_offset += 1;
        if self.frames.len() == self.capacity {
            self.frames.pop_front();
        }
        let frame = Arc::new(frame);
        self.frames.push_back(frame.clone());
        frame
    }

    pub fn tail(&self, n: usize) -> Vec<Arc<Frame>> {
        let n = n.min(self.frames.len());
        self.frames
            .iter()
            .skip(self.frames.len() - n)
            .cloned()
            .collect()
    }

    pub fn last_event_sequence(&self) -> u64 {
        self.frames.back().map(|f| f.event_sequence).unwrap_or(0)
    }

    pub fn stats(&self) -> RingStats {
        RingStats {
            count: self.frames.len(),
            oldest_offset: self.frames.front().map(|f| f.offset).unwrap_or(0),
            latest_offset: self.frames.back().map(|f| f.offset).unwrap_or(0),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_frame(event_sequence: u64) -> Frame {
    Frame {
        offset: 0,
        batch_sequence: event_sequence,
        event_sequence,
        bytes: Bytes::from(event_sequence.to_be_bytes().to_vec()),
        recv_at_us: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_and_tails() {
        let mut ring = Ring::new(3);
        assert_eq!(ring.stats(), RingStats::default());
        assert!(ring.tail(5).is_empty());
        for seq in 1..=5 {
            ring.push(test_frame(seq));
        }
        let stats = ring.stats();
        assert_eq!(stats.count, 3);
        assert_eq!(stats.oldest_offset, 3);
        assert_eq!(stats.latest_offset, 5);
        let tail: Vec<u64> = ring.tail(2).iter().map(|f| f.event_sequence).collect();
        assert_eq!(tail, [4, 5]);
        assert_eq!(ring.tail(10).len(), 3);
        assert_eq!(ring.last_event_sequence(), 5);
    }
}
