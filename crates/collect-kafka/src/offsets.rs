//! Offset bookkeeping for at-least-once delivery.
//!
//! Auto-commit is disabled on the consumer; offsets are committed only after
//! the batch containing a message has been durably written to local disk by
//! collect-core. The moving parts:
//!
//! - The forwarder task records, per Kafka message, how many text lines it
//!   will produce ([`PendingLines`]).
//! - [`OffsetTracker::on_line`] is called for every line the ingest loop
//!   consumes, pairing lines back to message offsets. A message's offset is
//!   applied one line late ("deferred") because collect-core may seal a batch
//!   *before* the line currently being processed is added to it; deferring
//!   keeps sealed snapshots conservative (never over-commit, at most one
//!   message replayed after a crash).
//! - [`OffsetTracker::seal`] snapshots consumed offsets per batch sequence
//!   number; [`OffsetTracker::durable`] advances through contiguous durable
//!   batches (write workers may finish out of order) and returns offsets that
//!   are safe to commit.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct PendingMessage {
    pub partition: i32,
    pub offset: i64,
    pub lines: u32,
}

/// Queue of message line-counts, pushed by the forwarder task in send order
/// and popped by the ingest loop in read order.
#[derive(Clone, Default)]
pub struct PendingLines {
    queue: Arc<Mutex<VecDeque<PendingMessage>>>,
}

impl PendingLines {
    pub fn push(&self, partition: i32, offset: i64, lines: u32) {
        self.queue
            .lock()
            .expect("pending lines lock poisoned")
            .push_back(PendingMessage {
                partition,
                offset,
                lines,
            });
    }

    fn pop(&self) -> Option<PendingMessage> {
        self.queue
            .lock()
            .expect("pending lines lock poisoned")
            .pop_front()
    }
}

pub struct OffsetTracker {
    pending: PendingLines,
    /// Message currently being consumed line by line.
    current: Option<PendingMessage>,
    /// Completed message whose offset is applied on the *next* line, so a
    /// batch sealed before the current line is pushed never includes it.
    deferred: Option<(i32, i64)>,
    /// Highest offset per partition fully consumed into the batch buffer.
    consumed: HashMap<i32, i64>,
    /// Snapshots of `consumed` per sealed batch, oldest first.
    sealed: VecDeque<(u64, HashMap<i32, i64>)>,
    /// Durable batch sequence numbers beyond the contiguous frontier.
    durable: BTreeSet<u64>,
    /// Every batch with seq <= this value is durable on disk.
    durable_through: u64,
}

impl OffsetTracker {
    pub fn new(pending: PendingLines) -> Self {
        Self {
            pending,
            current: None,
            deferred: None,
            consumed: HashMap::new(),
            sealed: VecDeque::new(),
            durable: BTreeSet::new(),
            durable_through: 0,
        }
    }

    /// Swap in the pending queue of a freshly opened stream. Any partially
    /// consumed message from the previous stream is abandoned; its offset was
    /// never marked consumed, so it will be re-delivered after the reconnect.
    pub fn reset_stream(&mut self, pending: PendingLines) {
        self.pending = pending;
        self.current = None;
    }

    /// Record that one line was consumed by the ingest loop.
    pub fn on_line(&mut self) {
        if let Some((partition, offset)) = self.deferred.take() {
            let entry = self.consumed.entry(partition).or_insert(offset);
            if *entry < offset {
                *entry = offset;
            }
        }

        if self.current.is_none() {
            self.current = self.pending.pop();
        }

        if let Some(message) = &mut self.current {
            message.lines = message.lines.saturating_sub(1);
            if message.lines == 0 {
                self.deferred = Some((message.partition, message.offset));
                self.current = None;
            }
        }
    }

    /// Record that a batch was sealed and queued for writing.
    pub fn seal(&mut self, seq: u64) {
        if !self.consumed.is_empty() {
            self.sealed.push_back((seq, self.consumed.clone()));
        }
    }

    /// Record that a batch is durable on disk. Returns per-partition offsets
    /// (highest consumed, inclusive) that are now safe to commit, if the
    /// contiguous durable frontier advanced past one or more sealed snapshots.
    pub fn durable(&mut self, seq: u64) -> Option<HashMap<i32, i64>> {
        self.durable.insert(seq);
        while self.durable.remove(&(self.durable_through + 1)) {
            self.durable_through += 1;
        }

        let mut commit = None;
        while let Some((sealed_seq, _)) = self.sealed.front() {
            if *sealed_seq <= self.durable_through {
                commit = self.sealed.pop_front().map(|(_, snapshot)| snapshot);
            } else {
                break;
            }
        }
        commit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker_with(messages: &[(i32, i64, u32)]) -> OffsetTracker {
        let pending = PendingLines::default();
        for &(partition, offset, lines) in messages {
            pending.push(partition, offset, lines);
        }
        OffsetTracker::new(pending)
    }

    #[test]
    fn defers_latest_message_and_commits_after_durable() {
        let mut tracker = tracker_with(&[(0, 10, 1), (0, 11, 1), (0, 12, 1)]);
        tracker.on_line();
        tracker.on_line();
        tracker.on_line();

        // Offset 12 completed on the most recent line and stays deferred.
        tracker.seal(1);
        let offsets = tracker.durable(1).expect("offsets ready to commit");
        assert_eq!(offsets.get(&0), Some(&11));
    }

    #[test]
    fn multi_line_messages_complete_only_after_all_lines() {
        let mut tracker = tracker_with(&[(0, 5, 3), (0, 6, 1)]);
        tracker.on_line();
        tracker.on_line();
        tracker.seal(1);
        assert!(tracker.durable(1).is_none(), "no message fully consumed");

        tracker.on_line(); // third line completes offset 5 (deferred)
        tracker.on_line(); // applies offset 5, consumes offset 6 (deferred)
        tracker.seal(2);
        let offsets = tracker.durable(2).expect("offset 5 committable");
        assert_eq!(offsets.get(&0), Some(&5));
    }

    #[test]
    fn out_of_order_durability_waits_for_contiguous_frontier() {
        let mut tracker = tracker_with(&[(0, 1, 1), (0, 2, 1), (0, 3, 1), (0, 4, 1)]);
        tracker.on_line();
        tracker.on_line();
        tracker.seal(1); // covers offset 1
        tracker.on_line();
        tracker.seal(2); // covers offsets 1-2
        tracker.on_line();
        tracker.seal(3); // covers offsets 1-3

        assert!(tracker.durable(2).is_none(), "seq 1 not durable yet");
        assert!(tracker.durable(3).is_none(), "seq 1 not durable yet");
        let offsets = tracker.durable(1).expect("frontier reaches seq 3");
        assert_eq!(offsets.get(&0), Some(&3));
    }

    #[test]
    fn tracks_partitions_independently() {
        let mut tracker = tracker_with(&[(0, 100, 1), (1, 200, 1), (0, 101, 1)]);
        tracker.on_line();
        tracker.on_line();
        tracker.on_line();
        tracker.seal(1);
        let offsets = tracker.durable(1).expect("offsets ready");
        assert_eq!(offsets.get(&0), Some(&100));
        assert_eq!(offsets.get(&1), Some(&200));
    }

    #[test]
    fn reset_stream_abandons_partial_message() {
        let mut tracker = tracker_with(&[(0, 7, 2)]);
        tracker.on_line(); // first of two lines
        tracker.reset_stream(PendingLines::default());
        tracker.on_line(); // no pending message in the new stream
        tracker.seal(1);
        assert!(
            tracker.durable(1).is_none(),
            "partially consumed message must not be committed"
        );
    }
}
