use std::collections::VecDeque;

use crate::recent_write::RecentWrite;


/// Contiguous cache of recent writes for a single aggregate.
/// Batch indexes are monotonic with no gaps, so we use a VecDeque
/// with a tracked starting index for O(1) access.
pub struct AggregateRecentWrites {
    pub first_batch_index: u64,
    pub writes: VecDeque<RecentWrite>,
}

impl AggregateRecentWrites {
    pub fn new(first_batch_index: u64) -> Self {
        Self {
            first_batch_index,
            writes: VecDeque::new(),
        }
    }

    /// Get a write by batch index
    pub fn get(&self, batch_index: u64) -> Option<&RecentWrite> {
        if batch_index < self.first_batch_index {
            return None;
        }
        let offset = (batch_index - self.first_batch_index) as usize;
        self.writes.get(offset)
    }

    /// Iterate from a starting batch index, yielding (batch_index, &RecentWrite)
    pub fn iter_from(&self, from_batch_index: u64) -> impl Iterator<Item = (u64, &RecentWrite)> {
        let start = from_batch_index.max(self.first_batch_index);
        let skip = (start - self.first_batch_index) as usize;
        let base_index = start;
        
        self.writes
            .iter()
            .skip(skip)
            .enumerate()
            .map(move |(i, write)| (base_index + i as u64, write))
    }

    /// Remove the oldest entry, returns true if something was removed
    pub fn pop_front(&mut self) -> bool {
        if self.writes.pop_front().is_some() {
            self.first_batch_index += 1;
            true
        } else {
            false
        }
    }

    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    pub fn push(&mut self, write: RecentWrite) {
        self.writes.push_back(write);
    }
}