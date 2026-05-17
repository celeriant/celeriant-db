use std::collections::VecDeque;

use crate::recent_write::RecentWrite;


/// Contiguous cache of recent writes for a single aggregate.
/// Aggregate versions are monotonic with no gaps, so we use a VecDeque
/// with a tracked starting index for O(1) access.
pub struct AggregateRecentWrites {
    pub first_version: u64,
    pub writes: VecDeque<RecentWrite>,
}

impl AggregateRecentWrites {
    pub fn new(first_version: u64) -> Self {
        Self {
            first_version,
            writes: VecDeque::new(),
        }
    }

    /// Get a write by version
    pub fn get(&self, aggregate_version: u64) -> Option<&RecentWrite> {
        if aggregate_version < self.first_version {
            return None;
        }
        let offset = (aggregate_version - self.first_version) as usize;
        self.writes.get(offset)
    }

    /// Iterate from a starting aggregate version, yielding (aggregate_version, &RecentWrite)
    pub fn iter_from(&self, from_version: u64) -> impl Iterator<Item = (u64, &RecentWrite)> {
        let start = from_version.max(self.first_version);
        let skip = (start - self.first_version) as usize;
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
            self.first_version += 1;
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