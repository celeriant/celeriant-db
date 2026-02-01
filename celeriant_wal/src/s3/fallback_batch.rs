use bincode::{Decode, Encode};
use crate::datablocks::datablock::Datablock;
use crate::metablocks::metablock::Metablock;

#[derive(Debug, Clone, Encode, Decode)]
pub struct FallbackBatch {
    pub fallback_index: u64,
    pub end_wal_index: u64,
    pub shard_id: u32,
    pub items: Vec<FallbackItem>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct FallbackItem {
    pub metablock: Metablock,
    pub datablock: Option<Datablock>,
}

impl FallbackBatch {
    pub fn new(fallback_index: u64, end_wal_index: u64, shard_id: u32) -> Self {
        Self {
            fallback_index,
            end_wal_index,
            shard_id,
            items: Vec::new(),
        }
    }

    pub fn push_item(&mut self, item: FallbackItem) {
        self.items.push(item);
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
}
