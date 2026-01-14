//! Hash chain for WAL entry integrity and divergence detection.
//!
//! Each WAL entry hash = blake3(previous_hash || wal_index || content).
//! Chain starts from genesis (all zeros). Divergence detected when follower
//! hash at index N != leader hash -> truncate and resync.

use bincode::{Decode, Encode};

/// Hash of a WAL entry in the chain.
pub type EntryHashBytes = [u8; 32];

/// Genesis hash (all zeros) for the start of the chain.
pub const GENESIS_HASH: EntryHashBytes = [0u8; 32];

pub fn compute_entry_hash(previous_hash: &EntryHashBytes, wal_index: u64, content: &[u8]) -> EntryHashBytes {
    let mut hasher = blake3::Hasher::new();
    hasher.update(previous_hash);
    hasher.update(&wal_index.to_le_bytes());
    hasher.update(content);
    *hasher.finalize().as_bytes()
}

/// Tracks the hash chain state for a single shard.
#[derive(Debug, Clone, Encode, Decode)]
pub struct HashChainState {
    /// Current hash at the tip of the chain
    pub current_hash: EntryHashBytes,
    /// WAL index of the tip entry (0 if at genesis)
    pub tip_wal_index: u64,
}

impl Default for HashChainState {
    fn default() -> Self {
        Self::genesis()
    }
}

impl HashChainState {
    /// Create a new chain at genesis.
    pub fn genesis() -> Self {
        Self {
            current_hash: GENESIS_HASH,
            tip_wal_index: 0,
        }
    }

    /// Advance the chain with a new entry.
    pub fn advance(&mut self, wal_index: u64, content: &[u8]) {
        debug_assert!(wal_index > self.tip_wal_index || self.tip_wal_index == 0);
        self.current_hash = compute_entry_hash(&self.current_hash, wal_index, content);
        self.tip_wal_index = wal_index;
    }

    /// Verify that the given hash matches expected for a WAL index.
    pub fn verify(&self, wal_index: u64, expected_hash: &EntryHashBytes) -> bool {
        wal_index == self.tip_wal_index && self.current_hash == *expected_hash
    }

    /// Reset chain to a known state (for recovery after divergence detection).
    pub fn reset_to(&mut self, wal_index: u64, hash: EntryHashBytes) {
        self.tip_wal_index = wal_index;
        self.current_hash = hash;
    }
}

/// A checkpoint in the hash chain, used for verification during replication.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct HashCheckpoint {
    pub wal_index: u64,
    pub hash: EntryHashBytes,
}

impl HashCheckpoint {
    pub fn new(wal_index: u64, hash: EntryHashBytes) -> Self {
        Self { wal_index, hash }
    }

    pub fn genesis() -> Self {
        Self {
            wal_index: 0,
            hash: GENESIS_HASH,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_genesis() {
        let state = HashChainState::genesis();
        assert_eq!(state.current_hash, GENESIS_HASH);
        assert_eq!(state.tip_wal_index, 0);
    }

    #[test]
    fn test_advance() {
        let mut state = HashChainState::genesis();
        let content = b"hello world";

        state.advance(1, content);
        let hash_1 = state.current_hash;
        assert_ne!(hash_1, GENESIS_HASH);
        assert_eq!(state.tip_wal_index, 1);

        state.advance(2, b"second entry");
        assert_ne!(state.current_hash, hash_1);
        assert_eq!(state.tip_wal_index, 2);
    }

    #[test]
    fn test_determinism() {
        let mut state1 = HashChainState::genesis();
        let mut state2 = HashChainState::genesis();

        state1.advance(1, b"same content");
        state2.advance(1, b"same content");

        assert_eq!(state1.current_hash, state2.current_hash);

        // Different content should produce different hash
        let mut state3 = HashChainState::genesis();
        state3.advance(1, b"different content");
        assert_ne!(state1.current_hash, state3.current_hash);
    }

    #[test]
    fn test_order_matters() {
        let mut state1 = HashChainState::genesis();
        state1.advance(1, b"first");
        state1.advance(2, b"second");

        // Compute expected hash manually to verify chain construction
        let direct_hash = compute_entry_hash(
            &compute_entry_hash(&GENESIS_HASH, 1, b"first"),
            2,
            b"second",
        );

        assert_eq!(state1.current_hash, direct_hash);
    }
}
