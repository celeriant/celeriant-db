use std::fmt;
use std::hash::{Hash, Hasher};

use bincode::de::{BorrowDecoder, Decoder};
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use bincode::{BorrowDecode, Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

/// High-performance composite key for aggregate tracking
/// Optimized for hashing and comparison operations
#[derive(Clone, PartialEq, Eq, Serialize, DeepSizeOf)]
pub struct AggregateKey {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    // Pre-computed hash for better performance — skipped in serde, recomputed on deserialize
    #[serde(skip)]
    hash: u64,
}

impl<'de> serde::Deserialize<'de> for AggregateKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct AggregateKeyFields {
            org_id: u128,
            aggregate_type_id: u128,
            aggregate_id: u128,
        }
        let fields = AggregateKeyFields::deserialize(deserializer)?;
        Ok(Self::new(fields.org_id, fields.aggregate_type_id, fields.aggregate_id))
    }
}

impl AggregateKey {
    // Wire format layout (bincode fixed-int encoding)
    // Note: hash field is NOT serialized (computed on decode)
    // Update these if field order or types change!

    const WIRE_SIZE_ORG_ID: usize = 16;
    const WIRE_SIZE_AGGREGATE_TYPE_ID: usize = 16;
    const WIRE_SIZE_AGGREGATE_ID: usize = 16;

    pub const OFFSET_ORG_ID: usize = 0;

    pub const OFFSET_AGGREGATE_TYPE_ID: usize = 
        Self::OFFSET_ORG_ID + Self::WIRE_SIZE_ORG_ID;

    pub const OFFSET_AGGREGATE_ID: usize = 
        Self::OFFSET_AGGREGATE_TYPE_ID + Self::WIRE_SIZE_AGGREGATE_TYPE_ID;

    /// Total wire size of AggregateKey (hash is not serialized)
    pub const WIRE_SIZE_TOTAL: usize = 
        Self::OFFSET_AGGREGATE_ID + Self::WIRE_SIZE_AGGREGATE_ID; // = 48 bytes
        
    pub fn new(org_id: u128, aggregate_type_id: u128, aggregate_id: u128) -> Self {
        let hash = Self::compute_hash(org_id, aggregate_type_id, aggregate_id);
        Self {
            org_id,
            aggregate_type_id,
            aggregate_id,
            hash,
        }
    }

    /// Stable hash used as the persisted bloom-filter key. xxh3-64 is a frozen
    /// spec (cross-version AND cross-arch deterministic), unlike `DefaultHasher`
    /// whose output std does not guarantee stable; critical because the bloom
    /// bits are written to disk and trusted on read.
    #[inline]
    pub fn bloom_hash(&self) -> u64 {
        self.hash
    }

    fn compute_hash(org_id: u128, aggregate_type_id: u128, aggregate_id: u128) -> u64 {
        let mut buf = [0u8; 48];
        buf[0..16].copy_from_slice(&org_id.to_le_bytes());
        buf[16..32].copy_from_slice(&aggregate_type_id.to_le_bytes());
        buf[32..48].copy_from_slice(&aggregate_id.to_le_bytes());
        xxhash_rust::xxh3::xxh3_64(&buf)
    }
}


impl Encode for AggregateKey {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.org_id.encode(encoder)?;
        self.aggregate_type_id.encode(encoder)?;
        self.aggregate_id.encode(encoder)?;
        Ok(())
    }
}

impl<Context> Decode<Context> for AggregateKey {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let org_id = <u128 as Decode<Context>>::decode(decoder)?;
        let aggregate_type_id = <u128 as Decode<Context>>::decode(decoder)?;
        let aggregate_id = <u128 as Decode<Context>>::decode(decoder)?;
        Ok(Self::new(org_id, aggregate_type_id, aggregate_id))
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for AggregateKey {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let org_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        let aggregate_type_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        let aggregate_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        Ok(Self::new(org_id, aggregate_type_id, aggregate_id))
    }
}

impl Hash for AggregateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use pre-computed hash for better performance
        state.write_u64(self.hash);
    }
}

impl fmt::Display for AggregateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", crate::format_uuid(self.org_id), crate::format_uuid(self.aggregate_type_id), crate::format_uuid(self.aggregate_id))
    }
}

impl fmt::Debug for AggregateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AggregateKey({})", self)
    }
}

impl Default for AggregateKey {
    fn default() -> Self {
        Self::new(0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_hash_deterministic_for_same_fields() {
        assert_eq!(AggregateKey::new(1, 2, 3).bloom_hash(), AggregateKey::new(1, 2, 3).bloom_hash());
        assert_ne!(AggregateKey::new(1, 2, 3).bloom_hash(), AggregateKey::new(1, 2, 4).bloom_hash());
    }

    /// FROZEN persisted-format pin: the bloom hash is xxh3-64 over a fixed field
    /// layout and is written to disk, so its exact value must never change across
    /// Rust versions, xxhash-crate versions, or CPU arch. A trip here means a hash
    /// change is about to silently invalidate every persisted bloom.
    #[test]
    fn bloom_hash_value_is_pinned() {
        assert_eq!(AggregateKey::new(1, 2, 3).bloom_hash(), 10086399049413766308);
    }
}