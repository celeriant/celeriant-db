use std::fmt;
use std::hash::{Hash, Hasher};

use bincode::de::{BorrowDecoder, Decoder};
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use bincode::{BorrowDecode, Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

/// Schema lookup key (cache key, WAL lookup key)
#[derive(Clone, PartialEq, Eq, Serialize, DeepSizeOf)]
pub struct SchemaKey {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub event_type_major: u64,
    pub event_type_minor: u64,
    // Pre-computed hash for better performance — skipped in serde, recomputed on deserialize
    #[serde(skip)]
    hash: u64,
}

impl<'de> serde::Deserialize<'de> for SchemaKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SchemaKeyFields {
            org_id: u128,
            aggregate_type_id: u128,
            event_type_major: u64,
            event_type_minor: u64,
        }
        let fields = SchemaKeyFields::deserialize(deserializer)?;
        Ok(Self::new(fields.org_id, fields.aggregate_type_id, fields.event_type_major, fields.event_type_minor))
    }
}

impl SchemaKey {
    // Wire format layout (bincode fixed-int encoding)
    // Note: hash field is NOT serialized (computed on decode)
    // Update these if field order or types change!

    const WIRE_SIZE_ORG_ID: usize = 16;
    const WIRE_SIZE_AGGREGATE_TYPE_ID: usize = 16;
    const WIRE_SIZE_EVENT_TYPE_MAJOR: usize = 8;
    const WIRE_SIZE_EVENT_TYPE_MINOR: usize = 8;

    pub const OFFSET_ORG_ID: usize = 0;

    pub const OFFSET_AGGREGATE_TYPE_ID: usize =
        Self::OFFSET_ORG_ID + Self::WIRE_SIZE_ORG_ID;

    pub const OFFSET_EVENT_TYPE_MAJOR: usize =
        Self::OFFSET_AGGREGATE_TYPE_ID + Self::WIRE_SIZE_AGGREGATE_TYPE_ID;

    pub const OFFSET_EVENT_TYPE_MINOR: usize =
        Self::OFFSET_EVENT_TYPE_MAJOR + Self::WIRE_SIZE_EVENT_TYPE_MAJOR;

    /// Total wire size of SchemaKey (hash is not serialized)
    pub const WIRE_SIZE_TOTAL: usize =
        Self::OFFSET_EVENT_TYPE_MINOR + Self::WIRE_SIZE_EVENT_TYPE_MINOR; // = 48 bytes

    pub fn new(org_id: u128, aggregate_type_id: u128, event_type_major: u64, event_type_minor: u64) -> Self {
        let hash = Self::compute_hash(org_id, aggregate_type_id, event_type_major, event_type_minor);
        Self {
            org_id,
            aggregate_type_id,
            event_type_major,
            event_type_minor,
            hash,
        }
    }

    /// Stable hash used as the persisted bloom-filter key. xxh3-64 is a frozen
    /// spec (cross-version AND cross-arch deterministic), unlike `DefaultHasher`.
    #[inline]
    pub fn bloom_hash(&self) -> u64 {
        self.hash
    }

    fn compute_hash(org_id: u128, aggregate_type_id: u128, event_type_major: u64, event_type_minor: u64) -> u64 {
        let mut buf = [0u8; 48];
        buf[0..16].copy_from_slice(&org_id.to_le_bytes());
        buf[16..32].copy_from_slice(&aggregate_type_id.to_le_bytes());
        buf[32..40].copy_from_slice(&event_type_major.to_le_bytes());
        buf[40..48].copy_from_slice(&event_type_minor.to_le_bytes());
        xxhash_rust::xxh3::xxh3_64(&buf)
    }
}

impl Encode for SchemaKey {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.org_id.encode(encoder)?;
        self.aggregate_type_id.encode(encoder)?;
        self.event_type_major.encode(encoder)?;
        self.event_type_minor.encode(encoder)?;
        Ok(())
    }
}

impl<Context> Decode<Context> for SchemaKey {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let org_id = <u128 as Decode<Context>>::decode(decoder)?;
        let aggregate_type_id = <u128 as Decode<Context>>::decode(decoder)?;
        let event_type_major = <u64 as Decode<Context>>::decode(decoder)?;
        let event_type_minor = <u64 as Decode<Context>>::decode(decoder)?;
        Ok(Self::new(org_id, aggregate_type_id, event_type_major, event_type_minor))
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for SchemaKey {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let org_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        let aggregate_type_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        let event_type_major = <u64 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        let event_type_minor = <u64 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        Ok(Self::new(org_id, aggregate_type_id, event_type_major, event_type_minor))
    }
}

impl Hash for SchemaKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for SchemaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaKey")
            .field("org_id", &self.org_id)
            .field("aggregate_type_id", &self.aggregate_type_id)
            .field("event_type_major", &self.event_type_major)
            .field("event_type_minor", &self.event_type_minor)
            .finish()
    }
}

impl Default for SchemaKey {
    fn default() -> Self {
        Self::new(0, 0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    #[test]
    fn wire_size_total_is_48() {
        assert_eq!(SchemaKey::WIRE_SIZE_TOTAL, 48);
    }

    #[test]
    fn bincode_round_trip_preserves_fields_and_recomputes_hash() {
        let cases = [
            (1u128, 2u128, 3u64, 4u64),
            (0, 0, 0, 0),
            (u128::MAX, u128::MAX, u64::MAX, u64::MAX),
            (0xDEAD, 0xBEEF, 42, 99),
        ];
        let config = bincode::config::standard().with_fixed_int_encoding();
        for (org, atype, major, minor) in cases {
            let key = SchemaKey::new(org, atype, major, minor);
            let encoded = bincode::encode_to_vec(&key, config).unwrap();
            assert_eq!(encoded.len(), SchemaKey::WIRE_SIZE_TOTAL);
            let (decoded, _): (SchemaKey, _) = bincode::decode_from_slice(&encoded, config).unwrap();
            assert_eq!(decoded, key);
            assert_eq!(decoded.bloom_hash(), key.bloom_hash());
        }
    }

    #[test]
    fn equal_fields_produce_equal_hash_trait_output() {
        let a = SchemaKey::new(10, 20, 30, 40);
        let b = SchemaKey::new(10, 20, 30, 40);
        let hash = |k: &SchemaKey| {
            let mut h = DefaultHasher::new();
            k.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn different_fields_are_not_equal() {
        let base = SchemaKey::new(1, 2, 3, 4);
        let variants = [
            SchemaKey::new(99, 2, 3, 4),
            SchemaKey::new(1, 99, 3, 4),
            SchemaKey::new(1, 2, 99, 4),
            SchemaKey::new(1, 2, 3, 99),
        ];
        for v in &variants {
            assert_ne!(&base, v);
        }
    }

    #[test]
    fn bloom_hash_deterministic_for_same_fields() {
        let a = SchemaKey::new(100, 200, 300, 400);
        let b = SchemaKey::new(100, 200, 300, 400);
        assert_eq!(a.bloom_hash(), b.bloom_hash());
        assert_ne!(a.bloom_hash(), SchemaKey::new(100, 200, 300, 401).bloom_hash());
    }

    /// FROZEN persisted-format pin: the bloom hash is xxh3-64 over a fixed field
    /// layout and is written to disk, so its exact value must never change
    /// (across Rust versions, xxhash-crate versions, or CPU arch). If this trips,
    /// a hash change is about to silently invalidate every persisted bloom.
    #[test]
    fn bloom_hash_value_is_pinned() {
        assert_eq!(SchemaKey::new(1, 2, 3, 4).bloom_hash(), 365921206506951139);
    }
}
