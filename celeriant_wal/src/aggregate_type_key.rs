use std::fmt;
use std::hash::{Hash, Hasher};

use bincode::de::{BorrowDecoder, Decoder};
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use bincode::{BorrowDecode, Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

/// High-performance composite key
/// Optimized for hashing and comparison operations
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct AggregateTypeKey {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    // Pre-computed hash for better performance
    hash: u64,
}

impl AggregateTypeKey {
    pub fn new(org_id: u128, aggregate_type_id: u128) -> Self {
        let hash = Self::compute_hash(org_id, aggregate_type_id);
        Self {
            org_id,
            aggregate_type_id,
            hash,
        }
    }

    fn compute_hash(org_id: u128, aggregate_type_id: u128) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        org_id.hash(&mut hasher);
        aggregate_type_id.hash(&mut hasher);
        hasher.finish()
    }
}


impl Encode for AggregateTypeKey {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.org_id.encode(encoder)?;
        self.aggregate_type_id.encode(encoder)?;
        Ok(())
    }
}

impl<Context> Decode<Context> for AggregateTypeKey {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let org_id = <u128 as Decode<Context>>::decode(decoder)?;
        let aggregate_type_id = <u128 as Decode<Context>>::decode(decoder)?;
        Ok(Self::new(org_id, aggregate_type_id))
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for AggregateTypeKey {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let org_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        let aggregate_type_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        Ok(Self::new(org_id, aggregate_type_id))
    }
}

impl Hash for AggregateTypeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use pre-computed hash for better performance
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for AggregateTypeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateTypeKey")
            .field("org_id", &self.org_id)
            .field("aggregate_type_id", &self.aggregate_type_id)
            .finish()
    }
}

impl Default for AggregateTypeKey {
    fn default() -> Self {
        Self::new(0, 0)
    }
}