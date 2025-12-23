use std::fmt;
use std::hash::{Hash, Hasher};

use bincode::de::{BorrowDecoder, Decoder};
use bincode::enc::Encoder;
use bincode::error::{DecodeError, EncodeError};
use bincode::{BorrowDecode, Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

use crate::aggregate_key::AggregateKey;

/// Optimized for hashing and comparison operations
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct AggregateClientKey {
    pub aggregate_key: AggregateKey,
    pub client_id: u128,
    // Pre-computed hash for better performance
    hash: u64,
}

impl AggregateClientKey {
    pub fn new(aggregate_key: AggregateKey, client_id: u128) -> Self {
        let hash = Self::compute_hash(&aggregate_key, client_id);
        Self {
            aggregate_key,
            client_id,
            hash,
        }
    }

    fn compute_hash(aggregate_key: &AggregateKey, client_id: u128) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        aggregate_key.org_id.hash(&mut hasher);
        aggregate_key.aggregate_type_id.hash(&mut hasher);
        aggregate_key.aggregate_id.hash(&mut hasher);
        client_id.hash(&mut hasher);
        hasher.finish()
    }
}


impl Encode for AggregateClientKey {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.aggregate_key.encode(encoder)?;
        self.client_id.encode(encoder)?;
        Ok(())
    }
}

impl<Context> Decode<Context> for AggregateClientKey {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let aggregate_key = <AggregateKey as Decode<Context>>::decode(decoder)?;
        let client_id = <u128 as Decode<Context>>::decode(decoder)?;
        Ok(Self::new(aggregate_key, client_id))
    }
}

impl<'de, Context> BorrowDecode<'de, Context> for AggregateClientKey {
    fn borrow_decode<D: BorrowDecoder<'de, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let aggregate_key = <AggregateKey as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        let client_id = <u128 as BorrowDecode<'de, Context>>::borrow_decode(decoder)?;
        Ok(Self::new(aggregate_key, client_id))
    }
}

impl Hash for AggregateClientKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use pre-computed hash for better performance
        state.write_u64(self.hash);
    }
}

impl fmt::Debug for AggregateClientKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateClientKey")
            .field("aggregate_key", &self.aggregate_key)
            .field("client_id", &self.client_id)
            .finish()
    }
}

impl Default for AggregateClientKey {
    fn default() -> Self {
        Self::new(AggregateKey::default(), 0)
    }
}