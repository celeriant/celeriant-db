use std::io;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::structures::constants::BINCODE_CONFIG_VARIABLE;
use crate::structures::{compression_type::CompressionType, event_item::EventItem};

use crate::serde_option_u128_base64;
use crate::serde_u128_base64;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventBatchItem {
    /// Unique, incremented integer assigned to each event batch when persisted on the server
    #[serde(rename = "si")]
    pub server_id: u64,

    /// Server side Unix timestamp in milliseconds when the event batch was persisted on the server
    #[serde(rename = "st")]
    pub server_time: u64,

    /// Unique identifyer of the machine that produced these events. Typically the truncated SHA256 of the clients' public key
    #[serde(with = "serde_u128_base64", rename = "ci")]
    pub client_id: u128,

    /// Unique identifyer of the user that produced these events. Typically the truncated SHA256 of the users' sub field
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "serde_option_u128_base64",
        rename = "ui"
    )]
    pub user_id: Option<u128>,

    /// Events present in this batch, all from the same client / user
    #[serde(rename = "ev")]
    pub events: Vec<EventItem>,
}

impl EventBatchItem {
    pub fn new(
        server_id: u64,
        server_time: u64,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
    ) -> Self {
        Self {
            server_id,
            server_time,
            client_id,
            user_id,
            events,
        }
    }

    /// Serialize and compress the event batch item into a wire format
    pub fn to_wire_format(
        &self,
        compression_type: CompressionType,
    ) -> io::Result<(usize, Vec<u8>)> {
        let serialized = bincode::encode_to_vec(self, BINCODE_CONFIG_VARIABLE)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let uncompressed_size = serialized.len();

        match compression_type {
            CompressionType::None => Ok((uncompressed_size, serialized)),
            CompressionType::Zstd { level } => {
                let compressed = zstd::bulk::compress(&serialized, level)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                Ok((uncompressed_size, compressed))
            }
            CompressionType::Snappy => {
                let compressed = snap::raw::Encoder::new()
                    .compress_vec(&serialized)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                Ok((uncompressed_size, compressed))
            }
            CompressionType::Brotli { level } => {
                let mut compressed = Vec::new();
                let params = brotli::enc::BrotliEncoderParams {
                    quality: level,
                    ..Default::default()
                };
                brotli::BrotliCompress(
                    &mut std::io::Cursor::new(&serialized),
                    &mut compressed,
                    &params,
                )
                .map_err(|e| io::Error::other(e.to_string()))?;
                Ok((uncompressed_size, compressed))
            }
            CompressionType::Gzip { level } => {
                use flate2::{Compression, write::GzEncoder};
                let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level as u32));
                std::io::Write::write_all(&mut encoder, &serialized)?;
                let compressed = encoder.finish()?;
                Ok((uncompressed_size, compressed))
            }
        }
    }

    /// Deserialize and decompress from wire format
    pub fn from_wire_format(
        data: &[u8],
        compression_type: CompressionType,
        capacity: usize,
    ) -> io::Result<Self> {
        let decompressed = match compression_type {
            CompressionType::None => data.to_vec(),
            CompressionType::Zstd { .. } => zstd::bulk::decompress(data, capacity)
                .map_err(|e| io::Error::other(e.to_string()))?,
            CompressionType::Snappy => snap::raw::Decoder::new()
                .decompress_vec(data)
                .map_err(|e| io::Error::other(e.to_string()))?,
            CompressionType::Brotli { .. } => {
                let mut decompressed = Vec::with_capacity(capacity);
                brotli::BrotliDecompress(&mut std::io::Cursor::new(data), &mut decompressed)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                decompressed
            }
            CompressionType::Gzip { .. } => {
                use flate2::read::GzDecoder;
                let mut decoder = GzDecoder::new(data);
                let mut decompressed = Vec::with_capacity(capacity);
                std::io::Read::read_to_end(&mut decoder, &mut decompressed)?;
                decompressed
            }
        };

        bincode::decode_from_slice(&decompressed, BINCODE_CONFIG_VARIABLE)
            .map(|(events, _)| events)
            .map_err(|e| io::Error::other(e.to_string()))
    }
}
