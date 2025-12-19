use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
pub enum CompressionType {
    None,
    Zstd { level: i32 },
    Snappy,
    Brotli { level: i32 },
    Gzip { level: i32 },
}

impl CompressionType {
    pub fn to_tuple(self) -> (u8, Option<i32>) {
        match self {
            CompressionType::None => (0, None),
            CompressionType::Zstd { level } => (1, Some(level)),
            CompressionType::Snappy => (2, None),
            CompressionType::Brotli { level } => (3, Some(level)),
            CompressionType::Gzip { level } => (4, Some(level)),
        }
    }

    pub fn from_tuple(type_id: u8, level: Option<i32>) -> Self {
        match type_id {
            0 => CompressionType::None,
            1 => CompressionType::Zstd {
                level: level.unwrap_or(6),
            },
            2 => CompressionType::Snappy,
            3 => CompressionType::Brotli {
                level: level.unwrap_or(6),
            },
            4 => CompressionType::Gzip {
                level: level.unwrap_or(6),
            },
            _ => CompressionType::default(),
        }
    }
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::Zstd { level: 6 }
    }
}
