use bincode::{Decode, Encode};
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode, DeepSizeOf)]
#[repr(u32)]
pub enum CompressionType {
    None = 0,
    ZstdDict = 1,
}

impl CompressionType {
    #[inline]
    pub fn is_zstd_dict(self) -> bool {
        matches!(self, CompressionType::ZstdDict)
    }

    #[inline]
    pub fn to_byte(self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::ZstdDict => 1,
        }
    }

    #[inline]
    pub fn from_byte(type_id: u8) -> Result<Self, u8> {
        match type_id {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::ZstdDict),
            _ => Err(type_id),
        }
    }
}
