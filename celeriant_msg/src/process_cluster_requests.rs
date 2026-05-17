use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::{
    codec::compression::DictCodec,
    network::{
        wire_error::WireError,
        wire_header::{WireHeader, wire_header_write_fixed_size, wire_header_write_variable_size_with_codec},
    },
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    read_wire_data_error::ReadWireDataError,
    request::requests::{HeartbeatRequest, KickFollowerRequest, ReplicationBatchRequest},
};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterRequestType {
    ReplicationBatch = 100,
    Heartbeat = 101,
    KickFollower = 102,
}

impl ClusterRequestType {
    pub fn from_u32(value: u32) -> Result<Self, ReadWireDataError> {
        match value {
            100 => Ok(ClusterRequestType::ReplicationBatch),
            101 => Ok(ClusterRequestType::Heartbeat),
            102 => Ok(ClusterRequestType::KickFollower),
            _ => Err(ReadWireDataError::UnknownMessageType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClusterRequest {
    ReplicationBatch(ReplicationBatchRequest),
    Heartbeat(HeartbeatRequest),
    KickFollower(KickFollowerRequest),
}

impl ClusterRequest {
    pub fn request_type(&self) -> ClusterRequestType {
        match self {
            ClusterRequest::ReplicationBatch(_) => ClusterRequestType::ReplicationBatch,
            ClusterRequest::Heartbeat(_) => ClusterRequestType::Heartbeat,
            ClusterRequest::KickFollower(_) => ClusterRequestType::KickFollower,
        }
    }

    pub fn correlation_id(&self) -> Option<u128> {
        match self {
            ClusterRequest::ReplicationBatch(req) => req.correlation_id,
            ClusterRequest::Heartbeat(req) => req.correlation_id,
            ClusterRequest::KickFollower(req) => req.correlation_id,
        }
    }

    pub async fn read_from_header<R>(wire_header: WireHeader, reader: &mut R, dict_codec: &DictCodec) -> Result<ClusterRequest, ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let request_type = ClusterRequestType::from_u32(wire_header.message_type)?;

        macro_rules! fixed {
            ($variant:ident) => {
                ClusterRequest::$variant(
                    wire_header
                        .read_fixed_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        Ok(match request_type {
            ClusterRequestType::Heartbeat => fixed!(Heartbeat),
            ClusterRequestType::KickFollower => fixed!(KickFollower),
            ClusterRequestType::ReplicationBatch => ClusterRequest::ReplicationBatch(
                wire_header
                    .read_variable_size_with_codec(reader, dict_codec)
                    .await
                    .map_err(ReadWireDataError::ReadBodyFailure)?,
            ),
        })
    }

    pub async fn write_request<W>(
        writer: &mut W,
        request: &ClusterRequest,
        compression_type: CompressionType,
        max_message_size: u64,
        version: u32,
        dict_codec: &DictCodec,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let request_type_id = request.request_type() as u32;

        match request {
            ClusterRequest::Heartbeat(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClusterRequest::KickFollower(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClusterRequest::ReplicationBatch(req) => wire_header_write_variable_size_with_codec(writer, req, request_type_id, compression_type, max_message_size, version, dict_codec).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::requests::ReplicationBatchRequest;
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, WIRE_HEADER_SIZE};
    use futures_lite::{future::block_on, io::Cursor};

    const REQUEST_TYPE_COUNT: usize = 3;
    const VERSIONS: [u32; 2] = [PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3];

    fn all_types() -> [ClusterRequestType; REQUEST_TYPE_COUNT] {
        [
            ClusterRequestType::ReplicationBatch,
            ClusterRequestType::Heartbeat,
            ClusterRequestType::KickFollower,
        ]
    }

    fn make_request(rt: ClusterRequestType) -> ClusterRequest {
        match rt {
            ClusterRequestType::ReplicationBatch => ClusterRequest::ReplicationBatch(ReplicationBatchRequest {
                correlation_id: Some(0x4444_5555_6666_7777),
                shard_id: 2,
                leader_timestamp_ms: 9999999,
                leader_confirmed_wal_seq: 12345,
                batches: vec![],
            }),
            ClusterRequestType::Heartbeat => ClusterRequest::Heartbeat(HeartbeatRequest {
                correlation_id: Some(0x6666_7777_8888_9999),
                shard_id: 0,
                leader_timestamp_ms: 1234567890123,
                lease_epoch: 7,
            }),
            ClusterRequestType::KickFollower => ClusterRequest::KickFollower(KickFollowerRequest {
                correlation_id: Some(0x7777_8888_9999_AAAA),
            }),
        }
    }

    fn is_variable_size(rt: ClusterRequestType) -> bool {
        matches!(rt, ClusterRequestType::ReplicationBatch)
    }

    fn test_codec() -> celeriant_wire::codec::compression::DictCodec {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        celeriant_wire::codec::compression::DictCodec::new(BUILTIN_DICT_BYTES, 3)
            .expect("builtin dict must compile")
    }

    async fn write_bytes(req: &ClusterRequest, version: u32, compression: CompressionType) -> Vec<u8> {
        write_bytes_with_dict(req, version, compression, &test_codec()).await
    }

    async fn write_bytes_with_dict(req: &ClusterRequest, version: u32, compression: CompressionType, dict_codec: &celeriant_wire::codec::compression::DictCodec) -> Vec<u8> {
        let mut buf = Vec::new();
        ClusterRequest::write_request(&mut buf, req, compression, 64 * 1024 * 1024, version, dict_codec)
            .await
            .expect("write_request");
        buf
    }

    async fn read_back(bytes: &[u8]) -> ClusterRequest {
        read_back_with_dict(bytes, &test_codec()).await
    }

    async fn read_back_with_dict(bytes: &[u8], dict_codec: &celeriant_wire::codec::compression::DictCodec) -> ClusterRequest {
        let header = WireHeader::from_reader(&mut Cursor::new(bytes.to_vec()), u64::MAX).await.expect("header");
        ClusterRequest::read_from_header(header, &mut Cursor::new(bytes[WIRE_HEADER_SIZE..].to_vec()), dict_codec)
            .await
            .expect("read_from_header")
    }

    #[test]
    fn type_id_parsing() {
        for rt in all_types() {
            assert_eq!(ClusterRequestType::from_u32(rt as u32).unwrap(), rt);
        }
        for id in 100..=102 {
            assert!(ClusterRequestType::from_u32(id).is_ok(), "missing cluster id {}", id);
        }
        for id in 1..=10 {
            assert!(ClusterRequestType::from_u32(id).is_err(), "client id {} should not parse as ClusterRequestType", id);
        }
        assert!(ClusterRequestType::from_u32(0).is_err());
        assert!(ClusterRequestType::from_u32(103).is_err());
    }

    #[test]
    fn request_type_accessor() {
        for rt in all_types() {
            assert_eq!(make_request(rt).request_type(), rt);
        }
    }

    #[test]
    fn round_trip_all_versions() {
        block_on(async {
            for rt in all_types() {
                for &v in &VERSIONS {
                    let req = make_request(rt);
                    let bytes1 = write_bytes(&req, v, CompressionType::None).await;
                    let parsed = read_back(&bytes1).await;

                    assert_eq!(parsed.request_type(), rt, "{:?} type mismatch", rt);
                    assert_eq!(parsed.correlation_id(), req.correlation_id(), "{:?} correlation_id lost", rt);

                    let bytes2 = write_bytes(&parsed, v, CompressionType::None).await;
                    assert_eq!(bytes1, bytes2, "{:?} v{} data not preserved", rt, v);
                }
            }
        });
    }

    #[test]
    fn fixed_vs_variable_categorization() {
        block_on(async {
            for rt in all_types() {
                let req = make_request(rt);
                let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;
                let compression_byte = bytes[16];
                if is_variable_size(rt) {
                    assert!(compression_byte == 0 || compression_byte > 0, "{:?} should use variable path", rt);
                } else {
                    assert_eq!(compression_byte, 0, "{:?} fixed-size should have no compression", rt);
                }
            }
        });
    }

    #[test]
    fn compression_round_trip() {
        block_on(async {
            let compressions = [
                CompressionType::None,
            ];

            for rt in all_types().into_iter().filter(|rt| is_variable_size(*rt)) {
                for &compression in &compressions {
                    for &v in &VERSIONS {
                        let req = make_request(rt);
                        let bytes1 = write_bytes(&req, v, compression).await;
                        let parsed = read_back(&bytes1).await;

                        assert_eq!(parsed.correlation_id(), req.correlation_id());

                        let bytes2 = write_bytes(&parsed, v, compression).await;
                        assert_eq!(bytes1, bytes2, "{:?} {:?} v{} compression broke data", rt, compression, v);
                    }
                }
            }
        });
    }

    #[test]
    fn truncated_stream_fails() {
        block_on(async {
            let req = make_request(ClusterRequestType::Heartbeat);
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            let codec = test_codec();
            for truncate_at in [0, 10, bytes.len() - 1] {
                let truncated = &bytes[..truncate_at];
                let header_result = WireHeader::from_reader(&mut Cursor::new(truncated.to_vec()), u64::MAX).await;
                if let Ok(header) = header_result {
                    let body_result = ClusterRequest::read_from_header(
                        header,
                        &mut Cursor::new(truncated[WIRE_HEADER_SIZE..].to_vec()),
                        &codec,
                    ).await;
                    assert!(body_result.is_err(), "should fail with {} bytes (full: {})", truncate_at, bytes.len());
                }
            }
        });
    }

    #[test]
    fn replication_batch_zstd_dict_round_trip() {
        block_on(async {
            let req = make_request(ClusterRequestType::ReplicationBatch);
            let codec = test_codec();

            let bytes = write_bytes_with_dict(&req, PROTOCOL_VERSION_V2, CompressionType::ZstdDict, &codec).await;
            assert_eq!(bytes[16], 1, "expected ZstdDict compression byte");
            let parsed = read_back_with_dict(&bytes, &codec).await;
            assert_eq!(parsed.request_type(), ClusterRequestType::ReplicationBatch);
            assert_eq!(parsed.correlation_id(), req.correlation_id());

            let bytes_none = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;
            assert_eq!(bytes_none[16], 0, "expected None compression byte");
            let parsed_none = read_back(&bytes_none).await;
            assert_eq!(parsed_none.correlation_id(), req.correlation_id());
        });
    }

    #[test]
    fn invalid_message_type_fails() {
        block_on(async {
            let req = make_request(ClusterRequestType::Heartbeat);
            let mut bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
            let header = WireHeader::from_reader(&mut Cursor::new(bytes.clone()), u64::MAX).await.expect("header");
            let result = ClusterRequest::read_from_header(
                header,
                &mut Cursor::new(bytes[WIRE_HEADER_SIZE..].to_vec()),
                &test_codec(),
            ).await;
            assert!(result.is_err());
        });
    }
}
