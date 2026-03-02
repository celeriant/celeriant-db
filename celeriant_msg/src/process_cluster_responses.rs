use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{WireHeader, wire_header_write_fixed_size},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    read_wire_data_error::ReadWireDataError,
    response::responses::{
        ErrorResponse, HeartbeatResponse, KickFollowerResponse,
        ProtocolErrorResponse, ReplicationBatchResponse,
    },
};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterResponseType {
    ReplicationBatch = 100,
    Heartbeat = 101,
    KickFollower = 102,
    ProtocolError = 106,
    GenericError = 107,
}

impl ClusterResponseType {
    pub fn from_u32(value: u32) -> Result<Self, ReadWireDataError> {
        match value {
            100 => Ok(ClusterResponseType::ReplicationBatch),
            101 => Ok(ClusterResponseType::Heartbeat),
            102 => Ok(ClusterResponseType::KickFollower),
            106 => Ok(ClusterResponseType::ProtocolError),
            107 => Ok(ClusterResponseType::GenericError),
            _ => Err(ReadWireDataError::UnknownMessageType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClusterResponse {
    ProtocolError(ProtocolErrorResponse),
    GenericError(ErrorResponse),
    ReplicationBatch(ReplicationBatchResponse),
    Heartbeat(HeartbeatResponse),
    KickFollower(KickFollowerResponse),
}

impl ClusterResponse {
    pub fn response_type(&self) -> ClusterResponseType {
        match self {
            ClusterResponse::ProtocolError(_) => ClusterResponseType::ProtocolError,
            ClusterResponse::GenericError(_) => ClusterResponseType::GenericError,
            ClusterResponse::ReplicationBatch(_) => ClusterResponseType::ReplicationBatch,
            ClusterResponse::Heartbeat(_) => ClusterResponseType::Heartbeat,
            ClusterResponse::KickFollower(_) => ClusterResponseType::KickFollower,
        }
    }

    pub async fn read_response<R>(reader: &mut R, max_response_size: u64) -> Result<ClusterResponse, ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let wire_header = WireHeader::from_reader(reader, max_response_size)
            .await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        Self::read_from_header(wire_header, reader).await
    }

    pub async fn read_from_header<R>(wire_header: WireHeader, reader: &mut R) -> Result<ClusterResponse, ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let response_type = ClusterResponseType::from_u32(wire_header.message_type)?;

        macro_rules! fixed {
            ($variant:ident) => {
                ClusterResponse::$variant(
                    wire_header
                        .read_fixed_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        Ok(match response_type {
            ClusterResponseType::ProtocolError => fixed!(ProtocolError),
            ClusterResponseType::GenericError => fixed!(GenericError),
            ClusterResponseType::ReplicationBatch => fixed!(ReplicationBatch),
            ClusterResponseType::Heartbeat => fixed!(Heartbeat),
            ClusterResponseType::KickFollower => fixed!(KickFollower),
        })
    }

    pub fn determine_compression_type(_response: &ClusterResponse, _server_compression_algorithm: CompressionType) -> CompressionType {
        CompressionType::None
    }

    pub async fn write_response<W>(
        writer: &mut W,
        response: &ClusterResponse,
        _compression_type: CompressionType,
        _max_message_size: u64,
        version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let response_type_id = response.response_type() as u32;

        match response {
            ClusterResponse::ProtocolError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClusterResponse::GenericError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClusterResponse::ReplicationBatch(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClusterResponse::Heartbeat(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClusterResponse::KickFollower(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::responses::{HeartbeatRejection, HeartbeatResult, ReplicationResult};
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, WIRE_HEADER_SIZE};
    use futures_lite::{future::block_on, io::Cursor};

    const COUNT: usize = 5;
    const VERSIONS: [u32; 2] = [PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3];

    fn all_types() -> [ClusterResponseType; COUNT] {
        [
            ClusterResponseType::ReplicationBatch,
            ClusterResponseType::Heartbeat,
            ClusterResponseType::KickFollower,
            ClusterResponseType::ProtocolError,
            ClusterResponseType::GenericError,
        ]
    }

    fn make_response(rt: ClusterResponseType) -> ClusterResponse {
        match rt {
            ClusterResponseType::ProtocolError => ClusterResponse::ProtocolError(ProtocolErrorResponse {}),
            ClusterResponseType::GenericError => ClusterResponse::GenericError(ErrorResponse {
                correlation_id: Some(0x1111_2222_3333_4444),
                error_code: 0xFFFF,
                error_message: "cluster error".into(),
            }),
            ClusterResponseType::ReplicationBatch => ClusterResponse::ReplicationBatch(ReplicationBatchResponse {
                correlation_id: Some(0x5555_6666_7777_8888),
                follower_timestamp_ms: 9999999,
                result: ReplicationResult::Success { last_follower_metablock: None },
            }),
            ClusterResponseType::Heartbeat => ClusterResponse::Heartbeat(HeartbeatResponse {
                correlation_id: Some(0x7777_8888_9999_AAAA),
                result: HeartbeatResult::Ack {
                    follower_timestamp_ms: 1234567890123,
                },
            }),
            ClusterResponseType::KickFollower => ClusterResponse::KickFollower(KickFollowerResponse {
                correlation_id: Some(0x8888_9999_AAAA_BBBB),
                acknowledged: true,
            }),
        }
    }

    async fn write_bytes(res: &ClusterResponse, version: u32, compression: CompressionType) -> Vec<u8> {
        let mut buf = Vec::new();
        ClusterResponse::write_response(&mut buf, res, compression, 64 * 1024 * 1024, version)
            .await
            .unwrap();
        buf
    }

    async fn read_back(bytes: &[u8]) -> ClusterResponse {
        let header = WireHeader::from_reader(&mut Cursor::new(bytes.to_vec()), u64::MAX).await.unwrap();
        ClusterResponse::read_from_header(header, &mut Cursor::new(bytes[WIRE_HEADER_SIZE..].to_vec()))
            .await
            .unwrap()
    }

    #[test]
    fn type_id_parsing() {
        for rt in all_types() {
            assert_eq!(ClusterResponseType::from_u32(rt as u32).unwrap(), rt);
        }
        for id in [100, 101, 102, 106, 107] {
            assert!(ClusterResponseType::from_u32(id).is_ok(), "missing cluster id {}", id);
        }
        // Client IDs should not parse as cluster responses
        for id in 1..=12 {
            assert!(ClusterResponseType::from_u32(id).is_err(), "client id {} should not parse as ClusterResponseType", id);
        }
        assert!(ClusterResponseType::from_u32(0).is_err());
        assert!(ClusterResponseType::from_u32(108).is_err());
    }

    #[test]
    fn response_type_accessor() {
        for rt in all_types() {
            assert_eq!(make_response(rt).response_type(), rt);
        }
    }

    #[test]
    fn round_trip_all_versions() {
        block_on(async {
            for rt in all_types() {
                for &v in &VERSIONS {
                    let res = make_response(rt);
                    let bytes = write_bytes(&res, v, CompressionType::None).await;
                    let parsed = read_back(&bytes).await;
                    assert_eq!(parsed.response_type(), rt, "{:?} v{} type mismatch", rt, v);
                }
            }
        });
    }

    #[test]
    fn all_cluster_responses_are_fixed_size() {
        block_on(async {
            for rt in all_types() {
                let res = make_response(rt);
                let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;
                let compression_byte = bytes[16];
                assert_eq!(compression_byte, 0, "{:?} should be fixed-size with no compression", rt);
            }
        });
    }

    #[test]
    fn compression_type_always_none() {
        let server_compression = CompressionType::Zstd { level: 6 };
        for rt in all_types() {
            let res = make_response(rt);
            let determined = ClusterResponse::determine_compression_type(&res, server_compression);
            assert_eq!(determined, CompressionType::None, "{:?} should not compress", rt);
        }
    }

    #[test]
    fn heartbeat_rejection_variants_round_trip() {
        block_on(async {
            let variants = [
                HeartbeatRejection::ClockDriftTooHigh {
                    leader_ms: 1000,
                    follower_ms: 9000,
                    max_allowed_ms: 5000,
                },
            ];

            for reason in variants {
                let res = ClusterResponse::Heartbeat(HeartbeatResponse {
                    correlation_id: Some(0x1234_5678_9ABC_DEF0),
                    result: HeartbeatResult::Rejected(reason),
                });

                for &v in &VERSIONS {
                    let bytes = write_bytes(&res, v, CompressionType::None).await;
                    let parsed = read_back(&bytes).await;
                    assert_eq!(parsed.response_type(), ClusterResponseType::Heartbeat);
                }
            }
        });
    }

    #[test]
    fn truncated_stream_fails() {
        block_on(async {
            let res = make_response(ClusterResponseType::Heartbeat);
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;

            for truncate_at in [0, 10, bytes.len() - 1] {
                let truncated = &bytes[..truncate_at];
                let header_result = WireHeader::from_reader(&mut Cursor::new(truncated.to_vec()), u64::MAX).await;
                if let Ok(header) = header_result {
                    let body_result = ClusterResponse::read_from_header(
                        header,
                        &mut Cursor::new(truncated[WIRE_HEADER_SIZE..].to_vec()),
                    ).await;
                    assert!(body_result.is_err(), "should fail with {} bytes (full: {})", truncate_at, bytes.len());
                }
            }
        });
    }

    #[test]
    fn invalid_message_type_fails() {
        block_on(async {
            let res = make_response(ClusterResponseType::Heartbeat);
            let mut bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;

            bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
            let header = WireHeader::from_reader(&mut Cursor::new(bytes.clone()), u64::MAX).await.unwrap();
            let result = ClusterResponse::read_from_header(
                header,
                &mut Cursor::new(bytes[WIRE_HEADER_SIZE..].to_vec()),
            ).await;
            assert!(result.is_err());
        });
    }
}
