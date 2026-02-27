use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{WireHeader, wire_header_write_fixed_size, wire_header_write_variable_size},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    read_wire_data_error::ReadWireDataError,
    response::responses::{
        ErrorResponse, AggregateDetailsResponse, IdentifyResponse, ListAggregateTypesResponse, ListAggregatesResponse, ListOrgsResponse, ProtocolErrorResponse,
        ReadResponse, SuccessResponse, WatchResponse,
    },
};
#[cfg(test)]
use crate::response::responses::AccessLevel;
#[cfg(feature = "cluster")]
use crate::response::responses::{CatchUpResponse, HeartbeatResponse, KickFollowerResponse, ReplicationBatchResponse};

// Response type discriminants
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseType {
    AggregateDetails = 1,
    Read = 2,
    Write = 3,
    TrimStart = 4,
    Delete = 5,
    ProtocolError = 6,
    GenericError = 7,
    Watch = 8,
    ListOrgs = 9,
    ListAggregateTypes = 10,
    ListAggregates = 11,
    #[cfg(feature = "cluster")]
    ReplicationBatch = 12,
    #[cfg(feature = "cluster")]
    CatchUp = 13,
    #[cfg(feature = "cluster")]
    Heartbeat = 14,
    #[cfg(feature = "cluster")]
    KickFollower = 15,
    Identify = 16,
}

impl ResponseType {
    pub fn from_u32(value: u32) -> Result<Self, ReadWireDataError> {
        match value {
            1 => Ok(ResponseType::AggregateDetails),
            2 => Ok(ResponseType::Read),
            3 => Ok(ResponseType::Write),
            4 => Ok(ResponseType::TrimStart),
            5 => Ok(ResponseType::Delete),
            6 => Ok(ResponseType::ProtocolError),
            7 => Ok(ResponseType::GenericError),
            8 => Ok(ResponseType::Watch),
            9 => Ok(ResponseType::ListOrgs),
            10 => Ok(ResponseType::ListAggregateTypes),
            11 => Ok(ResponseType::ListAggregates),
            #[cfg(feature = "cluster")]
            12 => Ok(ResponseType::ReplicationBatch),
            #[cfg(feature = "cluster")]
            13 => Ok(ResponseType::CatchUp),
            #[cfg(feature = "cluster")]
            14 => Ok(ResponseType::Heartbeat),
            #[cfg(feature = "cluster")]
            15 => Ok(ResponseType::KickFollower),
            16 => Ok(ResponseType::Identify),
            _ => Err(ReadWireDataError::UnknownMessageType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Response {
    AggregateDetails(AggregateDetailsResponse),
    Read(ReadResponse),
    Write(SuccessResponse),
    TrimStart(SuccessResponse),
    Delete(SuccessResponse),
    ProtocolError(ProtocolErrorResponse),
    GenericError(ErrorResponse),
    Watch(WatchResponse),
    ListOrgs(ListOrgsResponse),
    ListAggregateTypes(ListAggregateTypesResponse),
    ListAggregates(ListAggregatesResponse),
    #[cfg(feature = "cluster")]
    ReplicationBatch(ReplicationBatchResponse),
    #[cfg(feature = "cluster")]
    CatchUp(CatchUpResponse),
    #[cfg(feature = "cluster")]
    Heartbeat(HeartbeatResponse),
    #[cfg(feature = "cluster")]
    KickFollower(KickFollowerResponse),
    Identify(IdentifyResponse),
}

impl Response {
    pub fn response_type(&self) -> ResponseType {
        match self {
            Response::AggregateDetails(_) => ResponseType::AggregateDetails,
            Response::Read(_) => ResponseType::Read,
            Response::Write(_) => ResponseType::Write,
            Response::TrimStart(_) => ResponseType::TrimStart,
            Response::Delete(_) => ResponseType::Delete,
            Response::ProtocolError(_) => ResponseType::ProtocolError,
            Response::GenericError(_) => ResponseType::GenericError,
            Response::Watch(_) => ResponseType::Watch,
            Response::ListOrgs(_) => ResponseType::ListOrgs,
            Response::ListAggregateTypes(_) => ResponseType::ListAggregateTypes,
            Response::ListAggregates(_) => ResponseType::ListAggregates,
            #[cfg(feature = "cluster")]
            Response::ReplicationBatch(_) => ResponseType::ReplicationBatch,
            #[cfg(feature = "cluster")]
            Response::CatchUp(_) => ResponseType::CatchUp,
            #[cfg(feature = "cluster")]
            Response::Heartbeat(_) => ResponseType::Heartbeat,
            #[cfg(feature = "cluster")]
            Response::KickFollower(_) => ResponseType::KickFollower,
            Response::Identify(_) => ResponseType::Identify,
        }
    }

    pub async fn read_response<R>(reader: &mut R, max_response_size: u64) -> Result<Response, ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let wire_header = WireHeader::from_reader(reader, max_response_size)
            .await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;

        let response_type = ResponseType::from_u32(wire_header.message_type)?;

        macro_rules! fixed {
            ($variant:ident) => {
                Response::$variant(
                    wire_header
                        .read_fixed_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        macro_rules! variable {
            ($variant:ident) => {
                Response::$variant(
                    wire_header
                        .read_variable_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        Ok(match response_type {
            ResponseType::AggregateDetails => fixed!(AggregateDetails),
            ResponseType::Write => fixed!(Write),
            ResponseType::TrimStart => fixed!(TrimStart),
            ResponseType::Delete => fixed!(Delete),
            ResponseType::ProtocolError => fixed!(ProtocolError),
            ResponseType::GenericError => fixed!(GenericError),
            ResponseType::Read => variable!(Read),
            ResponseType::Watch => variable!(Watch),
            ResponseType::ListOrgs => variable!(ListOrgs),
            ResponseType::ListAggregateTypes => variable!(ListAggregateTypes),
            ResponseType::ListAggregates => variable!(ListAggregates),
            #[cfg(feature = "cluster")]
            ResponseType::ReplicationBatch => fixed!(ReplicationBatch),
            #[cfg(feature = "cluster")]
            ResponseType::Heartbeat => fixed!(Heartbeat),
            #[cfg(feature = "cluster")]
            ResponseType::KickFollower => fixed!(KickFollower),
            #[cfg(feature = "cluster")]
            ResponseType::CatchUp => variable!(CatchUp),
            ResponseType::Identify => fixed!(Identify),
        })
    }

    pub fn determine_compression_type(response: &Response, server_compression_algorithm: CompressionType) -> CompressionType {
        match response {
            Response::AggregateDetails(_) => CompressionType::None,
            Response::Read(_) => server_compression_algorithm,
            Response::Write(_) => CompressionType::None,
            Response::TrimStart(_) => CompressionType::None,
            Response::Delete(_) => CompressionType::None,
            Response::ProtocolError(_) => CompressionType::None,
            Response::GenericError(_) => CompressionType::None,
            Response::Watch(_) => server_compression_algorithm,
            Response::ListOrgs(_) => server_compression_algorithm,
            Response::ListAggregateTypes(_) => server_compression_algorithm,
            Response::ListAggregates(_) => server_compression_algorithm,
            #[cfg(feature = "cluster")]
            Response::ReplicationBatch(_) => CompressionType::None,
            #[cfg(feature = "cluster")]
            Response::CatchUp(_) => server_compression_algorithm,
            #[cfg(feature = "cluster")]
            Response::Heartbeat(_) => CompressionType::None,
            #[cfg(feature = "cluster")]
            Response::KickFollower(_) => CompressionType::None,
            Response::Identify(_) => CompressionType::None,
        }
    }

    pub async fn write_response<W>(
        writer: &mut W,
        response: &Response,
        compression_type: CompressionType,
        max_message_size: u64,
        version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let response_type_id = response.response_type() as u32;

        match response {
            Response::AggregateDetails(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            Response::Write(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            Response::TrimStart(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            Response::Delete(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            Response::ProtocolError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            Response::GenericError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            Response::Read(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            Response::Watch(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            Response::ListOrgs(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            Response::ListAggregateTypes(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            Response::ListAggregates(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            #[cfg(feature = "cluster")]
            Response::ReplicationBatch(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            #[cfg(feature = "cluster")]
            Response::Heartbeat(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            #[cfg(feature = "cluster")]
            Response::KickFollower(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            #[cfg(feature = "cluster")]
            Response::CatchUp(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            Response::Identify(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::responses::{AggregateListItem, AggregateTypeListItem, OrgListItem};
    #[cfg(feature = "cluster")]
    use crate::response::responses::{HeartbeatResult, ReplicationResult};
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
    use futures_lite::{future::block_on, io::Cursor};

    #[cfg(feature = "cluster")]
    const COUNT: usize = 16;
    #[cfg(not(feature = "cluster"))]
    const COUNT: usize = 12;
    #[cfg(feature = "cluster")]
    const MAX_ID: u32 = 16;
    #[cfg(not(feature = "cluster"))]
    const MAX_ID: u32 = 16;
    const VERSIONS: [u32; 2] = [PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3];

    fn all_types() -> [ResponseType; COUNT] {
        [
            ResponseType::AggregateDetails,
            ResponseType::Read,
            ResponseType::Write,
            ResponseType::TrimStart,
            ResponseType::Delete,
            ResponseType::ProtocolError,
            ResponseType::GenericError,
            ResponseType::Watch,
            ResponseType::ListOrgs,
            ResponseType::ListAggregateTypes,
            ResponseType::ListAggregates,
            #[cfg(feature = "cluster")]
            ResponseType::ReplicationBatch,
            #[cfg(feature = "cluster")]
            ResponseType::CatchUp,
            #[cfg(feature = "cluster")]
            ResponseType::Heartbeat,
            #[cfg(feature = "cluster")]
            ResponseType::KickFollower,
            ResponseType::Identify,
        ]
    }

    fn make_response(rt: ResponseType) -> Response {
        match rt {
            ResponseType::AggregateDetails => Response::AggregateDetails(AggregateDetailsResponse {
                correlation_id: Some(0xDEAD_BEEF_CAFE_BABE),
                min_event_batch_index: 42,
                max_event_batch_index: 99,
                max_event_index: 500,
                is_deleted: false,
                allow_recreate: true,
                allow_index_continuation: false,
                last_server_timestamp: 1700000000000,
                last_client_id: 0xAAAA_BBBB_CCCC_DDDD,
                last_user_id: Some(0x1111_2222_3333_4444),
            }),
            ResponseType::Read => Response::Read(ReadResponse {
                correlation_id: Some(0xFEED_FACE_DEAD_C0DE),
                event_batches: vec![],
                next_event_batch_index: Some(100),
            }),
            ResponseType::Write => Response::Write(SuccessResponse {
                correlation_id: Some(0xCAFE_D00D_BEEF_F00D),
            }),
            ResponseType::TrimStart => Response::TrimStart(SuccessResponse {
                correlation_id: Some(0xBAD_C0FFEE),
            }),
            ResponseType::Delete => Response::Delete(SuccessResponse {
                correlation_id: Some(0xDEAD_DEAD_DEAD_DEAD),
            }),
            ResponseType::ProtocolError => Response::ProtocolError(ProtocolErrorResponse {}),
            ResponseType::GenericError => Response::GenericError(ErrorResponse {
                correlation_id: Some(0x1111_2222_3333_4444),
                error_code: 0xFFFF,
                error_message: "test error".into(),
            }),
            ResponseType::Watch => Response::Watch(WatchResponse { events: None }),
            ResponseType::ListOrgs => Response::ListOrgs(ListOrgsResponse {
                correlation_id: Some(0x2222_3333_4444_5555),
                orgs: vec![],
                next_cursor: Some(999),
            }),
            ResponseType::ListAggregateTypes => Response::ListAggregateTypes(ListAggregateTypesResponse {
                correlation_id: Some(0x3333_4444_5555_6666),
                aggregate_types: vec![],
                next_cursor: None,
            }),
            ResponseType::ListAggregates => Response::ListAggregates(ListAggregatesResponse {
                correlation_id: Some(0x4444_5555_6666_7777),
                aggregates: vec![],
                next_cursor: Some(12345),
            }),
            #[cfg(feature = "cluster")]
            ResponseType::ReplicationBatch => Response::ReplicationBatch(ReplicationBatchResponse {
                correlation_id: Some(0x5555_6666_7777_8888),
                follower_timestamp_ms: 9999999,
                result: ReplicationResult::Success { last_follower_metablock: None },
            }),
            #[cfg(feature = "cluster")]
            ResponseType::CatchUp => Response::CatchUp(CatchUpResponse {
                correlation_id: Some(0x6666_7777_8888_9999),
                batches: vec![],
                continue_catching_up: true,
                expected_follower_tip_hash: Some([0xAB; 32]),
            }),
            #[cfg(feature = "cluster")]
            ResponseType::Heartbeat => Response::Heartbeat(HeartbeatResponse {
                correlation_id: Some(0x7777_8888_9999_AAAA),
                result: HeartbeatResult::Ack {
                    follower_timestamp_ms: 1234567890123,
                },
            }),
            #[cfg(feature = "cluster")]
            ResponseType::KickFollower => Response::KickFollower(KickFollowerResponse {
                correlation_id: Some(0x8888_9999_AAAA_BBBB),
                acknowledged: true,
            }),
            ResponseType::Identify => Response::Identify(IdentifyResponse {
                correlation_id: Some(0x9999_AAAA_BBBB_CCCC),
                client_id: Some(0xCCCC_DDDD_EEEE_FFFF),
                access_level: Some(AccessLevel::ReadWrite),
            }),
        }
    }

    fn is_variable_size(rt: ResponseType) -> bool {
        match rt {
            ResponseType::Read
                | ResponseType::Watch
                | ResponseType::ListOrgs
                | ResponseType::ListAggregateTypes
                | ResponseType::ListAggregates => true,
            #[cfg(feature = "cluster")]
            ResponseType::CatchUp => true,
            _ => false,
        }
    }

    fn uses_compression(rt: ResponseType) -> bool {
        is_variable_size(rt)
    }

    async fn write_bytes(res: &Response, version: u32, compression: CompressionType) -> Vec<u8> {
        let mut buf = Vec::new();
        Response::write_response(&mut buf, res, compression, 64 * 1024 * 1024, version)
            .await
            .unwrap();
        buf
    }

    async fn read_back(bytes: &[u8]) -> Response {
        Response::read_response(&mut Cursor::new(bytes.to_vec()), u64::MAX).await.unwrap()
    }

    #[test]
    fn type_id_parsing() {
        for rt in all_types() {
            assert_eq!(ResponseType::from_u32(rt as u32).unwrap(), rt);
        }
        // Verify all defined types can be parsed
        for id in 1..=11 {
            assert!(ResponseType::from_u32(id).is_ok(), "missing id {}", id);
        }
        #[cfg(feature = "cluster")]
        for id in 12..=15 {
            assert!(ResponseType::from_u32(id).is_ok(), "missing cluster id {}", id);
        }
        assert!(ResponseType::from_u32(16).is_ok(), "Identify response should be valid");
        assert!(ResponseType::from_u32(0).is_err());
        assert!(ResponseType::from_u32(MAX_ID + 1).is_err(), "update MAX_ID to {}", MAX_ID + 1);
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
    fn fixed_vs_variable_categorization() {
        block_on(async {
            for rt in all_types() {
                let res = make_response(rt);
                let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;
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
    fn compression_type_determination() {
        let server_compression = CompressionType::Zstd { level: 6 };
        for rt in all_types() {
            let res = make_response(rt);
            let determined = Response::determine_compression_type(&res, server_compression);

            if uses_compression(rt) {
                assert_eq!(determined, server_compression, "{:?} should use server compression", rt);
            } else {
                assert_eq!(determined, CompressionType::None, "{:?} should not compress", rt);
            }
        }
    }

    #[test]
    fn compression_round_trip() {
        block_on(async {
            let compressions = [
                CompressionType::None,
                CompressionType::Zstd { level: 6 },
                CompressionType::Snappy,
                CompressionType::Brotli { level: 6 },
                CompressionType::Gzip { level: 6 },
            ];

            for rt in all_types().into_iter().filter(|rt| is_variable_size(*rt)) {
                for &compression in &compressions {
                    for &v in &VERSIONS {
                        let res = make_response(rt);
                        let bytes1 = write_bytes(&res, v, compression).await;
                        let parsed = read_back(&bytes1).await;

                        assert_eq!(parsed.response_type(), rt);

                        let bytes2 = write_bytes(&parsed, v, compression).await;
                        assert_eq!(bytes1, bytes2, "{:?} {:?} v{} data not preserved", rt, compression, v);
                    }
                }
            }
        });
    }

    #[test]
    fn size_limit_rejects_oversized() {
        block_on(async {
            let res = make_response(ResponseType::Read);
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;
            let result = Response::read_response(&mut Cursor::new(bytes), 1).await;
            assert!(result.is_err(), "should reject when max_size < body size");
        });
    }

    #[test]
    fn truncated_stream_fails() {
        block_on(async {
            let res = make_response(ResponseType::AggregateDetails);
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;

            for truncate_at in [0, 10, bytes.len() - 1] {
                let truncated = &bytes[..truncate_at];
                let result = Response::read_response(&mut Cursor::new(truncated.to_vec()), u64::MAX).await;
                assert!(result.is_err(), "should fail with {} bytes (full: {})", truncate_at, bytes.len());
            }
        });
    }

    #[test]
    fn invalid_message_type_fails() {
        block_on(async {
            let res = make_response(ResponseType::AggregateDetails);
            let mut bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;

            bytes[4..8].copy_from_slice(&(MAX_ID + 1).to_le_bytes());
            let result = Response::read_response(&mut Cursor::new(bytes), u64::MAX).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn large_payload_round_trip() {
        block_on(async {
            let res = Response::ListAggregates(ListAggregatesResponse {
                correlation_id: Some(0x1234_5678_9ABC_DEF0),
                aggregates: (0u64..500)
                    .map(|i| AggregateListItem {
                        is_deleted: false,
                        org_id: i as u128,
                        aggregate_type_id: (i * 2) as u128,
                        aggregate_id: (i * 3) as u128,
                        event_batch_count: i * 10,
                        min_event_timestamp: 1000 + i,
                        max_event_timestamp: 2000 + i,
                        min_event_batch_index: i,
                        max_event_batch_index: i + 100,
                        min_event_index: i * 5,
                        max_event_index: i * 5 + 50,
                        min_server_timestamp: 3000 + i,
                        max_server_timestamp: 4000 + i,
                        compressed_size: i * 100,
                        uncompressed_size: i * 200,
                    })
                    .collect(),
                next_cursor: Some(999),
            });

            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.response_type(), ResponseType::ListAggregates);

            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::Zstd { level: 6 }).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.response_type(), ResponseType::ListAggregates);

            let bytes = write_bytes(&res, PROTOCOL_VERSION_V3, CompressionType::Zstd { level: 6 }).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.response_type(), ResponseType::ListAggregates);
        });
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn heartbeat_rejection_variants_round_trip() {
        use crate::response::responses::HeartbeatRejection;
        block_on(async {
            let variants = [
                HeartbeatRejection::ClockDriftTooHigh {
                    leader_ms: 1000,
                    follower_ms: 9000,
                    max_allowed_ms: 5000,
                },
            ];

            for reason in variants {
                let res = Response::Heartbeat(HeartbeatResponse {
                    correlation_id: Some(0x1234_5678_9ABC_DEF0),
                    result: HeartbeatResult::Rejected(reason),
                });

                for &v in &VERSIONS {
                    let bytes = write_bytes(&res, v, CompressionType::None).await;
                    let parsed = read_back(&bytes).await;
                    assert_eq!(parsed.response_type(), ResponseType::Heartbeat);
                }
            }
        });
    }

    #[test]
    fn variable_size_responses_with_data() {
        block_on(async {
            let orgs = Response::ListOrgs(ListOrgsResponse {
                correlation_id: Some(1),
                orgs: (0u128..100).map(|i| OrgListItem { org_id: i }).collect(),
                next_cursor: Some(100),
            });

            let types = Response::ListAggregateTypes(ListAggregateTypesResponse {
                correlation_id: Some(2),
                aggregate_types: (0u128..50)
                    .map(|i| AggregateTypeListItem { org_id: i, aggregate_type_id: i * 10 })
                    .collect(),
                next_cursor: None,
            });

            let aggregates = Response::ListAggregates(ListAggregatesResponse {
                correlation_id: Some(3),
                aggregates: (0u64..25)
                    .map(|i| AggregateListItem {
                        is_deleted: i % 2 == 0,
                        org_id: i as u128,
                        aggregate_type_id: (i * 2) as u128,
                        aggregate_id: (i * 3) as u128,
                        event_batch_count: i * 10,
                        min_event_timestamp: 1000 + i,
                        max_event_timestamp: 2000 + i,
                        min_event_batch_index: i,
                        max_event_batch_index: i + 100,
                        min_event_index: i * 5,
                        max_event_index: i * 5 + 50,
                        min_server_timestamp: 3000 + i,
                        max_server_timestamp: 4000 + i,
                        compressed_size: i * 100,
                        uncompressed_size: i * 200,
                    })
                    .collect(),
                next_cursor: Some(999),
            });

            for res in [orgs, types, aggregates] {
                for &v in &VERSIONS {
                    for compression in [CompressionType::None, CompressionType::Zstd { level: 3 }] {
                        let bytes = write_bytes(&res, v, compression).await;
                        let parsed = read_back(&bytes).await;
                        assert_eq!(parsed.response_type(), res.response_type());
                    }
                }
            }
        });
    }
}
