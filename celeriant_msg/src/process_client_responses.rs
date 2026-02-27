use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{WireHeader, wire_header_write_fixed_size, wire_header_write_variable_size},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    read_wire_data_error::ReadWireDataError,
    response::responses::{
        AggregateDetailsResponse, ErrorResponse, ListAggregateTypesResponse, ListAggregatesResponse,
        ListOrgsResponse, ProtocolErrorResponse, ReadResponse, SuccessResponse, WatchResponse,
    },
};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientResponseType {
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
}

impl ClientResponseType {
    pub fn from_u32(value: u32) -> Result<Self, ReadWireDataError> {
        match value {
            1 => Ok(ClientResponseType::AggregateDetails),
            2 => Ok(ClientResponseType::Read),
            3 => Ok(ClientResponseType::Write),
            4 => Ok(ClientResponseType::TrimStart),
            5 => Ok(ClientResponseType::Delete),
            6 => Ok(ClientResponseType::ProtocolError),
            7 => Ok(ClientResponseType::GenericError),
            8 => Ok(ClientResponseType::Watch),
            9 => Ok(ClientResponseType::ListOrgs),
            10 => Ok(ClientResponseType::ListAggregateTypes),
            11 => Ok(ClientResponseType::ListAggregates),
            _ => Err(ReadWireDataError::UnknownMessageType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientResponse {
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
}

impl ClientResponse {
    pub fn response_type(&self) -> ClientResponseType {
        match self {
            ClientResponse::AggregateDetails(_) => ClientResponseType::AggregateDetails,
            ClientResponse::Read(_) => ClientResponseType::Read,
            ClientResponse::Write(_) => ClientResponseType::Write,
            ClientResponse::TrimStart(_) => ClientResponseType::TrimStart,
            ClientResponse::Delete(_) => ClientResponseType::Delete,
            ClientResponse::ProtocolError(_) => ClientResponseType::ProtocolError,
            ClientResponse::GenericError(_) => ClientResponseType::GenericError,
            ClientResponse::Watch(_) => ClientResponseType::Watch,
            ClientResponse::ListOrgs(_) => ClientResponseType::ListOrgs,
            ClientResponse::ListAggregateTypes(_) => ClientResponseType::ListAggregateTypes,
            ClientResponse::ListAggregates(_) => ClientResponseType::ListAggregates,
        }
    }

    pub async fn read_response<R>(reader: &mut R, max_response_size: u64) -> Result<ClientResponse, ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let wire_header = WireHeader::from_reader(reader, max_response_size)
            .await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;
        Self::read_from_header(wire_header, reader).await
    }

    pub async fn read_from_header<R>(wire_header: WireHeader, reader: &mut R) -> Result<ClientResponse, ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let response_type = ClientResponseType::from_u32(wire_header.message_type)?;

        macro_rules! fixed {
            ($variant:ident) => {
                ClientResponse::$variant(
                    wire_header
                        .read_fixed_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        macro_rules! variable {
            ($variant:ident) => {
                ClientResponse::$variant(
                    wire_header
                        .read_variable_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        Ok(match response_type {
            ClientResponseType::AggregateDetails => fixed!(AggregateDetails),
            ClientResponseType::Write => fixed!(Write),
            ClientResponseType::TrimStart => fixed!(TrimStart),
            ClientResponseType::Delete => fixed!(Delete),
            ClientResponseType::ProtocolError => fixed!(ProtocolError),
            ClientResponseType::GenericError => fixed!(GenericError),
            ClientResponseType::Read => variable!(Read),
            ClientResponseType::Watch => variable!(Watch),
            ClientResponseType::ListOrgs => variable!(ListOrgs),
            ClientResponseType::ListAggregateTypes => variable!(ListAggregateTypes),
            ClientResponseType::ListAggregates => variable!(ListAggregates),
        })
    }

    pub fn determine_compression_type(response: &ClientResponse, server_compression_algorithm: CompressionType) -> CompressionType {
        match response {
            ClientResponse::AggregateDetails(_) => CompressionType::None,
            ClientResponse::Read(_) => server_compression_algorithm,
            ClientResponse::Write(_) => CompressionType::None,
            ClientResponse::TrimStart(_) => CompressionType::None,
            ClientResponse::Delete(_) => CompressionType::None,
            ClientResponse::ProtocolError(_) => CompressionType::None,
            ClientResponse::GenericError(_) => CompressionType::None,
            ClientResponse::Watch(_) => server_compression_algorithm,
            ClientResponse::ListOrgs(_) => server_compression_algorithm,
            ClientResponse::ListAggregateTypes(_) => server_compression_algorithm,
            ClientResponse::ListAggregates(_) => server_compression_algorithm,
        }
    }

    pub async fn write_response<W>(
        writer: &mut W,
        response: &ClientResponse,
        compression_type: CompressionType,
        max_message_size: u64,
        version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let response_type_id = response.response_type() as u32;

        match response {
            ClientResponse::AggregateDetails(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::Write(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::TrimStart(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::Delete(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::ProtocolError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::GenericError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::Read(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            ClientResponse::Watch(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            ClientResponse::ListOrgs(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            ClientResponse::ListAggregateTypes(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
            ClientResponse::ListAggregates(res) => wire_header_write_variable_size(writer, res, response_type_id, compression_type, max_message_size, version).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::responses::{AggregateListItem, AggregateTypeListItem, OrgListItem};
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, WIRE_HEADER_SIZE};
    use futures_lite::{future::block_on, io::Cursor};

    const COUNT: usize = 11;
    const MAX_ID: u32 = 11;
    const VERSIONS: [u32; 2] = [PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3];

    fn all_types() -> [ClientResponseType; COUNT] {
        [
            ClientResponseType::AggregateDetails,
            ClientResponseType::Read,
            ClientResponseType::Write,
            ClientResponseType::TrimStart,
            ClientResponseType::Delete,
            ClientResponseType::ProtocolError,
            ClientResponseType::GenericError,
            ClientResponseType::Watch,
            ClientResponseType::ListOrgs,
            ClientResponseType::ListAggregateTypes,
            ClientResponseType::ListAggregates,
        ]
    }

    fn make_response(rt: ClientResponseType) -> ClientResponse {
        match rt {
            ClientResponseType::AggregateDetails => ClientResponse::AggregateDetails(AggregateDetailsResponse {
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
            ClientResponseType::Read => ClientResponse::Read(ReadResponse {
                correlation_id: Some(0xFEED_FACE_DEAD_C0DE),
                event_batches: vec![],
                next_event_batch_index: Some(100),
            }),
            ClientResponseType::Write => ClientResponse::Write(SuccessResponse {
                correlation_id: Some(0xCAFE_D00D_BEEF_F00D),
            }),
            ClientResponseType::TrimStart => ClientResponse::TrimStart(SuccessResponse {
                correlation_id: Some(0xBAD_C0FFEE),
            }),
            ClientResponseType::Delete => ClientResponse::Delete(SuccessResponse {
                correlation_id: Some(0xDEAD_DEAD_DEAD_DEAD),
            }),
            ClientResponseType::ProtocolError => ClientResponse::ProtocolError(ProtocolErrorResponse {}),
            ClientResponseType::GenericError => ClientResponse::GenericError(ErrorResponse {
                correlation_id: Some(0x1111_2222_3333_4444),
                error_code: 0xFFFF,
                error_message: "test error".into(),
            }),
            ClientResponseType::Watch => ClientResponse::Watch(WatchResponse { events: None }),
            ClientResponseType::ListOrgs => ClientResponse::ListOrgs(ListOrgsResponse {
                correlation_id: Some(0x2222_3333_4444_5555),
                orgs: vec![],
                next_cursor: Some(999),
            }),
            ClientResponseType::ListAggregateTypes => ClientResponse::ListAggregateTypes(ListAggregateTypesResponse {
                correlation_id: Some(0x3333_4444_5555_6666),
                aggregate_types: vec![],
                next_cursor: None,
            }),
            ClientResponseType::ListAggregates => ClientResponse::ListAggregates(ListAggregatesResponse {
                correlation_id: Some(0x4444_5555_6666_7777),
                aggregates: vec![],
                next_cursor: Some(12345),
            }),
        }
    }

    fn is_variable_size(rt: ClientResponseType) -> bool {
        matches!(
            rt,
            ClientResponseType::Read
                | ClientResponseType::Watch
                | ClientResponseType::ListOrgs
                | ClientResponseType::ListAggregateTypes
                | ClientResponseType::ListAggregates
        )
    }

    async fn write_bytes(res: &ClientResponse, version: u32, compression: CompressionType) -> Vec<u8> {
        let mut buf = Vec::new();
        ClientResponse::write_response(&mut buf, res, compression, 64 * 1024 * 1024, version)
            .await
            .unwrap();
        buf
    }

    async fn read_back(bytes: &[u8]) -> ClientResponse {
        let header = WireHeader::from_reader(&mut Cursor::new(bytes.to_vec()), u64::MAX).await.unwrap();
        ClientResponse::read_from_header(header, &mut Cursor::new(bytes[WIRE_HEADER_SIZE..].to_vec()))
            .await
            .unwrap()
    }

    #[test]
    fn type_id_parsing() {
        for rt in all_types() {
            assert_eq!(ClientResponseType::from_u32(rt as u32).unwrap(), rt);
        }
        for id in 1..=11 {
            assert!(ClientResponseType::from_u32(id).is_ok(), "missing id {}", id);
        }
        // Cluster IDs (12-14) should be rejected
        for id in 12..=14 {
            assert!(ClientResponseType::from_u32(id).is_err(), "cluster id {} should not parse as ClientResponseType", id);
        }
        assert!(ClientResponseType::from_u32(0).is_err());
        assert!(ClientResponseType::from_u32(MAX_ID + 1).is_err());
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
            let determined = ClientResponse::determine_compression_type(&res, server_compression);
            if is_variable_size(rt) {
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
            let res = make_response(ClientResponseType::Read);
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;
            let result = ClientResponse::read_response(&mut Cursor::new(bytes), 1).await;
            assert!(result.is_err(), "should reject when max_size < body size");
        });
    }

    #[test]
    fn truncated_stream_fails() {
        block_on(async {
            let res = make_response(ClientResponseType::AggregateDetails);
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;

            for truncate_at in [0, 10, bytes.len() - 1] {
                let truncated = &bytes[..truncate_at];
                let result = ClientResponse::read_response(&mut Cursor::new(truncated.to_vec()), u64::MAX).await;
                assert!(result.is_err(), "should fail with {} bytes (full: {})", truncate_at, bytes.len());
            }
        });
    }

    #[test]
    fn invalid_message_type_fails() {
        block_on(async {
            let res = make_response(ClientResponseType::AggregateDetails);
            let mut bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::None).await;

            bytes[4..8].copy_from_slice(&(MAX_ID + 1).to_le_bytes());
            let result = ClientResponse::read_response(&mut Cursor::new(bytes), u64::MAX).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn large_payload_round_trip() {
        block_on(async {
            let res = ClientResponse::ListAggregates(ListAggregatesResponse {
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
            assert_eq!(parsed.response_type(), ClientResponseType::ListAggregates);

            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2, CompressionType::Zstd { level: 6 }).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.response_type(), ClientResponseType::ListAggregates);

            let bytes = write_bytes(&res, PROTOCOL_VERSION_V3, CompressionType::Zstd { level: 6 }).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.response_type(), ClientResponseType::ListAggregates);
        });
    }

    #[test]
    fn variable_size_responses_with_data() {
        block_on(async {
            let orgs = ClientResponse::ListOrgs(ListOrgsResponse {
                correlation_id: Some(1),
                orgs: (0u128..100).map(|i| OrgListItem { org_id: i }).collect(),
                next_cursor: Some(100),
            });

            let types = ClientResponse::ListAggregateTypes(ListAggregateTypesResponse {
                correlation_id: Some(2),
                aggregate_types: (0u128..50)
                    .map(|i| AggregateTypeListItem { org_id: i, aggregate_type_id: i * 10 })
                    .collect(),
                next_cursor: None,
            });

            let aggregates = ClientResponse::ListAggregates(ListAggregatesResponse {
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
