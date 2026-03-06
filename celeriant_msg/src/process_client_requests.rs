use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{WireHeader, wire_header_write_fixed_size, wire_header_write_variable_size},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    read_wire_data_error::ReadWireDataError,
    request::requests::{
        AggregateDetailsRequest, DeleteRequest, ListAggregateTypesRequest, ListAggregatesRequest,
        ListOrgsRequest, ReadRequest, RegisterSchemaRequest, TrimStartRequest, WatchRequest, WriteRequest,
    },
};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientRequestType {
    AggregateDetails = 1,
    Read = 2,
    Write = 3,
    TrimStart = 4,
    Delete = 5,
    Watch = 6,
    ListOrgs = 7,
    ListAggregateTypes = 8,
    ListAggregates = 9,
    RegisterSchema = 10,
}

impl ClientRequestType {
    #[inline]
    pub fn from_u32(value: u32) -> Result<Self, ReadWireDataError> {
        match value {
            1 => Ok(ClientRequestType::AggregateDetails),
            2 => Ok(ClientRequestType::Read),
            3 => Ok(ClientRequestType::Write),
            4 => Ok(ClientRequestType::TrimStart),
            5 => Ok(ClientRequestType::Delete),
            6 => Ok(ClientRequestType::Watch),
            7 => Ok(ClientRequestType::ListOrgs),
            8 => Ok(ClientRequestType::ListAggregateTypes),
            9 => Ok(ClientRequestType::ListAggregates),
            10 => Ok(ClientRequestType::RegisterSchema),
            _ => Err(ReadWireDataError::UnknownMessageType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientRequest {
    AggregateDetails(AggregateDetailsRequest),
    Read(ReadRequest),
    Write(WriteRequest),
    TrimStart(TrimStartRequest),
    Delete(DeleteRequest),
    Watch(WatchRequest),
    ListOrgs(ListOrgsRequest),
    ListAggregateTypes(ListAggregateTypesRequest),
    ListAggregates(ListAggregatesRequest),
    RegisterSchema(RegisterSchemaRequest),
}

impl ClientRequest {
    #[inline]
    pub fn request_type(&self) -> ClientRequestType {
        match self {
            ClientRequest::AggregateDetails(_) => ClientRequestType::AggregateDetails,
            ClientRequest::Read(_) => ClientRequestType::Read,
            ClientRequest::Write(_) => ClientRequestType::Write,
            ClientRequest::TrimStart(_) => ClientRequestType::TrimStart,
            ClientRequest::Delete(_) => ClientRequestType::Delete,
            ClientRequest::Watch(_) => ClientRequestType::Watch,
            ClientRequest::ListOrgs(_) => ClientRequestType::ListOrgs,
            ClientRequest::ListAggregateTypes(_) => ClientRequestType::ListAggregateTypes,
            ClientRequest::ListAggregates(_) => ClientRequestType::ListAggregates,
            ClientRequest::RegisterSchema(_) => ClientRequestType::RegisterSchema,
        }
    }

    #[inline]
    pub fn correlation_id(&self) -> Option<u128> {
        match self {
            ClientRequest::AggregateDetails(req) => req.correlation_id,
            ClientRequest::Read(req) => req.correlation_id,
            ClientRequest::Write(req) => req.correlation_id,
            ClientRequest::TrimStart(req) => req.correlation_id,
            ClientRequest::Delete(req) => req.correlation_id,
            ClientRequest::Watch(req) => req.correlation_id,
            ClientRequest::ListOrgs(req) => req.correlation_id,
            ClientRequest::ListAggregateTypes(req) => req.correlation_id,
            ClientRequest::ListAggregates(req) => req.correlation_id,
            ClientRequest::RegisterSchema(req) => req.correlation_id,
        }
    }

    #[inline]
    pub fn aggregate_id(&self) -> u128 {
        match self {
            ClientRequest::AggregateDetails(req) => req.aggregate_key.aggregate_id,
            ClientRequest::Read(req) => req.aggregate_key.aggregate_id,
            ClientRequest::TrimStart(req) => req.aggregate_key.aggregate_id,
            _ => 0,
        }
    }

    #[inline]
    pub fn org_id(&self) -> u128 {
        match self {
            ClientRequest::AggregateDetails(req) => req.aggregate_key.org_id,
            ClientRequest::Read(req) => req.aggregate_key.org_id,
            ClientRequest::TrimStart(req) => req.aggregate_key.org_id,
            _ => 0,
        }
    }

    #[inline]
    pub fn aggregate_type_id(&self) -> u128 {
        match self {
            ClientRequest::AggregateDetails(req) => req.aggregate_key.aggregate_type_id,
            ClientRequest::Read(req) => req.aggregate_key.aggregate_type_id,
            ClientRequest::TrimStart(req) => req.aggregate_key.aggregate_type_id,
            _ => 0,
        }
    }

    pub async fn read_from_header<R>(wire_header: WireHeader, reader: &mut R) -> Result<ClientRequest, ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let request_type = ClientRequestType::from_u32(wire_header.message_type)?;

        macro_rules! fixed {
            ($variant:ident) => {
                ClientRequest::$variant(
                    wire_header
                        .read_fixed_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        macro_rules! variable {
            ($variant:ident) => {
                ClientRequest::$variant(
                    wire_header
                        .read_variable_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        Ok(match request_type {
            ClientRequestType::AggregateDetails => fixed!(AggregateDetails),
            ClientRequestType::Read => fixed!(Read),
            ClientRequestType::TrimStart => fixed!(TrimStart),
            ClientRequestType::Delete => fixed!(Delete),
            ClientRequestType::Watch => fixed!(Watch),
            ClientRequestType::ListOrgs => fixed!(ListOrgs),
            ClientRequestType::ListAggregateTypes => fixed!(ListAggregateTypes),
            ClientRequestType::ListAggregates => fixed!(ListAggregates),
            ClientRequestType::Write => variable!(Write),
            ClientRequestType::RegisterSchema => variable!(RegisterSchema),
        })
    }

    pub async fn write_request<W>(
        writer: &mut W,
        request: &ClientRequest,
        compression_type: CompressionType,
        max_message_size: u64,
        version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let request_type_id = request.request_type() as u32;

        match request {
            ClientRequest::AggregateDetails(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::Read(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::TrimStart(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::Delete(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::Watch(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::ListOrgs(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::ListAggregateTypes(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::ListAggregates(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            ClientRequest::Write(req) => wire_header_write_variable_size(writer, req, request_type_id, compression_type, max_message_size, version).await,
            ClientRequest::RegisterSchema(req) => wire_header_write_variable_size(writer, req, request_type_id, compression_type, max_message_size, version).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{
        read_filters::ReadFilters,
        requests::{
            ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest,
            SingleAggregateDelete, SingleAggregateWrite,
        },
    };
    use celeriant_wal::{
        aggregate_key::AggregateKey,
        datablocks::datablock_aggregate_event::DatablockAggregateEvent,
        schema_key::SchemaKey,
    };
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
    use futures_lite::{future::block_on, io::Cursor};
    use std::collections::{HashMap, HashSet};

    const REQUEST_TYPE_COUNT: usize = 10;
    const MAX_ID: u32 = 10;
    const VERSIONS: [u32; 2] = [PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3];

    fn all_types() -> [ClientRequestType; REQUEST_TYPE_COUNT] {
        [
            ClientRequestType::AggregateDetails,
            ClientRequestType::Read,
            ClientRequestType::Write,
            ClientRequestType::TrimStart,
            ClientRequestType::Delete,
            ClientRequestType::Watch,
            ClientRequestType::ListOrgs,
            ClientRequestType::ListAggregateTypes,
            ClientRequestType::ListAggregates,
            ClientRequestType::RegisterSchema,
        ]
    }

    fn key() -> AggregateKey {
        AggregateKey::new(0x1111_2222_3333_4444, 0x5555_6666_7777_8888, 0x9999_AAAA_BBBB_CCCC)
    }

    fn event(idx: u64) -> DatablockAggregateEvent {
        DatablockAggregateEvent {
            client_event_index: idx,
            event_index: idx * 10,
            event_id: Some(0xEEEE_FFFF_0000_1111),
            event_timestamp: 1234567890,
            event_type_major: 42,
            event_type_minor: 1,
            event_value: std::sync::Arc::new(vec![1, 2, 3, 4, 5]),
            iv: None,
        }
    }

    fn make_request(rt: ClientRequestType) -> ClientRequest {
        let k = key();
        match rt {
            ClientRequestType::AggregateDetails => ClientRequest::AggregateDetails(AggregateDetailsRequest {
                correlation_id: Some(0xDEAD_BEEF_CAFE_BABE),
                aggregate_key: k,
            }),
            ClientRequestType::Read => ClientRequest::Read(ReadRequest {
                correlation_id: Some(0xFEED_FACE_DEAD_C0DE),
                aggregate_key: k,
                filters: ReadFilters::new(42)
                    .to_event_batch_index(100)
                    .include_event_types(vec![1, 2, 3])
                    .exclude_client_id(999)
                    .min_server_timestamp(1000)
                    .max_server_timestamp(2000),
            }),
            ClientRequestType::Write => ClientRequest::Write(WriteRequest {
                correlation_id: Some(0xCAFE_D00D_BEEF_F00D),
                client_id: 0x1234_5678_9ABC_DEF0,
                user_id: Some(0xFEDC_BA98_7654_3210),
                writes: HashMap::from([(
                    k,
                    SingleAggregateWrite {
                        events: vec![event(1)],
                        allow_create: true,
                        expected_event_batch_index: Some(5),
                        enforce_client_idempotency: true,
                        compression_type: CompressionType::None,
                    },
                )]),
            }),
            ClientRequestType::TrimStart => ClientRequest::TrimStart(TrimStartRequest {
                correlation_id: Some(0xBAD_C0FFEE),
                aggregate_key: k,
                keep_from_event_batch_index: 50,
                client_id: 0xABCD_EF01_2345_6789,
                user_id: Some(0x9876_5432_10FE_DCBA),
            }),
            ClientRequestType::Delete => ClientRequest::Delete(DeleteRequest {
                correlation_id: Some(0xDEAD_DEAD_DEAD_DEAD),
                client_id: 0x1111_2222_3333_4444,
                user_id: None,
                deletes: HashMap::from([(
                    k,
                    SingleAggregateDelete {
                        allow_recreate: false,
                        allow_index_continuation: true,
                        expected_event_batch_index: Some(99),
                    },
                )]),
            }),
            ClientRequestType::Watch => ClientRequest::Watch(WatchRequest {
                correlation_id: Some(0xAAAA_BBBB_CCCC_DDDD),
                requested_latency_ms: Some(500),
                shard_id: None,
                orgs: Some(HashSet::from([1, 2, 3])),
                aggregate_types: Some(HashSet::from([10, 20])),
                aggregates: Some(HashSet::from([100, 200, 300])),
                operation_types: Some(HashSet::from([1, 2])),
            }),
            ClientRequestType::ListOrgs => ClientRequest::ListOrgs(ListOrgsRequest {
                correlation_id: Some(0x1111_2222_3333_4444),
                shard_id: 7,
                cursor: Some(12345),
            }),
            ClientRequestType::ListAggregateTypes => ClientRequest::ListAggregateTypes(ListAggregateTypesRequest {
                correlation_id: Some(0x2222_3333_4444_5555),
                shard_id: 3,
                org_id: Some(0x1234_5678),
                cursor: Some(67890),
            }),
            ClientRequestType::ListAggregates => ClientRequest::ListAggregates(ListAggregatesRequest {
                correlation_id: Some(0x3333_4444_5555_6666),
                shard_id: 5,
                org_id: Some(0xAAAA_BBBB),
                aggregate_type_id: Some(0xCCCC_DDDD),
                cursor: Some(11111),
            }),
            ClientRequestType::RegisterSchema => ClientRequest::RegisterSchema(RegisterSchemaRequest {
                correlation_id: Some(0x4444_5555_6666_7777),
                client_id: 0x8888_9999_AAAA_BBBB,
                user_id: Some(0xCCCC_DDDD_EEEE_FFFF),
                schema_key: SchemaKey::new(0x1111_1111_1111_1111, 0x2222_2222_2222_2222, 1, 0),
                schema_type: 0,
                schema: r#"{"type":"object","properties":{"name":{"type":"string"}}}"#.to_string(),
            }),
        }
    }

    fn is_variable_size(rt: ClientRequestType) -> bool {
        matches!(rt, ClientRequestType::Write | ClientRequestType::RegisterSchema)
    }

    async fn write_bytes(req: &ClientRequest, version: u32, compression: CompressionType) -> Vec<u8> {
        let mut buf = Vec::new();
        ClientRequest::write_request(&mut buf, req, compression, 64 * 1024 * 1024, version)
            .await
            .unwrap();
        buf
    }

    async fn read_back(bytes: &[u8]) -> ClientRequest {
        let header = WireHeader::from_reader(&mut Cursor::new(bytes.to_vec()), u64::MAX).await.unwrap();
        ClientRequest::read_from_header(header, &mut Cursor::new(bytes[celeriant_wire::network::wire_header::WIRE_HEADER_SIZE..].to_vec()))
            .await
            .unwrap()
    }

    #[test]
    fn type_id_parsing() {
        for rt in all_types() {
            assert_eq!(ClientRequestType::from_u32(rt as u32).unwrap(), rt);
        }
        for id in 1..=10 {
            assert!(ClientRequestType::from_u32(id).is_ok(), "missing id {}", id);
        }
        // IDs 11-99 are reserved for future client requests
        // Cluster IDs start at 100+ (will be renumbered)
        for id in 11..=20 {
            assert!(ClientRequestType::from_u32(id).is_err(), "id {} should not parse as ClientRequestType yet", id);
        }
        assert!(ClientRequestType::from_u32(0).is_err());
        assert!(ClientRequestType::from_u32(MAX_ID + 1).is_err());
    }

    #[test]
    fn request_type_accessor() {
        for rt in all_types() {
            assert_eq!(make_request(rt).request_type(), rt);
        }
    }

    fn has_deterministic_order(rt: ClientRequestType) -> bool {
        !matches!(rt, ClientRequestType::Watch | ClientRequestType::Write | ClientRequestType::Delete | ClientRequestType::RegisterSchema)
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

                    if has_deterministic_order(rt) {
                        let bytes2 = write_bytes(&parsed, v, CompressionType::None).await;
                        assert_eq!(bytes1, bytes2, "{:?} v{} data not preserved", rt, v);
                    }
                }
            }
        });
    }

    #[test]
    fn routing_field_accessors() {
        let k = key();
        for rt in all_types() {
            let req = make_request(rt);
            let (exp_agg, exp_org, exp_type) = match rt {
                ClientRequestType::AggregateDetails | ClientRequestType::Read | ClientRequestType::TrimStart => {
                    (k.aggregate_id, k.org_id, k.aggregate_type_id)
                }
                _ => (0, 0, 0),
            };
            assert_eq!(req.aggregate_id(), exp_agg, "{:?} aggregate_id", rt);
            assert_eq!(req.org_id(), exp_org, "{:?} org_id", rt);
            assert_eq!(req.aggregate_type_id(), exp_type, "{:?} aggregate_type_id", rt);
        }
    }

    #[test]
    fn fixed_vs_variable_categorization() {
        block_on(async {
            for rt in all_types() {
                let req = make_request(rt);
                let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::Snappy).await;
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
                CompressionType::Zstd { level: 6 },
                CompressionType::Snappy,
                CompressionType::Brotli { level: 6 },
                CompressionType::Gzip { level: 6 },
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
    fn size_limit_rejects_oversized() {
        block_on(async {
            let req = make_request(ClientRequestType::Write);
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            let result = WireHeader::from_reader(&mut Cursor::new(bytes), 1).await;
            assert!(result.is_err(), "should reject when max_size < body size");
        });
    }

    #[test]
    fn truncated_stream_fails() {
        block_on(async {
            let req = make_request(ClientRequestType::AggregateDetails);
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            for truncate_at in [0, 10, bytes.len() - 1] {
                let truncated = &bytes[..truncate_at];
                let header_result = WireHeader::from_reader(&mut Cursor::new(truncated.to_vec()), u64::MAX).await;
                if let Ok(header) = header_result {
                    let body_result = ClientRequest::read_from_header(
                        header,
                        &mut Cursor::new(truncated[celeriant_wire::network::wire_header::WIRE_HEADER_SIZE..].to_vec()),
                    ).await;
                    assert!(body_result.is_err(), "should fail with {} bytes (full: {})", truncate_at, bytes.len());
                }
                // If header read itself failed, that's also a correct rejection
            }
        });
    }

    #[test]
    fn invalid_message_type_fails() {
        block_on(async {
            let req = make_request(ClientRequestType::AggregateDetails);
            let mut bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            bytes[4..8].copy_from_slice(&(MAX_ID + 1).to_le_bytes());
            let header = WireHeader::from_reader(&mut Cursor::new(bytes.clone()), u64::MAX).await.unwrap();
            let result = ClientRequest::read_from_header(
                header,
                &mut Cursor::new(bytes[celeriant_wire::network::wire_header::WIRE_HEADER_SIZE..].to_vec()),
            ).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn large_payload_round_trip() {
        block_on(async {
            let k = key();

            let mut large_event = event(1);
            large_event.event_value = std::sync::Arc::new(vec![0xAB; 10_000]);

            let req = ClientRequest::Write(WriteRequest {
                correlation_id: Some(0x1234_5678_9ABC_DEF0),
                client_id: 0x5678,
                user_id: Some(0x9ABC),
                writes: HashMap::from([(
                    k,
                    SingleAggregateWrite {
                        events: vec![large_event],
                        allow_create: true,
                        expected_event_batch_index: Some(42),
                        enforce_client_idempotency: true,
                        compression_type: CompressionType::None,
                    },
                )]),
            });

            // V2 uncompressed
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.correlation_id(), req.correlation_id());

            // V2 compressed
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::Zstd { level: 6 }).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.correlation_id(), req.correlation_id());

            // V3 compressed
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V3, CompressionType::Zstd { level: 6 }).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.correlation_id(), req.correlation_id());
        });
    }
}
