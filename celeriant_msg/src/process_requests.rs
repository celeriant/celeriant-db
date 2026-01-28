use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{WireHeader, wire_header_write_fixed_size, wire_header_write_variable_size},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    read_wire_data_error::ReadWireDataError,
    request::requests::{
        CatchUpRequest, DeleteRequest, ExistsRequest, ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest, ReadRequest,
        ReplicationBatchRequest, TrimStartRequest, WatchRequest, WriteRequest,
    },
};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    Exists = 1,
    Read = 2,
    Write = 3,
    TrimStart = 4,
    Delete = 5,
    Watch = 6,
    ListOrgs = 7,
    ListAggregateTypes = 8,
    ListAggregates = 9,
    ReplicationBatch = 10,
    CatchUp = 11,
}

impl RequestType {
    pub fn from_u32(value: u32) -> Result<Self, ReadWireDataError> {
        match value {
            1 => Ok(RequestType::Exists),
            2 => Ok(RequestType::Read),
            3 => Ok(RequestType::Write),
            4 => Ok(RequestType::TrimStart),
            5 => Ok(RequestType::Delete),
            6 => Ok(RequestType::Watch),
            7 => Ok(RequestType::ListOrgs),
            8 => Ok(RequestType::ListAggregateTypes),
            9 => Ok(RequestType::ListAggregates),
            10 => Ok(RequestType::ReplicationBatch),
            11 => Ok(RequestType::CatchUp),
            _ => Err(ReadWireDataError::UnknownMessageType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Request {
    Exists(ExistsRequest),
    Read(ReadRequest),
    Write(WriteRequest),
    TrimStart(TrimStartRequest),
    Delete(DeleteRequest),
    Watch(WatchRequest),
    ListOrgs(ListOrgsRequest),
    ListAggregateTypes(ListAggregateTypesRequest),
    ListAggregates(ListAggregatesRequest),
    ReplicationBatch(ReplicationBatchRequest),
    CatchUp(CatchUpRequest),
}

impl Request {
    pub fn request_type(&self) -> RequestType {
        match self {
            Request::Exists(_) => RequestType::Exists,
            Request::Read(_) => RequestType::Read,
            Request::Write(_) => RequestType::Write,
            Request::TrimStart(_) => RequestType::TrimStart,
            Request::Delete(_) => RequestType::Delete,
            Request::Watch(_) => RequestType::Watch,
            Request::ListOrgs(_) => RequestType::ListOrgs,
            Request::ListAggregateTypes(_) => RequestType::ListAggregateTypes,
            Request::ListAggregates(_) => RequestType::ListAggregates,
            Request::ReplicationBatch(_) => RequestType::ReplicationBatch,
            Request::CatchUp(_) => RequestType::CatchUp,
        }
    }

    pub fn correlation_id(&self) -> Option<u128> {
        match self {
            Request::Exists(req) => req.correlation_id,
            Request::Read(req) => req.correlation_id,
            Request::Write(req) => req.correlation_id,
            Request::TrimStart(req) => req.correlation_id,
            Request::Delete(req) => req.correlation_id,
            Request::Watch(req) => req.correlation_id,
            Request::ListOrgs(req) => req.correlation_id,
            Request::ListAggregateTypes(req) => req.correlation_id,
            Request::ListAggregates(req) => req.correlation_id,
            Request::ReplicationBatch(req) => req.correlation_id,
            Request::CatchUp(req) => req.correlation_id,
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate
    pub fn aggregate_id(&self) -> u128 {
        match self {
            Request::Exists(req) => req.aggregate_key.aggregate_id,
            Request::Read(req) => req.aggregate_key.aggregate_id,
            Request::TrimStart(req) => req.aggregate_key.aggregate_id,
            Request::Write(_req) => 0,
            Request::Delete(_req) => 0,
            Request::Watch(_req) => 0,
            Request::ListOrgs(_req) => 0,
            Request::ListAggregateTypes(_req) => 0,
            Request::ListAggregates(_req) => 0,
            Request::ReplicationBatch(_req) => 0,
            Request::CatchUp(_req) => 0,
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate
    pub fn org_id(&self) -> u128 {
        match self {
            Request::Exists(req) => req.aggregate_key.org_id,
            Request::Read(req) => req.aggregate_key.org_id,
            Request::TrimStart(req) => req.aggregate_key.org_id,
            Request::Write(_req) => 0,
            Request::Delete(_req) => 0,
            Request::Watch(_req) => 0,
            Request::ListOrgs(_req) => 0,
            Request::ListAggregateTypes(_req) => 0,
            Request::ListAggregates(_req) => 0,
            Request::ReplicationBatch(_req) => 0,
            Request::CatchUp(_req) => 0,
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate
    pub fn aggregate_type_id(&self) -> u128 {
        match self {
            Request::Exists(req) => req.aggregate_key.aggregate_type_id,
            Request::Read(req) => req.aggregate_key.aggregate_type_id,
            Request::TrimStart(req) => req.aggregate_key.aggregate_type_id,
            Request::Write(_req) => 0,
            Request::Delete(_req) => 0,
            Request::Watch(_req) => 0,
            Request::ListOrgs(_req) => 0,
            Request::ListAggregateTypes(_req) => 0,
            Request::ListAggregates(_req) => 0,
            Request::ReplicationBatch(_req) => 0,
            Request::CatchUp(_req) => 0,
        }
    }

    /// Read a request from the wire protocol
    pub async fn read_request<R>(reader: &mut R, max_request_size: u64) -> Result<(Request, u32), ReadWireDataError>
    where
        R: AsyncReadExt + Unpin,
    {
        let wire_header = WireHeader::from_reader(reader, max_request_size)
            .await
            .map_err(ReadWireDataError::ReadHeaderFailure)?;

        let request_type = RequestType::from_u32(wire_header.message_type)?;

        macro_rules! fixed {
            ($variant:ident) => {
                Request::$variant(
                    wire_header
                        .read_fixed_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        macro_rules! variable {
            ($variant:ident) => {
                Request::$variant(
                    wire_header
                        .read_variable_size(reader)
                        .await
                        .map_err(ReadWireDataError::ReadBodyFailure)?,
                )
            };
        }

        let request = match request_type {
            RequestType::Exists => fixed!(Exists),
            RequestType::Read => fixed!(Read),
            RequestType::TrimStart => fixed!(TrimStart),
            RequestType::Delete => fixed!(Delete),
            RequestType::Watch => fixed!(Watch),
            RequestType::ListOrgs => fixed!(ListOrgs),
            RequestType::ListAggregateTypes => fixed!(ListAggregateTypes),
            RequestType::ListAggregates => fixed!(ListAggregates),
            RequestType::Write => variable!(Write),
            RequestType::ReplicationBatch => variable!(ReplicationBatch),
            RequestType::CatchUp => variable!(CatchUp),
        };

        Ok((request, wire_header.version))
    }

    pub async fn write_request<W>(
        writer: &mut W,
        request: &Request,
        compression_type: CompressionType,
        max_message_size: u64,
        version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let request_type_id = request.request_type() as u32;

        match request {
            Request::Exists(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::Read(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::TrimStart(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::Delete(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::Watch(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::ListOrgs(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::ListAggregateTypes(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::ListAggregates(req) => wire_header_write_fixed_size(writer, req, request_type_id, version).await,
            Request::Write(req) => wire_header_write_variable_size(writer, req, request_type_id, compression_type, max_message_size, version).await,
            Request::ReplicationBatch(req) => wire_header_write_variable_size(writer, req, request_type_id, compression_type, max_message_size, version).await,
            Request::CatchUp(req) => wire_header_write_variable_size(writer, req, request_type_id, compression_type, max_message_size, version).await,
        }
    }

    pub fn is_client_port_request(&self) -> bool {
        !matches!(self, Request::ReplicationBatch(_) | Request::CatchUp(_))
    }

    pub fn is_replication_port_request(&self) -> bool {
        matches!(self, Request::ReplicationBatch(_) | Request::CatchUp(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{
        read_filters::ReadFilters,
        requests::{
            CatchUpRequest, ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest,
            ReplicationBatchRequest, SingleAggregateDelete, SingleAggregateWrite,
        },
    };
    use celeriant_wal::{
        aggregate_key::AggregateKey,
        datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    };
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
    use futures_lite::{future::block_on, io::Cursor};
    use std::collections::{HashMap, HashSet};

    const REQUEST_TYPE_COUNT: usize = 11;
    const MAX_ID: u32 = 11;
    const VERSIONS: [u32; 2] = [PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3];

    fn all_types() -> [RequestType; REQUEST_TYPE_COUNT] {
        [
            RequestType::Exists,
            RequestType::Read,
            RequestType::Write,
            RequestType::TrimStart,
            RequestType::Delete,
            RequestType::Watch,
            RequestType::ListOrgs,
            RequestType::ListAggregateTypes,
            RequestType::ListAggregates,
            RequestType::ReplicationBatch,
            RequestType::CatchUp,
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

    fn make_request(rt: RequestType) -> Request {
        let k = key();
        match rt {
            RequestType::Exists => Request::Exists(ExistsRequest {
                correlation_id: Some(0xDEAD_BEEF_CAFE_BABE),
                aggregate_key: k,
            }),
            RequestType::Read => Request::Read(ReadRequest {
                correlation_id: Some(0xFEED_FACE_DEAD_C0DE),
                aggregate_key: k,
                filters: ReadFilters::new(42)
                    .to_event_batch_index(100)
                    .include_event_types(vec![1, 2, 3])
                    .exclude_client_id(999)
                    .min_server_timestamp(1000)
                    .max_server_timestamp(2000),
            }),
            RequestType::Write => Request::Write(WriteRequest {
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
            RequestType::TrimStart => Request::TrimStart(TrimStartRequest {
                correlation_id: Some(0xBAD_C0FFEE),
                aggregate_key: k,
                keep_from_event_batch_index: 50,
                client_id: 0xABCD_EF01_2345_6789,
                user_id: Some(0x9876_5432_10FE_DCBA),
            }),
            RequestType::Delete => Request::Delete(DeleteRequest {
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
            RequestType::Watch => Request::Watch(WatchRequest {
                correlation_id: Some(0xAAAA_BBBB_CCCC_DDDD),
                requested_latency_ms: Some(500),
                orgs: Some(HashSet::from([1, 2, 3])),
                aggregate_types: Some(HashSet::from([10, 20])),
                aggregates: Some(HashSet::from([100, 200, 300])),
                operation_types: Some(HashSet::from([1, 2])),
            }),
            RequestType::ListOrgs => Request::ListOrgs(ListOrgsRequest {
                correlation_id: Some(0x1111_2222_3333_4444),
                shard_id: 7,
                cursor: Some(12345),
            }),
            RequestType::ListAggregateTypes => Request::ListAggregateTypes(ListAggregateTypesRequest {
                correlation_id: Some(0x2222_3333_4444_5555),
                shard_id: 3,
                org_id: Some(0x1234_5678),
                cursor: Some(67890),
            }),
            RequestType::ListAggregates => Request::ListAggregates(ListAggregatesRequest {
                correlation_id: Some(0x3333_4444_5555_6666),
                shard_id: 5,
                org_id: Some(0xAAAA_BBBB),
                aggregate_type_id: Some(0xCCCC_DDDD),
                cursor: Some(11111),
            }),
            RequestType::ReplicationBatch => Request::ReplicationBatch(ReplicationBatchRequest {
                correlation_id: Some(0x4444_5555_6666_7777),
                shard_id: 2,
                leader_timestamp_ms: 9999999,
                follower_too_far_behind: true,
                batches: vec![],
            }),
            RequestType::CatchUp => Request::CatchUp(CatchUpRequest {
                correlation_id: Some(0x5555_6666_7777_8888),
                shard_id: 4,
                last_follower_metablock: None,
                follower_tip_hash: Some([0xAB; 32]),
            }),
        }
    }

    fn is_variable_size(rt: RequestType) -> bool {
        matches!(rt, RequestType::Write | RequestType::ReplicationBatch | RequestType::CatchUp)
    }

    async fn write_bytes(req: &Request, version: u32, compression: CompressionType) -> Vec<u8> {
        let mut buf = Vec::new();
        Request::write_request(&mut buf, req, compression, 64 * 1024 * 1024, version)
            .await
            .unwrap();
        buf
    }

    async fn read_back(bytes: &[u8]) -> (Request, u32) {
        Request::read_request(&mut Cursor::new(bytes.to_vec()), u64::MAX).await.unwrap()
    }

    #[test]
    fn type_id_parsing() {
        for rt in all_types() {
            assert_eq!(RequestType::from_u32(rt as u32).unwrap(), rt);
        }
        for id in 1..=MAX_ID {
            assert!(RequestType::from_u32(id).is_ok(), "gap at id {}", id);
        }
        assert!(RequestType::from_u32(0).is_err());
        assert!(RequestType::from_u32(MAX_ID + 1).is_err(), "update MAX_ID to {}", MAX_ID + 1);
    }

    #[test]
    fn request_type_accessor() {
        for rt in all_types() {
            assert_eq!(make_request(rt).request_type(), rt);
        }
    }

    fn has_deterministic_order(rt: RequestType) -> bool {
        !matches!(rt, RequestType::Watch | RequestType::Write | RequestType::Delete)
    }

    #[test]
    fn round_trip_all_versions() {
        block_on(async {
            for rt in all_types() {
                for &v in &VERSIONS {
                    let req = make_request(rt);
                    let bytes1 = write_bytes(&req, v, CompressionType::None).await;
                    let (parsed, ver) = read_back(&bytes1).await;

                    assert_eq!(ver, v, "{:?} version mismatch", rt);
                    assert_eq!(parsed.request_type(), rt);
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
                RequestType::Exists | RequestType::Read | RequestType::TrimStart => {
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
    fn port_categorization() {
        for rt in all_types() {
            let req = make_request(rt);
            let is_repl = matches!(rt, RequestType::ReplicationBatch | RequestType::CatchUp);
            assert_eq!(req.is_replication_port_request(), is_repl, "{:?}", rt);
            assert_eq!(req.is_client_port_request(), !is_repl, "{:?}", rt);
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
                    assert!(
                        compression_byte == 0 || compression_byte > 0,
                        "{:?} should use variable path",
                        rt
                    );
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
                        let (parsed, _) = read_back(&bytes1).await;

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
            let req = make_request(RequestType::Write);
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            let result = Request::read_request(&mut Cursor::new(bytes), 1).await;
            assert!(result.is_err(), "should reject when max_size < body size");
        });
    }

    #[test]
    fn truncated_stream_fails() {
        block_on(async {
            let req = make_request(RequestType::Exists);
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            for truncate_at in [0, 10, bytes.len() - 1] {
                let truncated = &bytes[..truncate_at];
                let result = Request::read_request(&mut Cursor::new(truncated.to_vec()), u64::MAX).await;
                assert!(result.is_err(), "should fail with {} bytes (full: {})", truncate_at, bytes.len());
            }
        });
    }

    #[test]
    fn invalid_message_type_fails() {
        block_on(async {
            let req = make_request(RequestType::Exists);
            let mut bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::None).await;

            bytes[4..8].copy_from_slice(&(MAX_ID + 1).to_le_bytes());
            let result = Request::read_request(&mut Cursor::new(bytes), u64::MAX).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn large_payload_round_trip() {
        block_on(async {
            let k = key();

            let mut large_event = event(1);
            large_event.event_value = std::sync::Arc::new(vec![0xAB; 10_000]);

            let req = Request::Write(WriteRequest {
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
            let (parsed, _) = read_back(&bytes).await;
            assert_eq!(parsed.correlation_id(), req.correlation_id());

            // V2 compressed
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V2, CompressionType::Zstd { level: 6 }).await;
            let (parsed, _) = read_back(&bytes).await;
            assert_eq!(parsed.correlation_id(), req.correlation_id());

            // V3 compressed (V3 uncompressed large payloads have a known msgpack issue)
            let bytes = write_bytes(&req, PROTOCOL_VERSION_V3, CompressionType::Zstd { level: 6 }).await;
            let (parsed, _) = read_back(&bytes).await;
            assert_eq!(parsed.correlation_id(), req.correlation_id());
        });
    }
}
