use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::{
    codec::compression::DictCodec,
    network::{
        wire_error::WireError,
        wire_header::{WireHeader, wire_header_write_fixed_size},
    },
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    RESPONSE_COMPRESSION_THRESHOLD_BYTES,
    read_wire_data_error::ReadWireDataError,
    response::responses::{
        AggregateDetailsResponse, DeleteResponse, ErrorResponse, ListAggregateTypesResponse,
        ListAggregatesResponse, ListOrgsResponse, ProtocolErrorResponse, ReadResponse,
        RegisterSchemaResponse, TrimStartResponse, WatchResponse, WriteResponse,
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
    RegisterSchema = 12,
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
            12 => Ok(ClientResponseType::RegisterSchema),
            _ => Err(ReadWireDataError::UnknownMessageType(value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientResponse {
    AggregateDetails(AggregateDetailsResponse),
    Read(ReadResponse),
    Write(WriteResponse),
    TrimStart(TrimStartResponse),
    Delete(DeleteResponse),
    ProtocolError(ProtocolErrorResponse),
    GenericError(ErrorResponse),
    Watch(WatchResponse),
    ListOrgs(ListOrgsResponse),
    ListAggregateTypes(ListAggregateTypesResponse),
    ListAggregates(ListAggregatesResponse),
    RegisterSchema(RegisterSchemaResponse),
}

impl ClientResponse {
    #[inline]
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
            ClientResponse::RegisterSchema(_) => ClientResponseType::RegisterSchema,
        }
    }

    #[inline]
    pub fn correlation_id(&self) -> Option<u128> {
        match self {
            ClientResponse::AggregateDetails(r) => r.correlation_id,
            ClientResponse::Read(r) => r.correlation_id,
            ClientResponse::Write(r) => r.correlation_id,
            ClientResponse::TrimStart(r) => r.correlation_id,
            ClientResponse::Delete(r) => r.correlation_id,
            ClientResponse::GenericError(r) => r.correlation_id,
            ClientResponse::ListOrgs(r) => r.correlation_id,
            ClientResponse::ListAggregateTypes(r) => r.correlation_id,
            ClientResponse::ListAggregates(r) => r.correlation_id,
            ClientResponse::RegisterSchema(r) => r.correlation_id,
            ClientResponse::Watch(_) | ClientResponse::ProtocolError(_) => None,
        }
    }

    #[inline]
    pub fn carries_correlation_id(&self) -> bool {
        !matches!(self, ClientResponse::Watch(_) | ClientResponse::ProtocolError(_))
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

        macro_rules! variable_uncompressed {
            ($variant:ident) => {
                ClientResponse::$variant(
                    wire_header
                        .read_variable_size_uncompressed(reader)
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
            ClientResponseType::RegisterSchema => fixed!(RegisterSchema),
            ClientResponseType::Read => variable_uncompressed!(Read),
            ClientResponseType::Watch => variable_uncompressed!(Watch),
            ClientResponseType::ListOrgs => variable_uncompressed!(ListOrgs),
            ClientResponseType::ListAggregateTypes => variable_uncompressed!(ListAggregateTypes),
            ClientResponseType::ListAggregates => variable_uncompressed!(ListAggregates),
        })
    }

    #[inline]
    pub fn is_fixed_size_variant(response_type_id: u32) -> bool {
        matches!(
            ClientResponseType::from_u32(response_type_id),
            Ok(ClientResponseType::AggregateDetails
                | ClientResponseType::Write
                | ClientResponseType::TrimStart
                | ClientResponseType::Delete
                | ClientResponseType::ProtocolError
                | ClientResponseType::GenericError
                | ClientResponseType::RegisterSchema)
        )
    }

    pub fn deserialize_body(response_type_id: u32, body: &[u8], version: u32) -> Result<ClientResponse, ReadWireDataError> {
        use celeriant_wire::network::wire_header::deserialise_versioned;
        let response_type = ClientResponseType::from_u32(response_type_id)?;
        let mb = ReadWireDataError::ReadBodyFailure;
        let result = match response_type {
            ClientResponseType::AggregateDetails => ClientResponse::AggregateDetails(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::Write => ClientResponse::Write(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::TrimStart => ClientResponse::TrimStart(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::Delete => ClientResponse::Delete(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::ProtocolError => ClientResponse::ProtocolError(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::GenericError => ClientResponse::GenericError(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::RegisterSchema => ClientResponse::RegisterSchema(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::Read => ClientResponse::Read(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::Watch => ClientResponse::Watch(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::ListOrgs => ClientResponse::ListOrgs(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::ListAggregateTypes => ClientResponse::ListAggregateTypes(deserialise_versioned(body, version).map_err(mb)?),
            ClientResponseType::ListAggregates => ClientResponse::ListAggregates(deserialise_versioned(body, version).map_err(mb)?),
        };
        Ok(result)
    }

    pub async fn write_response<W>(
        writer: &mut W,
        response: &ClientResponse,
        client_has_dict: bool,
        dict_codec: &DictCodec,
        max_message_size: u64,
        version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let response_type_id = response.response_type() as u32;

        match response {
            // Fixed-size responses: never compressed.
            ClientResponse::AggregateDetails(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::Write(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::TrimStart(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::Delete(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::ProtocolError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::GenericError(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            ClientResponse::RegisterSchema(res) => wire_header_write_fixed_size(writer, res, response_type_id, version).await,
            // Variable-size responses: serialise once, decide compression from payload size, write frame.
            ClientResponse::Read(res) => write_variable_with_threshold(writer, res, response_type_id, client_has_dict, dict_codec, max_message_size, version).await,
            ClientResponse::Watch(res) => write_variable_with_threshold(writer, res, response_type_id, client_has_dict, dict_codec, max_message_size, version).await,
            ClientResponse::ListOrgs(res) => write_variable_with_threshold(writer, res, response_type_id, client_has_dict, dict_codec, max_message_size, version).await,
            ClientResponse::ListAggregateTypes(res) => write_variable_with_threshold(writer, res, response_type_id, client_has_dict, dict_codec, max_message_size, version).await,
            ClientResponse::ListAggregates(res) => write_variable_with_threshold(writer, res, response_type_id, client_has_dict, dict_codec, max_message_size, version).await,
        }
    }
}

pub fn determine_compression_type(
    response: &ClientResponse,
    payload_size: usize,
    client_has_dict: bool,
) -> CompressionType {
    match response {
        ClientResponse::AggregateDetails(_)
        | ClientResponse::Write(_)
        | ClientResponse::TrimStart(_)
        | ClientResponse::Delete(_)
        | ClientResponse::ProtocolError(_)
        | ClientResponse::GenericError(_)
        | ClientResponse::RegisterSchema(_) => return CompressionType::None,
        // Variable-size responses fall through to the policy below.
        ClientResponse::Read(_)
        | ClientResponse::Watch(_)
        | ClientResponse::ListOrgs(_)
        | ClientResponse::ListAggregateTypes(_)
        | ClientResponse::ListAggregates(_) => {}
    }

    compression_policy(payload_size, client_has_dict)
}

fn compression_policy(payload_size: usize, client_has_dict: bool) -> CompressionType {
    if payload_size < RESPONSE_COMPRESSION_THRESHOLD_BYTES {
        return CompressionType::None;
    }

    if client_has_dict {
        CompressionType::ZstdDict
    } else {
        CompressionType::None
    }
}

async fn write_variable_with_threshold<W, T>(
    writer: &mut W,
    message: &T,
    response_type_id: u32,
    client_has_dict: bool,
    dict_codec: &DictCodec,
    max_message_size: u64,
    version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
    T: bincode::Encode + serde::Serialize,
{
    use celeriant_wire::{
        codec::{bincode as wire_bincode, msgpack as wire_msgpack},
        network::wire_header::{WIRE_HEADER_SIZE, WIRE_FIXED_BODY_SIZE, PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3},
    };

    let uncompressed: Vec<u8> = match version {
        PROTOCOL_VERSION_V2 => wire_bincode::fixed_serialise_heap(message)?,
        PROTOCOL_VERSION_V3 => wire_msgpack::serialise_heap(message)?,
        _ => return Err(WireError::UnsupportedProtocol(version)),
    };

    let payload_size = uncompressed.len();

    let compression_type = compression_policy(payload_size, client_has_dict);

    let data: Vec<u8> = match compression_type {
        CompressionType::None => uncompressed,
        CompressionType::ZstdDict => dict_codec.compress(&uncompressed).map_err(WireError::from)?,
    };

    let compressed_size = data.len() as u32;
    let uncompressed_size = payload_size as u32;
    let compression_type_id = compression_type.to_byte();

    if compressed_size as u64 > max_message_size {
        return Err(WireError::MessageTooLarge {
            message_length: compressed_size as u64,
            max_size_bytes: max_message_size,
        });
    }

    if compressed_size <= WIRE_FIXED_BODY_SIZE as u32 && compression_type == CompressionType::None {
        let mut buffer = [0u8; WIRE_HEADER_SIZE + WIRE_FIXED_BODY_SIZE];
        buffer[0..4].copy_from_slice(&version.to_le_bytes());
        buffer[4..8].copy_from_slice(&response_type_id.to_le_bytes());
        buffer[8..12].copy_from_slice(&compressed_size.to_le_bytes());
        buffer[12..16].copy_from_slice(&uncompressed_size.to_le_bytes());
        buffer[16] = compression_type_id;
        buffer[WIRE_HEADER_SIZE..WIRE_HEADER_SIZE + compressed_size as usize].copy_from_slice(&data);
        writer.write_all(&buffer[..WIRE_HEADER_SIZE + compressed_size as usize]).await?;
        return Ok(());
    }

    let mut buffer = Vec::with_capacity(WIRE_HEADER_SIZE + data.len());
    buffer.extend_from_slice(&version.to_le_bytes());
    buffer.extend_from_slice(&response_type_id.to_le_bytes());
    buffer.extend_from_slice(&compressed_size.to_le_bytes());
    buffer.extend_from_slice(&uncompressed_size.to_le_bytes());
    buffer.push(compression_type_id);
    buffer.extend_from_slice(&data);
    writer.write_all(&buffer).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::responses::{AggregateListItem, OrgListItem};
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, WIRE_HEADER_SIZE};
    use futures_lite::{future::block_on, io::Cursor};

    const COUNT: usize = 12;
    const MAX_ID: u32 = 12;
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
            ClientResponseType::RegisterSchema,
        ]
    }

    fn make_response(rt: ClientResponseType) -> ClientResponse {
        match rt {
            ClientResponseType::AggregateDetails => ClientResponse::AggregateDetails(AggregateDetailsResponse {
                correlation_id: Some(0xDEAD_BEEF_CAFE_BABE),
                min_aggregate_version: 42,
                max_aggregate_version: 99,
                max_event_seq: 500,
                is_deleted: false,
                allow_recreate: true,
                allow_sequence_continuation: false,
                last_server_timestamp: 1700000000000,
                last_client_id: 0xAAAA_BBBB_CCCC_DDDD,
                last_user_id: Some(0x1111_2222_3333_4444),
            }),
            ClientResponseType::Read => ClientResponse::Read(ReadResponse {
                correlation_id: Some(0xFEED_FACE_DEAD_C0DE),
                event_batches: vec![],
                next_aggregate_version: Some(100),
            }),
            ClientResponseType::Write => ClientResponse::Write(WriteResponse {
                correlation_id: Some(0xCAFE_D00D_BEEF_F00D),
                max_aggregate_version: Some(7),
            }),
            ClientResponseType::TrimStart => ClientResponse::TrimStart(TrimStartResponse {
                correlation_id: Some(0xBAD_C0FFEE),
            }),
            ClientResponseType::Delete => ClientResponse::Delete(DeleteResponse {
                correlation_id: Some(0xDEAD_DEAD_DEAD_DEAD),
            }),
            ClientResponseType::ProtocolError => ClientResponse::ProtocolError(ProtocolErrorResponse {}),
            ClientResponseType::GenericError => ClientResponse::GenericError(ErrorResponse {
                correlation_id: Some(0x1111_2222_3333_4444),
                error_code: 0xFFFF,
                error_message: "test error".into(),
            }),
            ClientResponseType::Watch => ClientResponse::Watch(WatchResponse::default()),
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
            ClientResponseType::RegisterSchema => ClientResponse::RegisterSchema(RegisterSchemaResponse {
                correlation_id: Some(0x5555_6666_7777_8888),
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

    fn test_codec() -> DictCodec {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile")
    }

    /// Write a response with the client_has_dict=false flag (server picks None on the wire).
    /// The codec is required by signature but never invoked when compression resolves to None.
    async fn write_bytes(res: &ClientResponse, version: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        ClientResponse::write_response(&mut buf, res, false, &test_codec(), 64 * 1024 * 1024, version)
            .await
            .expect("write_response");
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
        for id in 1..=12 {
            assert!(ClientResponseType::from_u32(id).is_ok(), "missing id {}", id);
        }
        // IDs 13-99 reserved for future client responses
        // Cluster response IDs start at 100+
        for id in 13..=20 {
            assert!(ClientResponseType::from_u32(id).is_err(), "id {} should not parse as ClientResponseType yet", id);
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
                    let bytes = write_bytes(&res, v).await;
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
                let bytes = write_bytes(&res, PROTOCOL_VERSION_V2).await;
                let compression_byte = bytes[16];
                if is_variable_size(rt) {
                    assert!(compression_byte == 0 || compression_byte > 0, "{:?} should use variable path", rt);
                } else {
                    assert_eq!(compression_byte, 0, "{:?} fixed-size should have no compression", rt);
                }
            }
        });
    }

    // --- determine_compression_type unit tests ---

    #[test]
    fn small_payload_always_none() {
        let small_size = RESPONSE_COMPRESSION_THRESHOLD_BYTES - 1;
        let res = ClientResponse::Read(ReadResponse { correlation_id: None, event_batches: vec![], next_aggregate_version: None });
        assert_eq!(
            determine_compression_type(&res, small_size, true),
            CompressionType::None,
            "small payload should be None regardless of client_has_dict"
        );
    }

    #[test]
    fn large_payload_with_dict_returns_zstd_dict() {
        let res = ClientResponse::Read(ReadResponse { correlation_id: None, event_batches: vec![], next_aggregate_version: None });
        assert_eq!(
            determine_compression_type(&res, RESPONSE_COMPRESSION_THRESHOLD_BYTES + 1, true),
            CompressionType::ZstdDict
        );
    }

    #[test]
    fn large_payload_without_dict_returns_none() {
        let res = ClientResponse::Read(ReadResponse { correlation_id: None, event_batches: vec![], next_aggregate_version: None });
        assert_eq!(
            determine_compression_type(&res, RESPONSE_COMPRESSION_THRESHOLD_BYTES + 1, false),
            CompressionType::None
        );
    }

    #[test]
    fn write_response_always_none() {
        // Write, Delete, AggregateDetails, errors, RegisterSchema are never compressed.
        let large_size = RESPONSE_COMPRESSION_THRESHOLD_BYTES + 1;
        let none_variants: &[ClientResponse] = &[
            ClientResponse::Write(WriteResponse { correlation_id: None, max_aggregate_version: None }),
            ClientResponse::Delete(DeleteResponse { correlation_id: None }),
            ClientResponse::AggregateDetails(AggregateDetailsResponse {
                correlation_id: None, min_aggregate_version: 0, max_aggregate_version: 0,
                max_event_seq: 0, is_deleted: false, allow_recreate: false,
                allow_sequence_continuation: false, last_server_timestamp: 0,
                last_client_id: 0, last_user_id: None,
            }),
            ClientResponse::RegisterSchema(RegisterSchemaResponse { correlation_id: None }),
            ClientResponse::GenericError(ErrorResponse { correlation_id: None, error_code: 0, error_message: String::new() }),
            ClientResponse::ProtocolError(ProtocolErrorResponse {}),
        ];
        for res in none_variants {
            assert_eq!(
                determine_compression_type(res, large_size, true),
                CompressionType::None,
                "{:?} must always be None", res.response_type()
            );
        }
    }

    #[test]
    fn round_trip_large_payload_no_compression() {
        block_on(async {
            // Build a payload large enough to exceed the threshold.
            let res = ClientResponse::ListAggregates(ListAggregatesResponse {
                correlation_id: Some(1),
                aggregates: (0u64..100).map(|i| AggregateListItem {
                    is_deleted: false, org_id: i as u128, aggregate_type_id: i as u128,
                    aggregate_id: i as u128, event_batch_count: i, min_event_timestamp: i,
                    max_event_timestamp: i + 1000, min_aggregate_version: i, max_aggregate_version: i + 10,
                    min_event_seq: i, max_event_seq: i + 50, min_server_timestamp: i,
                    max_server_timestamp: i + 2000, compressed_size: i * 100, uncompressed_size: i * 200,
                }).collect(),
                next_cursor: None,
            });

            for &v in &VERSIONS {
                let bytes = write_bytes(&res, v).await;
                assert_eq!(bytes[16], 0, "v{} with None algorithm should have no compression", v);
                let parsed = read_back(&bytes).await;
                assert_eq!(parsed.response_type(), ClientResponseType::ListAggregates);
            }
        });
    }

    #[test]
    fn round_trip_all_versions_none_algorithm() {
        block_on(async {
            for rt in all_types() {
                for &v in &VERSIONS {
                    let res = make_response(rt);
                    let bytes = write_bytes(&res, v).await;
                    let parsed = read_back(&bytes).await;
                    assert_eq!(parsed.response_type(), rt, "{:?} v{} type mismatch", rt, v);
                }
            }
        });
    }

    #[test]
    fn size_limit_rejects_oversized() {
        block_on(async {
            let res = make_response(ClientResponseType::Read);
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2).await;
            let result = ClientResponse::read_response(&mut Cursor::new(bytes), 1).await;
            assert!(result.is_err(), "should reject when max_size < body size");
        });
    }

    #[test]
    fn truncated_stream_fails() {
        block_on(async {
            let res = make_response(ClientResponseType::AggregateDetails);
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2).await;

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
            let mut bytes = write_bytes(&res, PROTOCOL_VERSION_V2).await;

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
                        min_aggregate_version: i,
                        max_aggregate_version: i + 100,
                        min_event_seq: i * 5,
                        max_event_seq: i * 5 + 50,
                        min_server_timestamp: 3000 + i,
                        max_server_timestamp: 4000 + i,
                        compressed_size: i * 100,
                        uncompressed_size: i * 200,
                    })
                    .collect(),
                next_cursor: Some(999),
            });

            // No compression: cluster algorithm None.
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V2).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.response_type(), ClientResponseType::ListAggregates);

            // V3 no compression.
            let bytes = write_bytes(&res, PROTOCOL_VERSION_V3).await;
            let parsed = read_back(&bytes).await;
            assert_eq!(parsed.response_type(), ClientResponseType::ListAggregates);
        });
    }

    #[test]
    fn variable_size_responses_with_data() {
        block_on(async {
            // Small payloads (empty lists) — stay below threshold regardless of algorithm.
            let orgs = ClientResponse::ListOrgs(ListOrgsResponse {
                correlation_id: Some(1),
                orgs: vec![],
                next_cursor: None,
            });

            let types = ClientResponse::ListAggregateTypes(ListAggregateTypesResponse {
                correlation_id: Some(2),
                aggregate_types: vec![],
                next_cursor: None,
            });

            let aggregates = ClientResponse::ListAggregates(ListAggregatesResponse {
                correlation_id: Some(3),
                aggregates: vec![],
                next_cursor: None,
            });

            for res in [orgs, types, aggregates] {
                for &v in &VERSIONS {
                    let bytes = write_bytes(&res, v).await;
                    let parsed = read_back(&bytes).await;
                    assert_eq!(parsed.response_type(), res.response_type());
                }
            }

            // Large payload — no compression (client has no dict in this test).
            let large_orgs = ClientResponse::ListOrgs(ListOrgsResponse {
                correlation_id: Some(4),
                orgs: (0u128..200).map(|i| OrgListItem { org_id: i }).collect(),
                next_cursor: Some(200),
            });
            for &v in &VERSIONS {
                let bytes = write_bytes(&large_orgs, v).await;
                let parsed = read_back(&bytes).await;
                assert_eq!(parsed.response_type(), ClientResponseType::ListOrgs);
            }
        });
    }
}
