//! Tokio-side wire helpers.
//!
//! The wire layer (celeriant_wire) is thread-per-core / glommio-flavoured: its
//! compression-aware helpers take `&DictCodec`, which is `!Send` and so can't sit on a
//! tokio task that may move between worker threads at any await point. Tokio composes
//! its own send/receive path here using the wire layer's raw escape hatches plus the
//! stateless `compress_with_dict` / `decompress_with_dict` utilities.
//!
//! "Stateless" means the zstd dict is digested on every call — slower than the per-shard
//! compiled `DictCodec` the server uses, but tokio's client path is not the server hot
//! path. The simplicity is worth the throughput.
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::{compress_with_dict, decompress_with_dict};
use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{WireHeader, wire_header_write_variable_size_raw},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

/// Compression level used for client-side outbound writes. Mirrors the level the server
/// defaults to; the level affects compressed size only, not decompressibility.
const COMPRESSION_LEVEL: i32 = 3;

/// Sends a request. Fixed-size variants are always uncompressed (no dict path). Variable-
/// size variants may compress when `dict_bytes` is present and the caller asks for it.
///
/// Pre-Identify (no `dict_bytes`), variable-size variants are sent uncompressed.
pub(crate) async fn send_request<W>(
    writer: &mut W,
    request: &ClientRequest,
    compression: CompressionType,
    max_message_size: u64,
    version: u32,
    dict_bytes: Option<&[u8]>,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    if !request.is_variable_size() {
        return ClientRequest::write_request(writer, request, max_message_size, version).await;
    }

    let body = request.serialize_body(version)?;
    let uncompressed_size = body.len() as u32;
    let request_type_id = request.request_type() as u32;

    let (effective_compression, frame_body) = match (compression, dict_bytes) {
        (CompressionType::ZstdDict, Some(dict)) => (
            CompressionType::ZstdDict,
            compress_with_dict(&body, COMPRESSION_LEVEL, dict)?,
        ),
        _ => (CompressionType::None, body),
    };

    wire_header_write_variable_size_raw(
        writer,
        &frame_body,
        request_type_id,
        effective_compression,
        uncompressed_size,
        max_message_size,
        version,
    )
    .await
}

/// Reads a response. Variable-size frames may arrive `ZstdDict`-compressed when
/// `dict_bytes` is present; the wire layer hands back raw bytes and we decompress
/// + dispatch here.
pub(crate) async fn read_response<R>(
    reader: &mut R,
    max_response_size: u64,
    dict_bytes: Option<&[u8]>,
) -> Result<ClientResponse, ReadWireDataError>
where
    R: AsyncReadExt + Unpin,
{
    let header = WireHeader::from_reader(reader, max_response_size)
        .await
        .map_err(ReadWireDataError::ReadHeaderFailure)?;
    read_from_header(header, reader, dict_bytes).await
}

pub(crate) async fn read_from_header<R>(
    header: WireHeader,
    reader: &mut R,
    dict_bytes: Option<&[u8]>,
) -> Result<ClientResponse, ReadWireDataError>
where
    R: AsyncReadExt + Unpin,
{
    if ClientResponse::is_fixed_size_variant(header.message_type) {
        // Fixed-size frames are never compressed regardless of `dict_bytes`.
        return ClientResponse::read_from_header(header, reader).await;
    }

    let raw = header
        .read_variable_body_raw(reader)
        .await
        .map_err(ReadWireDataError::ReadBodyFailure)?;

    let body = match header.compression_type {
        CompressionType::None => raw,
        CompressionType::ZstdDict => {
            let dict = dict_bytes.ok_or_else(|| {
                ReadWireDataError::ReadBodyFailure(WireError::MalformedFrame(
                    "ZstdDict frame but client has no cached dict".into(),
                ))
            })?;
            decompress_with_dict(&raw, header.uncompressed_length as usize, dict)
                .map_err(|e| ReadWireDataError::ReadBodyFailure(WireError::from(e)))?
        }
    };

    ClientResponse::deserialize_body(header.message_type, &body, header.version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_msg::process_client_responses::ClientResponse;
    use celeriant_msg::request::requests::{WatchRequest, WriteRequest, SingleAggregateWrite};
    use celeriant_msg::response::responses::{AggregateDetailsResponse, WatchResponse};
    use celeriant_msg::response::watch_event::WatchResponseEvent;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
    use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
    use celeriant_wire::codec::compression::DictCodec;
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WIRE_HEADER_SIZE};
    use futures_lite::future::block_on;
    use futures_lite::io::Cursor;
    use std::collections::HashMap;

    const MAX_SIZE: u64 = 64 * 1024 * 1024;

    fn test_codec() -> DictCodec {
        DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile")
    }

    fn aggregate_details_response() -> ClientResponse {
        ClientResponse::AggregateDetails(AggregateDetailsResponse {
            correlation_id: Some(0xDEAD_BEEF),
            min_event_batch_index: 0,
            max_event_batch_index: 42,
            max_event_index: 100,
            is_deleted: false,
            allow_recreate: false,
            allow_index_continuation: true,
            last_server_timestamp: 1234,
            last_client_id: 5678,
            last_user_id: None,
        })
    }

    fn watch_response_with_events(n_events: usize) -> ClientResponse {
        let events = (0..n_events)
            .map(|i| WatchResponseEvent {
                org_id: 1,
                aggregate_type_id: 2,
                aggregate_id: (i as u128) + 3,
                operation: 1,
                from_event_batch_index: Some(i as u64),
                to_event_batch_index: Some(i as u64 + 1),
                keep_from_event_batch_index: None,
            })
            .collect();
        ClientResponse::Watch(WatchResponse { events })
    }

    /// Fixed-size responses are decoded the same regardless of `dict_bytes` — the codec
    /// branch never runs, so the dispatch must skip it.
    #[test]
    fn read_response_fixed_size_dispatches_to_uncompressed_path() {
        block_on(async {
            let response = aggregate_details_response();
            let codec = test_codec();
            let mut buf = Vec::new();
            ClientResponse::write_response(&mut buf, &response, true, &codec, MAX_SIZE, PROTOCOL_VERSION_V2)
                .await.expect("write_response");

            // Reading with dict_bytes=None must still succeed — fixed-size never compresses.
            let parsed = read_response(&mut Cursor::new(buf), MAX_SIZE, None).await
                .expect("read_response");
            assert!(matches!(parsed, ClientResponse::AggregateDetails(_)));
        });
    }

    /// Variable-size response compressed with the cluster dict roundtrips when the tokio
    /// reader is given the dict bytes.
    #[test]
    fn read_response_variable_size_zstd_dict_roundtrip() {
        block_on(async {
            // Use enough events to exceed the compression threshold.
            let response = watch_response_with_events(500);
            let codec = test_codec();
            let mut buf = Vec::new();
            ClientResponse::write_response(&mut buf, &response, true, &codec, MAX_SIZE, PROTOCOL_VERSION_V2)
                .await.expect("write_response");

            // Sanity: server picked ZstdDict for this payload.
            assert_eq!(buf[16], CompressionType::ZstdDict.to_byte());

            let parsed = read_response(&mut Cursor::new(buf), MAX_SIZE, Some(BUILTIN_DICT_BYTES)).await
                .expect("read_response");
            match parsed {
                ClientResponse::Watch(w) => assert_eq!(w.events.len(), 500),
                other => panic!("expected Watch, got {:?}", other.response_type()),
            }
        });
    }

    /// Variable-size response sent uncompressed (server picks `None` when the client
    /// hasn't Identified yet) decodes without a dict.
    #[test]
    fn read_response_variable_size_none_without_dict() {
        block_on(async {
            let response = watch_response_with_events(500);
            let codec = test_codec();
            let mut buf = Vec::new();
            ClientResponse::write_response(&mut buf, &response, false, &codec, MAX_SIZE, PROTOCOL_VERSION_V2)
                .await.expect("write_response");
            assert_eq!(buf[16], CompressionType::None.to_byte());

            let parsed = read_response(&mut Cursor::new(buf), MAX_SIZE, None).await
                .expect("read_response");
            match parsed {
                ClientResponse::Watch(w) => assert_eq!(w.events.len(), 500),
                other => panic!("expected Watch, got {:?}", other.response_type()),
            }
        });
    }

    /// A ZstdDict frame arriving when the client has no cached dict is rejected with
    /// `MalformedFrame`. Catches the cross-Identify race where the server emits a
    /// compressed frame before the client has stored the dict.
    #[test]
    fn read_response_zstd_dict_without_dict_errors() {
        block_on(async {
            let response = watch_response_with_events(500);
            let codec = test_codec();
            let mut buf = Vec::new();
            ClientResponse::write_response(&mut buf, &response, true, &codec, MAX_SIZE, PROTOCOL_VERSION_V2)
                .await.expect("write_response");
            assert_eq!(buf[16], CompressionType::ZstdDict.to_byte());

            let err = read_response(&mut Cursor::new(buf), MAX_SIZE, None).await
                .expect_err("must reject");
            match err {
                ReadWireDataError::ReadBodyFailure(WireError::MalformedFrame(_)) => {}
                other => panic!("expected MalformedFrame, got {:?}", other),
            }
        });
    }

    /// `send_request` on a fixed-size variant produces a frame readable by the server-
    /// side `ClientRequest::read_from_header` — no dict, no compression.
    #[test]
    fn send_request_fixed_size_writes_uncompressed_frame() {
        block_on(async {
            let request = ClientRequest::Watch(WatchRequest {
                correlation_id: Some(0x1234),
                requested_latency_ms: Some(100),
                shard_id: None,
                orgs: None,
                aggregate_types: None,
                aggregates: None,
                operation_types: None,
            });

            let mut buf = Vec::new();
            send_request(&mut buf, &request, CompressionType::ZstdDict, MAX_SIZE, PROTOCOL_VERSION_V2, Some(BUILTIN_DICT_BYTES))
                .await.expect("send_request");

            // Even with ZstdDict + dict requested, fixed-size must still be uncompressed.
            assert_eq!(buf[16], CompressionType::None.to_byte());

            let codec = test_codec();
            let header = WireHeader::from_reader(&mut Cursor::new(buf.clone()), MAX_SIZE)
                .await.expect("from_reader");
            let parsed = ClientRequest::read_from_header(
                header,
                &mut Cursor::new(buf[WIRE_HEADER_SIZE..].to_vec()),
                &codec,
            ).await.expect("read_from_header");
            assert!(matches!(parsed, ClientRequest::Watch(_)));
        });
    }

    /// `send_request` on a variable-size variant compresses with `ZstdDict` when the
    /// caller supplies dict bytes; the server-side codec decodes it.
    #[test]
    fn send_request_variable_size_zstd_dict_roundtrip() {
        block_on(async {
            // Build a Write with enough payload that the codec path is exercised.
            let key = AggregateKey::new(1, 2, 3);
            let event = DatablockAggregateEvent {
                client_event_index: 0,
                event_index: 0,
                event_id: Some(0xAA),
                event_timestamp: 100,
                event_type_major: 1,
                event_type_minor: 0,
                event_value: std::sync::Arc::new(vec![42u8; 4096]),
                iv: None,
            };
            let request = ClientRequest::Write(WriteRequest {
                correlation_id: Some(0xBEEF),
                client_id: 1,
                user_id: None,
                writes: HashMap::from([(key, SingleAggregateWrite {
                    events: vec![event],
                    allow_create: true,
                    expected_event_batch_index: None,
                    enforce_client_idempotency: false,
                })]),
            });

            let mut buf = Vec::new();
            send_request(&mut buf, &request, CompressionType::ZstdDict, MAX_SIZE, PROTOCOL_VERSION_V2, Some(BUILTIN_DICT_BYTES))
                .await.expect("send_request");
            assert_eq!(buf[16], CompressionType::ZstdDict.to_byte());

            let codec = test_codec();
            let header = WireHeader::from_reader(&mut Cursor::new(buf.clone()), MAX_SIZE)
                .await.expect("from_reader");
            let parsed = ClientRequest::read_from_header(
                header,
                &mut Cursor::new(buf[WIRE_HEADER_SIZE..].to_vec()),
                &codec,
            ).await.expect("read_from_header");
            assert!(matches!(parsed, ClientRequest::Write(_)));
        });
    }

    /// Pre-Identify: tokio has no cached dict, so variable-size variants must go out
    /// uncompressed even if `ZstdDict` is requested.
    #[test]
    fn send_request_variable_size_no_dict_falls_back_to_none() {
        block_on(async {
            let request = ClientRequest::Write(WriteRequest {
                correlation_id: Some(0x1),
                client_id: 1,
                user_id: None,
                writes: HashMap::new(),
            });

            let mut buf = Vec::new();
            send_request(&mut buf, &request, CompressionType::ZstdDict, MAX_SIZE, PROTOCOL_VERSION_V2, None)
                .await.expect("send_request");
            assert_eq!(buf[16], CompressionType::None.to_byte());
        });
    }

}
