
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::read_wire_data_error::ReadWireDataError;
use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::decompress_with_dict;
use celeriant_wire::network::{wire_error::WireError, wire_header::WireHeader};
use futures_lite::AsyncReadExt;

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
    use celeriant_msg::response::responses::{AggregateDetailsResponse, WatchResponse};
    use celeriant_msg::response::watch_event::WatchResponseEvent;
    use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
    use celeriant_wire::codec::compression::DictCodec;
    use celeriant_wire::network::wire_header::PROTOCOL_VERSION_V2;
    use futures_lite::future::block_on;
    use futures_lite::io::Cursor;

    const MAX_SIZE: u64 = 64 * 1024 * 1024;

    fn test_codec() -> DictCodec {
        DictCodec::new(BUILTIN_DICT_BYTES, 3).expect("builtin dict must compile")
    }

    fn aggregate_details_response() -> ClientResponse {
        ClientResponse::AggregateDetails(AggregateDetailsResponse {
            correlation_id: Some(0xDEAD_BEEF),
            min_aggregate_version: 0,
            max_aggregate_version: 42,
            max_event_seq: 100,
            is_deleted: false,
            allow_recreate: false,
            allow_sequence_continuation: true,
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
                from_aggregate_version: Some(i as u64),
                to_aggregate_version: Some(i as u64 + 1),
                keep_from_aggregate_version: None,
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

}
