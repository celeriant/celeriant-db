use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{
        WireHeader, wire_header_write_fixed_size, wire_header_write_variable_size_uncompressed,
    },
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    read_wire_data_error::ReadWireDataError,
    request::requests::IdentifyRequest,
    response::responses::IdentifyResponse,
};

pub const IDENTIFY_REQUEST_TYPE_ID: u32 = 14;
pub const IDENTIFY_RESPONSE_TYPE_ID: u32 = 16;

pub async fn read_identify_request<R>(header: WireHeader, reader: &mut R) -> Result<IdentifyRequest, ReadWireDataError>
where
    R: AsyncReadExt + Unpin,
{
    header.read_fixed_size(reader).await.map_err(ReadWireDataError::ReadBodyFailure)
}

pub async fn write_identify_request<W>(writer: &mut W, req: &IdentifyRequest, version: u32) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    wire_header_write_fixed_size(writer, req, IDENTIFY_REQUEST_TYPE_ID, version).await
}

pub async fn read_identify_response<R>(header: WireHeader, reader: &mut R) -> Result<IdentifyResponse, ReadWireDataError>
where
    R: AsyncReadExt + Unpin,
{
    header
        .read_variable_size_uncompressed(reader)
        .await
        .map_err(ReadWireDataError::ReadBodyFailure)
}

pub async fn write_identify_response<W>(writer: &mut W, res: &IdentifyResponse, version: u32) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    wire_header_write_variable_size_uncompressed(
        writer,
        res,
        IDENTIFY_RESPONSE_TYPE_ID,
        64 * 1024 * 1024,
        version,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response::responses::AccessLevel;
    use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, WIRE_HEADER_SIZE};
    use futures_lite::{future::block_on, io::Cursor};

    fn sample_request() -> IdentifyRequest {
        IdentifyRequest {
            correlation_id: Some(0x8888_9999_AAAA_BBBB),
            public_key: Some("MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA".to_string()),
            nonce: Some("1234567890000".to_string()),
            signature: Some("dGVzdHNpZ25hdHVyZQ==".to_string()),
            api_key: Some("SGVsbG9Xb3JsZEhlbGxvV29ybGRIZWxsb1dvcmxkSGVsbG9Xb3JsZA==".to_string()),
            known_dict_sha256: None,
        }
    }

    fn sample_response() -> IdentifyResponse {
        IdentifyResponse {
            correlation_id: Some(0x9999_AAAA_BBBB_CCCC),
            client_id: Some(0xCCCC_DDDD_EEEE_FFFF),
            access_level: Some(AccessLevel::ReadWrite),
            compression_dict_sha256: None,
            compression_dict_bytes: None,
        }
    }

    async fn roundtrip_response(res: &IdentifyResponse, version: u32) -> IdentifyResponse {
        let mut buf = Vec::new();
        write_identify_response(&mut buf, res, version).await.unwrap();
        let mut cursor = Cursor::new(buf.clone());
        let header = WireHeader::from_reader(&mut cursor, u64::MAX).await.unwrap();
        let mut body_cursor = Cursor::new(buf[WIRE_HEADER_SIZE..].to_vec());
        read_identify_response(header, &mut body_cursor).await.unwrap()
    }

    #[test]
    fn request_round_trip() {
        block_on(async {
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let req = sample_request();
                let mut buf = Vec::new();
                write_identify_request(&mut buf, &req, version).await.unwrap();

                let mut cursor = Cursor::new(buf.clone());
                let header = WireHeader::from_reader(&mut cursor, u64::MAX).await.unwrap();
                assert_eq!(header.message_type, IDENTIFY_REQUEST_TYPE_ID);
                assert_eq!(header.version, version);

                let mut body_cursor = Cursor::new(buf[WIRE_HEADER_SIZE..].to_vec());
                let parsed = read_identify_request(header, &mut body_cursor).await.unwrap();
                assert_eq!(parsed.correlation_id, req.correlation_id);
                assert_eq!(parsed.public_key, req.public_key);
                assert_eq!(parsed.nonce, req.nonce);
                assert_eq!(parsed.signature, req.signature);
                assert_eq!(parsed.api_key, req.api_key);
                assert_eq!(parsed.known_dict_sha256, req.known_dict_sha256);
            }
        });
    }

    #[test]
    fn request_round_trip_with_known_dict_sha() {
        block_on(async {
            let req = IdentifyRequest {
                correlation_id: None,
                public_key: None,
                nonce: None,
                signature: None,
                api_key: None,
                known_dict_sha256: Some("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890".to_string()),
            };
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let mut buf = Vec::new();
                write_identify_request(&mut buf, &req, version).await.unwrap();
                let mut cursor = Cursor::new(buf.clone());
                let header = WireHeader::from_reader(&mut cursor, u64::MAX).await.unwrap();
                let mut body_cursor = Cursor::new(buf[WIRE_HEADER_SIZE..].to_vec());
                let parsed = read_identify_request(header, &mut body_cursor).await.unwrap();
                assert_eq!(parsed.known_dict_sha256, req.known_dict_sha256);
            }
        });
    }

    #[test]
    fn response_round_trip() {
        block_on(async {
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let res = sample_response();
                let parsed = roundtrip_response(&res, version).await;
                assert_eq!(parsed.correlation_id, res.correlation_id);
                assert_eq!(parsed.client_id, res.client_id);
                assert_eq!(parsed.access_level, res.access_level);
                assert_eq!(parsed.compression_dict_sha256, None);
                assert_eq!(parsed.compression_dict_bytes, None);
            }
        });
    }

    #[test]
    fn response_always_uses_none_compression() {
        // Inv 6: IdentifyResponse must never be compressed with ZstdDict.
        block_on(async {
            let res = IdentifyResponse {
                correlation_id: None,
                client_id: None,
                access_level: None,
                compression_dict_sha256: Some("abc123".to_string()),
                compression_dict_bytes: Some(vec![1, 2, 3, 4]),
            };
            let mut buf = Vec::new();
            write_identify_response(&mut buf, &res, PROTOCOL_VERSION_V2).await.unwrap();
            let mut cursor = Cursor::new(buf);
            let header = WireHeader::from_reader(&mut cursor, u64::MAX).await.unwrap();
            assert_eq!(header.compression_type, celeriant_wal::compression_type::CompressionType::None);
        });
    }

    #[test]
    fn response_with_dict_bytes_round_trips() {
        // Simulates shipping ~14 KiB dict bytes on Identify when client has no dict.
        block_on(async {
            let dict_bytes = vec![0xABu8; 14_000];
            let sha = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string();
            let res = IdentifyResponse {
                correlation_id: Some(1),
                client_id: None,
                access_level: None,
                compression_dict_sha256: Some(sha.clone()),
                compression_dict_bytes: Some(dict_bytes.clone()),
            };
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let parsed = roundtrip_response(&res, version).await;
                assert_eq!(parsed.compression_dict_sha256.as_deref(), Some(sha.as_str()));
                assert_eq!(parsed.compression_dict_bytes.as_deref(), Some(dict_bytes.as_slice()));
            }
        });
    }

    #[test]
    fn response_omits_bytes_when_sha_only() {
        // Simulates client sha matched: only sha is sent, bytes are None.
        block_on(async {
            let sha = "cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe".to_string();
            let res = IdentifyResponse {
                correlation_id: None,
                client_id: None,
                access_level: None,
                compression_dict_sha256: Some(sha.clone()),
                compression_dict_bytes: None,
            };
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let parsed = roundtrip_response(&res, version).await;
                assert_eq!(parsed.compression_dict_sha256.as_deref(), Some(sha.as_str()));
                assert_eq!(parsed.compression_dict_bytes, None);
            }
        });
    }

    #[test]
    fn response_no_dict_when_algorithm_is_none() {
        // When cluster algorithm is not ZstdDict, both fields are None.
        block_on(async {
            let res = IdentifyResponse {
                correlation_id: None,
                client_id: None,
                access_level: None,
                compression_dict_sha256: None,
                compression_dict_bytes: None,
            };
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let parsed = roundtrip_response(&res, version).await;
                assert_eq!(parsed.compression_dict_sha256, None);
                assert_eq!(parsed.compression_dict_bytes, None);
            }
        });
    }
}
