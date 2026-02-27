use celeriant_wire::network::{
    wire_error::WireError,
    wire_header::{WireHeader, wire_header_write_fixed_size},
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
    header.read_fixed_size(reader).await.map_err(ReadWireDataError::ReadBodyFailure)
}

pub async fn write_identify_response<W>(writer: &mut W, res: &IdentifyResponse, version: u32) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    wire_header_write_fixed_size(writer, res, IDENTIFY_RESPONSE_TYPE_ID, version).await
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
        }
    }

    fn sample_response() -> IdentifyResponse {
        IdentifyResponse {
            correlation_id: Some(0x9999_AAAA_BBBB_CCCC),
            client_id: Some(0xCCCC_DDDD_EEEE_FFFF),
            access_level: Some(AccessLevel::ReadWrite),
        }
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
            }
        });
    }

    #[test]
    fn response_round_trip() {
        block_on(async {
            for &version in &[PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3] {
                let res = sample_response();
                let mut buf = Vec::new();
                write_identify_response(&mut buf, &res, version).await.unwrap();

                let mut cursor = Cursor::new(buf.clone());
                let header = WireHeader::from_reader(&mut cursor, u64::MAX).await.unwrap();
                assert_eq!(header.message_type, IDENTIFY_RESPONSE_TYPE_ID);
                assert_eq!(header.version, version);

                let mut body_cursor = Cursor::new(buf[WIRE_HEADER_SIZE..].to_vec());
                let parsed = read_identify_response(header, &mut body_cursor).await.unwrap();
                assert_eq!(parsed.correlation_id, res.correlation_id);
                assert_eq!(parsed.client_id, res.client_id);
                assert_eq!(parsed.access_level, res.access_level);
            }
        });
    }
}
