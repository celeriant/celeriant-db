use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::{constants::WIRE_FIXED_BODY_SIZE, wire_error::WireError, wire_header::WireHeader};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::request::requests::{DeleteRequest, ExistsRequest, ReadRequest, TrimStartRequest, WatchRequest, WriteRequest};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    Exists = 1,
    Read = 2,
    Write = 3,
    TrimStart = 4,
    Delete = 5,
    Watch = 6,
}

impl RequestType {
    pub fn from_u32(value: u32) -> Result<Self, WireError> {
        match value {
            1 => Ok(RequestType::Exists),
            2 => Ok(RequestType::Read),
            3 => Ok(RequestType::Write),
            4 => Ok(RequestType::TrimStart),
            5 => Ok(RequestType::Delete),
            6 => Ok(RequestType::Watch),
            _ => Err(WireError::UnknownRequestType(value)),
        }
    }

    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
                RequestType::Exists
                | RequestType::Read
                | RequestType::TrimStart
                | RequestType::Delete
                | RequestType::Watch
        )
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
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate
    pub fn aggregate_id(&self) -> u128 {
        match self {
            Request::Exists(req) => req.aggregate_key.aggregate_id,
            Request::Read(req) => req.aggregate_key.aggregate_id,
            Request::Write(_req) => 0,
            Request::TrimStart(req) => req.aggregate_key.aggregate_id,
            Request::Delete(_req) => 0,
            Request::Watch(_req) => 0,
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate
    pub fn org_id(&self) -> u128 {
        match self {
            Request::Exists(req) => req.aggregate_key.org_id,
            Request::Read(req) => req.aggregate_key.org_id,
            Request::Write(_req) => 0,
            Request::TrimStart(req) => req.aggregate_key.org_id,
            Request::Delete(_req) => 0,
            Request::Watch(_req) => 0,
        }
    }

    /// Returns the aggregate_id for routing purposes.
    /// Returns 0 for requests without a specific aggregate
    pub fn aggregate_type_id(&self) -> u128 {
        match self {
            Request::Exists(req) => req.aggregate_key.aggregate_type_id,
            Request::Read(req) => req.aggregate_key.aggregate_type_id,
            Request::Write(_req) => 0,
            Request::TrimStart(req) => req.aggregate_key.aggregate_type_id,
            Request::Delete(_req) => 0,
            Request::Watch(_req) => 0,
        }
    }

    /// Read a request from the wire protocol
    pub async fn read_request<R>(
        reader: &mut R,
        max_request_size: u64,
    ) -> Result<(Request, u32), WireError>
    where
        R: AsyncReadExt + Unpin,
    {
        let wire_header = WireHeader::from_reader(reader).await?;

        let request_type = RequestType::from_u32(wire_header.message_type)?;

        let request = if request_type.is_fixed_size() {
            let mut buffer = [0u8; WIRE_FIXED_BODY_SIZE];

            match request_type {
                RequestType::Exists => {
                    Request::Exists(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                RequestType::Read => {
                    Request::Read(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                RequestType::TrimStart => {
                    Request::TrimStart(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                RequestType::Delete => {
                    Request::Delete(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                RequestType::Watch => {
                    Request::Watch(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                _ => unreachable!(),
            }
        } else {
            match request_type {
                RequestType::Write => Request::Write(
                    wire_header
                        .read_variable_size(reader, max_request_size)
                        .await?,
                ),
                _ => unreachable!(),
            }
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
        let request_type = request.request_type();
        let request_type_id = request_type as u32;

        if request_type.is_fixed_size() {
            // Fixed-size requests - no compression needed
            match request {
                Request::Exists(req) => WireHeader::write_fixed_size(writer, req, request_type_id, version).await,
                Request::Read(req) => WireHeader::write_fixed_size(writer, req, request_type_id, version).await,
                Request::TrimStart(req) => WireHeader::write_fixed_size(writer, req, request_type_id, version).await,
                Request::Delete(req) => WireHeader::write_fixed_size(writer, req, request_type_id, version).await,
                Request::Watch(req) => WireHeader::write_fixed_size(writer, req, request_type_id, version).await,
                _ => unreachable!(),
            }
        } else {
            // Variable-size requests - with compression
            match request {
                Request::Write(req) => {
                    WireHeader::write_variable_size(
                        writer,
                        req,
                        request_type_id,
                        compression_type,
                        max_message_size,
                        version,
                    )
                    .await
                }
                _ => unreachable!(),
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wire::constants::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
    use futures_lite::{future::block_on, io::Cursor};
    use crate::request::{read_filters::ReadFilters, requests::{SingleAggregateDelete, SingleAggregateWrite}};

    // UPDATE THIS when adding new RequestTypes - tests will fail if mismatched
    const REQUEST_TYPE_COUNT: usize = 6;
    const REQUEST_TYPE_MAX_ID: u32 = 6;

    impl RequestType {
        /// Returns all RequestType variants. Adding a new variant without updating
        /// this function will cause a compile error due to non-exhaustive match.
        fn all() -> [RequestType; REQUEST_TYPE_COUNT] {
            [
                RequestType::Exists,
                RequestType::Read,
                RequestType::Write,
                RequestType::TrimStart,
                RequestType::Delete,
                RequestType::Watch,
            ]
        }
    }

    fn make_test_aggregate_key() -> AggregateKey {
        AggregateKey::new(1, 2, 3)
    }

    /// Creates a test Request for each RequestType. Non-exhaustive match will
    /// cause compile error if new variants are added without updating this.
    fn make_test_request(request_type: RequestType) -> Request {
        let key = make_test_aggregate_key();
        match request_type {
            RequestType::Exists => Request::Exists(ExistsRequest {
                correlation_id: Some(102),
                aggregate_key: key,
            }),
            RequestType::Read => Request::Read(ReadRequest {
                correlation_id: Some(103),
                aggregate_key: key,
                filters: ReadFilters::new(1),
            }),
            RequestType::Write => {
                let writes = std::collections::HashMap::from([(
                    key.clone(),
                    SingleAggregateWrite {
                        events: vec![],
                        allow_create: true,
                        expected_event_batch_index: None,
                        enforce_client_idempotency: false,
                        compression_type: CompressionType::None,
                    },
                )]);

                Request::Write(WriteRequest {
                    correlation_id: Some(104),
                    client_id: 999,
                    user_id: Some(888),
                    writes,
                })
            },
            RequestType::TrimStart => Request::TrimStart(TrimStartRequest {
                correlation_id: Some(106),
                aggregate_key: key,
                keep_from_event_batch_index: 10,
            }),
            RequestType::Delete => {
                let deletes = std::collections::HashMap::from([(
                    key.clone(),
                    SingleAggregateDelete {
                        allow_recreate: true,
                        allow_index_continuation: true,
                        expected_event_batch_index: None,
                    },
                )]);

                Request::Delete(DeleteRequest {
                    correlation_id: Some(104),
                    client_id: 999,
                    user_id: Some(888),
                    deletes,
                })
            },
            RequestType::Watch => Request::Watch(WatchRequest { 
                correlation_id: Some(109), 
                requested_latency_ms: Some(10),
                orgs: None,
                aggregate_types: None,
                aggregates: None,
                operation_types: None, 
            }),
        }
    }

    #[test]
    fn all_request_types_covered_in_from_u32() {
        for request_type in RequestType::all() {
            let id = request_type as u32;
            let parsed = RequestType::from_u32(id).expect("all variants should parse");
            assert_eq!(parsed, request_type);
        }
    }

    #[test]
    fn request_type_ids_are_contiguous_from_1() {
        // Ensures no gaps - if someone adds id=11 but skips 10, this catches it
        for id in 1..=REQUEST_TYPE_MAX_ID {
            RequestType::from_u32(id)
                .unwrap_or_else(|_| panic!("id {} should be a valid RequestType", id));
        }
    }

    #[test]
    fn invalid_request_type_id_zero_fails() {
        assert!(RequestType::from_u32(0).is_err());
    }

    #[test]
    fn invalid_request_type_id_above_max_fails() {
        // This test fails if someone adds a new variant but doesn't update REQUEST_TYPE_MAX_ID
        assert!(
            RequestType::from_u32(REQUEST_TYPE_MAX_ID + 1).is_err(),
            "If this fails, update REQUEST_TYPE_MAX_ID to {}", REQUEST_TYPE_MAX_ID + 1
        );
    }

    #[test]
    fn request_type_round_trip_matches() {
        for request_type in RequestType::all() {
            let request = make_test_request(request_type);
            assert_eq!(request.request_type(), request_type);
        }
    }

    #[test]
    fn is_fixed_size_consistent_with_variants() {
        // Verify every request type explicitly declares fixed/variable
        for request_type in RequestType::all() {
            let _ = request_type.is_fixed_size(); // Just ensure it doesn't panic
        }
    }

    #[test]
    fn request_write_read_round_trip_all_types_v2() {
        block_on(async {
            for request_type in RequestType::all() {
                let original = make_test_request(request_type);
                let version = PROTOCOL_VERSION_V2;

                // Write
                let mut buffer = Vec::new();
                Request::write_request(
                    &mut buffer,
                    &original,
                    CompressionType::None,
                    16 * 1024 * 1024,
                    version,
                )
                .await
                .unwrap_or_else(|e| panic!("write failed for {:?}: {:?}", request_type, e));

                // Read
                let mut cursor = Cursor::new(buffer);
                let (parsed, parsed_version) = Request::read_request(&mut cursor, u64::MAX)
                    .await
                    .unwrap_or_else(|e| panic!("read failed for {:?}: {:?}", request_type, e));

                assert_eq!(parsed_version, version);
                assert_eq!(parsed.request_type(), request_type);
            }
        });
    }

    #[test]
    fn request_write_read_round_trip_all_types_v3() {
        block_on(async {
            for request_type in RequestType::all() {
                let original = make_test_request(request_type);
                let version = PROTOCOL_VERSION_V3;

                // Write
                let mut buffer = Vec::new();
                Request::write_request(
                    &mut buffer,
                    &original,
                    CompressionType::None,
                    16 * 1024 * 1024,
                    version,
                )
                .await
                .unwrap_or_else(|e| panic!("write failed for {:?}: {:?}", request_type, e));

                // Read
                let mut cursor = Cursor::new(buffer);
                let (parsed, parsed_version) = Request::read_request(&mut cursor, u64::MAX)
                    .await
                    .unwrap_or_else(|e| panic!("read failed for {:?}: {:?}", request_type, e));

                assert_eq!(parsed_version, version);
                assert_eq!(parsed.request_type(), request_type);
            }
        });
    }
}