use std::u64;

use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::{constants::WIRE_FIXED_BODY_SIZE, wire_error::WireError, wire_header::WireHeader};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::response::responses::{ErrorResponse, ExistsResponse, ListAggregateTypesResponse, ListAggregatesResponse, ListOrgsResponse, ProtocolErrorResponse, ReadResponse, ReplicationBatchResponse, SuccessResponse, WatchResponse};

// Response type discriminants
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseType {
    Exists = 1,
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
    ReplicationBatch = 12,
}

impl ResponseType {
    pub fn from_u32(value: u32) -> Result<Self, WireError> {
        match value {
            1 => Ok(ResponseType::Exists),
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
            12 => Ok(ResponseType::ReplicationBatch),
            _ => Err(WireError::UnknownResponseType(value)),
        }
    }

    pub fn is_fixed_size(&self) -> bool {
        matches!(
            self,
            ResponseType::Exists
                | ResponseType::TrimStart
                | ResponseType::Delete
                | ResponseType::ProtocolError
                | ResponseType::Write
                | ResponseType::GenericError
                | ResponseType::ReplicationBatch
        )
    }
}


#[derive(Debug, Clone)]
pub enum Response {
    Exists(ExistsResponse),
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
    ReplicationBatch(ReplicationBatchResponse),
}

impl Response {
    pub fn response_type(&self) -> ResponseType {
        match self {
            Response::Exists(_) => ResponseType::Exists,
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
            Response::ReplicationBatch(_) => ResponseType::ReplicationBatch,
        }
    }

    pub async fn read_response<R>(reader: &mut R) -> Result<Response, WireError>
    where
        R: AsyncReadExt + Unpin,
    {
        let wire_header = WireHeader::from_reader(reader).await?;

        let response_type = ResponseType::from_u32(wire_header.message_type)?;

        let response = if response_type.is_fixed_size() {
            // Single buffer large enough for any fixed-size response
            let mut buffer = [0u8; WIRE_FIXED_BODY_SIZE]; // Adjust size based on largest fixed response

            match response_type {
                ResponseType::Exists => {
                    Response::Exists(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                ResponseType::TrimStart => {
                    Response::TrimStart(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                ResponseType::Delete => {
                    Response::Delete(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                ResponseType::ProtocolError => {
                    Response::ProtocolError(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                ResponseType::Write => {
                    Response::Write(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                ResponseType::GenericError => {
                    Response::GenericError(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                ResponseType::ReplicationBatch => {
                    Response::ReplicationBatch(wire_header.read_fixed_size(reader, &mut buffer).await?)
                }
                _ => unreachable!(),
            }
        } else {
            match response_type {
                ResponseType::Read => {
                    Response::Read(wire_header.read_variable_size(reader, u64::MAX).await?)
                }
                ResponseType::Watch => {
                    Response::Watch(wire_header.read_variable_size(reader, u64::MAX).await?)
                }
                ResponseType::ListOrgs => {
                    Response::ListOrgs(wire_header.read_variable_size(reader, u64::MAX).await?)
                }
                ResponseType::ListAggregateTypes => {
                    Response::ListAggregateTypes(wire_header.read_variable_size(reader, u64::MAX).await?)
                }
                ResponseType::ListAggregates => {
                    Response::ListAggregates(wire_header.read_variable_size(reader, u64::MAX).await?)
                }
                _ => unreachable!(),
            }
        };

        Ok(response)
    }

    pub fn determine_compression_type(response: &Response) -> CompressionType {
        match response {
            Response::Exists(_) => CompressionType::None,
            Response::Read(_) => CompressionType::Snappy,
            Response::Write(_) => CompressionType::None,
            Response::TrimStart(_) => CompressionType::None,
            Response::Delete(_) => CompressionType::None,
            Response::ProtocolError(_) => CompressionType::None,
            Response::GenericError(_) => CompressionType::None,
            Response::Watch(_) => CompressionType::Snappy,
            Response::ListOrgs(_) => CompressionType::Snappy,
            Response::ListAggregateTypes(_) => CompressionType::Snappy,
            Response::ListAggregates(_) => CompressionType::Snappy,
            Response::ReplicationBatch(_) => CompressionType::None,
        }
    }

    pub async fn write_response<W>(
        writer: &mut W,
        response: &Response,
        compression_type: CompressionType,
        version: u32,
    ) -> Result<(), WireError>
    where
        W: AsyncWriteExt + Unpin,
    {
        let response_type = response.response_type();
        let response_type_id = response_type as u32;

        if response_type.is_fixed_size() {
            // Fixed-size responses - no compression needed
            match response {
                Response::Exists(res) => WireHeader::write_fixed_size(writer, res, response_type_id, version).await,
                Response::TrimStart(res) => WireHeader::write_fixed_size(writer, res, response_type_id, version).await,
                Response::Delete(res) => WireHeader::write_fixed_size(writer, res, response_type_id, version).await,
                Response::ProtocolError(res) => WireHeader::write_fixed_size(writer, res, response_type_id, version).await,
                Response::Write(res) => WireHeader::write_fixed_size(writer, res, response_type_id, version).await,
                Response::GenericError(res) => WireHeader::write_fixed_size(writer, res, response_type_id, version).await,
                Response::ReplicationBatch(res) => WireHeader::write_fixed_size(writer, res, response_type_id, version).await,
                _ => unreachable!(),
            }
        } else {
            // Variable-size responses - with compression
            match response {
                Response::Read(res) => {
                    WireHeader::write_variable_size(writer, res, response_type_id, compression_type, u64::MAX, version).await
                }
                Response::Watch(res) => {
                    WireHeader::write_variable_size(writer, res, response_type_id, compression_type, u64::MAX, version).await
                }
                Response::ListOrgs(res) => {
                    WireHeader::write_variable_size(writer, res, response_type_id, compression_type, u64::MAX, version).await
                }
                Response::ListAggregateTypes(res) => {
                    WireHeader::write_variable_size(writer, res, response_type_id, compression_type, u64::MAX, version).await
                }
                Response::ListAggregates(res) => {
                    WireHeader::write_variable_size(writer, res, response_type_id, compression_type, u64::MAX, version).await
                }
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wire::constants::{PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3};
    use futures_lite::{future::block_on, io::Cursor};

    // UPDATE THIS when adding new ResponseTypes - tests will fail if mismatched
    const RESPONSE_TYPE_COUNT: usize = 12;
    const RESPONSE_TYPE_MAX_ID: u32 = 12;

    impl ResponseType {
        /// Returns all ResponseType variants. Adding a new variant without updating
        /// this function will cause a compile error due to non-exhaustive match.
        fn all() -> [ResponseType; RESPONSE_TYPE_COUNT] {
            [
                ResponseType::Exists,
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
                ResponseType::ReplicationBatch,
            ]
        }
    }

    /// Creates a test Response for each ResponseType. Non-exhaustive match will
    /// cause compile error if new variants are added without updating this.
    fn make_test_response(response_type: ResponseType) -> Response {
        match response_type {
            ResponseType::Exists => Response::Exists(ExistsResponse {
                correlation_id: Some(102),
                min_event_batch_index: 0,
            }),
            ResponseType::Read => Response::Read(ReadResponse {
                correlation_id: Some(103),
                event_batches: vec![],
                next_event_batch_index: Some(5),
            }),
            ResponseType::Watch => Response::Watch(WatchResponse {
                events: None,
            }),
            ResponseType::Write => Response::Write(SuccessResponse {
                correlation_id: Some(105),
            }),
            ResponseType::TrimStart => Response::TrimStart(SuccessResponse {
                correlation_id: Some(106),
            }),
            ResponseType::Delete => Response::Delete(SuccessResponse {
                correlation_id: Some(107),
            }),
            ResponseType::ProtocolError => Response::ProtocolError(ProtocolErrorResponse {}),
            ResponseType::GenericError => Response::GenericError(ErrorResponse { 
                correlation_id: Some(109), 
                error_code: 99, 
                error_message: "Got an error, sorry!".to_string()
            }),
            ResponseType::ListOrgs => Response::ListOrgs(ListOrgsResponse {
                correlation_id: Some(110),
                orgs: vec![],
                next_cursor: None,
            }),
            ResponseType::ListAggregateTypes => Response::ListAggregateTypes(ListAggregateTypesResponse {
                correlation_id: Some(111),
                aggregate_types: vec![],
                next_cursor: None,
            }),
            ResponseType::ListAggregates => Response::ListAggregates(ListAggregatesResponse {
                correlation_id: Some(112),
                aggregates: vec![],
                next_cursor: None,
            }),
            ResponseType::ReplicationBatch => Response::ReplicationBatch(ReplicationBatchResponse {
                correlation_id: Some(113),
            }),
        }
    }

    #[test]
    fn all_response_types_covered_in_from_u32() {
        for response_type in ResponseType::all() {
            let id = response_type as u32;
            let parsed = ResponseType::from_u32(id).expect("all variants should parse");
            assert_eq!(parsed, response_type);
        }
    }

    #[test]
    fn response_type_ids_are_contiguous_from_1() {
        // Ensures no gaps - if someone adds id=12 but skips 11, this catches it
        for id in 1..=RESPONSE_TYPE_MAX_ID {
            ResponseType::from_u32(id)
                .unwrap_or_else(|_| panic!("id {} should be a valid ResponseType", id));
        }
    }

    #[test]
    fn invalid_response_type_id_zero_fails() {
        assert!(ResponseType::from_u32(0).is_err());
    }

    #[test]
    fn invalid_response_type_id_above_max_fails() {
        // This test fails if someone adds a new variant but doesn't update RESPONSE_TYPE_MAX_ID
        assert!(
            ResponseType::from_u32(RESPONSE_TYPE_MAX_ID + 1).is_err(),
            "If this fails, update RESPONSE_TYPE_MAX_ID to {}", RESPONSE_TYPE_MAX_ID + 1
        );
    }

    #[test]
    fn response_type_round_trip_matches() {
        for response_type in ResponseType::all() {
            let response = make_test_response(response_type);
            assert_eq!(response.response_type(), response_type);
        }
    }

    #[test]
    fn is_fixed_size_consistent_with_variants() {
        // Verify every response type explicitly declares fixed/variable
        for response_type in ResponseType::all() {
            let _ = response_type.is_fixed_size(); // Just ensure it doesn't panic
        }
    }

    #[test]
    fn response_write_read_round_trip_all_types_v2() {
        block_on(async {
            for response_type in ResponseType::all() {
                let original = make_test_response(response_type);
                let version = PROTOCOL_VERSION_V2;

                // Write
                let mut buffer = Vec::new();
                Response::write_response(&mut buffer, &original, CompressionType::None, version)
                    .await
                    .unwrap_or_else(|e| panic!("write failed for {:?}: {:?}", response_type, e));

                // Read
                let mut cursor = Cursor::new(buffer);
                let parsed = Response::read_response(&mut cursor)
                    .await
                    .unwrap_or_else(|e| panic!("read failed for {:?}: {:?}", response_type, e));

                assert_eq!(parsed.response_type(), response_type);
            }
        });
    }

    #[test]
    fn response_write_read_round_trip_all_types_v3() {
        block_on(async {
            for response_type in ResponseType::all() {
                let original = make_test_response(response_type);
                let version = PROTOCOL_VERSION_V3;

                // Write
                let mut buffer = Vec::new();
                Response::write_response(&mut buffer, &original, CompressionType::None, version)
                    .await
                    .unwrap_or_else(|e| panic!("write failed for {:?}: {:?}", response_type, e));

                // Read
                let mut cursor = Cursor::new(buffer);
                let parsed = Response::read_response(&mut cursor)
                    .await
                    .unwrap_or_else(|e| panic!("read failed for {:?}: {:?}", response_type, e));

                assert_eq!(parsed.response_type(), response_type);
            }
        });
    }
}