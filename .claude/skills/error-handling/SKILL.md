---
name: error-handling
description: Guidelines for error handling in Celeriant. Use when creating new error types, handling errors, or converting errors to client responses. Emphasizes strongly-typed errors with contextual data over string parsing.
---

# Error Handling in Celeriant

## Core Philosophy

**Errors are data, not strings.** Every error should carry strongly-typed, contextual information that allows callers to make decisions without parsing strings.

### Anti-patterns to Avoid

```rust
// BAD: String-based errors lose type information
return Err("aggregate not found".to_string());

// BAD: Parsing errors from strings is fragile and expensive
if error_message.contains("not found") { ... }

// BAD: thiserror encourages string-heavy error messages
#[derive(thiserror::Error)]
enum MyError {
    #[error("failed to read file {0}: {1}")]
    ReadFailed(String, std::io::Error),  // Context lost in formatting
}
```

### Preferred Patterns

```rust
// GOOD: Strongly typed variant with contextual data
enum ShardWriteError {
    OptimisticConcurrencyViolation {
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },
    TrimIndexOutOfRange {
        requested: u64,
        max_event_batch_index: u64,
    },
}

// GOOD: Caller can match and make decisions
match error {
    ShardWriteError::OptimisticConcurrencyViolation { expected, current } => {
        // Retry with updated index, no string parsing needed
    }
}
```

## Error Type Structure

### Define Enum Variants with Context

Each error variant should contain the data needed for:
1. **Decision making** - Can the caller retry? With what parameters?
2. **Debugging** - What values caused the failure?
3. **User feedback** - What should the client see?

```rust
// Reference: celeriant_shard/src/error/shard_write_error.rs
#[derive(Debug, Clone)]
pub enum ShardWriteError {
    /// Disk I/O failure (infrastructure, may be transient)
    IoError(String),  // OK: I/O errors are inherently string-like

    /// Serialization or deserialization failure
    WireFormat(WireFormatError),  // Wrap lower-level error type

    /// Write request contained no events
    EmptyEventsList,  // No context needed - self-explanatory

    /// Event type 0 is reserved as sentinel value
    ZeroEventType { client_event_index: u64 },  // Which event?

    /// Client already wrote event with this or higher index
    ClientIdempotencyViolation {
        last_client_event_index: u64,
        attempted_client_event_index: u64,
    },

    /// Expected event_batch_index doesn't match aggregate state
    OptimisticConcurrencyViolation {
        expected_event_batch_index: u64,
        current_event_batch_index: u64,
    },
}
```

### When to Use Strings in Errors

Strings are acceptable for:
- **I/O errors** - External system messages are inherently string-like
- **Catch-all infrastructure failures** - When you can't enumerate all cases

Strings are NOT acceptable for:
- **Domain/business logic errors** - Use typed variants
- **Recoverable errors** - Caller needs structured data to recover
- **Errors that cross API boundaries** - Client needs to match on them

## Error Conversion Chain

Build explicit `From` implementations to convert between error layers:

```
std::io::Error / GlommioError / bincode::Error
                ↓ From impl
    RotatingLogError / WireFormatError
                ↓ From impl
    ShardCacheError / ShardFsyncError
                ↓ From impl
    ShardReadError / ShardWriteError
                ↓ From impl
            ShardError
                ↓ error_to_response()
            ErrorResponse (to client)
```

Only do this if the context of the error can be maintained, if information is lost in the conversion it should be done explicitly. For example two seprate calls that generate the same error in a function should not use the ? into behavior.

### Manual From Implementations

```rust
// Reference: celeriant_shard/src/error/shard_write_error.rs
impl From<ShardFsyncError> for ShardWriteError {
    fn from(e: ShardFsyncError) -> Self {
        match e {
            // Preserve context when possible
            ShardFsyncError::IoError(msg) => Self::IoError(msg),
            ShardFsyncError::WireFormat(wire_err) => Self::WireFormat(wire_err),

            // Convert with context when collapsing
            ShardFsyncError::HeaderCorrupted { log_id } => {
                Self::IoError(format!("Log header corrupted: log_id={:?}", log_id))
            }
            ShardFsyncError::NotEnoughLogFreeSpace { required, available } => {
                Self::IoError(format!(
                    "Not enough free log space, required: {} but available: {}",
                    required, available
                ))
            }
        }
    }
}
```

### Why Not thiserror?

thiserror encourages:
- Stringly-typed error messages via `#[error("...")]`
- String interpolation at error creation time (expensive)
- Losing structured data once formatted

Manual `From` implementations:
- Make error flow explicit and auditable
- Preserve typed data through the chain
- Only convert to strings at the final boundary (client response)

## Client Error Response

All errors sent to clients go through `ErrorResponse`:

```rust
// Reference: celeriant_msg/src/response/responses.rs
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ErrorResponse {
    pub correlation_id: Option<u128>,  // Request matching
    pub error_code: u32,                // HTTP-like status code
    pub error_message: String,          // Human-readable description
}
```

### Error Code Semantics

Use HTTP-like status codes for consistency:

| Code | Meaning | Examples |
|------|---------|----------|
| 400 | Client error (bad request) | Empty events, invalid parameters, zero event type |
| 404 | Not found | Aggregate doesn't exist |
| 409 | Conflict | OCC violation, idempotency violation |
| 410 | Gone | Batch index no longer available (trimmed) |
| 500 | Server error | I/O failure, serialization error |

### Converting Errors to Responses

String conversion happens ONLY at the boundary:

```rust
// Reference: celeriant_runtimes/src/sharded/connection_handler.rs:471-517
fn error_to_response(correlation_id: Option<u128>, error: ShardError) -> Response {
    let (error_code, error_message) = match error {
        ShardError::Write(write_error) => match write_error {
            // Include contextual data in message for debugging
            ShardWriteError::OptimisticConcurrencyViolation {
                expected_event_batch_index,
                current_event_batch_index
            } => (
                409,
                format!(
                    "Optimistic concurrency violation: expected={}, current={}",
                    expected_event_batch_index, current_event_batch_index
                ),
            ),

            // Simple errors need minimal context
            ShardWriteError::EmptyEventsList => (400, "Empty events list".to_string()),

            // I/O errors pass through their message
            ShardWriteError::IoError(msg) => (500, format!("IO error: {}", msg)),
        },
        // ... other error types
    };

    Response::GenericError(ErrorResponse { correlation_id, error_code, error_message })
}
```

## Checklist for New Error Types

When adding a new error type:

1. **Define the enum** with contextual data in variants
   - What information does the caller need to handle this error?
   - What values caused the failure?

2. **Implement `From` traits** for lower-level errors
   - Map each lower-level variant explicitly
   - Preserve structured data where possible
   - Convert to strings only when collapsing infrastructure errors

3. **Implement `Debug`** (derive is usually fine)
   - Used for logging and troubleshooting

4. **Add to error_to_response()** if errors can reach clients
   - Choose appropriate HTTP-like status code
   - Include contextual values in message for debugging

5. **Do NOT use thiserror** unless interfacing with external crates that require `std::error::Error`

## Example: Adding a New Error Variant

```rust
// 1. Add variant with context
pub enum ShardWriteError {
    // ... existing variants ...

    /// Payload exceeds maximum allowed size
    PayloadTooLarge {
        actual_bytes: usize,
        max_bytes: usize,
    },
}

// 2. Handle in error_to_response
ShardWriteError::PayloadTooLarge { actual_bytes, max_bytes } => (
    400,
    format!("Payload too large: {} bytes exceeds max of {} bytes", actual_bytes, max_bytes),
),

// 3. Caller can make decisions without parsing
match write_result {
    Err(ShardWriteError::PayloadTooLarge { max_bytes, .. }) => {
        // Split payload and retry, using max_bytes as guide
    }
}
```

## Files Reference

| File | Purpose |
|------|---------|
| `celeriant_shard/src/error/shard_write_error.rs` | Write operation errors |
| `celeriant_shard/src/error/shard_read_error.rs` | Read operation errors |
| `celeriant_shard/src/error/shard_error.rs` | Top-level error wrapper |
| `celeriant_wire/src/wire_format_error.rs` | Serialization errors |
| `celeriant_rotating_log/src/rotating_log_error.rs` | Log file errors |
| `celeriant_msg/src/response/responses.rs` | Client response types |
| `celeriant_runtimes/src/sharded/connection_handler.rs` | Error-to-response conversion |
