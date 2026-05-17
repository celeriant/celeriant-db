---
name: error-handling
description: Error handling patterns in Celeriant. Typed enum variants over strings, manual From impls, no thiserror. Use when creating new error types, handling errors, or converting errors to client responses.
---

# Error Handling

## Typed Enum Variants, Not Strings

Every error is a typed enum variant with typed fields carrying contextual data. Callers match on variants and use the fields to make decisions. No string parsing.

```rust
// This is what errors look like here
ShardWriteError::OptimisticConcurrencyViolation {
    expected_version: u64,
    current_aggregate_version: u64,
}
```

Strings are acceptable for: I/O errors, catch-all infrastructure failures where you can't enumerate the cases, and wrapping errors that don't implement Clone/Send (common with glommio and io_uring error types).

## No thiserror

thiserror encourages `#[error("...")]` string formatting at creation time, losing structured data. We use manual `From` implementations instead. They make error flow explicit, preserve typed data through the chain, and only convert to strings at the client boundary.

## Conversion Chain

```
std::io::Error / GlommioError / bincode::Error
            -> RotatingLogError / WireFormatError
            -> ShardCacheError / ShardFsyncError
            -> ShardReadError / ShardWriteError
            -> ShardError
            -> ErrorResponse (to client, via error_to_response())
```

Only use `?` (auto-From) when context is preserved. If two different calls in the same function produce the same error type, convert explicitly so you know which one failed.

## Client Boundary

All errors sent to clients become `ErrorResponse { correlation_id, error_code, error_message }`. String formatting happens here and nowhere else. Error codes are defined in `celeriant_msg/src/error_codes.rs`. The conversion lives in `celeriant_runtimes/src/sharded/connection_handler.rs`.

## Adding a New Error

1. Add a variant with typed fields to the appropriate error enum
2. Write manual `From` impl if it converts from a lower-level error
3. Add the mapping in `error_to_response()` if it can reach clients
4. Pick the right error code from `error_codes.rs`
