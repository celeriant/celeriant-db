### New Test File: `test_read_write/test_concurrency_and_idempotency.rs`

This file should focus on safe concurrent access patterns and writer idempotency logic.

*   **Idempotency & Concurrency:**
    *   `test_optimistic_concurrency_violation`: Have two writer tasks attempt to write using the same `expected_event_batch_index`. Verify the first succeeds and the second fails with `OptimisticConcurrencyViolation`.
    *   `test_client_idempotency_violation`: Attempt to write an event batch containing a `client_event_index` that has already been seen for that client, and verify it fails with `ClientIdempotencyViolation`.
*   **Resource Initialization:**
    *   `test_concurrent_get_reader_initializes_once`: Spawn several tasks that call `get_reader` concurrently on a new aggregate. Verify that the underlying files are opened only once.
    *   `test_concurrent_get_writer_initializes_once`: Do the same as above for `get_writer`.
*   **Sync Coalescing:**
    *   `test_sync_with_delay_coalesces_requests`: Spawn multiple tasks that call `sync_with_delay` at roughly the same time. Verify that only one underlying sync operation is performed and all tasks are notified upon its completion.