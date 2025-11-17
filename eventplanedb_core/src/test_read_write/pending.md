### New Test File: `test_read_write/test_trimming_and_prepending.rs`

This file should focus on testing the lifecycle of data archival, including removing old data and prepending historical data.

*   **Trimming Start:**
    *   `test_trim_start_successfully_removes_data`: Write several batches, call `trim_start` to remove the first few, then read to verify they are gone and the `minimum_available_event_batch_index` has been updated.
    *   `test_trim_start_reading_trimmed_index_fails`: After a trim, attempt to read a batch index that was removed and assert that it returns an `UnavailableBatchIndex` error.
    *   `test_trim_start_invalidates_caches`: Create reader/writers, write batches, then prepare another writer and update the reader's cache, perform a trim, then check reader and writer caches are cleared/invalidated.
    *   `test_trim_start_then_write`: Write batches, trim some batches from start, then try to write more batches. Should succeed.
*   **Trimming End:**
    *   `test_trim_end_removes_recent_data`: Write several batches, call `trim_end` to remove the last few, and verify they are no longer readable.
*   **Prepending Data:**
    *   `test_prepend_successfully_adds_older_data`: Write batches 10-15, then prepend batches 5-9 and verify that all batches from 5 to 15 are now readable.
    *   `test_prepend_with_index_gap_fails`: Attempt to prepend batches 5-7 when the oldest existing batch is 10, and verify it fails with a `PrependCreatesEventBatchIndexGap` error.
    *   `test_prepend_with_non_contiguous_data_fails`: Attempt to prepend a list of batches that are not contiguous (e.g., batches 5, 6, and 8) and verify it fails with a `PrependNonContiguousBatches` error.
*   **File Position Calculation:**
    *   `test_get_file_positions_accurate`: Test the `get_file_positions` read operation to ensure it accurately calculates the byte offsets needed for a future `trim_start` operation.

### New Test File: `test_read_write/test_pagination.rs`

This file should be dedicated to testing the `max_bytes` pagination feature for both general reads and cached reads.

*   **Reader Pagination:**
    *   `test_read_with_max_bytes_returns_first_page`: Perform a read with a `max_bytes` limit that only fits a subset of batches. Verify the correct batches are returned and `next_event_batch_index` points to the next batch.
    *   `test_read_fetches_subsequent_pages`: Use the `next_event_batch_index` from a previous paginated read as the `from_event_batch_index` to fetch the next page of results.
    *   `test_pagination_with_filters`: Combine `max_bytes` with other read filters to ensure the byte limit is correctly applied to the filtered metadata set.
    *   `test_read_all_pagination`: Repeat the pagination tests for the `read_all` method.
*   **Writer Cache Pagination:**
    *   `test_writer_cache_read_with_pagination`: Write several batches and test `maybe_read_cached_events` with a `max_bytes` limit, verifying it correctly paginates from the in-memory cache.
*   **Error Conditions:**
    *   `test_pagination_max_bytes_too_small_errors`: Set `max_bytes` to a value smaller than the first matching batch's compressed size and verify a `MaxBytesTooSmall` error is returned.

### New Test File: `test_read_write/test_writer_cache.rs`

This file should test the functionality and edge cases of the writer's in-memory cache.

*   **Cache Hits & Misses:**
    *   `test_cache_hit_with_various_filters`: Write data, sync it, then use `maybe_read_cached_events` with a variety of filters (by time, user, event type) to verify correct filtering from the cache.
    *   `test_cache_miss_for_older_data`: Write batches 10-20. Attempt to read from batch 5 and verify a `CacheMiss` error is returned for the missing range (5-9).
    *   `test_cache_miss_after_cache_is_cleared`: Manually clear the cache, then attempt a read and verify it triggers a `CacheMiss`.
*   **Cache Management:**
    *   `test_cache_trims_when_oversize`: Write data in excess of `max_data_cache_size_bytes` to trigger the cache trimming logic. Verify that the oldest batches are evicted and reading them now causes a `CacheMiss`.
    *   `test_sync_rollback_on_failure`: Simulate an IO error during `sync` (eg. delete the file or lock it?) and verify that the in-memory state (e.g., `next_event_index`, `client_event_indexes`) is correctly rolled back to its pre-sync state.
*   **Dynamic Adjustments:**
    *   `test_update_max_data_cache_size_bytes`: Verify that dynamically adjusting `max_data_cache_size_bytes` triggers the cache trimming logic.

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