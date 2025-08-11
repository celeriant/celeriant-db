# EventPlaneDB Storage Crate

The `eventplanedb_storage` crate provides a durable, high-performance storage solution for event data. It is designed to be used as a backend for applications that need to persist and efficiently retrieve large streams of events.

## Purpose

This crate aims to provide:

- **Durable Storage:** Events are written to disk, ensuring that data is not lost in case of application crashes or system failures.
- **Efficient Retrieval:** Optimized read operations allow applications to quickly retrieve event data based on various criteria.
- **Crash Recovery:** Mechanisms to recover from incomplete writes and corrupted files are built-in, ensuring data consistency.
- **Flexible Data Model:** Supports storing a variety of event data types, including integers, floats, booleans, strings, and byte arrays.
- **Caching:** Includes a multi-level caching system to reduce disk I/O and improve read performance.
- **Filtering:** Allows filtering of events based on event type and client ID.
- **Duplicate Event Prevention:** Prevents duplicate events from being written by the same client.

## Key Components

- **`EventItem`:** Represents a single event in the system. It contains fields for:
    - `local_index`: A client-assigned index for duplicate detection.
    - `event_date`: A timestamp associated with the event.
    - `event_type`: An identifier for the type of event.
    - Various optional fields for storing different types of data (`int_values`, `uint_values`, `f32_values`, `f64_values`, `bool_values`, `string_values`, `iv_arrays`, `byte_arrays`).

- **`EventBatchItem`:** A batch of `EventItem`s. Batches are the unit of write and are compressed for efficient storage.  It contains fields for:
    - `server_id`: A server-assigned unique ID.
    - `server_date`: A timestamp associated with the batch.
    - `client_id`: An identifier for the client that created the batch.
    - `user_id`: An optional identifier for the user associated with the batch.
    - `events`: A vector of `EventItem`s.

- **`CatchupResult`:** Represents the result of reading events from storage. It contains a vector of `EventBatchItem`s and an optional `next_server_id` for pagination.

- **`EventStorage`:** Provides the core functionality for appending and reading event batches from a file. Key functions include:
    - `append_event_batch`: Appends an `EventBatchItem` to the storage file with compression.
    - `read_from_si`: Reads `EventBatchItem`s from the storage file starting from a specified server ID (`si`).
    - `find_last_si`: Finds the last server ID written to the file.
    - `find_last_li`: Finds the last local index written by a specific client.
    - `find_last_valid_event_batch`: Finds the last valid event batch in the file, used for crash recovery.

- **`EventStorageCache`:** Implements a caching layer on top of the `EventStorage` to improve read performance. It uses a multi-level caching system:
    - **Memory Cache:** Stores recently accessed `EventBatchItem`s in memory for fast retrieval.
    - **Local Index Cache:** Stores the last local index written by each client to prevent duplicate events.
    - **Last SI Cache:** Stores the last server ID written to each file.
    - **File Cache:** Caches file handles to reduce the overhead of opening and closing files.
    - It handles transaction management and crash recovery.

- **`FileCache`:** Manages file handles (readers and writers) to avoid repeatedly opening and closing files.

- **`LastSiCache`:** Stores the last server ID for each file in a simple HashMap.

- **`LocalIndexCache`:** Stores the last local index for each client and file.

- **`MemoryCache`:** Stores `EventBatchItem`s in memory with a TTL (time-to-live).

## Data Serialization and Compression

- **Serialization:** Uses `bincode` for efficient serialization and deserialization of event data.
- **Compression:** Employs `zstd` for compressing event batches before writing them to disk, reducing storage space and I/O overhead.
- **Base64 Encoding:** Uses `base64` encoding for serializing and deserializing `u128` client IDs and byte arrays.

## Crash Recovery

- **Transaction Files:** Utilizes transaction files to track incomplete writes. If a crash occurs during a write operation, the transaction file is used to truncate the storage file to its last consistent state.
- **Magic Number:** Appends a magic number to each event batch to detect corruption. During reads, the magic number is checked to ensure data integrity.
- **`find_last_valid_event_batch`:** This function efficiently scans the event store to identify the last valid record before an incomplete write or corruption. It ensures recovery truncates only invalid data.

## Usage

To use the `eventplanedb_storage` crate, add it as a dependency to your `Cargo.toml`:

```toml
[dependencies]
eventplanedb_storage = "0.1.0" # Replace with the actual version
```

Here's a basic example of how to use the crate:

```rust
use eventplanedb_storage::event_storage_cache::EventStorageCache;
use eventplanedb_storage::event_batch_item::EventBatchItem;
use eventplanedb_storage::event_item::EventItem;
use std::io;

fn main() -> io::Result<()> {
    let mut storage = EventStorageCache::new(30, 1000000, 10000);
    let file_path = "events.bin";

    // Create a new event batch
    let mut event_batch = EventBatchItem::new();
    event_batch.client_id = 123;
    event_batch.add_event(EventItem::new());

    // Write the event batch to storage
    let last_si = storage.write(file_path, true, false, event_batch)?;
    println!("Last Server ID: {}", last_si);

    // Read events from storage
    let events = storage.read(file_path, 0, 1024, None, None)?;
    println!("Number of events read: {}", events.event_batches.len());

    Ok(())
}
```

## Benchmarks

The crate includes benchmark tests to measure the performance of key operations:

- **`bench_event_item`:** Benchmarks serialization and deserialization of `EventItem`s.
- **`bench_event_storage`:** Benchmarks appending and reading event batches.
- **`bench_storage_cache`:** Benchmarks read performance with the cache enabled.

## Testing

The crate includes a comprehensive suite of unit tests to ensure correctness and reliability. These tests cover various scenarios, including:

- File creation, reading, and deletion
- Crash recovery
- Cache operations
- Data serialization and compression
- Filtering
- Duplicate event prevention

## Future Enhancements

- Implement more sophisticated caching strategies (e.g., LRU eviction).
- Add support for data replication and sharding.
- Provide a more flexible API for querying event data.
- Explore different compression algorithms for improved performance.