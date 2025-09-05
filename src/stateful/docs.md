# Initial Prompt Using Stateless Layer As Reference

lets build a statefull, single threaded layer over the stateless implementation. Focus on:
- reuse of file handles (write and read handles per aggregate id)
- on write, update the in-memory cache for following readers that need to catch up the event stream
- a cache to store the most recent event batch index for each aggregate so we know what the next event batch index is when writing
- a cache to store the most recent client event index for each aggregate/client_id combination - basically producer idempotency to ensure we don't allow clients to write the same event twice on write call

the statefull layer has both readers and writers, so we can on write:
- get last event batch id for aggregate
- create an event batch and set id
- validate client id is not sending duplicate events (filter them out)

and on read:
- look at the catch up position (event batch index) the client is requesting from. do we have it in memory? otherwise execute a read to get it from disk
- if getting from memory make sure to apply filters as per usual...
- on read we can also catch errors (file corruption) and attempt to recover, then read again

and finally on delete we need to clear handles and caches...

Let's use moka-lite crate (in single-threaded mode, no concurrency support required) for the caching layer.

# Stateful Layer Implementation Strategy

## Overview

This document outlines the design for building a stateful, single-threaded layer over the existing stateless implementation. The stateful layer will provide caching for improved read performance, create batches with an incremented index for events and preventing duplicate writes.

## Core Components

### 1. Stateful Engine (`StatefulEngine`)

The main orchestrator that manages all stateful operations, containing:
- File handle pools for readers and writers for each aggregate
- Caching of last event batch index for each aggregate
- Caching of last client event index for each client within each aggregate
- Error recovery mechanisms
All cached data should preference using moka-lite for the implementation and be configurable in the stateful engine constructor.
The stateful engine's job on write is to:
- Set the event batch's event batch index to the next value
- If client has specified an event batch index check it is the latest in the aggregate for optimistic concurrency control
- Trim any events from the client that have already been written using the client event index

### 2. File Handle Management

#### Writer Handle Pool
- **Key**: `aggregate_id` (String or similar identifier)
- **Value**: `WriterHandle` containing:
  - `BufWriter<File>` for event batch data
  - `BufWriter<File>` for metadata
  - `last_write_time` for idle cleanup

#### Reader Handle Pool
- **Key**: `aggregate_id`
- **Value**: `ReaderHandle` containing:
  - `BufReader<File>` for event batch data
  - `BufReader<File>` for metadata
  - `last_read_time` for idle cleanup
  - `file_size_at_last_read` for change detection

### 3. Caching Strategy

#### Cache 1: Last Event Batch Index (`last_event_batch_cache`)
- **Purpose**: Track the next available event batch index for each aggregate
- **Key**: `aggregate_id: String`
- **Value**: `u64` (next event batch index to assign)

#### Cache 2: Client Event Index Tracking (`client_event_index_cache`)
- **Purpose**: Prevent duplicate event writes from clients (producer idempotency)
- **Key**: `(aggregate_id: String, client_id: u128)`
- **Value**: `u64` (highest client_event_index seen from this client)

#### Cache 3: Recent Event Batches (`recent_batches_cache`)
- **Purpose**: Serve recent reads from memory without disk I/O
- **Key**: `(aggregate_id: String)`
- **Value**: A struct that contains EventBatchItem[] and EventBatchMetadata (we need compressed size for max byte limit pagination!)
- **TTL**: 30 minutes (configurable)
- **Size Limit**: 5,000 entries
Because we can store a lot of event batches for a single aggregate, we also need to limit this in the cache too.

## API Design

### Core Traits

#### `StatefulWriter`
```rust
trait StatefulWriter {
    fn append_events(
        &mut self,
        aggregate_id: &str,
        client_id: u128,
        user_id: Option<u128>,
        events: Vec<EventItem>,
        expected_event_batch_index: Option<u64>, // for optimistic concurrency control
    ) -> Result<EventBatchMetadata>;
}
```

#### `StatefulReader` 
```rust
trait StatefulReader {
    fn read_filtered(
        &mut self,
        aggregate_id: &str,
        filters: &ReadFilters,
    ) -> Result<ReadResult>;
    fn exists(&mut self, aggregate_id: &str) -> Result<bool>;
}
```

#### `StatefulDestructive`
```rust  
trait StatefulDestructive {
    fn trim_end(&mut self, aggregate_id: &str, event_batch_trim_position: u64, metadata_trim_position: u64) -> Result<()>;
    fn trim_start(&mut self, aggregate_id: &str, keep_from_event_batch_index: u64) -> Result<()>;
    fn delete(&mut self, aggregate_id: &str) -> Result<()>;
}
```

## Implementation Details

### Write Operations

#### Flow for `append_event_batch`:

1. **Get or Create Writer Handle**
   - Check writer handle pool for existing handle
   - If not found, create new files and handles
   - Update `aggregate_metadata_cache` with file existence

2. **Determine Next Event Batch Index**
   - Check `last_event_batch_cache` for cached value
   - If cache miss, read from disk using stateless layer
   - Increment and cache the result

3. **Optimistic Concurrency Check**
   - If the client has passed in an event batch index, check it is still the latest
   - If its not the latest, the client invariant check is based on outdated data, fail with optimistic concurrency error

3. **Producer Idempotency Check**
   - Extract client_id from event batch
   - Check `client_event_index_cache` for highest seen index
   - Filter out any events with client_event_index <= cached value
   - Update cache with new highest index

4. **Write to Disk**
   - Set event_batch_index on the batch
   - Use stateless layer to write to files

5. **Update Caches**
   - Update `last_event_batch_cache` with next index
   - Add written batch to `recent_batches_cache`
   - Update `client_event_index_cache`

Just like read below, we need to catch corruption errors and recover the file if required.

### Read Operations

#### Flow for `read_filtered`:

1. **Cache Check for Recent Data**
   - If `from_event_batch_index` is recent (within cache TTL), attempt memory-only read. Because batch index is incremented without gaps,
   can check first batch in cache array and calculate if the required read from batch is present using array length bounds
   - Build result from `recent_batches_cache` entries
   - Apply filters to cached data - use same filter engine/logic in stateless, don't rewrite it

2. **Fallback to Disk Read**
   - Get or create reader handle from pool
   - Use stateless layer for disk read
   - Note that reads don't add to the cache, only writes. This is because the most common pattern is a single write and muliple fan-out read at the index.

3. **Error Recovery**
   - If corruption detected, attempt recovery:
     - Use `detect_corruption` to find corruption point
     - Perform `trim_end` to remove corrupted data
     - Clear affected caches
     - Retry the read operation

### Handle Management

#### Writer Handle Lifecycle:
- **Creation**: On first write to aggregate
- **Reuse**: Subsequent writes to same aggregate reuse handle
- **Cleanup**: After 15 minutes of inactivity

#### Reader Handle Lifecycle:
- **Creation**: On first read from aggregate  
- **Reuse**: Subsequent reads reuse handle if file unchanged
- **Cleanup**: After 15 minutes of inactivity

For our handles we can assume exclusive use at the OS level.

### Cache Management

#### Eviction Policies:
- **Size-based**: Each cache has maximum entry limits
- **Time-based**: TTL varies by cache purpose and data sensitivity
- **Manual**: Caches cleared on destructive operations

#### Invalidation Strategy:
- **Write Operations**: Update relevant caches with new data
- **Destructive Operations**: Clear all caches for affected aggregate
- **Corruption Recovery**: Clear all caches for affected aggregate

## Error Handling & Recovery

### Corruption Detection:
1. On read errors, automatically run corruption detection
2. If corruption found:
   - Attempt automatic recovery via trim_end
   - Clear all caches for aggregate
   - Retry original operation

### Handle Recovery:
1. On I/O errors with handles:
   - Close and remove handle from pool
   - Clear related cache entries
   - Retry operation with fresh handle

### Cache Consistency:
1. On any destructive operation:
   - Clear all cache entries for affected aggregate
   - Ensure subsequent operations rebuild from disk

## Configuration

### Configurable Parameters:
```rust
pub struct StatefulEngineConfig {
    // Cache configurations
    pub last_event_batch_cache_size: usize,        // default: 10,000
    pub last_event_batch_cache_ttl: Duration,      // default: 1 hour
    
    pub client_event_index_cache_size: usize,      // default: 50,000  
    pub client_event_index_cache_ttl: Duration,    // default: 4 hours
    
    pub recent_batches_cache_size: usize,          // default: 5,000
    pub recent_batches_cache_ttl: Duration,        // default: 30 minutes
    
    // Handle management
    pub writer_handle_idle_timeout: Duration,      // default: 5 minutes
    pub reader_handle_idle_timeout: Duration,      // default: 10 minutes
    
    // File paths
    pub base_path: PathBuf,
    pub compression_type: CompressionType,
    
    // Stateless engine configuration
    pub stateless_config: StatelessEngineConfig,
}
```

## File Organization

### Directory Structure:
```
base_path/
├── aggregate_1/
│   ├── event_batches.bin
│   └── metadata.bin
├── aggregate_2/
│   ├── event_batches.bin  
│   └── metadata.bin
└── ...
```

### Path Resolution:
- Each aggregate gets its own subdirectory
- Standard file names within each directory
- Automatic directory creation on first write

## Performance Considerations

### Memory Usage:
- Cache sizes should be tunable based on available memory
- Handle pools prevent excessive file descriptor usage
- Recent batches cache provides significant read performance boost

### I/O Optimization:
- Buffered writers for better write performance
- Handle reuse eliminates open/close overhead
- Batch writes reduce system calls

### Concurrency:
- Single-threaded design eliminates locking overhead
- All operations are synchronous and deterministic
- Cache operations are atomic within single thread

## Testing Strategy

### Unit Tests:
- Individual cache behavior validation
- Handle lifecycle management
- Producer idempotency enforcement

### Integration Tests:  
- End-to-end write/read cycles
- Cache hit/miss scenarios
- Error recovery workflows
- Multi-aggregate scenarios

### Performance Tests:
- Cache effectiveness measurement
- Handle reuse validation
- Memory usage profiling
- I/O reduction verification

This design provides a robust, performant stateful layer that leverages caching and handle reuse while maintaining data consistency and providing automatic error recovery capabilities.