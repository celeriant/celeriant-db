# StatelessEngine Testing Strategy

## 1. Basic Write Operations

### 1.1 Single Event Batch Write
- **Title**: Write single event batch with minimal data
- **Sequence**: Create engine → Write single event batch with one event → Verify metadata
- **Expected Output**: Successful write with correct metadata values

### 1.2 Multiple Event Batch Write
- **Title**: Write multiple event batches sequentially
- **Sequence**: Create engine → Write 3 event batches → Verify all metadata entries
- **Expected Output**: All batches written with incrementing server_ids

### 1.3 Empty Event Batch Rejection
- **Title**: Attempt to write empty event batch
- **Sequence**: Create engine → Attempt to write event batch with empty events vector
- **Expected Output**: IO error indicating empty batch rejection

### 1.4 Large Event Batch Write
- **Title**: Write event batch with maximum reasonable size
- **Sequence**: Create engine → Write batch with 1000+ events → Verify successful write
- **Expected Output**: Successful write with correct compression and metadata

### 1.5 Unicode and Binary Data Handling
- **Title**: Write events containing unicode and binary data
- **Sequence**: Create engine → Write events with UTF-8 strings and binary payloads
- **Expected Output**: Data preserved correctly after write/read cycle

## 2. Compression Testing

### 2.1 All Compression Types
- **Title**: Test each compression algorithm (None, Zstd, Snappy, Brotli, Gzip)
- **Sequence**: For each type → Write same data → Verify compression ratios and integrity
- **Expected Output**: Each algorithm produces different compressed sizes but identical decompressed data

### 2.2 Compression Level Variations
- **Title**: Test different compression levels for algorithms that support it
- **Sequence**: Write same data with levels 1,3,6,9 for Zstd/Brotli/Gzip
- **Expected Output**: Higher levels produce better compression with correct decompression

### 2.3 Highly Compressible Data
- **Title**: Write highly repetitive data to test compression efficiency
- **Sequence**: Create events with repetitive patterns → Write with different compression types
- **Expected Output**: Significant compression achieved, data integrity maintained

### 2.4 Incompressible Data
- **Title**: Write random/encrypted data that doesn't compress well
- **Sequence**: Create events with random binary data → Test all compression types
- **Expected Output**: Minimal compression achieved, but no data corruption

## 3. Event Type Handling

### 3.1 Direct Event Type Storage (≤4 types)
- **Title**: Write batches with 1-4 unique event types
- **Sequence**: Create batches with 1,2,3,4 unique event types → Verify Direct storage used
- **Expected Output**: Metadata uses Direct array, not bloom filter

### 3.2 Bloom Filter Storage (>4 types)
- **Title**: Write batch with 5+ unique event types
- **Sequence**: Create batch with 10 different event types → Verify bloom filter used
- **Expected Output**: Metadata uses bloom filter, all types detectable

### 3.3 Event Type Deduplication
- **Title**: Write batch with duplicate event types
- **Sequence**: Create batch with repeated event types → Verify deduplication in metadata
- **Expected Output**: Only unique types stored in metadata

### 3.4 Event Type Boundary Testing
- **Title**: Test exact boundary between direct and bloom storage
- **Sequence**: Write batch with exactly 4 types, then add 1 more event with new type
- **Expected Output**: First uses Direct, second uses Bloom filter

## 4. Basic Read Operations

### 4.1 Simple Read All
- **Title**: Read all written data without filters
- **Sequence**: Write 3 batches → Read from server_id 1
- **Expected Output**: All 3 batches returned in correct order

### 4.2 Read From Specific Server ID
- **Title**: Read starting from middle server ID
- **Sequence**: Write 5 batches → Read from server_id 3
- **Expected Output**: Batches 3,4,5 returned

### 4.3 Read Non-existent Server ID (Future)
- **Title**: Request server ID that hasn't been written yet
- **Sequence**: Write 3 batches → Read from server_id 10
- **Expected Output**: Empty result with no next_server_id

### 4.4 Read Non-existent Server ID (Past/Trimmed)
- **Title**: Request server ID that was trimmed
- **Sequence**: Write batches 1-5 → Trim start to keep from 3 → Read from server_id 1
- **Expected Output**: Error indicating server_id not available

## 5. Filtering Tests

### 5.1 Server ID Range Filtering
- **Title**: Filter by server ID range (from_server_id to to_server_id)
- **Sequence**: Write 10 batches → Read from server_id 3 to 7
- **Expected Output**: Batches 3,4,5,6,7 returned

### 5.2 Client ID Filtering (Include/Exclude)
- **Title**: Filter by specific client ID
- **Sequence**: Write batches from different clients → Filter by include_client_id
- **Expected Output**: Only batches from specified client returned

### 5.3 User ID Filtering (Include/Exclude)
- **Title**: Filter by user ID including None values
- **Sequence**: Write batches with Some(user_id) and None → Test both include/exclude filters
- **Expected Output**: Correct filtering based on user ID presence/absence

### 5.4 Server Time Range Filtering
- **Title**: Filter by server timestamp range
- **Sequence**: Write batches with different timestamps → Filter by time range
- **Expected Output**: Only batches within time range returned

### 5.5 Local Index Range Filtering
- **Title**: Filter by local index range
- **Sequence**: Write batches with varying local index ranges → Filter by min/max local index
- **Expected Output**: Only batches overlapping with index range returned

### 5.6 Event Time Range Filtering
- **Title**: Filter by event timestamp range
- **Sequence**: Write events with different event times → Filter by event time range
- **Expected Output**: Only events within time range returned (may filter individual events within batches)

### 5.7 Event Type Filtering with Direct Storage
- **Title**: Filter by event types when metadata uses direct array
- **Sequence**: Write batches with ≤4 event types → Filter by specific types
- **Expected Output**: Only matching events returned

### 5.8 Event Type Filtering with Bloom Filter
- **Title**: Filter by event types when metadata uses bloom filter
- **Sequence**: Write batches with >4 event types → Filter by specific types
- **Expected Output**: Correct filtering accounting for bloom filter false positives

### 5.9 Combined Filter Scenarios
- **Title**: Apply multiple filters simultaneously
- **Sequence**: Write diverse data → Apply client_id + time range + event type filters
- **Expected Output**: Only data matching ALL criteria returned

## 6. Pagination Testing

### 6.1 Max Bytes Pagination
- **Title**: Limit response size with max_bytes
- **Sequence**: Write large batches → Read with small max_bytes limit
- **Expected Output**: Partial results with next_server_id for continuation

### 6.2 Exact Boundary Pagination
- **Title**: Test pagination at exact batch boundaries
- **Sequence**: Write batches of known sizes → Set max_bytes to exact total of N batches
- **Expected Output**: Exactly N batches returned, correct next_server_id

### 6.3 Single Byte Under/Over Boundary
- **Title**: Test pagination sensitivity
- **Sequence**: Write batches → Set max_bytes to 1 byte under/over a batch boundary
- **Expected Output**: Different number of batches returned based on boundary

### 6.4 Pagination with Filtering
- **Title**: Combine pagination with filters
- **Sequence**: Write mixed data → Apply filters + max_bytes limit
- **Expected Output**: Filtered AND paginated results

## 7. Corruption Detection

### 7.1 No Corruption Detection
- **Title**: Verify clean files show no corruption
- **Sequence**: Write valid data → Run corruption detection
- **Expected Output**: None returned (no corruption detected)

### 7.2 Truncated Metadata File
- **Title**: Detect partial metadata entry corruption
- **Sequence**: Write data → Truncate metadata file mid-entry → Detect corruption
- **Expected Output**: Corruption detected at truncation point

### 7.3 Corrupted Metadata Deserialization
- **Title**: Detect metadata deserialization failures
- **Sequence**: Write data → Corrupt metadata bytes → Detect corruption
- **Expected Output**: Corruption detected at corrupted metadata position

### 7.4 Event Batch File Too Short
- **Title**: Detect when event batch file doesn't contain expected data
- **Sequence**: Write data → Truncate event batch file → Detect corruption
- **Expected Output**: Corruption detected when event batch data is insufficient

### 7.5 CRC Mismatch Detection
- **Title**: Detect CRC checksum failures
- **Sequence**: Write data → Corrupt event batch data → Detect corruption
- **Expected Output**: Corruption detected due to CRC mismatch

### 7.6 Empty File Corruption
- **Title**: Handle empty files as corruption
- **Sequence**: Create empty files → Run corruption detection
- **Expected Output**: Corruption detected at position 0

### 7.7 Mismatched File Lengths
- **Title**: Detect when metadata and event batch files are inconsistent
- **Sequence**: Write data → Truncate one file but not the other → Detect corruption
- **Expected Output**: Corruption detected at inconsistency point

## 8. Metadata Query Operations

### 8.1 Last Server ID Retrieval
- **Title**: Get most recent server ID
- **Sequence**: Write multiple batches → Query last_server_id
- **Expected Output**: Server ID of most recently written batch

### 8.2 Last Server ID from Empty File
- **Title**: Handle last_server_id query on empty file
- **Sequence**: Create empty metadata file → Query last_server_id
- **Expected Output**: Error indicating insufficient data

### 8.3 Last Local Index Retrieval
- **Title**: Get highest local index from last batch
- **Sequence**: Write batches with various local indices → Query last_local_index
- **Expected Output**: Highest local index from most recent batch

### 8.4 Last Local Index from Empty File
- **Title**: Handle last_local_index query on empty file
- **Sequence**: Create empty metadata file → Query last_local_index
- **Expected Output**: Error indicating insufficient data

## 9. Destructive Operations

### 9.1 Trim End Operation
- **Title**: Truncate files at specific positions
- **Sequence**: Write data → Trim end at middle position → Verify truncation
- **Expected Output**: Files truncated to specified positions, remaining data intact

### 9.2 Trim End at File Boundaries
- **Title**: Trim at exact batch boundaries
- **Sequence**: Write 3 batches → Trim at end of batch 2 → Verify batch 3 removed
- **Expected Output**: Only first 2 batches remain

### 9.3 Trim Start Operation
- **Title**: Remove data from beginning of files
- **Sequence**: Write 5 batches → Trim start to keep from batch 3 → Verify removal
- **Expected Output**: Only batches 3,4,5 remain, files start with batch 3

### 9.4 Trim Start to Position 0 Rejection
- **Title**: Reject invalid trim start position
- **Sequence**: Write data → Attempt trim_start to position 0
- **Expected Output**: Error indicating invalid position

### 9.5 Trim Start with Temporary Files
- **Title**: Verify temp file cleanup during trim start
- **Sequence**: Write data → Trim start → Check no .tmp files remain
- **Expected Output**: Operation succeeds, no temporary files left behind

### 9.6 File Deletion Operation
- **Title**: Delete both event batch and metadata files
- **Sequence**: Write data → Delete files → Verify removal
- **Expected Output**: Both files no longer exist

### 9.7 Delete Non-existent Files
- **Title**: Handle deletion of missing files gracefully
- **Sequence**: Attempt to delete non-existent files
- **Expected Output**: Error indicating files not found

## 10. I/O Uring Integration (Linux-specific)

### 10.1 I/O Uring Availability Detection
- **Title**: Verify I/O Uring detection works correctly
- **Sequence**: Create engine → Check is_io_uring_available status
- **Expected Output**: Correct detection based on system capabilities

### 10.2 I/O Uring vs Standard I/O Equivalence
- **Title**: Verify I/O Uring and standard I/O produce identical results
- **Sequence**: Write data → Read with both methods → Compare results
- **Expected Output**: Identical read results from both methods

### 10.3 I/O Uring Queue Depth Configuration
- **Title**: Test different I/O Uring queue depths
- **Sequence**: Create engines with different queue depths → Perform operations
- **Expected Output**: All operations succeed regardless of queue depth

### 10.4 I/O Uring Fallback Behavior
- **Title**: Verify fallback to standard I/O when I/O Uring unavailable
- **Sequence**: Force I/O Uring disabled → Perform read operations
- **Expected Output**: Operations succeed using standard I/O

## 11. Concurrency and Thread Safety

### 11.1 Multiple Reader Safety
- **Title**: Verify multiple concurrent readers don't interfere
- **Sequence**: Write data → Start multiple concurrent read operations
- **Expected Output**: All readers get correct, consistent results

### 11.2 Reader During Write Safety
- **Title**: Test reading while writing is in progress
- **Sequence**: Start long write operation → Start read operation during write
- **Expected Output**: Reader gets consistent view (either before or after write)

## 12. Memory and Resource Management

### 12.1 Large File Handling
- **Title**: Handle files larger than available RAM
- **Sequence**: Create very large files → Perform read operations
- **Expected Output**: Operations succeed without excessive memory usage

### 12.2 Memory Efficiency with Many Small Batches
- **Title**: Verify efficient handling of many small event batches
- **Sequence**: Write thousands of small batches → Read with various filters
- **Expected Output**: Operations complete without memory issues

### 12.3 Resource Cleanup
- **Title**: Verify proper cleanup of file handles and memory
- **Sequence**: Perform many operations → Check resource usage
- **Expected Output**: No resource leaks detected

## 13. Platform Compatibility

### 13.1 Cross-Platform File Handling
- **Title**: Verify consistent behavior across platforms
- **Sequence**: Write on one platform → Read on another → Compare results
- **Expected Output**: Identical data read regardless of platform

### 13.2 Windows-Specific Behavior
- **Title**: Test Windows-specific code paths (no I/O Uring)
- **Sequence**: Run all operations on Windows
- **Expected Output**: All operations work correctly using standard I/O

### 13.3 Unix-Specific Behavior
- **Title**: Test Unix-specific optimizations
- **Sequence**: Run operations on Unix systems with/without I/O Uring
- **Expected Output**: Correct behavior in both scenarios

## 14. Error Handling and Edge Cases

### 14.1 Disk Space Exhaustion
- **Title**: Handle running out of disk space during write
- **Sequence**: Fill disk → Attempt write operation
- **Expected Output**: Appropriate error without data corruption

### 14.2 Permission Denied Scenarios
- **Title**: Handle insufficient file permissions
- **Sequence**: Remove write permissions → Attempt operations
- **Expected Output**: Clear permission errors without crashes

### 14.3 Network File System Behavior
- **Title**: Verify behavior on network-mounted filesystems
- **Sequence**: Perform operations on NFS/SMB mounts
- **Expected Output**: Consistent behavior with potential performance differences

### 14.4 Interrupted Operations
- **Title**: Handle system interruptions gracefully
- **Sequence**: Start long operation → Simulate interruption → Check consistency
- **Expected Output**: Either complete success or clean failure, no corruption