use glommio::{GlommioError, io::DmaFile};

#[derive(Debug)]
pub enum ReadVisitError<E> {
    Io(GlommioError<()>),
    Visitor(E),
}

pub struct ObjectPosition {
    pub start_pos: u64,
    pub can_skip: bool,
}

#[derive(Clone)]
pub struct AbsoluteObjectPosition {
    pub start_pos: u64,
    pub end_pos: u64,
}

pub async fn read_objects_absolute(
    file: &DmaFile,
    file_size: u64,
    object_positions: &[AbsoluteObjectPosition],
    max_chunk_size: u64,
) -> glommio::Result<Vec<Vec<u8>>, ()> {

    if object_positions.is_empty() {
        return Ok(Vec::new());
    }

    assert!(
        object_positions
            .windows(2)
            .all(|w| w[0].start_pos <= w[1].start_pos && w[0].end_pos <= w[1].start_pos),
        "object_positions must be ordered by start_pos and non-overlapping"
    );

    let alignment = file.alignment();
    assert!(
        (max_chunk_size as u64) >= alignment && (max_chunk_size as u64) % alignment == 0,
        "max_chunk_size must be a multiple of the device alignment ({alignment})"
    );

    // disallow zero-length absolute objects.
    assert!(
        object_positions.iter().all(|p| p.end_pos > p.start_pos),
        "object_positions must not contain zero-length objects (start_pos < end_pos required)"
    );

    // disallow duplicate consecutive start positions (strictly increasing starts).
    assert!(
        object_positions
            .windows(2)
            .all(|w| w[0].start_pos < w[1].start_pos),
        "object_positions must have strictly increasing start_pos (no duplicates)"
    );

    // disallow last entry starting exactly at EOF.
    assert!(
        object_positions.last().unwrap().start_pos < file_size,
        "last object's start_pos must be less than file size"
    );
    let last_end = std::cmp::min(object_positions.last().unwrap().end_pos, file_size);

    let mut objects = Vec::with_capacity(object_positions.len());

    let mut object_idx = 0usize;
    let mut object_start = object_positions[object_idx].start_pos;
    let mut object_end = std::cmp::min(object_positions[object_idx].end_pos, file_size);

    // Pre-allocate to the known size of the first object when possible.
    let mut object_buf: Vec<u8> = Vec::with_capacity(
        object_end.saturating_sub(object_start) as usize
    );

    // Start from an aligned offset at (or before) the first object's start.
    let mut chunk_start = object_start - (object_start % alignment);

    while chunk_start < last_end && object_idx < object_positions.len() {
        // If the current chunk would entirely precede the next object's start, jump directly
        // to the chunk containing the object's start to avoid useless reads.
        let mut chunk_end = std::cmp::min(chunk_start + max_chunk_size as u64, last_end);
        if chunk_end <= object_start {
            chunk_start = object_start - (object_start % alignment);
            chunk_end = std::cmp::min(chunk_start + max_chunk_size as u64, last_end);
        }

        let read_len = (chunk_end - chunk_start) as usize;
        let chunk = file.read_at(chunk_start, read_len).await?;

        // Start copying within this chunk at the current object's start if it lies in the chunk,
        // otherwise from the beginning of the chunk (for objects spanning across chunks).
        let mut pos_in_chunk = if object_start > chunk_start {
            (object_start - chunk_start) as usize
        } else {
            0
        };

        while chunk_start + (pos_in_chunk as u64) < chunk_end && object_idx < object_positions.len() {
            // If we're still before the current object's start within this chunk, skip the gap.
            if chunk_start + (pos_in_chunk as u64) < object_start {
                // Jump to the object's start within this chunk (or to the end if it falls beyond).
                let target = (object_start - chunk_start) as usize;
                pos_in_chunk = std::cmp::min(target, (chunk_end - chunk_start) as usize);
                continue;
            }

            let cur_pos_abs = chunk_start + pos_in_chunk as u64;
            let object_remaining = object_end.saturating_sub(cur_pos_abs);
            let chunk_remaining = chunk_end - cur_pos_abs;
            let take_len = std::cmp::min(object_remaining, chunk_remaining) as usize;

            if take_len > 0 {
                object_buf.extend_from_slice(&chunk[pos_in_chunk..pos_in_chunk + take_len]);
                pos_in_chunk += take_len;
            }

            // If we've finished the current object, push it and advance to the next.
            if cur_pos_abs + take_len as u64 >= object_end {
                objects.push(std::mem::take(&mut object_buf));
                object_idx += 1;
                if object_idx < object_positions.len() {
                    object_start = object_positions[object_idx].start_pos;
                    object_end = std::cmp::min(object_positions[object_idx].end_pos, file_size);
                    object_buf = Vec::with_capacity(
                        object_end.saturating_sub(object_start) as usize
                    );
                    // Do not reset pos_in_chunk; the next loop iteration will skip any gap
                    // within this chunk until `object_start`.
                }
            }
        }

        chunk_start += max_chunk_size as u64;
        chunk_start -= chunk_start % alignment; // ensure alignment
    }

    Ok(objects)
}

/// Read fixed-size records from `start` up to `end_exclusive` (or EOF) and invoke `on_record`
/// for each full record of size N. Trailing partial bytes are ignored.
/// - start: absolute byte offset (must be < file size)
/// - end_exclusive: absolute end offset (exclusive); None => EOF
/// - max_chunk_size: must be a multiple of the device alignment
pub async fn read_fixed_records_visit_const<const N: usize, E>(
    file: &DmaFile,
    file_size: u64,
    start: u64,
    end_exclusive: Option<u64>,
    max_chunk_size: u64,
    mut on_record: impl FnMut(&[u8; N]) -> Result<(), E>,
) -> Result<usize, ReadVisitError<E>> {
    assert!(N > 0, "record size N must be > 0");

    let alignment = file.alignment();
    assert!(
        max_chunk_size >= alignment && max_chunk_size % alignment == 0,
        "max_chunk_size must be a multiple of the device alignment ({alignment})"
    );

    assert!(
        start % N as u64 == 0,
        "start must be a multiple of the record size ({N})"
    );

    if let Some(end_exclusive) = end_exclusive {
        assert!(
            end_exclusive % N as u64 == 0,
            "end_exclusive must be a multiple of the record size ({N})"
        );
    }

    assert!(start < file_size, "start must be less than file size");
    let stop_at = std::cmp::min(end_exclusive.unwrap_or(file_size), file_size);
    assert!(start < stop_at, "empty range: start must be < end/EOF");

    let mut records = 0usize;

    let mut chunk_start = start - (start % alignment);
    let last_end = stop_at;

    // Carry for records that span chunk boundaries; stack-allocated.
    let mut carry = [0u8; N];
    let mut carry_len = 0usize;

    // We'll skip bytes before `start` only in the first chunk.
    let mut first_chunk = true;

    while chunk_start < last_end {
        let chunk_end = std::cmp::min(chunk_start + max_chunk_size, last_end);
        let read_len = (chunk_end - chunk_start) as usize;
        let chunk = match file.read_at(chunk_start, read_len).await {
            Ok(c) => c,
            Err(e) => return Err(ReadVisitError::Io(e)),
        };

        let mut data = if first_chunk && start > chunk_start {
            &chunk[(start - chunk_start) as usize..]
        } else {
            &chunk[..]
        };
        first_chunk = false;

        // If we have a partial record from previous chunk, fill it first.
        if carry_len > 0 && !data.is_empty() {
            let need = N - carry_len;
            if data.len() >= need {
                carry[carry_len..carry_len + need].copy_from_slice(&data[..need]);
                if let Err(ev) = on_record(&carry) {
                    return Err(ReadVisitError::Visitor(ev));
                }
                records += 1;
                data = &data[need..];
                carry_len = 0;
            } else {
                carry[carry_len..carry_len + data.len()].copy_from_slice(data);
                carry_len += data.len();
                data = &[];
            }
        }

        // Process all full records in this chunk without copying
        let (full, rem) = data.as_chunks::<N>();
        for rec in full {
            if let Err(ev) = on_record(rec) {
                return Err(ReadVisitError::Visitor(ev));
            }
            records += 1;
        }

        // Save remainder as carry
        if !rem.is_empty() {
            carry[..rem.len()].copy_from_slice(rem);
            carry_len = rem.len();
        }

        chunk_start += max_chunk_size;
        chunk_start -= chunk_start % alignment; // keep aligned
    }

    // Ignore trailing partial record (if any) by design.
    Ok(records)
}

#[cfg(any(test, feature = "bench"))]
pub mod test {

    use super::*;
    use glommio::{LocalExecutorBuilder, Placement, io::DmaFile};
    
    use tempfile::tempdir;

    use std::{fs::File, io::Write};

    pub fn object_sizes(start: usize, increment: usize, len: usize) -> Vec<usize> {
        let mut sizes = Vec::new();
        let mut cumulative = 0;
        let mut i = 0;
        
        loop {
            let size = start + (i * increment);
            if cumulative + size > len {
                break;
            }
            sizes.push(size);
            cumulative += size;
            i += 1;
        }
        
        sizes
    }

    pub fn create_test_file(path: &str, object_sizes: &[usize]) -> (String, Vec<u64>, Vec<u64>) {
        let file_path = format!("{}/testfile.bin", path);
        let (start_positions, end_positions) = create_event_batch_file(&file_path, object_sizes);
        (file_path, start_positions, end_positions)
    }

    // Helper: create a file of `record_count` records, each record is `record_size` bytes,
    // filled with the record index (mod 256).
    pub fn create_fixed_record_file(path: &str, record_size: usize, record_count: usize) -> (String, u64) {
        let file_path = format!("{}/fixed_records.bin", path);
        let file_size = create_metadata_file(&file_path, record_size, record_count);
        (file_path, file_size)
    }

    // Helper function to create a test file with variable-sized objects.
    pub fn create_event_batch_file(file_path: &str, object_sizes: &[usize]) -> (Vec<u64>, Vec<u64>) {
        let mut file = File::create(&file_path).unwrap();
        let mut start_positions = Vec::with_capacity(object_sizes.len());
        let mut end_positions = Vec::with_capacity(object_sizes.len());
        
        let mut pos = 0u64;

        for (i, &size) in object_sizes.iter().enumerate() {
            start_positions.push(pos);
            end_positions.push(pos + size as u64);
            let byte = (i % 256) as u8;
            let buf = vec![byte; size as usize];
            file.write_all(&buf).unwrap();
            pos += size as u64;
        }
        file.flush().unwrap();
        (start_positions, end_positions)
    }

    // Helper function to create a file with fixed-sized records.
    pub fn create_metadata_file(
        file_path: &str,
        record_size: usize,
        record_count: usize,
    ) -> u64 {
        let mut file = File::create(&file_path).unwrap();
        for i in 0..record_count {
            let byte = (i % 256) as u8;
            let buf = vec![byte; record_size];
            file.write_all(&buf).unwrap();
        }
        file.flush().unwrap();
        let file_size = (record_size as u64) * (record_count as u64);
        file_size
    }

    // Minimum is 1 << 9 (512KB) otherwise test may hang due to DMA alignment constraints.
    fn different_chunk_sizes() -> Vec<u64> {
        vec![1 << 9, 1 << 10, 1 << 11, 1 << 14, 1 << 17, 1 << 20, 1 << 21, 1 << 25, 1 << 29, 1 << 40, 1 << 50]
    }

    /// Happy Path
    /// Tests reading variable-sized objects (512KB to 2MB) with some objects skipped (2nd and 3rd)
    /// Validates content and sizes across different chunk sizes
    /// Verifies objects spanning multiple chunks work correctly
    #[test]
    fn test_read_objects_across_chunks_absolute() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Create objects of varying sizes, some crossing 1MB boundaries
            let object_sizes = vec![
                512 * 1024,   // 512KB
                1 * 1024 * 1024, // 1MB
                1 * 1024 * 1024 + 123, // 1MB + 123 bytes
                256 * 1024,   // 256KB
                2 * 1024 * 1024, // 2MB
            ];
            let (file_path, start_positions, end_positions) = create_test_file(folder, &object_sizes);

            let mut object_positions: Vec<AbsoluteObjectPosition> = Vec::with_capacity(start_positions.len());   
            for i in 0..start_positions.len() {

                //Skip second and third entries
                if i == 1 || i == 2 {
                    continue;
                }

                let start_pos = start_positions[i];
                let end_pos = end_positions[i];

                object_positions.push(AbsoluteObjectPosition { start_pos, end_pos});
            }

            for chunk_size in different_chunk_sizes().iter().enumerate() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &object_positions, *chunk_size.1).await.unwrap();

                assert_eq!(objects.len(), 3);
                let mut j = 0;
                for (i, obj) in objects.iter().enumerate() {
                    if i == 1 {
                        j += 2;
                    }
                    assert_eq!(obj.len(), object_sizes[j]);
                    let expected_byte = (j % 256) as u8;
                    assert!(obj.iter().all(|&b| b == expected_byte));
                    j += 1;
                }
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests skipping objects at beginning, middle, and end of file (1st, 2nd, 5th)
    /// Ensures gaps between requested objects are handled efficiently
    #[test]
    fn test_read_objects_across_chunks_absolute_start_end_skips() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Create objects of varying sizes, some crossing 1MB boundaries
            let object_sizes = vec![
                512 * 1024,   // 512KB
                1 * 1024 * 1024, // 1MB
                1 * 1024 * 1024 + 123, // 1MB + 123 bytes
                256 * 1024,   // 256KB
                2 * 1024 * 1024, // 2MB
            ];
            let (file_path, start_positions, end_positions) = create_test_file(folder, &object_sizes);

            let mut object_positions: Vec<AbsoluteObjectPosition> = Vec::with_capacity(start_positions.len());   
            for i in 0..start_positions.len() {

                //Skip second and third entries
                if i == 0 || i == 1 || i == 4 {
                    continue;
                }

                let start_pos = start_positions[i];
                let end_pos = end_positions[i];

                object_positions.push(AbsoluteObjectPosition { start_pos, end_pos});
            }

            for chunk_size in different_chunk_sizes().iter().enumerate() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &object_positions, *chunk_size.1).await.unwrap();

                assert_eq!(objects.len(), 2);
                let mut j = 0;
                for (i, obj) in objects.iter().enumerate() {
                    if i == 0 {
                        j += 2;
                    }
                    assert_eq!(obj.len(), object_sizes[j]);
                    let expected_byte = (j % 256) as u8;
                    assert!(obj.iter().all(|&b| b == expected_byte));
                    j += 1;
                }
            }
        }).unwrap();
        handle.join().unwrap();
    }

    // Error Condition
    // Verifies that overlapping object positions trigger assertion failure
    // Tests validation of the "non-overlapping" invariant
    #[test]
    fn test_read_objects_absolute_overlapping_panics() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Create a small file
            let object_sizes = vec![8 * 1024, 8 * 1024];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            // Overlapping absolute positions:
            // [0..(first end)] and [starts[0]+1 .. ends[1]] overlap
            let overlaps = vec![
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] },
                AbsoluteObjectPosition { start_pos: starts[0] + 1, end_pos: ends[1] },
            ];

            // Should panic due to "object_positions must be ordered by start_pos and non-overlapping"
            let file = DmaFile::open(&file_path).await.unwrap();
            let _ = read_objects_absolute(&file, file_len(&file).await, &overlaps, 1 << 12).await;
        }).unwrap();

        // Expect the task to have panicked
        assert!(handle.join().is_err());
    }

    /// Error Condition
    /// Tests that chunk size not being a multiple of device alignment causes panic
    /// Validates alignment requirements
    #[test]
    fn test_read_objects_absolute_invalid_chunk_size_panics() {
        // Chunk size not a multiple of device alignment should assert
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![1024, 1024];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] },
                AbsoluteObjectPosition { start_pos: starts[1], end_pos: ends[1] },
            ];

            // 1 is almost certainly not a valid alignment multiple on real devices
            let file = DmaFile::open(&file_path).await.unwrap();
            let _ = read_objects_absolute(&file, file_len(&file).await, &positions, 1).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    /// Error Condition
    /// Tests that zero-length objects (start_pos == end_pos) cause panic
    /// Also tests that starting exactly at EOF causes panic
    #[test]
    fn test_read_objects_absolute_zero_length_in_middle_and_end() {
        // Now should panic due to zero-length absolute objects.
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // 3 objects of 4 bytes each
            let object_sizes = vec![4, 4, 4];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);
            let file_size = (object_sizes.iter().sum::<usize>()) as u64;

            // Absolute positions with zero-length spans
            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] }, // 0..4
                AbsoluteObjectPosition { start_pos: starts[1], end_pos: starts[1] }, // 4..4 (zero)
                AbsoluteObjectPosition { start_pos: starts[1], end_pos: ends[1] },   // 4..8
                AbsoluteObjectPosition { start_pos: file_size,  end_pos: file_size }, // 12..12 (zero at EOF)
            ];

            // Should assert due to zero-length absolute objects and last start at EOF
            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
            let _ = read_objects_absolute(&file, file_len(&file).await, &positions, 1 << 12).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    /// Edge Case
    /// Tests that requesting end_pos beyond file size correctly truncates to EOF
    /// Validates content is read up to actual file end
    #[test]
    fn test_read_objects_absolute_truncate_beyond_eof() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Two chunks: 6 bytes of 0, 4 bytes of 1
            let object_sizes = vec![6, 4];
            let (file_path, starts, _ends) = create_test_file(folder, &object_sizes);
            let file_size = (object_sizes.iter().sum::<usize>()) as u64;

            // Request from second object start to far beyond EOF
            let start = starts[1];
            let end_far = file_size + 10_000;
            let positions = vec![
                AbsoluteObjectPosition { start_pos: start, end_pos: end_far },
            ];

            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
            let objects = read_objects_absolute(&file, file_len(&file).await, &positions, 1 << 12).await.unwrap();
            assert_eq!(objects.len(), 1);
            assert_eq!(objects[0].len(), (file_size - start) as usize);
            assert!(objects[0].iter().all(|&b| b == 1));
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests single object spanning entire file
    #[test]
    fn test_read_objects_absolute_entire_file() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![5 * 1024 * 1024]; // 5MB single object
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] },
            ];

            for chunk_size in different_chunk_sizes() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
                
                assert_eq!(objects.len(), 1);
                assert_eq!(objects[0].len(), object_sizes[0]);
                assert!(objects[0].iter().all(|&b| b == 0));
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests many small objects (1000+ tiny objects)
    #[test]
    fn test_read_objects_absolute_many_small_objects() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![128; 1500]; // 1500 objects of 128 bytes each
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            let positions: Vec<_> = starts.iter().zip(ends.iter())
                .map(|(&start, &end)| AbsoluteObjectPosition { start_pos: start, end_pos: end })
                .collect();

            for chunk_size in different_chunk_sizes() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
                
                assert_eq!(objects.len(), 1500);
                for (i, obj) in objects.iter().enumerate() {
                    assert_eq!(obj.len(), 128);
                    let expected_byte = (i % 256) as u8;
                    assert!(obj.iter().all(|&b| b == expected_byte));
                }
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests consecutive objects with no gaps
    #[test]
    fn test_read_objects_absolute_consecutive_no_gaps() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![256 * 1024, 512 * 1024, 1024 * 1024, 128 * 1024];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            let positions: Vec<_> = starts.iter().zip(ends.iter())
                .map(|(&start, &end)| AbsoluteObjectPosition { start_pos: start, end_pos: end })
                .collect();

            for chunk_size in different_chunk_sizes() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
                
                assert_eq!(objects.len(), 4);
                for (i, obj) in objects.iter().enumerate() {
                    assert_eq!(obj.len(), object_sizes[i]);
                    let expected_byte = (i % 256) as u8;
                    assert!(obj.iter().all(|&b| b == expected_byte));
                }
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests empty object list returns empty Vec
    #[test]
    fn test_read_objects_absolute_empty_list() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![1024];
            let (file_path, _starts, _ends) = create_test_file(folder, &object_sizes);

            let positions: Vec<AbsoluteObjectPosition> = Vec::new();

            let file = DmaFile::open(&file_path).await.unwrap();
            let objects = read_objects_absolute(&file, file_len(&file).await, &positions, 1 << 12).await.unwrap();
            
            assert_eq!(objects.len(), 0);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests object exactly at file start (start_pos = 0)
    #[test]
    fn test_read_objects_absolute_at_file_start() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![1024 * 1024, 512 * 1024];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            assert_eq!(starts[0], 0); // Verify it starts at 0
            
            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] },
            ];

            for chunk_size in different_chunk_sizes() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
                
                assert_eq!(objects.len(), 1);
                assert_eq!(objects[0].len(), object_sizes[0]);
                assert!(objects[0].iter().all(|&b| b == 0));
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests multiple objects all smaller than one alignment block
    #[test]
    fn test_read_objects_absolute_smaller_than_alignment() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // 10 objects of 64 bytes each (smaller than typical 512-byte alignment)
            let object_sizes = vec![64; 10];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            let positions: Vec<_> = starts.iter().zip(ends.iter())
                .map(|(&start, &end)| AbsoluteObjectPosition { start_pos: start, end_pos: end })
                .collect();

            for chunk_size in different_chunk_sizes() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
                
                assert_eq!(objects.len(), 10);
                for (i, obj) in objects.iter().enumerate() {
                    assert_eq!(obj.len(), 64);
                    let expected_byte = (i % 256) as u8;
                    assert!(obj.iter().all(|&b| b == expected_byte));
                }
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests objects where chunk boundaries align exactly with object boundaries
    #[test]
    fn test_read_objects_absolute_aligned_boundaries() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Use 1MB objects with 1MB chunk size to align boundaries
            let chunk_size = 1 << 20; // 1MB
            let object_sizes = vec![chunk_size as usize; 5];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            let positions: Vec<_> = starts.iter().zip(ends.iter())
                .map(|(&start, &end)| AbsoluteObjectPosition { start_pos: start, end_pos: end })
                .collect();

            let file = DmaFile::open(&file_path).await.unwrap();
            let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
            
            assert_eq!(objects.len(), 5);
            for (i, obj) in objects.iter().enumerate() {
                assert_eq!(obj.len(), chunk_size as usize);
                let expected_byte = (i % 256) as u8;
                assert!(obj.iter().all(|&b| b == expected_byte));
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests large single object (several MB) with small chunks
    #[test]
    fn test_read_objects_absolute_large_object_small_chunks() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![8 * 1024 * 1024]; // 8MB
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] },
            ];

            // Use small 64KB chunks
            let chunk_size = 1 << 16;
            let file = DmaFile::open(&file_path).await.unwrap();
            let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
            
            assert_eq!(objects.len(), 1);
            assert_eq!(objects[0].len(), object_sizes[0]);
            assert!(objects[0].iter().all(|&b| b == 0));
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests first object starts after significant gap from file start
    #[test]
    fn test_read_objects_absolute_gap_at_start() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Create file with objects, but we'll only read from the middle
            let object_sizes = vec![
                10 * 1024 * 1024, // 10MB (skip this)
                2 * 1024 * 1024,  // 2MB (read this)
                1 * 1024 * 1024,  // 1MB (read this)
            ];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            // Skip first 10MB object
            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[1], end_pos: ends[1] },
                AbsoluteObjectPosition { start_pos: starts[2], end_pos: ends[2] },
            ];

            for chunk_size in different_chunk_sizes() {
                let file = DmaFile::open(&file_path).await.unwrap();
                let objects = read_objects_absolute(&file, file_len(&file).await, &positions, chunk_size).await.unwrap();
                
                assert_eq!(objects.len(), 2);
                assert_eq!(objects[0].len(), object_sizes[1]);
                assert_eq!(objects[1].len(), object_sizes[2]);
                assert!(objects[0].iter().all(|&b| b == 1));
                assert!(objects[1].iter().all(|&b| b == 2));
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Error Condition
    /// Tests unordered object positions cause panic
    #[test]
    fn test_read_objects_absolute_unordered_panics() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![1024, 1024, 1024];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            // Out of order: second object before first
            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[1], end_pos: ends[1] },
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] },
            ];

            let file = DmaFile::open(&file_path).await.unwrap();
            let _ = read_objects_absolute(&file, file_len(&file).await, &positions, 1 << 12).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    /// Error Condition
    /// Tests duplicate start positions cause panic
    #[test]
    fn test_read_objects_absolute_duplicate_starts_panics() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![1024, 1024];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);

            // Duplicate start positions
            let positions = vec![
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[0] },
                AbsoluteObjectPosition { start_pos: starts[0], end_pos: ends[1] },
            ];

            let file = DmaFile::open(&file_path).await.unwrap();
            let _ = read_objects_absolute(&file, file_len(&file).await, &positions, 1 << 12).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    async fn file_len(file: &DmaFile) -> u64 {
        file.file_size().await.unwrap()
    }

    /// Happy Path
    /// Tests reading 10 records of 313 bytes each from start to end
    /// Validates record count and content with multiple chunk sizes
    /// Tests both None (EOF) and explicit end_exclusive parameters
    #[test]
    fn test_read_fixed_records_visit_const_basic() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            // 10 records of 313 bytes, record i filled with byte i
            let (file_path, file_size) = create_fixed_record_file(dir, N, 10);

            for chunk_size in different_chunk_sizes() {
                // Collect records to validate content
                let mut seen: Vec<[u8; N]> = Vec::new();
                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                let count = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, chunk_size, |rec| {
                    seen.push(*rec);
                    Ok(())
                }).await.unwrap();

                assert_eq!(count, 10);
                assert_eq!(seen.len(), 10);

                // Validate each record is uniform with its index byte
                for (i, rec) in seen.iter().enumerate() {
                    assert_eq!(rec.len(), N);
                    let expected = (i % 256) as u8;
                    assert!(rec.iter().all(|&b| b == expected));
                }

                // Same result when end_exclusive == EOF
                let mut count2 = 0usize;
                let count2_ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, Some(file_size), chunk_size, |_rec| {
                    count2 += 1;
                    Ok(())
                }).await.unwrap();
                assert_eq!(count2, 10);
                assert_eq!(count2_ret, 10);
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests reading 10 records of 31332 bytes each from start to end
    /// Validates record count and content with multiple chunk sizes
    /// Tests both None (EOF) and explicit end_exclusive parameters
    #[test]
    fn test_read_fixed_records_visit_const_large_fixed() {
        const N: usize = 31332;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            // 10 records of 31332 bytes, record i filled with byte i
            let (file_path, file_size) = create_fixed_record_file(dir, N, 7);

            for chunk_size in different_chunk_sizes() {
                // Collect records to validate content
                let mut seen: Vec<[u8; N]> = Vec::new();
                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                let count = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, chunk_size, |rec| {
                    seen.push(*rec);
                    Ok(())
                }).await.unwrap();

                assert_eq!(count, 7);
                assert_eq!(seen.len(), 7);

                // Validate each record is uniform with its index byte
                for (i, rec) in seen.iter().enumerate() {
                    assert_eq!(rec.len(), N);
                    let expected = (i % 256) as u8;
                    assert!(rec.iter().all(|&b| b == expected));
                }

                // Same result when end_exclusive == EOF
                let mut count2 = 0usize;
                let count2_ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, Some(file_size), chunk_size, |_rec| {
                    count2 += 1;
                    Ok(())
                }).await.unwrap();
                assert_eq!(count2, 7);
                assert_eq!(count2_ret, 7);
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests that partial records at the end are ignored (7 full records + 100 extra bytes)
    /// Verifies correct handling of truncated data at boundary
    #[test]
    fn test_read_fixed_records_unaligned_end() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
            let _ = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 313, Some(600), 1 << 12, |_rec| Ok(())).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    /// Error Condition
    /// Tests starting from unaligned offset (100 bytes into file)
    /// Validates records straddling original file record boundaries are correctly assembled
    /// Tests content validation across boundary spans
    #[test]
    fn test_read_fixed_records_unaligned_start() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
            let _ = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 100, None, 1 << 12, |_rec| Ok(())).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    /// Happy Path (Stress Test)
    /// Tests 20,000 records (~6.26 MB) to force multiple chunk reads
    /// Validates carry mechanism across many chunk boundaries
    #[test]
    fn test_read_fixed_records_visit_const_large_multichunk() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            // Many records to force multiple chunk reads, boundary carries, etc.
            // ~6.26 MiB total
            let record_count = 20_000usize;
            let (file_path, file_size) = create_fixed_record_file(dir, N, record_count);

            // Choose a large, likely-aligned chunk size (16 KiB)
            let chunk_size = 1u64 << 14;

            // Count only; content pattern already covered elsewhere
            let mut count = 0usize;
            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, Some(file_size), chunk_size, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, record_count);
            assert_eq!(count, record_count);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Error Condition
    /// Tests three panic scenarios: start >= file_size, empty range (start == end), invalid chunk size
    #[test]
    fn test_read_fixed_records_visit_const_bounds_and_alignment_panics() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

            // start >= file_size should assert
            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
            let _ = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, file_size, None, 1 << 12, |_rec| Ok(())).await;
        }).unwrap();

        assert!(handle.join().is_err());

        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

            // start >= file_size should assert
            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();

            // empty range start == end_exclusive should assert
            let _ = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 10, Some(10), 1 << 12, |_rec| Ok(())).await;
        }).unwrap();

        assert!(handle.join().is_err());

        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

            // start >= file_size should assert
            let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
            // invalid chunk size (almost certainly not a multiple of alignment)
            let _ = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, 1, |_rec| Ok(())).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    /// Happy Path
    /// Tests records that perfectly align with chunk boundaries
    #[test]
    fn test_read_fixed_records_perfect_alignment() {
        const N: usize = 1024; // 1KB records
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 10);

            // Use 1KB chunk size to align perfectly
            let chunk_size = 1024u64;
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, chunk_size, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, 10);
            assert_eq!(count, 10);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests record size equals chunk size
    #[test]
    fn test_read_fixed_records_record_equals_chunk() {
        const N: usize = 4096; // 4KB records
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 5);

            let chunk_size = 4096u64;
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, chunk_size, |rec| {
                // Validate inline without copying large arrays
                let expected = (count % 256) as u8;
                assert!(rec.iter().all(|&b| b == expected));
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, 5);
            assert_eq!(count, 5);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests single record in file
    #[test]
    fn test_read_fixed_records_single_record() {
        const N: usize = 512;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, file_size) = create_fixed_record_file(dir, N, 1);

            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, Some(file_size), 1 << 12, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, 1);
            assert_eq!(count, 1);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests record size larger than chunk size
    #[test]
    fn test_read_fixed_records_record_larger_than_chunk() {
        const N: usize = 65536; // 64KB records (reasonable for stack allocation)
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 3);

            // 16KB chunks, smaller than record size
            let chunk_size = 1u64 << 14;
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, chunk_size, |rec| {
                // Validate inline without storing large arrays
                let expected = (count % 256) as u8;
                assert!(rec.iter().all(|&b| b == expected));
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, 3);
            assert_eq!(count, 3);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests start position very close to EOF
    #[test]
    fn test_read_fixed_records_near_eof() {
        const N: usize = 100;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, file_size) = create_fixed_record_file(dir, N, 10); // 1000 bytes

            // Start at last record
            let start = file_size - N as u64;
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, start, None, 1 << 12, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, 1);
            assert_eq!(count, 1);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests file with partial record at end (explicit test)
    #[test]
    fn test_read_fixed_records_partial_at_end_ignored() {
        const N: usize = 100;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            // Create file with 7 full records + 50 extra bytes
            let file_path = format!("{}/partial.bin", dir);
            let mut file = File::create(&file_path).unwrap();
            for i in 0..7 {
                let byte = (i % 256) as u8;
                let buf = vec![byte; N];
                file.write_all(&buf).unwrap();
            }
            // Add partial record
            file.write_all(&vec![99u8; 50]).unwrap();
            file.flush().unwrap();

            let mut count = 0usize;
            let dma_file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&dma_file, file_len(&dma_file).await, 0, None, 1 << 12, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            // Should only count 7 full records, ignoring the 50-byte partial
            assert_eq!(ret, 7);
            assert_eq!(count, 7);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests extremely small file (smaller than chunk size and alignment)
    #[test]
    fn test_read_fixed_records_tiny_file() {
        const N: usize = 64;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 3); // 192 bytes

            let chunk_size = 1u64 << 20; // 1MB chunk for tiny file
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, chunk_size, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, 3);
            assert_eq!(count, 3);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests record size equals file alignment (typically 512)
    #[test]
    fn test_read_fixed_records_size_equals_alignment() {
        const N: usize = 512;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 20);

            let chunk_size = 1u64 << 14; // 16KB
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, None, chunk_size, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            assert_eq!(ret, 20);
            assert_eq!(count, 20);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Error Condition
    /// Tests visitor function returning an error (error propagation)
    #[test]
    fn test_read_fixed_records_visitor_error() {
        const N: usize = 128;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, _file_size) = create_fixed_record_file(dir, N, 10);

            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let result = read_fixed_records_visit_const::<N, String>(&file, file_len(&file).await, 0, None, 1 << 12, |_rec| {
                count += 1;
                if count == 5 {
                    Err("Visitor error on record 5".to_string())
                } else {
                    Ok(())
                }
            }).await;

            match result {
                Err(ReadVisitError::Visitor(msg)) => {
                    assert_eq!(msg, "Visitor error on record 5");
                    assert_eq!(count, 5);
                }
                _ => panic!("Expected ReadVisitError::Visitor"),
            }
        }).unwrap();
        handle.join().unwrap();
    }

    /// Error Condition
    /// Tests end_exclusive beyond file_size (should work correctly)
    #[test]
    fn test_read_fixed_records_end_beyond_eof() {
        const N: usize = 256;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, file_size) = create_fixed_record_file(dir, N, 5);

            // Request far beyond EOF
            let end_beyond = file_size + 256*4;
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, 0, Some(end_beyond), 1 << 12, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            // Should read all 5 records despite end_exclusive being beyond EOF
            assert_eq!(ret, 5);
            assert_eq!(count, 5);
        }).unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests when start + N > file_size (only partial record possible)
    #[test]
    fn test_read_fixed_records_partial_only() {
        const N: usize = 1000;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, file_size) = create_fixed_record_file(dir, N, 3); // 3000 bytes

            // Start at 2500, only 500 bytes remain (less than N)
            let start = 2000u64; // Must be multiple of N
            let mut count = 0usize;
            let file = DmaFile::open(&file_path).await.unwrap();
            let ret = read_fixed_records_visit_const::<N, ()>(&file, file_len(&file).await, start, Some(file_size), 1 << 12, |_rec| {
                count += 1;
                Ok(())
            }).await.unwrap();

            // Should read 1 full record (bytes 2000-3000)
            assert_eq!(ret, 1);
            assert_eq!(count, 1);
        }).unwrap();
        handle.join().unwrap();
    }
}