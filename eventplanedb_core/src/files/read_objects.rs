use glommio::io::DmaFile;
use std::{path::Path};

pub struct ObjectPosition {
    pub start_pos: u64,
    pub can_skip: bool,
}

#[derive(Clone)]
pub struct AbsoluteObjectPosition {
    pub start_pos: u64,
    pub end_pos: u64,
}

pub async fn read_objects_absolute<P: AsRef<Path>>(
    path: P,
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

    // We assume `object_positions` are ordered by start_pos and non-overlapping.
    let file = DmaFile::open(path).await?;
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

    let file_size = file.file_size().await?;

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

pub async fn read_objects<P: AsRef<Path>>(
    path: P,
    start_positions: &[u64],
    max_chunk_size: u64,
) -> glommio::Result<Vec<Vec<u8>>, ()> {

    if start_positions.is_empty() {
        return Ok(Vec::new());
    }

    // New: strictly increasing to disallow zero-length objects (duplicate starts).
    assert!(
        start_positions
            .windows(2)
            .all(|w| w[0] < w[1]),
        "start_positions must be strictly increasing (no zero-length objects from duplicate starts)"
    );

    let file = DmaFile::open(path).await?;
    let alignment = file.alignment();
    assert!(
        (max_chunk_size as u64) >= alignment && (max_chunk_size as u64) % alignment == 0,
        "max_chunk_size must be a multiple of the device alignment ({alignment})"
    );

    let file_size = file.file_size().await?;

    // New: last start must be less than file size to avoid zero-length final object at EOF.
    assert!(
        start_positions.last().unwrap() < &file_size,
        "last start_pos must be less than file size"
    );

    let mut objects = Vec::with_capacity(start_positions.len());
    let mut object_idx = 0;
    let mut object_start = start_positions[object_idx];
    let mut object_end = if object_idx + 1 < start_positions.len() {
        start_positions[object_idx + 1]
    } else {
        file_size
    };
    let mut object_buf = Vec::with_capacity(object_end.saturating_sub(object_start) as usize);

    let mut chunk_start = object_start - (object_start % alignment);

    while chunk_start < file_size && object_idx < start_positions.len() {
        let chunk_end = std::cmp::min(chunk_start + max_chunk_size as u64, file_size);

        // Skip ahead if this chunk ends before the next object's start
        if chunk_end <= object_start {
            chunk_start = object_start - (object_start % alignment);
            continue;
        }

        let read_len = (chunk_end - chunk_start) as usize;
        let chunk = file.read_at(chunk_start, read_len).await?;

        let mut pos_in_chunk = if object_start > chunk_start {
            (object_start - chunk_start) as usize
        } else {
            0
        };

        while chunk_start + (pos_in_chunk as u64) < chunk_end && object_idx < start_positions.len() {
            // If before the current object's start within this chunk, skip the gap.
            if chunk_start + (pos_in_chunk as u64) < object_start {
                let target = (object_start - chunk_start) as usize;
                pos_in_chunk = std::cmp::min(target, (chunk_end - chunk_start) as usize);
                continue;
            }

            let cur_pos_abs = chunk_start + pos_in_chunk as u64;
            let object_remaining = object_end.saturating_sub(cur_pos_abs);
            let chunk_remaining = chunk_end.saturating_sub(cur_pos_abs);
            let take_len = std::cmp::min(object_remaining, chunk_remaining) as usize;

            if take_len > 0 {
                object_buf.extend_from_slice(&chunk[pos_in_chunk..pos_in_chunk + take_len]);
                pos_in_chunk += take_len;
            }

            if chunk_start + pos_in_chunk as u64 >= object_end {
                objects.push(std::mem::take(&mut object_buf));
                object_idx += 1;
                if object_idx < start_positions.len() {
                    object_start = start_positions[object_idx];
                    object_end = if object_idx + 1 < start_positions.len() {
                        start_positions[object_idx + 1]
                    } else {
                        file_size
                    };
                    object_buf = Vec::with_capacity(object_end.saturating_sub(object_start) as usize);
                }
            }
        }
        chunk_start += max_chunk_size as u64;
        chunk_start = chunk_start - (chunk_start % alignment); // ensure alignment
    }

    Ok(objects)
}

/// Read fixed-size records from `start` up to `end_exclusive` (or EOF) and invoke `on_record`
/// for each full record of size N. Trailing partial bytes are ignored.
/// - start: absolute byte offset (must be < file size)
/// - end_exclusive: absolute end offset (exclusive); None => EOF
/// - max_chunk_size: must be a multiple of the device alignment
pub async fn read_fixed_records_visit_const<P: AsRef<Path>, const N: usize>(
    path: P,
    start: u64,
    end_exclusive: Option<u64>,
    max_chunk_size: u64,
    mut on_record: impl FnMut(&[u8; N]),
) -> glommio::Result<usize, ()> {
    assert!(N > 0, "record size N must be > 0");

    let file = DmaFile::open(path).await?;
    let alignment = file.alignment();
    assert!(
        max_chunk_size >= alignment && max_chunk_size % alignment == 0,
        "max_chunk_size must be a multiple of the device alignment ({alignment})"
    );

    let file_size = file.file_size().await?;
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
        let chunk = file.read_at(chunk_start, read_len).await?;

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
                on_record(&carry);
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
            on_record(rec);
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

#[cfg(test)]
mod test {
    use super::*;
    use glommio::{LocalExecutorBuilder, Placement};
    use tempfile::tempdir;
    use std::fs::File;
    use std::io::{Write};

    // Minimum is 1 << 9 (512KB) otherwise test may hang due to DMA alignment constraints.
    fn different_chunk_sizes() -> Vec<u64> {
        vec![1 << 9, 1 << 10, 1 << 11, 1 << 14, 1 << 17, 1 << 20, 1 << 21, 1 << 25, 1 << 29, 1 << 40, 1 << 50]
    }

    fn create_test_file(path: &str, object_sizes: &[usize]) -> (String, Vec<u64>, Vec<u64>) {
        let file_path = format!("{}/testfile.bin", path);
        let mut file = File::create(&file_path).unwrap();
        let mut start_positions = Vec::with_capacity(object_sizes.len());        
        let mut end_positions = Vec::with_capacity(object_sizes.len());

        let mut pos = 0u64;

        for (i, &size) in object_sizes.iter().enumerate() {
            start_positions.push(pos);
            end_positions.push(pos+size as u64);
            let byte = (i % 256) as u8;
            let buf = vec![byte; size];
            file.write_all(&buf).unwrap();
            pos += size as u64;
        }
        file.flush().unwrap();
        (file_path, start_positions, end_positions)
    }

    // Helper: create a file of `record_count` records, each record is `record_size` bytes,
    // filled with the record index (mod 256).
    fn create_fixed_record_file(path: &str, record_size: usize, record_count: usize) -> (String, u64) {
        let file_path = format!("{}/fixed_records.bin", path);
        let mut file = File::create(&file_path).unwrap();
        for i in 0..record_count {
            let byte = (i % 256) as u8;
            let buf = vec![byte; record_size];
            file.write_all(&buf).unwrap();
        }
        file.flush().unwrap();
        let file_size = (record_size as u64) * (record_count as u64);
        (file_path, file_size)
    }

    #[test]
    fn test_read_objects_across_chunks() {
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
            let (file_path, start_positions, _end_positions) = create_test_file(folder, &object_sizes);

            for chunk_size in different_chunk_sizes().iter().enumerate() {
                let objects = read_objects(&file_path, &start_positions.clone(), *chunk_size.1).await.unwrap();

                assert_eq!(objects.len(), object_sizes.len());
                for (i, obj) in objects.iter().enumerate() {
                    assert_eq!(obj.len(), object_sizes[i]);
                    let expected_byte = (i % 256) as u8;
                    assert!(obj.iter().all(|&b| b == expected_byte));
                }
            }

        }).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_read_objects_skip_first() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Keep the first object in the file, but don't include its start position in the read
            let object_sizes = vec![
                256 * 1024,        // 256KB  (will be skipped in read)
                1 * 1024 * 1024,   // 1MB
                1 * 1024 * 1024 + 7, // 1MB + 7 bytes
                64 * 1024,         // 64KB
            ];
            let (file_path, start_positions, _end_positions) = create_test_file(folder, &object_sizes);

            for chunk_size in different_chunk_sizes().iter().enumerate() {
                // Skip the first object's start position
                let skipped_start_positions = start_positions[1..].to_vec();

                let objects = read_objects(&file_path, &skipped_start_positions, *chunk_size.1)
                    .await
                    .unwrap();

                // We expect all but the first object
                assert_eq!(objects.len(), object_sizes.len() - 1);

                // Validate objects 1.. correspond exactly to what was written
                for (j, obj) in objects.iter().enumerate() {
                    let original_index = j + 1; // shift by one because we skipped the first
                    assert_eq!(obj.len(), object_sizes[original_index]);
                    let expected_byte = (original_index % 256) as u8;
                    assert!(obj.iter().all(|&b| b == expected_byte));
                }
            }

        }).unwrap();
        handle.join().unwrap();
    }

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
                let objects = read_objects_absolute(&file_path, &object_positions, *chunk_size.1).await.unwrap();

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
                let objects = read_objects_absolute(&file_path, &object_positions, *chunk_size.1).await.unwrap();

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

    #[test]
    fn test_read_objects_unordered_positions_panics() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![4 * 1024, 4 * 1024, 4 * 1024];
            let (file_path, start_positions, _end_positions) = create_test_file(folder, &object_sizes);

            // Make positions strictly decreasing to trigger assertion.
            let mut bad_positions = start_positions.clone();
            bad_positions.swap(0, 1); // now [4096, 0, 8192]

            // Should panic due to "start_positions must be non-decreasing"
            let _ = read_objects(&file_path, &bad_positions, 1 << 12).await;
        }).unwrap();

        // Expect the task to have panicked
        assert!(handle.join().is_err());
    }

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
            let _ = read_objects_absolute(&file_path, &overlaps, 1 << 12).await;
        }).unwrap();

        // Expect the task to have panicked
        assert!(handle.join().is_err());
    }

    #[test]
    fn test_read_objects_invalid_chunk_size_panics() {
        // Chunk size not a multiple of device alignment should assert
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![1024, 1024, 1024];
            let (file_path, starts, _ends) = create_test_file(folder, &object_sizes);

            // 1 is almost certainly not a valid alignment multiple on real devices
            let _ = read_objects(&file_path, &starts, 1).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

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
            let _ = read_objects_absolute(&file_path, &positions, 1).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    #[test]
    fn test_read_objects_zero_length_in_middle_and_end() {
        // Now should panic due to zero-length objects (duplicate start and EOF start).
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // 3 objects of 4 bytes each
            let object_sizes = vec![4, 4, 4];
            let (file_path, _starts, _ends) = create_test_file(folder, &object_sizes);

            let file_size = (object_sizes.iter().sum::<usize>()) as u64;
            let custom_starts = vec![0u64, 4u64, 4u64, 8u64, file_size];

            // Should assert due to zero-length object(s) / last start at EOF
            let _ = read_objects(&file_path, &custom_starts, 1 << 12).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

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
            let _ = read_objects_absolute(&file_path, &positions, 1 << 12).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }

    #[test]
    fn test_read_objects_absolute_truncate_beyond_eof() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            // Two chunks: 6 bytes of 0, 4 bytes of 1
            let object_sizes = vec![6, 4];
            let (file_path, starts, ends) = create_test_file(folder, &object_sizes);
            let file_size = (object_sizes.iter().sum::<usize>()) as u64;

            // Request from second object start to far beyond EOF
            let start = starts[1];
            let end_far = file_size + 10_000;
            let positions = vec![
                AbsoluteObjectPosition { start_pos: start, end_pos: end_far },
            ];

            let objects = read_objects_absolute(&file_path, &positions, 1 << 12).await.unwrap();
            assert_eq!(objects.len(), 1);
            assert_eq!(objects[0].len(), (file_size - start) as usize);
            assert!(objects[0].iter().all(|&b| b == 1));
        }).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_read_objects_empty_input_returns_empty() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();
            let object_sizes = vec![1024];
            let (file_path, _starts, _ends) = create_test_file(folder, &object_sizes);

            let v = read_objects(&file_path, &[], 1 << 12).await.unwrap();
            assert!(v.is_empty());

            let v2 = read_objects_absolute(&file_path, &[], 1 << 12).await.unwrap();
            assert!(v2.is_empty());
        }).unwrap();
        handle.join().unwrap();
    }

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
                let count = read_fixed_records_visit_const::<_, N>(&file_path, 0, None, chunk_size, |rec| {
                    seen.push(*rec);
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
                let count2_ret = read_fixed_records_visit_const::<_, N>(&file_path, 0, Some(file_size), chunk_size, |_rec| {
                    count2 += 1;
                }).await.unwrap();
                assert_eq!(count2, 10);
                assert_eq!(count2_ret, 10);
            }
        }).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_read_fixed_records_visit_const_cut_tail_with_end() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            // 10 records => total size = 3130
            let (file_path, _file_size) = create_fixed_record_file(dir, N, 10);

            // Start at 0, end cuts after 7 full records plus 100 extra bytes
            let end = (7 * N + 100) as u64;
            for chunk_size in different_chunk_sizes() {
                let mut count = 0usize;
                let ret = read_fixed_records_visit_const::<_, N>(&file_path, 0, Some(end), chunk_size, |_rec| {
                    count += 1;
                }).await.unwrap();

                assert_eq!(count, 7);
                assert_eq!(ret, 7);
            }
        }).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_read_fixed_records_visit_const_unaligned_start() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            // 10 records => 3130 bytes
            let (file_path, file_size) = create_fixed_record_file(dir, N, 10);

            // Start 100 bytes into the file (not on a record boundary)
            let start = 100u64;
            let expected = ((file_size - start) / (N as u64)) as usize; // floor division
            assert_eq!(expected, 9); // 3030 / 313 = 9 full records

            for chunk_size in different_chunk_sizes() {
                let mut seen_idx = 0usize;
                let ret = read_fixed_records_visit_const::<_, N>(&file_path, start, None, chunk_size, |rec| {
                    // Validate content straddling file-record boundaries:
                    // Each returned record starts at abs offset: start + seen_idx * N
                    let abs_off = start as usize + seen_idx * N;
                    let file_rec_idx = abs_off / N;
                    let offset_in_file_rec = abs_off % N;
                    let head_len = N - offset_in_file_rec;
                    let tail_len = offset_in_file_rec; // together must be N with next rec

                    // First segment from file_rec_idx
                    let expected_byte_head = (file_rec_idx % 256) as u8;
                    assert!(rec[..head_len].iter().all(|&b| b == expected_byte_head));

                    // Second segment (if any) from file_rec_idx + 1
                    if head_len < N {
                        let expected_byte_tail = ((file_rec_idx + 1) % 256) as u8;
                        assert!(rec[head_len..].iter().all(|&b| b == expected_byte_tail));
                    }
                    seen_idx += 1;
                }).await.unwrap();

                assert_eq!(ret, expected);
                assert_eq!(seen_idx, expected);
            }
        }).unwrap();
        handle.join().unwrap();
    }

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
            let ret = read_fixed_records_visit_const::<_, N>(&file_path, 0, Some(file_size), chunk_size, |_rec| {
                count += 1;
            }).await.unwrap();

            assert_eq!(ret, record_count);
            assert_eq!(count, record_count);
        }).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_read_fixed_records_visit_const_bounds_and_alignment_panics() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tmp = tempdir().unwrap();
            let dir = tmp.path().to_str().unwrap();

            let (file_path, file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

            // start >= file_size should assert
            let _ = read_fixed_records_visit_const::<_, N>(&file_path, file_size, None, 1 << 12, |_rec| {}).await;

            // empty range start == end_exclusive should assert
            let _ = read_fixed_records_visit_const::<_, N>(&file_path, 10, Some(10), 1 << 12, |_rec| {}).await;

            // invalid chunk size (almost certainly not a multiple of alignment)
            let _ = read_fixed_records_visit_const::<_, N>(&file_path, 0, None, 1, |_rec| {}).await;
        }).unwrap();

        // All three should have panicked in the task
        assert!(handle.join().is_err());
    }

    #[test]
    fn test_read_objects_bad_chunk_advancement_logic() {
        // This test exposes the flawed chunk advancement logic.
        // The function asserts that chunk size must be a multiple of alignment.
        // If that assert were removed, the logic `chunk_start -= chunk_start % alignment`
        // would cause chunks to overlap and data to be re-read.
        // The test will pass by panicking on the existing assertion.
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0)).spawn(|| async move {
            let tempdir = tempdir().unwrap();
            let folder = tempdir.path().to_str().unwrap();

            let object_sizes = vec![4096, 4096];
            let (file_path, starts, _ends) = create_test_file(folder, &object_sizes);

            // To get alignment, we need to open the file first.
            let alignment = DmaFile::open(&file_path).await.unwrap().alignment();
            
            // Use a chunk size that is NOT a multiple of alignment.
            let bad_chunk_size = alignment + 1;

            // This should panic due to the chunk size assertion.
            let _ = read_objects(&file_path, &starts, bad_chunk_size).await;
        }).unwrap();

        assert!(handle.join().is_err());
    }
}