use glommio::io::DmaFile;
use std::{path::Path, time::Duration};

pub struct ObjectPosition {
    pub start_pos: u64,
    pub can_skip: bool,
}

pub struct AbsoluteObjectPosition {
    pub start_pos: u64,
    pub end_pos: u64,
}

pub async fn read_objects_absolute<P: AsRef<Path>>(
    path: P,
    object_positions: &Vec<AbsoluteObjectPosition>,
    max_chunk_size: u64,
) -> glommio::Result<Vec<Vec<u8>>, ()> {
    // We assume `object_positions` are ordered by start_pos and non-overlapping.
    let file = DmaFile::open(path).await?;
    let alignment = file.alignment();
    assert!(
        (max_chunk_size as u64) >= alignment && (max_chunk_size as u64) % alignment == 0,
        "max_chunk_size must be a multiple of the device alignment ({alignment})"
    );

    if object_positions.is_empty() {
        return Ok(Vec::new());
    }

    let file_size = file.file_size().await?;
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


pub async fn read_objects_skippable<P: AsRef<Path>>(
    path: P,
    start_positions: &Vec<ObjectPosition>,
    max_chunk_size: u64,
) -> glommio::Result<Vec<Vec<u8>>, ()> {
    let file = DmaFile::open(path).await?;
    let alignment = file.alignment();
    assert!(
        (max_chunk_size as u64) >= alignment && (max_chunk_size as u64) % alignment == 0,
        "max_chunk_size must be a multiple of the device alignment ({alignment})"
    );

    let file_size = file.file_size().await?;
    if start_positions.is_empty() {
        return Ok(Vec::new());
    }

    let non_skipped_count = start_positions.iter().filter(|p| !p.can_skip).count();
    let mut objects = Vec::with_capacity(non_skipped_count);

    let mut object_idx = 0usize;
    let mut object_start = start_positions[object_idx].start_pos;
    let mut object_end = if object_idx + 1 < start_positions.len() {
        start_positions[object_idx + 1].start_pos
    } else {
        file_size
    };
    let mut object_can_skip = start_positions[object_idx].can_skip;

    let mut object_buf: Vec<u8> = Vec::new();

    let mut chunk_start = object_start - (object_start % alignment);

    while chunk_start < file_size && object_idx < start_positions.len() {
        let chunk_end = std::cmp::min(chunk_start + max_chunk_size as u64, file_size);
        let read_len = (chunk_end - chunk_start) as usize;
        let chunk = file.read_at(chunk_start, read_len).await?;

        let mut pos_in_chunk = if object_start > chunk_start {
            (object_start - chunk_start) as usize
        } else {
            0
        };

        while chunk_start + (pos_in_chunk as u64) < chunk_end && object_idx < start_positions.len() {
            let cur_pos_abs = chunk_start + pos_in_chunk as u64;
            let object_remaining = object_end - cur_pos_abs;
            let chunk_remaining = chunk_end - cur_pos_abs;
            let take_len = std::cmp::min(object_remaining, chunk_remaining) as usize;

            if !object_can_skip {
                object_buf.extend_from_slice(&chunk[pos_in_chunk..pos_in_chunk + take_len]);
            }
            pos_in_chunk += take_len;

            // Completed current object
            if chunk_start + pos_in_chunk as u64 >= object_end {
                if !object_can_skip {
                    objects.push(std::mem::take(&mut object_buf));
                }
                object_idx += 1;
                if object_idx < start_positions.len() {
                    object_start = start_positions[object_idx].start_pos;
                    object_end = if object_idx + 1 < start_positions.len() {
                        start_positions[object_idx + 1].start_pos
                    } else {
                        file_size
                    };
                    object_can_skip = start_positions[object_idx].can_skip;
                }
            }
        }

        chunk_start += max_chunk_size as u64;
        chunk_start -= chunk_start % alignment; // ensure alignment
    }

    Ok(objects)
}

/// Reads objects from a file given their start positions.
/// Reads in 1MB aligned chunks, splits objects, and carries over bytes if needed.
pub async fn read_objects<P: AsRef<Path>>(
    path: P,
    start_positions: &Vec<u64>,
    max_chunk_size: u64,
) -> glommio::Result<Vec<Vec<u8>>, ()> {
    let file = DmaFile::open(path).await?;
    let alignment = file.alignment();
    assert!(
        (max_chunk_size as u64) >= alignment && (max_chunk_size as u64) % alignment == 0,
        "max_chunk_size must be a multiple of the device alignment ({alignment})"
    );

    let file_size = file.file_size().await?;

    let mut objects = Vec::with_capacity(start_positions.len());
    let mut object_idx = 0;
    let mut object_start = start_positions[object_idx];
    let mut object_end = if object_idx + 1 < start_positions.len() {
        start_positions[object_idx + 1]
    } else {
        file_size
    };
    let mut object_buf = Vec::new();

    let mut chunk_start = object_start - (object_start % alignment);

    while chunk_start < file_size && object_idx < start_positions.len() {
        let chunk_end = std::cmp::min(chunk_start + max_chunk_size as u64, file_size);
        let read_len = (chunk_end - chunk_start) as usize;
        let chunk = file.read_at(chunk_start, read_len).await?;

        let mut pos_in_chunk = if object_start > chunk_start {
            (object_start - chunk_start) as usize
        } else {
            0
        };

        while chunk_start + (pos_in_chunk as u64) < chunk_end && object_idx < start_positions.len() {
            let object_remaining = object_end - (chunk_start + pos_in_chunk as u64);
            let chunk_remaining = chunk_end - (chunk_start + pos_in_chunk as u64);
            let take_len = std::cmp::min(object_remaining, chunk_remaining) as usize;

            object_buf.extend_from_slice(
                &chunk[pos_in_chunk..pos_in_chunk + take_len]
            );
            pos_in_chunk += take_len;

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
                }
            }
        }
        chunk_start += max_chunk_size as u64;
        chunk_start = chunk_start - (chunk_start % alignment); // ensure alignment
    }

    Ok(objects)
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
    fn test_read_objects_across_chunks_skippable() {
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

            let mut skipped_test_start_positions: Vec<ObjectPosition> = 
                start_positions.iter().map(|&x| ObjectPosition{can_skip: false, start_pos:x}).collect();
            skipped_test_start_positions[1].can_skip = true;            
            skipped_test_start_positions[2].can_skip = true;            
            skipped_test_start_positions[4].can_skip = true;

            for chunk_size in different_chunk_sizes().iter().enumerate() {
                let objects = read_objects_skippable(&file_path, &skipped_test_start_positions, *chunk_size.1).await.unwrap();

                assert_eq!(objects.len(), 2);
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

}