use glommio::{GlommioError, io::DmaFile};

#[derive(Debug)]
pub enum ReadVisitError<E> {
    Io(GlommioError<()>),
    Visitor(E),
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

    use crate::files::read_objects_absolute::test::{
        create_fixed_record_file, different_chunk_sizes, file_len,
    };

    use super::*;
    use glommio::{LocalExecutorBuilder, Placement, io::DmaFile};

    use tempfile::tempdir;

    use std::{fs::File, io::Write};

    /// Happy Path
    /// Tests reading 10 records of 313 bytes each from start to end
    /// Validates record count and content with multiple chunk sizes
    /// Tests both None (EOF) and explicit end_exclusive parameters
    #[test]
    fn test_read_fixed_records_visit_const_basic() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                // 10 records of 313 bytes, record i filled with byte i
                let (file_path, file_size) = create_fixed_record_file(dir, N, 10);

                for chunk_size in different_chunk_sizes() {
                    // Collect records to validate content
                    let mut seen: Vec<[u8; N]> = Vec::new();
                    let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                    let count = read_fixed_records_visit_const::<N, ()>(
                        &file,
                        file_len(&file).await,
                        0,
                        None,
                        chunk_size,
                        |rec| {
                            seen.push(*rec);
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();

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
                    let count2_ret = read_fixed_records_visit_const::<N, ()>(
                        &file,
                        file_len(&file).await,
                        0,
                        Some(file_size),
                        chunk_size,
                        |_rec| {
                            count2 += 1;
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
                    assert_eq!(count2, 10);
                    assert_eq!(count2_ret, 10);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests reading 10 records of 31332 bytes each from start to end
    /// Validates record count and content with multiple chunk sizes
    /// Tests both None (EOF) and explicit end_exclusive parameters
    #[test]
    fn test_read_fixed_records_visit_const_large_fixed() {
        const N: usize = 31332;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                // 10 records of 31332 bytes, record i filled with byte i
                let (file_path, file_size) = create_fixed_record_file(dir, N, 7);

                for chunk_size in different_chunk_sizes() {
                    // Collect records to validate content
                    let mut seen: Vec<[u8; N]> = Vec::new();
                    let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                    let count = read_fixed_records_visit_const::<N, ()>(
                        &file,
                        file_len(&file).await,
                        0,
                        None,
                        chunk_size,
                        |rec| {
                            seen.push(*rec);
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();

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
                    let count2_ret = read_fixed_records_visit_const::<N, ()>(
                        &file,
                        file_len(&file).await,
                        0,
                        Some(file_size),
                        chunk_size,
                        |_rec| {
                            count2 += 1;
                            Ok(())
                        },
                    )
                    .await
                    .unwrap();
                    assert_eq!(count2, 7);
                    assert_eq!(count2_ret, 7);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests that partial records at the end are ignored (7 full records + 100 extra bytes)
    /// Verifies correct handling of truncated data at boundary
    #[test]
    fn test_read_fixed_records_unaligned_end() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    313,
                    Some(600),
                    1 << 12,
                    |_rec| Ok(()),
                )
                .await;
            })
            .unwrap();

        assert!(handle.join().is_err());
    }

    /// Error Condition
    /// Tests starting from unaligned offset (100 bytes into file)
    /// Validates records straddling original file record boundaries are correctly assembled
    /// Tests content validation across boundary spans
    #[test]
    fn test_read_fixed_records_unaligned_start() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    100,
                    None,
                    1 << 12,
                    |_rec| Ok(()),
                )
                .await;
            })
            .unwrap();

        assert!(handle.join().is_err());
    }

    /// Happy Path (Stress Test)
    /// Tests 20,000 records (~6.26 MB) to force multiple chunk reads
    /// Validates carry mechanism across many chunk boundaries
    #[test]
    fn test_read_fixed_records_visit_const_large_multichunk() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
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
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    Some(file_size),
                    chunk_size,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, record_count);
                assert_eq!(count, record_count);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Error Condition
    /// Tests three panic scenarios: start >= file_size, empty range (start == end), invalid chunk size
    #[test]
    fn test_read_fixed_records_visit_const_bounds_and_alignment_panics() {
        const N: usize = 313;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

                // start >= file_size should assert
                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    file_size,
                    None,
                    1 << 12,
                    |_rec| Ok(()),
                )
                .await;
            })
            .unwrap();

        assert!(handle.join().is_err());

        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

                // start >= file_size should assert
                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();

                // empty range start == end_exclusive should assert
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    10,
                    Some(10),
                    1 << 12,
                    |_rec| Ok(()),
                )
                .await;
            })
            .unwrap();

        assert!(handle.join().is_err());

        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 2); // 626 bytes

                // start >= file_size should assert
                let file: DmaFile = DmaFile::open(&file_path).await.unwrap();
                // invalid chunk size (almost certainly not a multiple of alignment)
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    None,
                    1,
                    |_rec| Ok(()),
                )
                .await;
            })
            .unwrap();

        assert!(handle.join().is_err());
    }

    /// Happy Path
    /// Tests records that perfectly align with chunk boundaries
    #[test]
    fn test_read_fixed_records_perfect_alignment() {
        const N: usize = 1024; // 1KB records
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 10);

                // Use 1KB chunk size to align perfectly
                let chunk_size = 1024u64;
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    None,
                    chunk_size,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, 10);
                assert_eq!(count, 10);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests record size equals chunk size
    #[test]
    fn test_read_fixed_records_record_equals_chunk() {
        const N: usize = 4096; // 4KB records
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 5);

                let chunk_size = 4096u64;
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    None,
                    chunk_size,
                    |rec| {
                        // Validate inline without copying large arrays
                        let expected = (count % 256) as u8;
                        assert!(rec.iter().all(|&b| b == expected));
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, 5);
                assert_eq!(count, 5);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Happy Path
    /// Tests single record in file
    #[test]
    fn test_read_fixed_records_single_record() {
        const N: usize = 512;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, file_size) = create_fixed_record_file(dir, N, 1);

                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    Some(file_size),
                    1 << 12,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, 1);
                assert_eq!(count, 1);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests record size larger than chunk size
    #[test]
    fn test_read_fixed_records_record_larger_than_chunk() {
        const N: usize = 65536; // 64KB records (reasonable for stack allocation)
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 3);

                // 16KB chunks, smaller than record size
                let chunk_size = 1u64 << 14;
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    None,
                    chunk_size,
                    |rec| {
                        // Validate inline without storing large arrays
                        let expected = (count % 256) as u8;
                        assert!(rec.iter().all(|&b| b == expected));
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, 3);
                assert_eq!(count, 3);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests start position very close to EOF
    #[test]
    fn test_read_fixed_records_near_eof() {
        const N: usize = 100;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, file_size) = create_fixed_record_file(dir, N, 10); // 1000 bytes

                // Start at last record
                let start = file_size - N as u64;
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    start,
                    None,
                    1 << 12,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, 1);
                assert_eq!(count, 1);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests file with partial record at end (explicit test)
    #[test]
    fn test_read_fixed_records_partial_at_end_ignored() {
        const N: usize = 100;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
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
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &dma_file,
                    file_len(&dma_file).await,
                    0,
                    None,
                    1 << 12,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                // Should only count 7 full records, ignoring the 50-byte partial
                assert_eq!(ret, 7);
                assert_eq!(count, 7);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests extremely small file (smaller than chunk size and alignment)
    #[test]
    fn test_read_fixed_records_tiny_file() {
        const N: usize = 64;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 3); // 192 bytes

                let chunk_size = 1u64 << 20; // 1MB chunk for tiny file
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    None,
                    chunk_size,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, 3);
                assert_eq!(count, 3);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests record size equals file alignment (typically 512)
    #[test]
    fn test_read_fixed_records_size_equals_alignment() {
        const N: usize = 512;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 20);

                let chunk_size = 1u64 << 14; // 16KB
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    None,
                    chunk_size,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                assert_eq!(ret, 20);
                assert_eq!(count, 20);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Error Condition
    /// Tests visitor function returning an error (error propagation)
    #[test]
    fn test_read_fixed_records_visitor_error() {
        const N: usize = 128;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, _file_size) = create_fixed_record_file(dir, N, 10);

                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let result = read_fixed_records_visit_const::<N, String>(
                    &file,
                    file_len(&file).await,
                    0,
                    None,
                    1 << 12,
                    |_rec| {
                        count += 1;
                        if count == 5 {
                            Err("Visitor error on record 5".to_string())
                        } else {
                            Ok(())
                        }
                    },
                )
                .await;

                match result {
                    Err(ReadVisitError::Visitor(msg)) => {
                        assert_eq!(msg, "Visitor error on record 5");
                        assert_eq!(count, 5);
                    }
                    _ => panic!("Expected ReadVisitError::Visitor"),
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Error Condition
    /// Tests end_exclusive beyond file_size (should work correctly)
    #[test]
    fn test_read_fixed_records_end_beyond_eof() {
        const N: usize = 256;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, file_size) = create_fixed_record_file(dir, N, 5);

                // Request far beyond EOF
                let end_beyond = file_size + 256 * 4;
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    0,
                    Some(end_beyond),
                    1 << 12,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                // Should read all 5 records despite end_exclusive being beyond EOF
                assert_eq!(ret, 5);
                assert_eq!(count, 5);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Edge Case
    /// Tests when start + N > file_size (only partial record possible)
    #[test]
    fn test_read_fixed_records_partial_only() {
        const N: usize = 1000;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();

                let (file_path, file_size) = create_fixed_record_file(dir, N, 3); // 3000 bytes

                // Start at 2500, only 500 bytes remain (less than N)
                let start = 2000u64; // Must be multiple of N
                let mut count = 0usize;
                let file = DmaFile::open(&file_path).await.unwrap();
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file,
                    file_len(&file).await,
                    start,
                    Some(file_size),
                    1 << 12,
                    |_rec| {
                        count += 1;
                        Ok(())
                    },
                )
                .await
                .unwrap();

                // Should read 1 full record (bytes 2000-3000)
                assert_eq!(ret, 1);
                assert_eq!(count, 1);
            })
            .unwrap();
        handle.join().unwrap();
    }
}
