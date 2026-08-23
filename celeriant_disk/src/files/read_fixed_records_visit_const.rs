use glommio::{GlommioError, io::DmaFile};

#[derive(Debug)]
pub enum ReadVisitError<E> {
    Io(GlommioError<()>),
    ShortRead { pos: u64, requested: usize, got: usize },
    Visitor(E),
}


/// Read fixed-size records from a file, invoking a visitor for each record.
/// Requirements:
/// - N >= alignment and N % alignment == 0
/// - chunk_size >= N and chunk_size % N == 0
/// - start % N == 0
/// 
/// This eliminates the need for carry buffers since chunk boundaries
/// always align with record boundaries.
pub async fn read_fixed_records_visit_const<const N: usize, E>(
    file: &DmaFile,
    move_in_reverse: bool,
    start: u64,
    end: u64,
    chunk_size: u64,
    on_record: impl FnMut(u64, &[u8; N]) -> Result<bool, E>,
) -> Result<usize, ReadVisitError<E>> {
    assert!(N > 0, "record size N must be > 0");

    let alignment = file.alignment() as u64;
    assert!(
        N as u64 >= alignment && N as u64 % alignment == 0,
        "record size N ({N}) must be >= alignment and a multiple of alignment ({alignment})"
    );
    assert!(
        chunk_size >= N as u64 && chunk_size % N as u64 == 0,
        "chunk_size ({chunk_size}) must be >= N and a multiple of record size ({N})"
    );
    assert!(
        start % N as u64 == 0,
        "start must be a multiple of the record size ({N})"
    );
    assert!(start < end, "empty range: start must be < end");

    if move_in_reverse {
        read_reverse_aligned::<N, E>(file, start, end, chunk_size, on_record).await
    } else {
        read_forward_aligned::<N, E>(file, start, end, chunk_size, on_record).await
    }
}

async fn read_forward_aligned<const N: usize, E>(
    file: &DmaFile,
    start: u64,
    end: u64,
    chunk_size: u64,
    mut on_record: impl FnMut(u64, &[u8; N]) -> Result<bool, E>,
) -> Result<usize, ReadVisitError<E>> {
    let n = N as u64;
    let valid_end = start + ((end - start) / n) * n;

    let mut records = 0usize;
    let mut pos = start;

    while pos < valid_end {
        let read_end = std::cmp::min(pos + chunk_size, valid_end);
        let read_len = (read_end - pos) as usize;

        let chunk = match file.read_at(pos, read_len).await {
            Ok(c) => c,
            Err(e) => return Err(ReadVisitError::Io(e)),
        };
        if chunk.len() < read_len {
            return Err(ReadVisitError::ShortRead { pos, requested: read_len, got: chunk.len() });
        }

        let (full, _) = chunk.as_chunks::<N>();
        for (i, rec) in full.iter().enumerate() {
            let record_pos = pos + (i as u64 * n);
            match on_record(record_pos, rec) {
                Ok(true) => {
                    records += 1;
                    return Ok(records); // early exit requested
                }
                Ok(false) => {
                    records += 1;
                }
                Err(ev) => return Err(ReadVisitError::Visitor(ev)),
            }
        }

        pos = read_end;
    }

    Ok(records)
}

async fn read_reverse_aligned<const N: usize, E>(
    file: &DmaFile,
    start: u64,
    end: u64,
    chunk_size: u64,
    mut on_record: impl FnMut(u64, &[u8; N]) -> Result<bool, E>,
) -> Result<usize, ReadVisitError<E>> {
    let n = N as u64;
    let valid_end = start + ((end - start) / n) * n;

    if valid_end <= start {
        return Ok(0);
    }

    let mut records = 0usize;
    let mut pos = valid_end;

    while pos > start {
        let read_start = std::cmp::max(start, pos.saturating_sub(chunk_size));
        let read_len = (pos - read_start) as usize;

        let chunk = match file.read_at(read_start, read_len).await {
            Ok(c) => c,
            Err(e) => return Err(ReadVisitError::Io(e)),
        };
        if chunk.len() < read_len {
            return Err(ReadVisitError::ShortRead { pos: read_start, requested: read_len, got: chunk.len() });
        }

        let (full, _) = chunk.as_chunks::<N>();
        for (i, rec) in full.iter().enumerate().rev() {
            let record_pos = read_start + (i as u64 * n);
            match on_record(record_pos, rec) {
                Ok(true) => {
                    records += 1;
                    return Ok(records); // early exit requested
                }
                Ok(false) => {
                    records += 1;
                }
                Err(ev) => return Err(ReadVisitError::Visitor(ev)),
            }
        }

        pos = read_start;
    }

    Ok(records)
}

#[cfg(test)]
mod test_aligned {
    use super::*;
    use crate::files::read_objects_absolute::test::{create_fixed_record_file, file_len};
    use glommio::{LocalExecutorBuilder, Placement, io::DmaFile};
    use std::{fs::File, io::Write};
    use tempfile::tempdir;

    /// Chunk sizes that are multiples of common record sizes (512, 1024, 4096)
    fn aligned_chunk_sizes() -> Vec<u64> {
        vec![1024, 4096, 16384, 65536, 262144]
    }

    /// Forward: read 10 records, validate count and content
    #[test]
    fn test_aligned_forward_basic() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 10);

                for chunk_size in aligned_chunk_sizes() {
                    let mut seen: Vec<u8> = Vec::new();
                    let file = DmaFile::open(&file_path).await.unwrap();
                    let count = read_fixed_records_visit_const::<N, ()>(
                        &file,
                        false,
                        0,
                        file_len(&file).await,
                        chunk_size,
                        |_pos, rec| {
                            assert!(rec.iter().all(|&b| b == rec[0]));
                            seen.push(rec[0]);
                            Ok(false)
                        },
                    )
                    .await
                    .unwrap();

                    assert_eq!(count, 10);
                    assert_eq!(seen, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Reverse: read 10 records in reverse order
    #[test]
    fn test_aligned_reverse_basic() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 10);

                for chunk_size in aligned_chunk_sizes() {
                    let mut seen: Vec<u8> = Vec::new();
                    let file = DmaFile::open(&file_path).await.unwrap();
                    let count = read_fixed_records_visit_const::<N, ()>(
                        &file,
                        true,
                        0,
                        file_len(&file).await,
                        chunk_size,
                        |_pos, rec| {
                            seen.push(rec[0]);
                            Ok(false)
                        },
                    )
                    .await
                    .unwrap();

                    assert_eq!(count, 10);
                    assert_eq!(seen, vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Forward and reverse produce same records (reversed)
    #[test]
    fn test_aligned_forward_reverse_equivalence() {
        const N: usize = 512;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 100);

                for chunk_size in aligned_chunk_sizes() {
                    let file = DmaFile::open(&file_path).await.unwrap();
                    let end = file_len(&file).await;

                    let mut forward: Vec<u8> = Vec::new();
                    read_fixed_records_visit_const::<N, ()>(
                        &file, false, 0, end, chunk_size,
                        |_pos, rec| { forward.push(rec[0]); Ok(false) },
                    ).await.unwrap();

                    let mut reverse: Vec<u8> = Vec::new();
                    read_fixed_records_visit_const::<N, ()>(
                        &file, true, 0, end, chunk_size,
                        |_pos, rec| { reverse.push(rec[0]); Ok(false) },
                    ).await.unwrap();

                    reverse.reverse();
                    assert_eq!(forward, reverse);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Stress test with many records
    #[test]
    fn test_aligned_stress() {
        const N: usize = 4096;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let record_count = 5_000usize;
                let (file_path, _) = create_fixed_record_file(dir, N, record_count);

                let file = DmaFile::open(&file_path).await.unwrap();
                let mut count = 0;
                let ret = read_fixed_records_visit_const::<N, ()>(
                    &file, false, 0, file_len(&file).await, 65536,
                    |_,_| { count += 1; Ok(false) },
                ).await.unwrap();

                assert_eq!(ret, record_count);
                assert_eq!(count, record_count);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Single record
    #[test]
    fn test_aligned_single_record() {
        const N: usize = 4096;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 1);

                let file = DmaFile::open(&file_path).await.unwrap();
                
                for reverse in [false, true] {
                    let mut count = 0;
                    read_fixed_records_visit_const::<N, ()>(
                        &file, reverse, 0, file_len(&file).await, 4096,
                        |_pos, rec| { assert!(rec.iter().all(|&b| b == 0)); count += 1; Ok(false) },
                    ).await.unwrap();
                    assert_eq!(count, 1);
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Partial record at end is ignored
    #[test]
    fn test_aligned_partial_ignored() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let file_path = format!("{}/partial.bin", dir);

                let mut file = File::create(&file_path).unwrap();
                for i in 0..5 {
                    file.write_all(&vec![(i % 256) as u8; N]).unwrap();
                }
                file.write_all(&vec![99u8; 500]).unwrap(); // partial
                file.flush().unwrap();

                let dma = DmaFile::open(&file_path).await.unwrap();

                let mut fwd: Vec<u8> = Vec::new();
                read_fixed_records_visit_const::<N, ()>(
                    &dma, false, 0, file_len(&dma).await, 4096,
                    |_pos, rec| { fwd.push(rec[0]); Ok(false) },
                ).await.unwrap();
                assert_eq!(fwd, vec![0, 1, 2, 3, 4]);

                let mut rev: Vec<u8> = Vec::new();
                read_fixed_records_visit_const::<N, ()>(
                    &dma, true, 0, file_len(&dma).await, 4096,
                    |_pos, rec| { rev.push(rec[0]); Ok(false) },
                ).await.unwrap();
                assert_eq!(rev, vec![4, 3, 2, 1, 0]);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Mid-file start position
    #[test]
    fn test_aligned_mid_start() {
        const N: usize = 512;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 10);

                let file = DmaFile::open(&file_path).await.unwrap();
                let start = (5 * N) as u64;

                let mut fwd: Vec<u8> = Vec::new();
                read_fixed_records_visit_const::<N, ()>(
                    &file, false, start, file_len(&file).await, 4096,
                    |_pos, rec| { fwd.push(rec[0]); Ok(false) },
                ).await.unwrap();
                assert_eq!(fwd, vec![5, 6, 7, 8, 9]);

                let mut rev: Vec<u8> = Vec::new();
                read_fixed_records_visit_const::<N, ()>(
                    &file, true, start, file_len(&file).await, 4096,
                    |_pos, rec| { rev.push(rec[0]); Ok(false) },
                ).await.unwrap();
                assert_eq!(rev, vec![9, 8, 7, 6, 5]);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Visitor error propagation
    #[test]
    fn test_aligned_visitor_error() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 10);

                let file = DmaFile::open(&file_path).await.unwrap();
                let mut count = 0;
                let result = read_fixed_records_visit_const::<N, &str>(
                    &file, false, 0, file_len(&file).await, 4096,
                    |_,_| {
                        count += 1;
                        if count == 3 { Err("stop") } else { Ok(false) }
                    },
                ).await;

                assert!(matches!(result, Err(ReadVisitError::Visitor("stop"))));
                assert_eq!(count, 3);
            })
            .unwrap();
        handle.join().unwrap();
    }

    /// Panic: chunk_size not multiple of N
    #[test]
    fn test_aligned_panic_chunk_not_multiple() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 2);

                let file = DmaFile::open(&file_path).await.unwrap();
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file, false, 0, file_len(&file).await,
                    1500, // not multiple of 1024
                    |_,_| Ok(false),
                ).await;
            })
            .unwrap();
        assert!(handle.join().is_err());
    }

    /// Panic: start not multiple of N
    #[test]
    fn test_aligned_panic_start_unaligned() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 2);

                let file = DmaFile::open(&file_path).await.unwrap();
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file, false, 100, file_len(&file).await, 4096,
                    |_,_| Ok(false),
                ).await;
            })
            .unwrap();
        assert!(handle.join().is_err());
    }

    /// Panic: empty range
    #[test]
    fn test_aligned_panic_empty_range() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 2);

                let file = DmaFile::open(&file_path).await.unwrap();
                let _ = read_fixed_records_visit_const::<N, ()>(
                    &file, false, 1024, 1024, 4096,
                    |_,_| Ok(false),
                ).await;
            })
            .unwrap();
        assert!(handle.join().is_err());
    }

    #[test]
    fn test_aligned_early_exit() {
        const N: usize = 1024;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 10);

                let file = DmaFile::open(&file_path).await.unwrap();
                
                // Forward: stop after 5 records
                let mut seen_fwd: Vec<u8> = Vec::new();
                let count = read_fixed_records_visit_const::<N, ()>(
                    &file, false, 0, file_len(&file).await, 4096,
                    |_pos, rec| {
                        seen_fwd.push(rec[0]);
                        Ok(seen_fwd.len() >= 5) // return true to stop
                    },
                ).await.unwrap();
                assert_eq!(count, 5);
                assert_eq!(seen_fwd, vec![0, 1, 2, 3, 4]);

                // Reverse: stop after 3 records
                let mut seen_rev: Vec<u8> = Vec::new();
                let count = read_fixed_records_visit_const::<N, ()>(
                    &file, true, 0, file_len(&file).await, 4096,
                    |_pos, rec| {
                        seen_rev.push(rec[0]);
                        Ok(seen_rev.len() >= 3)
                    },
                ).await.unwrap();
                assert_eq!(count, 3);
                assert_eq!(seen_rev, vec![9, 8, 7]);
            })
            .unwrap();
        handle.join().unwrap();
    }
    
    /// Verify pos is correct in callback, including with initial offset
    #[test]
    fn test_aligned_pos_correctness() {
        const N: usize = 512;
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tmp = tempdir().unwrap();
                let dir = tmp.path().to_str().unwrap();
                let (file_path, _) = create_fixed_record_file(dir, N, 20);

                let file = DmaFile::open(&file_path).await.unwrap();
                let start_offset = (7 * N) as u64; // start at record 7
                let end = file_len(&file).await;

                // Forward: positions should be start_offset, start_offset + N, ...
                let mut positions_fwd: Vec<u64> = Vec::new();
                read_fixed_records_visit_const::<N, ()>(
                    &file, false, start_offset, end, 4096,
                    |pos, rec| {
                        // Verify pos matches expected record value
                        let expected_record_idx = (pos / N as u64) as u8;
                        assert_eq!(rec[0], expected_record_idx, "record content mismatch at pos {}", pos);
                        positions_fwd.push(pos);
                        Ok(false)
                    },
                ).await.unwrap();

                let expected_fwd: Vec<u64> = (7..20).map(|i| (i * N) as u64).collect();
                assert_eq!(positions_fwd, expected_fwd);

                // Reverse: positions should be (19*N), (18*N), ..., start_offset
                let mut positions_rev: Vec<u64> = Vec::new();
                read_fixed_records_visit_const::<N, ()>(
                    &file, true, start_offset, end, 4096,
                    |pos, rec| {
                        let expected_record_idx = (pos / N as u64) as u8;
                        assert_eq!(rec[0], expected_record_idx, "record content mismatch at pos {}", pos);
                        positions_rev.push(pos);
                        Ok(false)
                    },
                ).await.unwrap();

                let expected_rev: Vec<u64> = (7..20).rev().map(|i| (i * N) as u64).collect();
                assert_eq!(positions_rev, expected_rev);
            })
            .unwrap();
        handle.join().unwrap();
    }
}