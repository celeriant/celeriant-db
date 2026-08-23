//! Black-box contract tests for [`read_fixed_records_visit_const`].
//!
//! A read that lands inside `[start, end)` but past the physical end of the
//! file must surface as `ReadVisitError::ShortRead`, never as a silent `Ok`
//! with fewer records visited.

use crate::files::read_fixed_records_visit_const::{
    ReadVisitError, read_fixed_records_visit_const,
};
use glommio::{LocalExecutorBuilder, Placement, io::DmaFile};
use std::{fs::File, io::Write};
use tempfile::tempdir;

macro_rules! glommio_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}

/// 4096 is a valid record size on every device alignment we support.
const N: usize = 4096;

/// Writes `records` records of `N` bytes (record `i` filled with `i as u8`)
/// followed by `trailing` bytes of 0xff. Returns the path and physical length.
fn write_file(dir: &str, records: usize, trailing: usize) -> (String, u64) {
    let path = format!("{dir}/short_read.bin");
    let mut file = File::create(&path).unwrap();
    for i in 0..records {
        file.write_all(&[(i % 256) as u8; N]).unwrap();
    }
    if trailing > 0 {
        file.write_all(&vec![0xffu8; trailing]).unwrap();
    }
    file.flush().unwrap();
    (path, (records * N + trailing) as u64)
}

fn expect_short_read<E>(result: Result<usize, ReadVisitError<E>>) -> (u64, usize, usize) {
    match result {
        Err(ReadVisitError::ShortRead { pos, requested, got }) => (pos, requested, got),
        Err(ReadVisitError::Io(_)) => panic!("expected ShortRead, got Io"),
        Err(ReadVisitError::Visitor(_)) => panic!("expected ShortRead, got Visitor"),
        Ok(n) => panic!("expected ShortRead, got Ok({n})"),
    }
}

fn expect_ok<E>(result: Result<usize, ReadVisitError<E>>) -> usize {
    match result {
        Ok(n) => n,
        Err(ReadVisitError::ShortRead { pos, requested, got }) => {
            panic!("expected Ok, got ShortRead {{ pos: {pos}, requested: {requested}, got: {got} }}")
        }
        Err(ReadVisitError::Io(_)) => panic!("expected Ok, got Io"),
        Err(ReadVisitError::Visitor(_)) => panic!("expected Ok, got Visitor"),
    }
}

/// Case 1 (forward): end one full record past EOF, one chunk spanning the whole
/// range. Must error, and nothing at or past the physical end may be visited.
#[test]
fn forward_end_one_record_past_eof_returns_short_read() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 0);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            false,
            0,
            5 * N as u64,
            8 * N as u64,
            |pos, _record| {
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        expect_short_read(result);
        assert!(
            visited.iter().all(|&pos| pos < physical_len),
            "visited a record at or past physical EOF {physical_len}: {visited:?}"
        );
        file.close().await.unwrap();
    });
}

/// Case 2 (forward): end many records past EOF with a chunk smaller than the
/// gap. The physically present records are visited, then the read past EOF
/// errors.
#[test]
fn forward_end_many_records_past_eof_visits_present_records_then_errors() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, _) = write_file(tempdir.path().to_str().unwrap(), 4, 0);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            false,
            0,
            12 * N as u64,
            2 * N as u64,
            |pos, record| {
                let expected = (pos / N as u64) as u8;
                assert!(
                    record.iter().all(|&b| b == expected),
                    "record at {pos} does not hold its index byte {expected}"
                );
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        expect_short_read(result);
        let expected: Vec<u64> = (0..4).map(|i| i * N as u64).collect();
        assert_eq!(visited, expected, "forward must visit every present record");
        file.close().await.unwrap();
    });
}

/// Case 3 (reverse): the first reverse chunk read starts past EOF, so it comes
/// back short.
#[test]
fn reverse_end_past_eof_returns_short_read() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 0);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            true,
            0,
            6 * N as u64,
            2 * N as u64,
            |pos, _record| {
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        expect_short_read(result);
        assert!(
            visited.iter().all(|&pos| pos < physical_len),
            "visited a record at or past physical EOF {physical_len}: {visited:?}"
        );
        file.close().await.unwrap();
    });
}

/// Case 3b (reverse): end one record past EOF with a chunk spanning the whole
/// range.
#[test]
fn reverse_end_one_record_past_eof_returns_short_read() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 0);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            true,
            0,
            5 * N as u64,
            8 * N as u64,
            |pos, _record| {
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        expect_short_read(result);
        assert!(
            visited.iter().all(|&pos| pos < physical_len),
            "visited a record at or past physical EOF {physical_len}: {visited:?}"
        );
        file.close().await.unwrap();
    });
}

/// Case 4 (forward control): end exactly at physical EOF stays `Ok`.
#[test]
fn forward_end_at_physical_eof_reads_every_record() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 0);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            false,
            0,
            physical_len,
            2 * N as u64,
            |pos, _record| {
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        assert_eq!(expect_ok(result), 4);
        let expected: Vec<u64> = (0..4).map(|i| i * N as u64).collect();
        assert_eq!(visited, expected);
        file.close().await.unwrap();
    });
}

/// Case 4 (reverse control).
#[test]
fn reverse_end_at_physical_eof_reads_every_record() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 0);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            true,
            0,
            physical_len,
            2 * N as u64,
            |pos, _record| {
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        assert_eq!(expect_ok(result), 4);
        let mut expected: Vec<u64> = (0..4).map(|i| i * N as u64).collect();
        expected.reverse();
        assert_eq!(visited, expected);
        file.close().await.unwrap();
    });
}

/// Case 5 (forward control): a trailing partial record inside the range is
/// trimmed before reading, not reported as a short read.
#[test]
fn forward_trailing_partial_record_is_trimmed_without_error() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 500);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            false,
            0,
            physical_len,
            2 * N as u64,
            |pos, _record| {
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        assert_eq!(expect_ok(result), 4);
        let expected: Vec<u64> = (0..4).map(|i| i * N as u64).collect();
        assert_eq!(visited, expected);
        file.close().await.unwrap();
    });
}

/// Case 5 (reverse control).
#[test]
fn reverse_trailing_partial_record_is_trimmed_without_error() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 500);
        let file = DmaFile::open(&path).await.unwrap();

        let mut visited = Vec::new();
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            true,
            0,
            physical_len,
            2 * N as u64,
            |pos, _record| {
                visited.push(pos);
                Ok(false)
            },
        )
        .await;

        assert_eq!(expect_ok(result), 4);
        let mut expected: Vec<u64> = (0..4).map(|i| i * N as u64).collect();
        expected.reverse();
        assert_eq!(visited, expected);
        file.close().await.unwrap();
    });
}

/// Case 6: the reported fields describe the read that actually came short.
#[test]
fn short_read_fields_describe_the_failing_read() {
    glommio_test!({
        let tempdir = tempdir().unwrap();
        let (path, physical_len) = write_file(tempdir.path().to_str().unwrap(), 4, 0);
        let file = DmaFile::open(&path).await.unwrap();

        let end = 5 * N as u64;
        let result = read_fixed_records_visit_const::<N, ()>(
            &file,
            false,
            0,
            end,
            8 * N as u64,
            |_pos, _record| Ok(false),
        )
        .await;

        let (pos, requested, got) = expect_short_read(result);
        assert!(got < requested, "got {got} must be short of requested {requested}");
        assert!(pos < end, "pos {pos} must lie inside the range ending at {end}");
        assert!(
            pos + requested as u64 > physical_len,
            "read at {pos} for {requested} bytes must reach past physical length {physical_len}"
        );
        file.close().await.unwrap();
    });
}
