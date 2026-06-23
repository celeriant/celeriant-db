//! Send-safe client frame I/O over `celeriant_wire`.
//!
//! A tokio client connection compresses request bodies with the cluster's zstd dictionary. The
//! compression dictionary is held as raw bytes (`&[u8]`, which is `Send + Sync`) and digested per
//! call via the stateless [`compress_with_dict`]/[`decompress_with_dict`] helpers — NOT as a
//! precompiled `DictCodec`, which is `RefCell`-backed (`!Sync`) and would make a request future
//! `!Send` if held across an `.await`, and which (measured) does not improve end-to-end throughput
//! on a server-bound write path while costing one resident codec per connection.
//!
//! The codec is only touched in the **synchronous** [`build_frame`]/[`decompress`] calls; the
//! **async** [`write_frame`]/[`read_frame`] move only bytes across `.await`. Both the storage and
//! queue tokio clients drive the same four functions.

use std::borrow::Cow;

use celeriant_wal::compression_type::CompressionType;
use celeriant_wire::codec::compression::{compress_with_dict, decompress_with_dict};
use celeriant_wire::network::wire_error::WireError;
use celeriant_wire::network::wire_header::{WireHeader, wire_header_write_variable_size_raw};
use futures_lite::{AsyncReadExt, AsyncWriteExt};

/// zstd level for dictionary compression. Affects compressed size only, not decompressibility;
/// mirrors the server default.
const COMPRESSION_LEVEL: i32 = 3;

/// A serialized, possibly-compressed request frame ready to write. Built synchronously by
/// [`build_frame`] so the dict bytes never reach an `.await`.
pub struct OutFrame {
    type_id: u32,
    compression: CompressionType,
    uncompressed_size: u32,
    body: Vec<u8>,
}

/// Compress an already-serialized body with the cluster `dict` when `compress` is set and a dict is
/// present, else pass it through uncompressed (e.g. a client that has not yet negotiated a dict).
/// Synchronous: the `&[u8]` dict borrow ends here.
pub fn build_frame(
    type_id: u32,
    uncompressed: Vec<u8>,
    dict: Option<&[u8]>,
    compress: bool,
) -> Result<OutFrame, WireError> {
    let uncompressed_size = uncompressed.len() as u32;
    let (compression, body) = match (compress, dict) {
        (true, Some(dict)) => (
            CompressionType::ZstdDict,
            compress_with_dict(&uncompressed, COMPRESSION_LEVEL, dict)?,
        ),
        _ => (CompressionType::None, uncompressed),
    };
    Ok(OutFrame { type_id, compression, uncompressed_size, body })
}

/// Write a prepared frame. Dict-free, so the returned future is `Send`.
pub async fn write_frame<W>(
    writer: &mut W,
    frame: &OutFrame,
    max_size_bytes: u64,
    version: u32,
) -> Result<(), WireError>
where
    W: AsyncWriteExt + Unpin,
{
    wire_header_write_variable_size_raw(
        writer,
        &frame.body,
        frame.type_id,
        frame.compression,
        frame.uncompressed_size,
        max_size_bytes,
        version,
    )
    .await
}

/// A frame read off the wire: its message type plus the raw (still-compressed) body. Produced by
/// [`read_frame`]; decompress with [`decompress`].
pub struct InFrame {
    pub message_type: u32,
    compression: CompressionType,
    uncompressed_size: u32,
    body: Vec<u8>,
}

/// Read one frame's header and raw body. Dict-free, so the returned future is `Send`.
pub async fn read_frame<R>(reader: &mut R, max_size_bytes: u64) -> Result<InFrame, WireError>
where
    R: AsyncReadExt + Unpin,
{
    let header = WireHeader::from_reader(reader, max_size_bytes).await?;
    let body = header.read_variable_body_raw(reader).await?;
    Ok(InFrame {
        message_type: header.message_type,
        compression: header.compression_type,
        uncompressed_size: header.uncompressed_length,
        body,
    })
}

/// Decompress a frame's body with the cluster `dict`. Synchronous, and zero-copy when the frame is
/// uncompressed (borrows the body in place).
pub fn decompress<'a>(frame: &'a InFrame, dict: Option<&[u8]>) -> Result<Cow<'a, [u8]>, WireError> {
    decompress_body(frame.compression, frame.uncompressed_size, &frame.body, dict)
}

/// Decompress a body from its header parts — for callers (e.g. the storage client) that read the
/// header separately to dispatch fixed- vs variable-size frames before reading the body. Synchronous,
/// zero-copy when uncompressed. A `ZstdDict` body with no dict is a malformed frame.
pub fn decompress_body<'a>(
    compression: CompressionType,
    uncompressed_size: u32,
    body: &'a [u8],
    dict: Option<&[u8]>,
) -> Result<Cow<'a, [u8]>, WireError> {
    match (compression, dict) {
        (CompressionType::None, _) => Ok(Cow::Borrowed(body)),
        (CompressionType::ZstdDict, Some(dict)) => {
            Ok(Cow::Owned(decompress_with_dict(body, uncompressed_size as usize, dict)?))
        }
        (CompressionType::ZstdDict, None) => Err(WireError::MalformedFrame(
            "ZstdDict frame but the connection has no compression dictionary".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_wire::network::wire_header::PROTOCOL_VERSION_V2;
    use futures_lite::future::block_on;
    use futures_lite::io::Cursor;

    const MAX: u64 = 1 << 20;

    fn dict() -> Vec<u8> {
        let mut d = vec![0u8; 14 * 1024];
        for (i, b) in d.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        d
    }

    /// Mirrors a real client: holds the dict bytes, compresses/decompresses synchronously, and only
    /// awaits dict-free I/O.
    struct Conn {
        dict: Vec<u8>,
    }

    impl Conn {
        async fn round_trip(&mut self, type_id: u32, body: Vec<u8>, compress: bool) -> Result<Vec<u8>, WireError> {
            let frame = build_frame(type_id, body, Some(&self.dict), compress)?;
            let mut wire = Vec::new();
            write_frame(&mut wire, &frame, MAX, PROTOCOL_VERSION_V2).await?;
            let mut reader = Cursor::new(wire);
            let inframe = read_frame(&mut reader, MAX).await?;
            assert_eq!(inframe.message_type, type_id);
            Ok(decompress(&inframe, Some(&self.dict))?.into_owned())
        }
    }

    /// The request future is `Send` (compiles iff so — the regression guard) AND the payload
    /// survives compress → write → read → decompress unchanged.
    #[test]
    fn request_future_is_send_and_round_trips_compressed() {
        fn assert_send<T: Send>(_: &T) {}
        let mut conn = Conn { dict: dict() };
        let payload = vec![7u8; 4096];
        let fut = conn.round_trip(3, payload.clone(), true);
        assert_send(&fut);
        assert_eq!(block_on(fut).unwrap(), payload);
    }

    /// Uncompressed frames round-trip too (the zero-copy `decompress` borrow path).
    #[test]
    fn uncompressed_round_trips() {
        let mut conn = Conn { dict: dict() };
        let payload = b"small control verb".to_vec();
        assert_eq!(block_on(conn.round_trip(8, payload.clone(), false)).unwrap(), payload);
    }
}
