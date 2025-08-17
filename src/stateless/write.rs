use std::{
    fs::File,
    io::{self, BufWriter, Write},
};

use crate::structures::event_batch_item::EventBatchItem;

/// Writes an event batch item to a binary stream
///
/// # Arguments
/// * `writer` - Any writable destination (BufWriter<File>, Vec<u8>, Cursor<Vec<u8>>, etc.)
/// * `event_batch_item` - The event batch item to serialize and write
///
/// # Returns
/// * `usize` - The size of the compressed event_batch_item data in bytes
pub fn write<W: Write>(writer: &mut W, event_batch_item: &EventBatchItem) -> io::Result<usize> {
    let total_size = 9999;
    let mut write_buffer = Vec::with_capacity(total_size);

    //TODO: Implement

    writer.write_all(&write_buffer)?;

    // Force write to the disk to ensure we don't lose data if the program crashes
    writer.flush()?;

    Ok(999)
}
