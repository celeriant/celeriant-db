use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

/// Truncate the end of the file at a specific position (potentially due to corruption)
pub fn trim_end(writer: &mut BufWriter<File>, position: u64) -> io::Result<()> {
    writer.flush()?;
    writer.get_ref().set_len(position)?;
    writer.seek(SeekFrom::Start(position))?;
    Ok(())
}

/// Truncate the start of the file at a specific position (typically after those chunks have been moved to object storage)
pub fn trim_start(
    reader: &mut BufReader<File>,
    file_path: &str,
    keep_from_pos: u64,
) -> io::Result<()> {
    // If we're keeping everything from the beginning, nothing to trim
    if keep_from_pos == 0 {
        return Ok(());
    }

    // Get the original file path
    // You'll need to store this or pass it as a parameter
    let temp_path = format!("{file_path}.tmp");

    // Create temporary file and copy the data we want to keep
    {
        let mut temp_file = BufWriter::new(File::create(&temp_path)?);
        reader.seek(SeekFrom::Start(keep_from_pos))?;
        io::copy(reader, &mut temp_file)?;
        temp_file.flush()?;
    } // temp_file is dropped here

    // Replace original file with temp file
    fs::rename(&temp_path, file_path)?;

    Ok(())
}

/// Delete the file entirely from disk
pub fn delete<P: AsRef<Path>>(file_path: P) -> io::Result<()> {
    fs::remove_file(file_path)
}
