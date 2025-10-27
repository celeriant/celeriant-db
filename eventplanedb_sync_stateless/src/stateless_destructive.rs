use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::stateless_engine::StatelessEngine;

pub trait StatelessDestructive {
    /// Truncate the end of the file at a specific position (potentially due to corruption)
    fn trim_end(
        &self,
        event_batch_writer: &mut File,
        event_batch_trim_position: u64,
        metadata_writer: &mut File,
        metadata_trim_position: u64,
    ) -> io::Result<()>;

    /// Truncate the start of the file at a specific position (typically after those chunks have been moved to object storage)
    fn trim_start<R: Read + Seek>(
        &self,
        event_batch_reader: &mut R,
        event_batch_keep_from_position: u64,
        event_batch_file_path: &str,
        metadata_reader: &mut R,
        metadata_keep_from_position: u64,
        metadata_file_path: &str,
    ) -> io::Result<()>;

    /// Delete the event batch and metadata files permanently
    fn delete<P: AsRef<Path>>(
        &self,
        event_batch_file_path: P,
        metadata_file_path: P,
    ) -> io::Result<()>;
}

impl StatelessDestructive for StatelessEngine {
    fn trim_end(
        &self,
        event_batch_writer: &mut File,
        event_batch_trim_position: u64,
        metadata_writer: &mut File,
        metadata_trim_position: u64,
    ) -> io::Result<()> {
        // Trim event batch file
        event_batch_writer.flush()?;
        event_batch_writer.set_len(event_batch_trim_position)?;
        event_batch_writer.seek(SeekFrom::Start(event_batch_trim_position))?;

        // Trim metadata file
        metadata_writer.flush()?;
        metadata_writer.set_len(metadata_trim_position)?;
        metadata_writer.seek(SeekFrom::Start(metadata_trim_position))?;

        Ok(())
    }

    fn trim_start<R: Read + Seek>(
        &self,
        event_batch_reader: &mut R,
        event_batch_keep_from_position: u64,
        event_batch_file_path: &str,
        metadata_reader: &mut R,
        metadata_keep_from_position: u64,
        metadata_file_path: &str,
    ) -> io::Result<()> {
        if event_batch_keep_from_position == 0 || metadata_keep_from_position == 0 {
            //Raise an io error as this is going to result in an empty file
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot trim to position 0",
            ));
        }

        // Trim event batch file start
        let temp_path_event_batches = format!("{}.tmp", event_batch_file_path);

        {
            let mut temp_file = File::create(&temp_path_event_batches)?;
            event_batch_reader.seek(SeekFrom::Start(event_batch_keep_from_position))?;
            io::copy(event_batch_reader, &mut temp_file)?;
            temp_file.flush()?;
        }

        // Trim metadata file start
        let temp_path_metadata = format!("{}.tmp", metadata_file_path);

        {
            let mut temp_file = File::create(&temp_path_metadata)?;
            metadata_reader.seek(SeekFrom::Start(metadata_keep_from_position))?;
            io::copy(metadata_reader, &mut temp_file)?;
            temp_file.flush()?;
        }

        // Commit by renaming over the existing files
        fs::rename(&temp_path_event_batches, event_batch_file_path)?;
        fs::rename(&temp_path_metadata, metadata_file_path)?;

        Ok(())
    }

    fn delete<P: AsRef<Path>>(
        &self,
        event_batch_file_path: P,
        metadata_file_path: P,
    ) -> io::Result<()> {
        // Ensure files exist before attempting to delete
        if !event_batch_file_path.as_ref().exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Event batch file not found: {}",
                    event_batch_file_path.as_ref().display()
                ),
            ));
        }

        if !metadata_file_path.as_ref().exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "Metadata file not found: {}",
                    metadata_file_path.as_ref().display()
                ),
            ));
        }

        // Delete event batch file
        fs::remove_file(event_batch_file_path)?;

        // Delete metadata file
        fs::remove_file(metadata_file_path)?;

        Ok(())
    }
}
