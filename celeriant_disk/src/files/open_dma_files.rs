use std::path::Path;

use glommio::{GlommioError, io::{DmaFile, OpenOptions}};

pub async fn existing_file_read_only_dma<P: AsRef<Path>>(
    path: P,
) -> Result<DmaFile, GlommioError<()>> {
    OpenOptions::new()
        .read(true)
        .dma_open(path.as_ref())
        .await
}

pub async fn create_and_write_only_dma<P: AsRef<Path>>(
    path: P,
) -> Result<DmaFile, GlommioError<()>> {
    let file = OpenOptions::new()
        .read(false) // See test below, will fail if set to true
        .write(true)
        .create_new(true)
        .truncate(true)
        .dma_open(path.as_ref())
        .await?;

    file.close().await?;

    OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .truncate(false)
        .dma_open(path.as_ref())
        .await
}

pub async fn existing_file_write_only_dma<P: AsRef<Path>>(
    path: P,
) -> Result<DmaFile, GlommioError<()>> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .truncate(false)
        .dma_open(path.as_ref())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use glommio::{LocalExecutorBuilder, Placement};
    use tempfile::tempdir;

    /// Tests that creating a new file with read(true) and create_new(true) fails in glommio
    /// This is why create_and_write_only_dma uses the close-and-reopen workaround
    #[test]
    fn test_create_and_read() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                let tempdir = tempdir().unwrap();
                let file_path = tempdir.path().join("test_file.bin");

                let file = create_and_write_only_dma(&file_path).await.unwrap();

                // Write 512 bytes of 0xAB
                let mut write_buf = file.alloc_dma_buffer(512);
                write_buf.as_bytes_mut().fill(0xAB);
                file.write_at(write_buf, 0).await.unwrap();
                file.fdatasync().await.unwrap();

                // Try to read it back
                let file2 = existing_file_read_only_dma(&file_path).await.unwrap();
                let read_buf = file2.read_at_aligned(0, 512).await.unwrap();
                assert!(
                    read_buf.iter().all(|&b| b == 0xAB),
                    "Expected workaround to allow correct read-back"
                );
            })
            .unwrap();
        handle.join().unwrap();
    }
}