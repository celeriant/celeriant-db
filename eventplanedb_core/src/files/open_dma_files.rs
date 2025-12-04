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
        .read(true)
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