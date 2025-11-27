use std::path::Path;

use glommio::{GlommioError, io::{DmaFile}};

pub async fn read_only_dma<P: AsRef<Path>>(
    path: P,
) -> Result<DmaFile, GlommioError<()>> {
    DmaFile::open(path).await
}

pub async fn write_only_dma<P: AsRef<Path>>(
    path: P,
) -> Result<DmaFile, GlommioError<()>> {
    DmaFile::create(path).await
}