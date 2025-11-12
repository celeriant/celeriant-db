use std::path::Path;

use glommio::{
    GlommioError,
    io::{DmaFile, OpenOptions},
};

pub async fn get_existing_file_as_dma<P: AsRef<Path>>(
    path: P,
) -> Result<DmaFile, GlommioError<()>> {
    let dma_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .append(false)
        .dma_open(path)
        .await?;

    Ok(dma_file)
}
