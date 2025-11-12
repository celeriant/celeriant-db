use std::rc::Rc;
use lru::LruCache;
use eventplanedb_structures::aggregate_key::AggregateKey;
use glommio::{GlommioError, sync::RwLock};

use crate::files::{read_operations::{AggregateReadConfig, ReadError, ReadOperations}, write_operations::{AggregateWriteConfig, WriteOperations}};


impl AggregateResources {
    pub fn new(writer: Rc<RwLock<WriteOperations>>) -> Self {
        Self {
            writer,
            reader: None,
        }
    }
}

pub async fn get_or_create_writer(
    aggregate_key: &AggregateKey,
    data_root_folder: &str,
    create_if_not_exists: bool,
    aggregates_cache: &RwLock<LruCache<AggregateKey, AggregateResources>>,
    aggregate_read_config: &AggregateReadConfig,
    aggregate_write_config: &AggregateWriteConfig,
) -> Result<Rc<RwLock<WriteOperations>>, ReadError> {
    // Check if writer already exists
    {
        let mut cache = aggregates_cache.write().await?;
        if let Some(resources) = cache.get(aggregate_key) {
            return Ok(resources.writer.clone());
        }
    }

    // Need to create writer - first get or create reader to access DmaFiles
    let reader_ref = get_or_create_reader_internal(
        aggregate_key, 
        data_root_folder, 
        create_if_not_exists, 
        aggregate_read_config
    ).await?;

    let data_requirements = {
        let reader_ref = reader_ref.read().await?;
        reader_ref.get_write_operations_data_requirements().await?
    };
    
    // Update metadata cache if needed
    if !data_requirements.uncached_metadata_set.is_empty() {
        let mut borrow = reader_ref.write().await?;
        borrow.update_metadata_cache(data_requirements.uncached_metadata_set);
    }

    let instance = WriteOperations::open(
        data_requirements.write_operations_data_requirements, 
        aggregate_write_config.clone()
    )?;

    let writer = Rc::new(RwLock::new(instance));
    
    // Store in cache with reader
    {
        let mut cache = aggregates_cache.write().await?;
        let resources = AggregateResources {
            writer: writer.clone(),
            reader: Some(reader_ref),
        };
        cache.put(aggregate_key.clone(), resources);
    }

    Ok(writer)
}

pub async fn get_or_create_reader(
    aggregate_key: &AggregateKey,
    data_root_folder: &str,
    create_if_not_exists: bool,
    aggregates_cache: &RwLock<LruCache<AggregateKey, AggregateResources>>,
    aggregate_read_config: &AggregateReadConfig,
) -> Result<Rc<RwLock<ReadOperations>>, GlommioError<()>> {
    // Check if reader already exists in cache
    {
        let mut cache = aggregates_cache.write().await?;
        if let Some(resources) = cache.get_mut(aggregate_key) {
            if let Some(reader) = &resources.reader {
                return Ok(reader.clone());
            }
        }
    }

    // Create new reader
    let reader = get_or_create_reader_internal(
        aggregate_key,
        data_root_folder,
        create_if_not_exists,
        aggregate_read_config,
    ).await?;

    // Try to store it in existing resources, or create new resources
    {
        let mut cache = aggregates_cache.write().await?;
        if let Some(resources) = cache.get_mut(aggregate_key) {
            // Resources exist (writer was created first), just add reader
            resources.reader = Some(reader.clone());
        }
        // If no resources exist, reader will be stored when writer is created
    }

    Ok(reader)
}

// Internal helper that creates a reader without cache interaction
async fn get_or_create_reader_internal(
    aggregate_key: &AggregateKey,
    data_root_folder: &str,
    create_if_not_exists: bool,
    aggregate_read_config: &AggregateReadConfig,
) -> Result<Rc<RwLock<ReadOperations>>, GlommioError<()>> {
    // All sync operations, no concurrency issues
    let base_folder = format!(
        "{}/{}/{}/{}", 
        data_root_folder, 
        aggregate_key.org_id, 
        aggregate_key.aggregate_type_id, 
        aggregate_key.aggregate_id
    );
    let path_metadata = format!("{}/metadata.bin", base_folder);
    let path_event_batches = format!("{}/event_batches.bin", base_folder);

    if create_if_not_exists {
        if !std::path::Path::new(&base_folder).exists() {
            std::fs::create_dir_all(&base_folder)?;
        }

        if !std::fs::exists(&path_metadata)? {
            std::fs::File::create(&path_metadata)?;
        }

        if !std::fs::exists(&path_event_batches)? {
            std::fs::File::create(&path_event_batches)?;
        }
    }

    let instance = ReadOperations::open(
        path_metadata, 
        path_event_batches, 
        aggregate_read_config.clone()
    ).await?;

    Ok(Rc::new(RwLock::new(instance)))
}
