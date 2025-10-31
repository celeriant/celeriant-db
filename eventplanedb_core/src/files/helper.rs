use std::{cell::RefCell, collections::HashMap, rc::Rc};

use eventplanedb_structures::aggregate_key::AggregateKey;
use glommio::{GlommioError};

use crate::files::{read_operations::{AggregateReadConfig, ReadError, ReadOperations, WriteOperationsDataRequirementsAndCachedData}, write_operations::{AggregateWriteConfig, WriteOperations}};

pub async fn get_or_create_writer(
    aggregate_key: &AggregateKey,
    data_root_folder: &str,
    create_if_not_exists: bool,
    read_operations_cache: &Rc<RefCell<HashMap<AggregateKey, Rc<RefCell<ReadOperations>>>>>,
    aggregate_read_config: &AggregateReadConfig,
    write_operations_cache: &Rc<RefCell<HashMap<AggregateKey, Rc<RefCell<WriteOperations>>>>>,
    aggregate_write_config: &AggregateWriteConfig,
) -> Result<Rc<RefCell<WriteOperations>>, ReadError> {
    let reference = write_operations_cache.borrow().get(&aggregate_key).cloned();
    let reference = match reference {
        Some(reference) => reference,
        None => {

            let reader_ref = get_or_create_reader(aggregate_key, data_root_folder, create_if_not_exists, read_operations_cache, aggregate_read_config).await?;
            let data_requirements = reader_ref.borrow().get_write_operations_data_requirements().await?;
            
            // Idempotent cache update, ok under concurrent conditions
            //TODO: Can't borrow mut here due to another borrow?
            // if data_requirements.uncached_metadata_set.len() > 0 {
            //     reader_ref.borrow_mut().update_metadata_cache(data_requirements.uncached_metadata_set);
            // }

            let instance = WriteOperations::open(data_requirements.write_operations_data_requirements, aggregate_write_config.clone())?;

            let reference = Rc::new(RefCell::new(instance));
            write_operations_cache.borrow_mut().insert(aggregate_key.clone(), reference.clone());
            reference
        }
    };

    Ok(reference)
}

pub async fn get_or_create_reader(
    aggregate_key: &AggregateKey,
    data_root_folder: &str,
    create_if_not_exists: bool,
    read_operations_cache: &Rc<RefCell<HashMap<AggregateKey, Rc<RefCell<ReadOperations>>>>>,
    aggregate_read_config: &AggregateReadConfig,
) -> Result<Rc<RefCell<ReadOperations>>, GlommioError<()>> {
    let reference = read_operations_cache.borrow().get(&aggregate_key).cloned();
    let reference = match reference {
        Some(reference) => reference,
        None => {

            //This part is all sync so no concurrency issues in folder or file creation logic
            //TODO: allow configurable folder and file names
            let base_folder = format!("{}/{}/{}/{}", data_root_folder, aggregate_key.org_id, aggregate_key.aggregate_type_id, aggregate_key.aggregate_id);
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
    
            let instance = ReadOperations::open(path_metadata, path_event_batches, aggregate_read_config.clone()).await?;

            // Due to async boundary could double-insert but any existing value for that key will be dropped
            let reference = Rc::new(RefCell::new(instance));
            read_operations_cache.borrow_mut().insert(aggregate_key.clone(), reference.clone());

            reference
        }
    };

    Ok(reference)
}