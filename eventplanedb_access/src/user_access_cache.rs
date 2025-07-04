use event_storage::event_storage_cache::EventStorageCache;

pub struct UserAccessCache<'a> {
    event_storage_cache: &'a mut EventStorageCache,
}

impl<'a> UserAccessCache<'a> {

    pub fn new(event_storage_cache: &'a mut EventStorageCache) -> Self {
        Self {
            event_storage_cache,
        }
    }
    
}