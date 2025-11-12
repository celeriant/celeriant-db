
// pub struct AggregateResources {
//     pub writer: Rc<RwLock<WriteOperations>>,
//     pub reader: Option<Rc<RwLock<ReadOperations>>>,
//     pub semaphore: Rc<Semaphore>,
//     pub wal_sync_event: Rc<RwLock<Option<Rc<LocalEvent<SyncResult>>>>>,
// }