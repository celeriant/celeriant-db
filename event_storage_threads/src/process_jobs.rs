use crate::job::Job;
use crate::job_error::JobError;
use core_affinity;
use crossbeam::channel::{Receiver, Sender, unbounded};
use event_storage::catchup_result::CatchupResult;
use event_storage::event_storage_cache::EventStorageCache;
use eventplanedb_access::share_links_cache::ShareLinksCache;
use eventplanedb_access::user_access_cache::UserAccessCache;

pub fn create_thread_pool(required_thread_count: usize) -> Vec<Sender<Job>> {
    let cores = core_affinity::get_core_ids().unwrap();
    let num_available_cores = cores.len(); // Get the total number of cores
    let num_threads_to_use = std::cmp::min(required_thread_count, num_available_cores); // Use min to not exceed available cores

    let mut senders = Vec::new();

    for i in 0..num_threads_to_use {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = unbounded();
        let core_id = cores[i];

        // Spawn pinned thread
        std::thread::spawn(move || {
            core_affinity::set_for_current(core_id);

            let mut event_storage_cache = EventStorageCache::new(30, 1000000, 10000);
            let mut share_links_cache = ShareLinksCache::new(&mut event_storage_cache);
            let mut user_access_cache = UserAccessCache::new(&mut event_storage_cache);

            for job in rx.iter() {
                match job {
                    Job::Write {
                        file_path,
                        allow_create,
                        share_key,
                        event_batch_item,
                        responder,
                    } => {
                        //TODO: Check user has write access or provide access using share link

                        let result: Result<u64, JobError> = event_storage_cache
                            .write(&file_path, allow_create, event_batch_item)
                            .map_err(Into::into);
                        let _ = responder.send(result);
                    }

                    Job::Read {
                        file_path,
                        from_si,
                        cb,
                        share_key,
                        max_bytes,
                        responder,
                    } => {
                        //TODO: Check user has read access or provide access using share link

                        let result: Result<CatchupResult, JobError> = event_storage_cache
                            .read(&file_path, from_si, max_bytes)
                            .map_err(Into::into);
                        let _ = responder.send(result);
                    }

                    Job::Shutdown { responder } => {
                        let _ = responder.send(());
                        break; // Exit the worker loop
                    }
                }
            }
        });

        senders.push(tx);
    }

    senders
}
