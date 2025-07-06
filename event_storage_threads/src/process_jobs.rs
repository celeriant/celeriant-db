use crate::job::Job;
use crate::process_read::handle_read_job;
use crate::process_share::handle_share_job;
use crate::process_write::handle_write_job;
use core_affinity;
use crossbeam::channel::{Receiver, Sender, unbounded};
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
            let mut share_links_cache = ShareLinksCache::new(10000);
            let mut user_access_cache = UserAccessCache::new(10000);

            for job in rx.iter() {
                match job {

                    Job::Share {
                        file_path,
                        cb,
                        share_hash,
                        access_level,
                        is_single_use,
                        iv,
                        description,
                        expires_on,
                        responder,
                    } => {
                        let _ = responder.send(handle_share_job(
                            file_path,
                            cb,
                            share_hash,
                            access_level,
                            is_single_use,
                            iv,
                            description,
                            expires_on,
                            &mut event_storage_cache,
                            &mut share_links_cache,
                            &mut user_access_cache,
                        ));
                    }

                    Job::Write {
                        file_path,
                        allow_create,
                        event_batch_item,
                        responder,
                    } => {
                        let _ = responder.send(handle_write_job(
                            file_path,
                            allow_create,
                            event_batch_item,
                            &mut event_storage_cache,
                            &mut share_links_cache,
                            &mut user_access_cache,
                        ));
                    }

                    Job::Read {
                        file_path,
                        from_si,
                        cb,
                        share_key,
                        max_bytes,
                        own_events,
                        responder,
                    } => {
                        let _ = responder.send(handle_read_job(
                            file_path,
                            from_si,
                            cb,
                            share_key,
                            max_bytes,
                            own_events,
                            &mut event_storage_cache,
                            &mut share_links_cache,
                            &mut user_access_cache,
                        ));
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
