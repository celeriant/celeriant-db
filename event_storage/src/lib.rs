pub mod event_item;
pub mod event_storage;
pub mod file_cache;
pub mod wire_format;
pub mod event_storage_cache;
pub mod last_si_cache;
pub mod memory_cache;
pub mod event_batch_item;
pub mod catchup_result;

#[cfg(feature = "tikv-jemallocator")]
mod jemalloc {
    use tikv_jemallocator::Jemalloc;

    #[global_allocator]
    static GLOBAL: Jemalloc = Jemalloc;
}

pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut event = event_item::EventItem::new();
        event.int_values = Some(vec![1, 2, 3]);

        let ints = event.int_values.unwrap();

        let result = add(
            ints[0] as i32, 
            ints[1] as i32);
        assert_eq!(result, 3);
    }
}