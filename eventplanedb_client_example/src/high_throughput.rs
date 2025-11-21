// //! Example demonstrating high-throughput writes with pipelining

// use eventplanedb_client::{ClientConfig, ConnectionPoolConfig, EventPlaneDBClient};
// use eventplanedb_structures::{
//     compression_type::CompressionType,
//     event_item::EventItem,
// };
// use std::sync::Arc;
// use std::time::Instant;
// use tokio::task::JoinSet;

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Configure client for high throughput
//     let pool_config = ConnectionPoolConfig {
//         min_connections: 5,
//         max_connections: 20,
//         health_check_interval_secs: 30,
//         max_idle_time_secs: 300,
//         fail_fast: true,
//     };

//     let config = ClientConfig::new("127.0.0.1:10000".to_string())
//         .with_timeout(10000)
//         .with_retries(3, 50)
//         .with_compression(CompressionType::Zstd { level: 3 })
//         .with_pool_config(pool_config);

//     let client = Arc::new(EventPlaneDBClient::new(config).await?);

//     println!("Starting high-throughput test...");
//     let start = Instant::now();

//     // Create concurrent write tasks
//     let mut join_set = JoinSet::new();
//     let num_aggregates = 100;
//     let events_per_aggregate = 10;

//     for aggregate_id in 0..num_aggregates {
//         let client_clone = client.clone();
        
//         join_set.spawn(async move {
//             let mut total_written = 0;
            
//             for batch_num in 0..events_per_aggregate {
//                 let mut events = Vec::new();
                
//                 for i in 0..1 {
//                     let client_event_index = (batch_num * 100 + i) as u64 + 1;
//                     events.push(EventItem::new(
//                         client_event_index,
//                         0,
//                         Some(client_event_index as u128),
//                         chrono::Utc::now().timestamp_millis() as u64,
//                         1,
//                         0,
//                         format!("Event {} for aggregate {}", client_event_index, aggregate_id)
//                             .into_bytes(),
//                     ));
//                 }

//                 match client_clone
//                     .write(
//                         1,
//                         100,
//                         aggregate_id as u128,
//                         999,
//                         None,
//                         events,
//                         true,
//                         None,
//                         false,
//                         None,
//                         CompressionType::Zstd { level: 3 },
//                         None,
//                     )
//                     .await
//                 {
//                     Ok(response) => {
//                         if response.error.is_none() {
//                             total_written += events_per_aggregate;
//                         }
//                     }
//                     Err(e) => {
//                         eprintln!("Error writing to aggregate {}: {:?}", aggregate_id, e);
//                     }
//                 }
//             }
            
//             total_written
//         });
//     }

//     // Wait for all tasks to complete
//     let mut total_events = 0;
//     while let Some(result) = join_set.join_next().await {
//         if let Ok(count) = result {
//             total_events += count;
//         }
//     }

//     let duration = start.elapsed();
//     let events_per_sec = total_events as f64 / duration.as_secs_f64();

//     println!("\nResults:");
//     println!("  Total events written: {}", total_events);
//     println!("  Duration: {:.2}s", duration.as_secs_f64());
//     println!("  Throughput: {:.0} events/sec", events_per_sec);

//     let stats = client.stats().await;
//     println!("\nFinal Statistics:");
//     println!("  Active connections: {}", stats.active_connections);
//     println!("  Idle connections: {}", stats.idle_connections);
//     println!("  Total requests: {}", stats.total_requests);
//     println!("  Failed requests: {}", stats.failed_requests);

//     client.close().await;

//     Ok(())
// }