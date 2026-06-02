//! Watch API Integration Tests
//!
//! Tests the watch API functionality with a self-managed server instance.
//! Creates a temporary data directory and spawns the server automatically.
//!
//! Run with: cargo run --bin watch_test_main

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::u64;

use crate::TestServer;
use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::{
    process_client_requests::ClientRequest,
    process_client_responses::ClientResponse,
    request::requests::{SingleAggregateWrite, WatchRequest, WriteRequest},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
};
use celeriant_wire::network::wire_header::PROTOCOL_VERSION_V2;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_util::compat::TokioAsyncReadCompatExt;

const CLIENT_ID: u128 = 12345;


pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Watch API Integration Tests ===\n");

    println!("Starting test server...");
    let server = TestServer::start().await?;
    println!("Server started at {}\n", server.address());

    let mut passed = 0;
    let mut failed = 0;

    // Test 1: Basic watch connection
    match test_basic_watch(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_basic_watch");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_basic_watch: {}", e);
            failed += 1;
        }
    }

    // Test 2: Watch receives write events
    match test_watch_receives_writes(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_watch_receives_writes");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_watch_receives_writes: {}", e);
            failed += 1;
        }
    }

    // Test 3: Watch with aggregate filter
    match test_watch_with_aggregate_filter(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_watch_with_aggregate_filter");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_watch_with_aggregate_filter: {}", e);
            failed += 1;
        }
    }

    // Test 4: Watch heartbeat
    match test_watch_heartbeat(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_watch_heartbeat");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_watch_heartbeat: {}", e);
            failed += 1;
        }
    }

    // Test 5: Multiple concurrent watchers
    match test_multiple_watchers(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_multiple_watchers");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_multiple_watchers: {}", e);
            failed += 1;
        }
    }

    // Test 6: Write then watch on same connection (pipelining)
    match test_write_then_watch_same_connection(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_write_then_watch_same_connection");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_write_then_watch_same_connection: {}", e);
            failed += 1;
        }
    }

    // Test 7: Dial completes promptly (immediate ACK, not the 5s heartbeat)
    match test_watch_dial_is_prompt(server.address()).await {
        Ok(()) => {
            println!("[PASS] test_watch_dial_is_prompt");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_watch_dial_is_prompt: {}", e);
            failed += 1;
        }
    }

    // Test 8: Server detects client disconnect promptly (no CLOSE-WAIT leak)
    match test_watch_disconnect_detected_promptly(&server).await {
        Ok(()) => {
            println!("[PASS] test_watch_disconnect_detected_promptly");
            passed += 1;
        }
        Err(e) => {
            println!("[FAIL] test_watch_disconnect_detected_promptly: {}", e);
            failed += 1;
        }
    }

    println!("\n=== Results: {} passed, {} failed ===", passed, failed);

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Helper to create a watch connection that can receive streaming responses
struct WatchConnection {
    stream: tokio_util::compat::Compat<TcpStream>,
}

impl WatchConnection {
    async fn connect(address: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(address).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream: stream.compat(),
        })
    }

    async fn send_watch_request(
        &mut self,
        request: &WatchRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let req = ClientRequest::Watch(request.clone());
        ClientRequest::write_request(
            &mut self.stream,
            &req,
            10_000_000,
            PROTOCOL_VERSION_V2,
        )
        .await
        .map_err(|e| format!("Wire error: {:?}", e))?;
        Ok(())
    }

    async fn send_write_request(
        &mut self,
        request: &WriteRequest,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let req = ClientRequest::Write(request.clone());
        ClientRequest::write_request(
            &mut self.stream,
            &req,
            10_000_000,
            PROTOCOL_VERSION_V2,
        )
        .await
        .map_err(|e| format!("Wire error: {:?}", e))?;
        Ok(())
    }

    async fn read_response(
        &mut self,
    ) -> Result<ClientResponse, Box<dyn std::error::Error + Send + Sync>> {
        let response = ClientResponse::read_response(&mut self.stream, u64::MAX)
            .await
            .map_err(|e| format!("Wire error: {:?}", e))?;
        Ok(response)
    }

    async fn read_response_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<Option<ClientResponse>, Box<dyn std::error::Error + Send + Sync>> {
        match timeout(duration, self.read_response()).await {
            Ok(Ok(response)) => Ok(Some(response)),
            Ok(Err(e)) => Err(e),
            Err(_) => Ok(None), // Timeout
        }
    }
}

/// Test basic watch connection establishment
async fn test_basic_watch(address: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Connecting watch client...");

    let mut watch = WatchConnection::connect(address).await?;

    // Send watch request watching a specific aggregate
    let watch_request = WatchRequest {
        correlation_id: Some(1),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([1000])), // Watch aggregate 1000
        operation_types: None,
    };

    watch.send_watch_request(&watch_request).await?;
    println!("  Watch request sent");

    // We should receive at least a heartbeat within the requested latency
    let response = watch
        .read_response_timeout(Duration::from_millis(500))
        .await?;

    match response {
        Some(ClientResponse::Watch(watch_response)) => {
            println!("  Received watch response: {:?}", watch_response);
            // Heartbeat has events = None
            Ok(())
        }
        Some(ClientResponse::GenericError(err)) => {
            Err(format!("Server returned error: {}", err.error_message).into())
        }
        Some(other) => Err(format!("Unexpected response type: {:?}", other).into()),
        None => {
            // Timeout is acceptable - no events yet
            println!("  No response within timeout (expected for idle watch)");
            Ok(())
        }
    }
}

/// Test that watch receives write events
async fn test_watch_receives_writes(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let aggregate_id = 2000u128;
    let aggregate = AggregateKey::new(1, 1, aggregate_id);

    // Start watching before we write
    let mut watch = WatchConnection::connect(address).await?;

    let watch_request = WatchRequest {
        correlation_id: Some(2),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([aggregate_id])),
        operation_types: None, // Watch all operations
    };

    watch.send_watch_request(&watch_request).await?;
    println!("  Watch started for aggregate {}", aggregate_id);

    // Small delay to ensure watch is established
    sleep(Duration::from_millis(50)).await;

    // Now write to the aggregate using a separate client
    let mut write_client = CeleriantClient::connect(address).await?;

    let event = create_event(0, "Watch test event".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_request = ClientRequest::Write(WriteRequest {
        correlation_id: Some(100),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    });

    let write_response = write_client
        .send_request(&write_request)
        .await?;
    println!("  Write completed: {:?}", write_response);

    // Now check for watch event
    let mut received_write_event = false;
    for _ in 0..10 {
        match watch
            .read_response_timeout(Duration::from_millis(200))
            .await?
        {
            Some(ClientResponse::Watch(watch_response)) => {
                println!("  Watch response: {:?}", watch_response);
                if watch_response.events.iter().any(|e| e.org_id == aggregate.org_id && e.aggregate_type_id == aggregate.aggregate_type_id && e.aggregate_id == aggregate.aggregate_id) {
                    received_write_event = true;
                    println!("  Received write event for aggregate!");
                    break;
                }
            }
            Some(ClientResponse::GenericError(err)) => {
                return Err(format!("Watch error: {}", err.error_message).into());
            }
            _ => continue,
        }
    }

    if !received_write_event {
        return Err("Did not receive write event on watch".into());
    }

    Ok(())
}

/// Test watch with aggregate filter
async fn test_watch_with_aggregate_filter(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let watched_aggregate_id = 3000u128;
    let unwatched_aggregate_id = 3001u128;

    let watched_aggregate = AggregateKey::new(1, 1, watched_aggregate_id);
    let unwatched_aggregate = AggregateKey::new(1, 1, unwatched_aggregate_id);

    // Start watching only one aggregate
    let mut watch = WatchConnection::connect(address).await?;

    let watch_request = WatchRequest {
        correlation_id: Some(3),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([watched_aggregate_id])),
        operation_types: None,
    };

    watch.send_watch_request(&watch_request).await?;
    sleep(Duration::from_millis(50)).await;

    // Write to both aggregates
    let mut write_client = CeleriantClient::connect(address).await?;

    // Write to unwatched aggregate first
    let event = create_event(0, "Unwatched event".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        unwatched_aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    write_client
        .send_request(
            &ClientRequest::Write(WriteRequest {
                correlation_id: Some(200),
                client_id: CLIENT_ID,
                user_id: None,
                writes,
            })
        )
        .await?;

    // Write to watched aggregate
    let event = create_event(0, "Watched event".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        watched_aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    write_client
        .send_request(
            &ClientRequest::Write(WriteRequest {
                correlation_id: Some(201),
                client_id: CLIENT_ID,
                user_id: None,
                writes,
            })
        )
        .await?;

    // Check that we only receive event for watched aggregate
    let mut received_watched = false;
    let mut received_unwatched = false;

    for _ in 0..10 {
        match watch
            .read_response_timeout(Duration::from_millis(200))
            .await?
        {
            Some(ClientResponse::Watch(watch_response)) => {
                {
                    if watch_response.events.iter().any(|e| e.org_id == watched_aggregate.org_id && e.aggregate_type_id == watched_aggregate.aggregate_type_id && e.aggregate_id == watched_aggregate.aggregate_id) {
                        received_watched = true;
                        println!("  Received event for watched aggregate (expected)");
                    }
                    if watch_response.events.iter().any(|e| e.org_id == unwatched_aggregate.org_id && e.aggregate_type_id == unwatched_aggregate.aggregate_type_id && e.aggregate_id == unwatched_aggregate.aggregate_id) {
                        received_unwatched = true;
                        println!("  Received event for unwatched aggregate (unexpected!)");
                    }
                }
            }
            _ => continue,
        }
    }

    if !received_watched {
        return Err("Did not receive event for watched aggregate".into());
    }

    if received_unwatched {
        return Err("Unexpectedly received event for unwatched aggregate".into());
    }

    println!("  Filter correctly excluded unwatched aggregate");
    Ok(())
}

/// Test that watch sends heartbeats when idle
/// Note: Heartbeats are sent on a 5-second default timeout when no events are pending.
/// The requested_latency only affects how long to wait before flushing accumulated events.
async fn test_watch_heartbeat(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut watch = WatchConnection::connect(address).await?;

    let watch_request = WatchRequest {
        correlation_id: Some(4),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([4000])), // Watch an aggregate that won't have events
        operation_types: None,
    };

    watch.send_watch_request(&watch_request).await?;
    println!("  Watch started, waiting for heartbeat (may take up to 5s)...");

    // The server sends heartbeats on a 5-second default timeout when idle
    // We'll wait for at least one heartbeat
    let start = std::time::Instant::now();
    let max_wait = Duration::from_secs(7);

    while start.elapsed() < max_wait {
        match watch
            .read_response_timeout(Duration::from_secs(6))
            .await?
        {
            Some(ClientResponse::Watch(watch_response)) => {
                if watch_response.events.is_empty() {
                    println!("  Received heartbeat after {:?}", start.elapsed());
                    return Ok(());
                } else {
                    println!("  Received watch event (unexpected)");
                }
            }
            Some(ClientResponse::GenericError(err)) => {
                return Err(format!("Watch error: {}", err.error_message).into());
            }
            None => {
                println!("  Read timeout, continuing...");
            }
            _ => {}
        }
    }

    Err("Did not receive heartbeat within expected time".into())
}

/// Test multiple concurrent watchers
async fn test_multiple_watchers(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let aggregate_id = 5000u128;
    let aggregate = AggregateKey::new(1, 1, aggregate_id);

    // Start multiple watchers
    let mut watch1 = WatchConnection::connect(address).await?;
    let mut watch2 = WatchConnection::connect(address).await?;

    let watch_request = WatchRequest {
        correlation_id: Some(5),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([aggregate_id])),
        operation_types: None,
    };

    watch1.send_watch_request(&watch_request).await?;
    watch2.send_watch_request(&watch_request).await?;
    println!("  Started 2 concurrent watchers");

    sleep(Duration::from_millis(50)).await;

    // Write an event
    let mut write_client = CeleriantClient::connect(address).await?;

    let event = create_event(0, "Multi-watcher test".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    write_client
        .send_request(
            &ClientRequest::Write(WriteRequest {
                correlation_id: Some(300),
                client_id: CLIENT_ID,
                user_id: None,
                writes,
            })
        )
        .await?;

    // Both watchers should receive the event
    let mut watch1_received = false;
    let mut watch2_received = false;

    for _ in 0..10 {
        if !watch1_received {
            if let Some(ClientResponse::Watch(wr)) = watch1
                .read_response_timeout(Duration::from_millis(100))
                .await?
            {
                if !wr.events.is_empty() {
                    watch1_received = true;
                    println!("  Watcher 1 received event");
                }
            }
        }

        if !watch2_received {
            if let Some(ClientResponse::Watch(wr)) = watch2
                .read_response_timeout(Duration::from_millis(100))
                .await?
            {
                if !wr.events.is_empty() {
                    watch2_received = true;
                    println!("  Watcher 2 received event");
                }
            }
        }

        if watch1_received && watch2_received {
            break;
        }
    }

    if !watch1_received || !watch2_received {
        return Err(format!(
            "Not all watchers received event: watch1={}, watch2={}",
            watch1_received, watch2_received
        )
        .into());
    }

    println!("  Both watchers received event successfully");
    Ok(())
}

/// Test that a connection can perform a write, then convert to a watch (pipelining),
/// and receive notifications from writes on other connections.
async fn test_write_then_watch_same_connection(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let aggregate_id = 6000u128;
    let aggregate = AggregateKey::new(1, 1, aggregate_id);

    // Open a connection that will first write, then watch
    let mut conn = WatchConnection::connect(address).await?;
    println!("  Connected for write-then-watch test");

    // Step 1: Perform a write on this connection
    let event = create_event(0, "Initial write before watch".to_string());
    let mut writes = HashMap::new();
    writes.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_request = WriteRequest {
        correlation_id: Some(600),
        client_id: CLIENT_ID,
        user_id: None,
        writes,
    };

    conn.send_write_request(&write_request).await?;
    println!("  Sent initial write request on connection");

    // Read the write response
    let write_response = conn.read_response().await?;
    match &write_response {
        ClientResponse::Write(wr) => {
            println!("  Received write response: {:?}", wr);
        }
        ClientResponse::GenericError(err) => {
            return Err(format!("Write failed: {}", err.error_message).into());
        }
        other => {
            return Err(format!("Unexpected response to write: {:?}", other).into());
        }
    }

    // Step 2: Convert the same connection to a watch
    let watch_request = WatchRequest {
        correlation_id: Some(601),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([aggregate_id])),
        operation_types: None,
    };

    conn.send_watch_request(&watch_request).await?;
    println!("  Sent watch request on same connection (pipelining)");

    // Small delay to ensure watch is established
    sleep(Duration::from_millis(50)).await;

    // Step 3: Use a separate connection to write another event
    let mut write_client = CeleriantClient::connect(address).await?;

    let event2 = create_event(1, "Second write from separate connection".to_string());
    let mut writes2 = HashMap::new();
    writes2.insert(
        aggregate.clone(),
        SingleAggregateWrite {
            events: vec![event2],
            allow_create: false, // Aggregate already exists
            expected_version: None,
            enforce_client_idempotency: false,
        },
    );

    let write_request2 = ClientRequest::Write(WriteRequest {
        correlation_id: Some(602),
        client_id: CLIENT_ID + 1, // Different client
        user_id: None,
        writes: writes2,
    });

    let write_response2 = write_client
        .send_request(&write_request2)
        .await?;
    println!("  Second write completed from separate connection: {:?}", write_response2);

    // Step 4: Verify the watch connection receives the notification
    let mut received_notification = false;
    for _ in 0..10 {
        match conn
            .read_response_timeout(Duration::from_millis(200))
            .await?
        {
            Some(ClientResponse::Watch(watch_response)) => {
                println!("  Watch response: {:?}", watch_response);
                if watch_response.events.iter().any(|e| e.org_id == aggregate.org_id && e.aggregate_type_id == aggregate.aggregate_type_id && e.aggregate_id == aggregate.aggregate_id) {
                    received_notification = true;
                    println!("  Watch received notification for write from other connection!");
                    break;
                }
            }
            Some(ClientResponse::GenericError(err)) => {
                return Err(format!("Watch error: {}", err.error_message).into());
            }
            _ => continue,
        }
    }

    if !received_notification {
        return Err("Watch did not receive notification from other connection's write".into());
    }

    println!("  Write-then-watch pipelining works correctly");
    Ok(())
}

/// A watch dial must complete promptly: the server should ACK the subscription
/// immediately, not leave the client blocked until the first 5s idle heartbeat.
/// Pre-fix the first frame on an idle aggregate only arrives after ~5s, so this
/// 1s bound fails.
async fn test_watch_dial_is_prompt(
    address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut watch = WatchConnection::connect(address).await?;

    let watch_request = WatchRequest {
        correlation_id: Some(7),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([7000])), // idle aggregate, no events
        operation_types: None,
    };

    let start = std::time::Instant::now();
    watch.send_watch_request(&watch_request).await?;

    match watch.read_response_timeout(Duration::from_secs(1)).await? {
        Some(ClientResponse::Watch(_)) => {
            println!("  Dial acked in {:?}", start.elapsed());
            Ok(())
        }
        Some(ClientResponse::GenericError(err)) => {
            Err(format!("Server returned error: {}", err.error_message).into())
        }
        Some(other) => Err(format!("Unexpected response type: {:?}", other).into()),
        None => Err(format!(
            "No subscription ack within 1s (dial took >= {:?}) — server isn't sending an immediate watch ack",
            start.elapsed()
        )
        .into()),
    }
}

/// When a watch client disconnects, the server must notice the FIN and tear the
/// session down promptly. Pre-fix the watch loop only ever writes, so it doesn't
/// see the disconnect until its next heartbeat write (~5s); the session — and the
/// underlying socket — lingers. We observe teardown via `watch_subscribers_active`.
async fn test_watch_disconnect_detected_promptly(
    sibling: &TestServer,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Use a dedicated, quiet server so the process-global `watch_subscribers_active`
    // gauge is a clean 1->0 signal, not contaminated by lingering sessions from the
    // other tests in this binary. Sibling ports (+10) avoid colliding with the
    // shared server's client/replication/metrics ports.
    let server = TestServer::start_with_port(sibling.port() + 10)
        .await
        .map_err(|e| format!("failed to start dedicated test server: {}", e))?;
    let metrics_port = server.config().metrics_port;
    let active = || async move {
        crate::scrape_counter("127.0.0.1", metrics_port, "celeriant_watch_subscribers_active")
            .await
            .map_err(|e| format!("metrics scrape failed: {}", e))
    };

    let mut watch = WatchConnection::connect(server.address()).await?;
    let watch_request = WatchRequest {
        correlation_id: Some(8),
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: Some(HashSet::from([8000])),
        operation_types: None,
    };
    watch.send_watch_request(&watch_request).await?;
    // Wait for the subscription to register.
    watch.read_response_timeout(Duration::from_secs(1)).await?;
    sleep(Duration::from_millis(100)).await;

    let with_watcher = active().await?;
    if with_watcher < 1 {
        return Err(format!(
            "subscriber gauge did not rise after subscribe (got {})",
            with_watcher
        )
        .into());
    }

    // Disconnect: dropping the connection sends a FIN to the server.
    drop(watch);

    // The server must drop to zero well before the 5s heartbeat cadence.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        sleep(Duration::from_millis(100)).await;
        let now = active().await?;
        if now == 0 {
            println!("  Subscriber gauge returned to 0 after disconnect");
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "server still reports {} active watcher(s) 2s after client disconnect — FIN not detected",
                now
            )
            .into());
        }
    }
}

fn create_event(client_seq: u64, message: String) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: Some(rand::random()),
        event_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(message.into_bytes()),
        iv: None,
    }
}
