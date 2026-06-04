// Compile-time verification of every code sample in docs/guide.md.
// This module never runs — it just needs to compile.
#[allow(
    dead_code,
    unused_variables,
    unused_imports,
    unreachable_code,
    unused_mut
)]
const _: () = {
    use std::collections::{HashMap, HashSet};

    use celeriant_client_tokio::list_operations::ListOptions;
    use celeriant_client_tokio::server_error::*;
    use celeriant_client_tokio::{
        CeleriantClient, CeleriantPool, ClientError, ClientIdentityConfig, ClientTlsConfig,
        PoolOptions, WatchOptions, WriteEventsOptions, from_json, json_event,
    };
    use celeriant_crypto::Crypto;
    use celeriant_crypto::pki::PkiManager;
    use celeriant_msg::request::read_filters::ReadFilters;
    use celeriant_msg::request::requests::*;
    use celeriant_wal::aggregate_key::AggregateKey;
    use celeriant_wal::schema_key::SchemaKey;
    use celeriant_wal::schema_type::SchemaType;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    // Stand-in for user domain types. The guide uses Uuid fields but the compile check
    // avoids needing uuid's serde feature by using u128.
    #[derive(Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: u128,
        amount: f64,
    }

    fn order(id: u128) -> OrderPlaced {
        OrderPlaced { order_id: id, amount: 99.95 }
    }

    // --- Aggregates and keys ---

    fn aggregates_and_keys() {
        let org_id: u128 = 1;
        let aggregate_type_id: u128 = 2;
        let aggregate_id: u128 = 3;
        let key = AggregateKey::new(org_id, aggregate_type_id, aggregate_id);
    }

    // --- Connections ---

    async fn connect() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = CeleriantClient::connect("localhost:10000").await?;
        Ok(())
    }

    // --- Pool ---

    fn pool_construction() {
        let pool = CeleriantPool::new(
            PoolOptions::new("localhost:10000")
                .with_seed_addresses(vec!["localhost:10002".into()])
                .with_max_connections(20)
                .with_route_reads_to_followers(true),
        );
    }

    // --- TLS ---

    async fn tls() -> Result<(), Box<dyn std::error::Error>> {
        use std::path::Path;

        let ca = PkiManager::load_ca_bundle(Path::new("ca.crt"))?;
        let (certs, key) =
            PkiManager::load_identity(Path::new("client.crt"), Path::new("client.key"))?;
        let tls_config = PkiManager::build_client_config(&ca, certs, key)?;
        let tls = ClientTlsConfig::new(tls_config, "localhost".try_into()?);

        let mut client =
            CeleriantClient::connect_tls("localhost:10000", tls.clone()).await?;

        let pool = CeleriantPool::new(PoolOptions::new("localhost:10000").with_tls(tls));
        Ok(())
    }

    // --- Client identity: API key ---

    fn identity_api_key() {
        let identity = ClientIdentityConfig::from_api_key("base64-encoded-32-byte-key");
    }

    // --- Client identity: RSA key pair ---

    async fn identity_rsa() -> Result<(), Box<dyn std::error::Error>> {
        use base64::Engine;

        let keypair = Crypto::generate_keypair(None)?;

        let identity = ClientIdentityConfig::from_key_pair(
            keypair.public_key_base64.clone(),
            keypair.private_key_base64,
        );

        let public_key_bytes = base64::engine::general_purpose::STANDARD
            .decode(&keypair.public_key_base64)?;
        let my_client_id = Crypto::generate_short_client_identity(&public_key_bytes);
        Ok(())
    }

    // --- Using identity ---

    async fn using_identity() -> Result<(), Box<dyn std::error::Error>> {
        let identity = ClientIdentityConfig::from_api_key("test");

        let mut client = CeleriantClient::connect("localhost:10000").await?;
        client.identify(&identity).await?;

        let pool =
            CeleriantPool::new(PoolOptions::new("localhost:10000").with_identity(identity));
        Ok(())
    }

    // --- Serialization ---

    async fn serialization() -> Result<(), Box<dyn std::error::Error>> {
        let evt = json_event(1, &order(123))?;
        Ok(())
    }

    // --- Client ID (Uuid::new_v5 needs the v5 feature — the guide is correct, but the
    //     workspace only enables v4. We verify Uuid::as_u128 compiles.) ---

    fn client_id() {
        let my_client_id: u128 = Uuid::new_v4().as_u128();
    }

    // --- Writing events ---

    async fn write_events(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);
        let my_client_id: u128 = 42;
        let events = vec![json_event(1, &order(123))?];
        pool.write_events(key.clone(), events, my_client_id).await?;
        Ok(())
    }

    // --- OCC ---

    async fn occ(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);
        let my_client_id: u128 = 42;
        let current_version: u64 = 5;
        let events = vec![json_event(1, &order(123))?];

        pool.write_events_with(
            key,
            events,
            my_client_id,
            WriteEventsOptions {
                expected_version: Some(current_version),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    // --- OCC error match ---

    async fn occ_error_match(pool: &CeleriantPool) {
        let key = AggregateKey::new(1, 2, 3);
        let my_client_id: u128 = 42;
        let events = vec![json_event(1, &order(123)).unwrap()];
        let result = pool.write_events(key, events, my_client_id).await;

        match result {
            Err(ClientError::Server(ServerError::Write {
                kind:
                    WriteError::OptimisticConcurrencyViolation {
                        expected_version,
                        current_aggregate_version,
                    },
                ..
            })) => {
                // Re-read, re-validate, retry
            }
            _ => {}
        }
    }

    // --- Dynamic consistency boundaries ---

    async fn multi_aggregate_write(
        pool: &CeleriantPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let from_key = AggregateKey::new(1, 2, 3);
        let to_key = AggregateKey::new(1, 2, 4);
        let my_client_id: u128 = 42;
        let from_version: u64 = 5;
        let to_version: u64 = 10;
        let transfer_out_event = json_event(1, &order(1))?;
        let transfer_in_event = json_event(1, &order(2))?;

        let writes = HashMap::from([
            (
                from_key,
                SingleAggregateWrite {
                    events: vec![transfer_out_event],
                    allow_create: true,
                    expected_version: Some(from_version),
                    enforce_client_idempotency: true,
                },
            ),
            (
                to_key,
                SingleAggregateWrite {
                    events: vec![transfer_in_event],
                    allow_create: true,
                    expected_version: Some(to_version),
                    enforce_client_idempotency: true,
                },
            ),
        ]);

        pool.write(WriteRequest {
            correlation_id: None,
            client_id: my_client_id,
            user_id: None,
            writes,
        })
        .await?;
        Ok(())
    }

    // --- Reading events ---

    async fn read_events(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);
        let my_client_id: u128 = 42;
        let start_ts: u64 = 0;
        let end_ts: u64 = u64::MAX;

        let response = pool
            .read(ReadRequest {
                correlation_id: None,
                aggregate_key: key.clone(),
                filters: ReadFilters::new(1),
            })
            .await?;

        let filters = ReadFilters::new(1)
            .to_aggregate_version(100)
            .include_event_types(vec![1, 2, 3])
            .min_event_timestamp(start_ts)
            .max_event_timestamp(end_ts)
            .include_client_id(my_client_id);

        Ok(())
    }

    // --- Streaming reads ---

    async fn streaming_reads(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);

        let mut iter = pool
            .read_all(key.clone(), Some(ReadFilters::new(1)))
            .await?;
        while let Some(result) = iter.next().await {
            let batch = result?;
            for evt in &batch.events {
                let order: OrderPlaced = from_json(evt)?;
            }
        }

        let all_batches = pool
            .read_all(key.clone(), Some(ReadFilters::new(1)))
            .await?
            .collect()
            .await?;
        Ok(())
    }

    // --- Aggregate details ---

    async fn aggregate_details(
        pool: &CeleriantPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);
        let details = pool
            .aggregate_details(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: key,
            })
            .await?;

        let _ = details.min_aggregate_version;
        let _ = details.max_aggregate_version;
        let _ = details.is_deleted;
        let _ = details.last_server_timestamp;
        Ok(())
    }

    // --- Schemas ---

    async fn register_schema(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let org_id: u128 = 1;
        let aggregate_type_id: u128 = 2;
        let my_client_id: u128 = 42;
        let json_schema_string = String::from("{}");

        pool.register_schema(RegisterSchemaRequest {
            correlation_id: None,
            client_id: my_client_id,
            user_id: None,
            schema_key: SchemaKey::new(org_id, aggregate_type_id, 1, 0),
            schema_type: SchemaType::Json.into(),
            schema: json_schema_string,
        })
        .await?;
        Ok(())
    }

    // --- Watching ---

    async fn watch(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let org_id: u128 = 1;
        let order_type_id: u128 = 2;

        let request = WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: Some(HashSet::from([org_id])),
            aggregate_types: Some(HashSet::from([order_type_id])),
            aggregates: None,
            operation_types: Some(HashSet::from([1])),
        };

        let mut watch = pool.watch(request, WatchOptions::default()).await?;

        let response = watch.next().await?;
        for evt in &response.events {
            let _ = evt.org_id;
            let _ = evt.aggregate_type_id;
            let _ = evt.aggregate_id;
            let _ = evt.operation;
            let _ = evt.from_aggregate_version;
            let _ = evt.to_aggregate_version;
        }

        let _ = watch
            .next_timeout(std::time::Duration::from_secs(1))
            .await?;
        Ok(())
    }

    // --- Trimming ---

    async fn trim(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);
        let my_client_id: u128 = 42;

        pool.trim_start(TrimStartRequest {
            correlation_id: None,
            aggregate_key: key,
            keep_from_aggregate_version: 100,
            client_id: my_client_id,
            user_id: None,
        })
        .await?;
        Ok(())
    }

    // --- Deleting ---

    async fn delete(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);
        let my_client_id: u128 = 42;

        pool.delete(DeleteRequest {
            correlation_id: None,
            client_id: my_client_id,
            user_id: None,
            deletes: HashMap::from([(
                key,
                SingleAggregateDelete {
                    allow_recreate: true,
                    allow_sequence_continuation: false,
                    expected_version: None,
                },
            )]),
        })
        .await?;
        Ok(())
    }

    // --- Listing ---

    async fn listing(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let org_id: u128 = 1;
        let type_id: u128 = 2;

        let mut iter = pool.list_orgs(ListOptions::default()).await?;
        while let Some(result) = iter.next().await {
            let org = result?;
            println!("{}", org.org_id);
        }

        let mut iter = pool
            .list_aggregate_types(Some(org_id), ListOptions::default())
            .await?;

        let mut iter = pool
            .list_aggregates(Some(org_id), Some(type_id), ListOptions::default())
            .await?;
        while let Some(result) = iter.next().await {
            let agg = result?;
            let _ = agg.aggregate_id;
            let _ = agg.event_batch_count;
            let _ = agg.min_event_timestamp;
            let _ = agg.max_event_timestamp;
            let _ = agg.compressed_size;
            let _ = agg.uncompressed_size;
            let _ = agg.is_deleted;
        }

        let all_orgs = pool
            .list_orgs(ListOptions::default())
            .await?
            .collect()
            .await?;

        let options = ListOptions {
            include_deleted: true,
            max_shard_hint: Some(4),
            ..Default::default()
        };
        Ok(())
    }

    // --- Error handling ---

    async fn error_handling(pool: &CeleriantPool) -> Result<(), Box<dyn std::error::Error>> {
        let key = AggregateKey::new(1, 2, 3);
        let events = vec![json_event(1, &order(123))?];

        let request = WriteRequest {
            correlation_id: None,
            client_id: 42,
            user_id: None,
            writes: HashMap::from([(
                key,
                SingleAggregateWrite {
                    events,
                    allow_create: true,
                    expected_version: None,
                    enforce_client_idempotency: false,
                },
            )]),
        };

        match pool.write(request).await {
            Ok(response) => { /* success */ }
            Err(ClientError::Server(ServerError::Write {
                kind:
                    WriteError::OptimisticConcurrencyViolation {
                        expected_version,
                        current_aggregate_version,
                    },
                ..
            })) => { /* OCC conflict - retry with fresh state */ }
            Err(ClientError::Server(ServerError::Write {
                kind: WriteError::ClientIdempotencyViolation { .. },
                ..
            })) => { /* prior attempt already landed - treat as success */ }
            Err(ClientError::NotLeader {
                leader_address, ..
            }) => { /* pool handles this automatically */ }
            Err(ClientError::ServerBusy) => { /* back off and retry */ }
            Err(ClientError::RequestTimeout) => {
                /* ambiguous - hold idempotency key constant on retry */
            }
            Err(e) => { /* unexpected error */ }
        }
        Ok(())
    }

    // --- README quick-start snippet (verbatim imports + main body) ---

    mod readme_quickstart {
        use celeriant_client_tokio::{CeleriantPool, PoolOptions, from_json, json_event};
        use celeriant_msg::request::read_filters::ReadFilters;
        use celeriant_msg::request::requests::ReadRequest;
        use celeriant_wal::aggregate_key::AggregateKey;
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        struct OrderPlaced { order_id: u64, amount_cents: u64 }

        async fn run() -> Result<(), Box<dyn std::error::Error>> {
            let pool = CeleriantPool::new(
                PoolOptions::new("localhost:10000"));

            let key = AggregateKey::new(1, 1, 1001);

            let order_event = OrderPlaced { order_id: 42, amount_cents: 9995 };
            let events = vec![json_event(1, &order_event)?];
            let my_client_id: u128 = 42;
            pool.write_events(key.clone(), events, my_client_id).await?;

            let response = pool.read(ReadRequest {
                correlation_id: None,
                aggregate_key: key,
                filters: ReadFilters::new(1),
            }).await?;

            let order: OrderPlaced = from_json(&response.event_batches[0].events[0])?;
            println!("order_id={}, amount_cents={}", order.order_id, order.amount_cents);
            Ok(())
        }
    }
};
