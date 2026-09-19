//! Client-routing invariant: schema registration sent to a non-leader redirects
//! to the leader, the same way write/delete/trim do. The follower answers
//! `REGISTER_SCHEMA_CANNOT_ACCEPT_WRITES` (2027) with a `leader_address` hint;
//! the pool must follow it instead of surfacing a schema error.
//! Finding: schema-register-not-leader-no-redirect.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_msg::request::requests::RegisterSchemaRequest;
use celeriant_wal::schema_key::SchemaKey;
use std::time::Duration;

use crate::common::{R, port_for};
use crate::{MinioContainer, TestServer, s3_cluster_config};

const SCHEMA: &str =
    r#"{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"]}"#;

fn register_request() -> RegisterSchemaRequest {
    RegisterSchemaRequest {
        correlation_id: None,
        client_id: 7,
        user_id: None,
        schema_key: SchemaKey::new(1, 1, 100, 0),
        schema_type: 0,
        schema: SCHEMA.to_string(),
    }
}

pub async fn redirects_from_non_leader() -> R {
    let leader_port = port_for("invariant_schema_redirect_leader");
    let follower_port = port_for("invariant_schema_redirect_follower");
    let minio_port = port_for("invariant_schema_redirect_minio");

    println!("Starting MinIO...");
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();

    let config =
        s3_cluster_config(1, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);

    println!("Starting cluster...");
    let leader =
        TestServer::start_with_config_labeled(leader_port, config.clone(), "leader".into()).await?;
    let follower =
        TestServer::start_with_config_labeled(follower_port, config, "follower".into()).await?;

    println!("Waiting for election + replication...");
    tokio::time::sleep(Duration::from_secs(12)).await;

    // Control: the follower's raw answer is a NotLeader-class error with a
    // leader hint — the wire shape the redirect depends on.
    let mut follower_client = CeleriantClient::connect(follower.address()).await?;
    let hint = match follower_client.register_schema(register_request()).await {
        Err(ClientError::NotLeader { leader_address: Some(addr), .. }) => addr,
        Err(ClientError::NotLeader { leader_address: None, .. }) => {
            return Err("control: follower refused schema registration without a leader hint".into());
        }
        other => {
            return Err(format!(
                "control: follower must answer schema registration with a leader hint, got {other:?}"
            )
            .into());
        }
    };
    println!("Follower hinted leader at {hint}");

    // The invariant: a pool whose only configured address is the follower must
    // follow the hint and land the registration on the leader.
    let pool = CeleriantPool::new(PoolOptions::new(follower.address()));
    pool.register_schema(register_request())
        .await
        .map_err(|e| format!("pool must redirect schema registration to the leader, got {e:?}"))?;

    println!("PASS: schema registration redirected from non-leader");
    drop(leader);
    Ok(())
}
