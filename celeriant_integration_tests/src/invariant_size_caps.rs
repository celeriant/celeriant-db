//! Wire size-cap invariants, both sides of one contract.
//!
//! Client side (read-response-too-large-masked-as-unreachable): a response
//! past the client's own `max_response_size` is a deterministic client-side
//! rejection. The pool must surface `MessageTooLarge` to the caller instead
//! of failing over every candidate and reporting "all nodes unreachable".
//!
//! Server side (write-side-uncompressed-size-not-max-checked): the server's
//! response writer must reject a body whose UNCOMPRESSED size exceeds the
//! server's `max_response_size`, even when compression squeezes the frame
//! under the cap. Before the fix only the compressed size was checked, so a
//! highly compressible oversized body sailed past the server's own limit and
//! was delivered whole.

use std::sync::Arc;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wire::network::wire_error::WireError;

use crate::common::{R, port_for, unique_key};
use crate::{ServerConfig, TestServer, fill_incompressible};

/// Client cap for the read in phase A: far below the 8KB event.
const CLIENT_RESPONSE_CAP: u64 = 1024;
/// Server cap for phase B: far below the 256KB (compressible) event.
const SERVER_RESPONSE_CAP: u64 = 64 * 1024;

fn payload_event(client_seq: u64, payload: Vec<u8>) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1000 + client_seq,
        event_type_major: 100,
        event_type_minor: 0,
        event_value: Arc::new(payload),
        iv: None,
    }
}

/// Phase A: the pool's own response cap surfaces as `MessageTooLarge`.
async fn client_cap_surfaces_message_too_large() -> R {
    let server = TestServer::start_with_port(port_for("invariant_size_caps_client")).await?;

    // Write an 8KB incompressible event with a default-cap client.
    let key = unique_key("invariant_size_caps_client");
    let mut payload = vec![0u8; 8 * 1024];
    fill_incompressible(&mut payload, 42);
    let mut writer = CeleriantClient::connect(server.address()).await?;
    writer
        .write_events_with(
            key.clone(),
            vec![payload_event(1, payload)],
            0,
            WriteEventsOptions { allow_create: true, ..Default::default() },
        )
        .await?;

    // Read it back through a pool whose response cap is smaller than the event.
    let mut options = PoolOptions::new(server.address().to_string());
    options.max_response_size = CLIENT_RESPONSE_CAP;
    let pool = CeleriantPool::new(options);
    let err = pool
        .read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters: ReadFilters::new(1),
        })
        .await
        .expect_err("a response past max_response_size must error");

    match err {
        ClientError::WireError(WireError::MessageTooLarge { .. }) => {}
        other => {
            return Err(format!(
                "a deterministic oversize must surface as MessageTooLarge, not be masked \
                 by failover; got: {other:?}"
            )
            .into());
        }
    }

    client_request_cap_surfaces_message_too_large(server.address()).await
}

/// Phase B: the server enforces its response cap on the uncompressed size.
async fn server_cap_holds_for_compressible_bodies() -> R {
    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        max_response_size: SERVER_RESPONSE_CAP,
        ..Default::default()
    };
    let server =
        TestServer::start_with_config(port_for("invariant_size_caps_server"), config).await?;

    // 256KB of zeros: compresses to well under the 64KB server cap, so only
    // an uncompressed-size check can stop the response. The pool identifies
    // with a keypair so the server ships its dict and actually compresses
    // read responses — without the dict both sizes are equal and the old
    // compressed-size check would catch the frame anyway.
    let key = unique_key("invariant_size_caps_server");
    let keypair = celeriant_crypto::Crypto::generate_keypair(None)?;
    let mut pool_options = PoolOptions::new(server.address().to_string());
    pool_options.identity_config =
        Some(celeriant_client_tokio::celeriant_client::ClientIdentityConfig {
            public_key: Some(keypair.public_key_base64.clone()),
            private_key: Some(keypair.private_key_base64.clone()),
            api_key: None,
        });
    let pool = CeleriantPool::new(pool_options);

    // Identified connections pin the verified client id, so writes must carry it.
    let nonce = celeriant_crypto::Crypto::generate_nonce()?;
    let signature =
        celeriant_crypto::Crypto::sign_nonce(&keypair.private_key_base64, &nonce)?;
    let client_id = celeriant_crypto::Crypto::validate_with_public_key(
        &keypair.public_key_base64,
        &nonce,
        &signature,
    )?;
    pool.write_events_with(
        key.clone(),
        vec![payload_event(1, vec![0u8; 256 * 1024])],
        client_id,
        WriteEventsOptions { allow_create: true, ..Default::default() },
    )
    .await
    .map_err(|e| format!("the write itself is within every cap and must land: {e:?}"))?;

    match pool
        .read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters: ReadFilters::new(1),
        })
        .await
    {
        Err(_) => Ok(()),
        Ok(resp) => {
            let bytes: usize = resp
                .event_batches
                .iter()
                .flat_map(|b| &b.events)
                .map(|e| e.event_value.len())
                .sum();
            Err(format!(
                "server delivered {bytes} bytes of events in one response past its \
                 configured max_response_size of {SERVER_RESPONSE_CAP}; the compressible \
                 body slipped past a compressed-size-only check"
            )
            .into())
        }
    }
}

/// Phase C: the write-path arm of the same masking. A request past the pool's
/// own `max_request_size` is rejected client-side before the send; leader_route
/// must hand that `MessageTooLarge` to the caller instead of walking every
/// candidate and reporting "all nodes unreachable".
async fn client_request_cap_surfaces_message_too_large(server_address: &str) -> R {
    let mut options = PoolOptions::new(server_address.to_string());
    options.max_request_size = 1024;
    let pool = CeleriantPool::new(options);

    let mut payload = vec![0u8; 8 * 1024];
    fill_incompressible(&mut payload, 43);
    let err = pool
        .write_events_with(
            unique_key("invariant_size_caps_write"),
            vec![payload_event(1, payload)],
            0,
            WriteEventsOptions { allow_create: true, ..Default::default() },
        )
        .await
        .expect_err("a request past max_request_size must error");

    match err {
        ClientError::WireError(WireError::MessageTooLarge { .. }) => Ok(()),
        other => Err(format!(
            "an over-cap WRITE is deterministic and client-side; it must surface as \
             MessageTooLarge, not be masked by failover; got: {other:?}"
        )
        .into()),
    }
}

pub async fn caps_enforced_end_to_end() -> R {
    client_cap_surfaces_message_too_large().await?;
    server_cap_holds_for_compressible_bodies().await
}
