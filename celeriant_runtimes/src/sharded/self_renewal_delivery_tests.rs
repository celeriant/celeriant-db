//! White-box tests for shard 0's self-delivery renewal channel
//! (`IntrashardLeaseRenewalRequester::new` + `spawn_self_renewal_handler`).
//!
//! Glommio's `Full` mesh has no self slot: `try_send_to(peer_id())` always fails and
//! `Receivers::streams()` never yields a self stream. Shard 0 therefore cannot ask itself
//! to renew the S3 lease over the mesh, which is fatal at `num_shards == 1` where shard 0
//! is the only data shard. These tests pin the local channel that closes that hole.
//!
//! Companion black-box tests live in `lease_renewal_contract_tests.rs`.

use std::rc::Rc;
use std::time::Duration;

use celeriant_shard::shard_wal::LeaseRenewalRequester;
use glommio::channels::channel_mesh::{Full, MeshBuilder};
use glommio::{LocalExecutorBuilder, LocalExecutorPoolBuilder, Placement, PoolPlacement};
use metrics_exporter_prometheus::PrometheusBuilder;

use super::intrashard_messages::IntrashardMessages;
use super::shard::IntrashardLeaseRenewalRequester;

const MESH_CHANNEL_SIZE: usize = 128;

/// `request_renewal()` emits `celeriant_s3_lease_renewal_requested_total`. The recorder is
/// process-global and `lease_renewal_contract_tests` installs one to measure that exact
/// counter, so emitting into it from here would corrupt its arithmetic when the suite runs
/// in parallel. Scope every emission to a throwaway local recorder instead.
fn isolating_metrics<T>(f: impl FnOnce() -> T) -> T {
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, f)
}

/// The pairing contract: a shard 0 requester comes with a receiver, and what it sends
/// arrives there. This is the delivery the mesh cannot provide.
#[test]
fn shard_zero_request_renewal_arrives_on_its_self_channel() {
    let mesh = MeshBuilder::<IntrashardMessages, Full>::full(1, MESH_CHANNEL_SIZE);
    LocalExecutorBuilder::new(Placement::Unbound)
        .spawn(move || async move {
            let (sender, _receivers) = mesh.join().await.expect("join mesh");
            let (requester, rx) = IntrashardLeaseRenewalRequester::new(Rc::new(sender), 0);
            let rx = rx.expect("shard 0 must be given a self-delivery receiver");

            isolating_metrics(|| requester.request_renewal());

            let got = glommio::timer::timeout(Duration::from_secs(5), async { Ok(rx.recv().await) })
                .await
                .expect("renewal request never arrived on shard 0's self channel");
            assert_eq!(got, Some(0), "the requesting shard id must survive delivery");
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Depth 1 is deliberate: a queued request is already a pending renewal, so a burst from
/// the replication spin loop must coalesce rather than either growing without bound or
/// reporting itself lost. Every call still counts as sent.
#[test]
fn shard_zero_burst_coalesces_without_reporting_a_drop() {
    let mesh = MeshBuilder::<IntrashardMessages, Full>::full(1, MESH_CHANNEL_SIZE);
    LocalExecutorBuilder::new(Placement::Unbound)
        .spawn(move || async move {
            let (sender, _receivers) = mesh.join().await.expect("join mesh");
            let (requester, rx) = IntrashardLeaseRenewalRequester::new(Rc::new(sender), 0);
            let rx = rx.expect("shard 0 must be given a self-delivery receiver");

            // Nothing is draining, so every call after the first hits a full channel.
            isolating_metrics(|| {
                for _ in 0..64 {
                    requester.request_renewal();
                }
            });

            let got = glommio::timer::timeout(Duration::from_secs(5), async { Ok(rx.recv().await) })
                .await
                .expect("the coalesced request must still be deliverable");
            assert_eq!(got, Some(0));

            // Exactly one: the other 63 coalesced into it rather than queueing.
            let second = glommio::timer::timeout(Duration::from_millis(200), async { Ok(rx.recv().await) }).await;
            assert!(second.is_err(), "depth-1 channel must hold exactly one coalesced request, found more");
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Regression guard (adversarial review): a dead drain task must be reported, not masked.
///
/// `LocalReceiver::drop` clears the receiver waiter list but leaves the queued item in the
/// buffer, so a depth-1 channel that lost its receiver mid-request reports `is_full()` forever.
/// An earlier version of `request_renewal` treated full as sent unconditionally, which recorded
/// `result="sent"` while delivery was structurally impossible — the counter reading healthy in
/// exactly the state it exists to expose. Matching on `Closed` instead of asking `is_full()` is
/// what keeps this honest.
#[test]
fn closed_self_channel_is_reported_dropped_not_sent() {
    let mesh = MeshBuilder::<IntrashardMessages, Full>::full(1, MESH_CHANNEL_SIZE);
    let render = LocalExecutorBuilder::new(Placement::Unbound)
        .spawn(move || async move {
            let (sender, _receivers) = mesh.join().await.expect("join mesh");
            let (requester, rx) = IntrashardLeaseRenewalRequester::new(Rc::new(sender), 0);
            let rx = rx.expect("shard 0 receiver");

            let recorder = PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            metrics::with_local_recorder(&recorder, || {
                // One request lands and occupies the depth-1 buffer.
                requester.request_renewal();
                // The drain task dies (panic / cancellation / teardown) with it still queued.
                drop(rx);
                for _ in 0..4 {
                    requester.request_renewal();
                }
            });
            handle.render()
        })
        .unwrap()
        .join()
        .unwrap();

    let total = |result: &str| -> u64 {
        render
            .lines()
            .filter(|l| {
                l.starts_with("celeriant_s3_lease_renewal_requested_total")
                    && l.contains(r#"shard_id="0""#)
                    && l.contains(&format!(r#"result="{result}""#))
            })
            .filter_map(|l| l.rsplit_once(' ').and_then(|(_, v)| v.trim().parse::<f64>().ok()))
            .map(|v| v as u64)
            .sum()
    };

    assert_eq!(
        (total("sent"), total("dropped")),
        (1, 4),
        "requests issued into a CLOSED self channel must be reported dropped, not sent\n{render}"
    );
}

/// The fix must not divert shards that the mesh already serves correctly. Shard 1 gets no
/// self channel and its request still travels the mesh to shard 0.
#[test]
fn non_zero_shard_still_routes_through_the_mesh() {
    let mesh = MeshBuilder::<IntrashardMessages, Full>::full(2, MESH_CHANNEL_SIZE);
    let results = LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(2))
        .on_all_shards({
            let mesh = mesh.clone();
            move || async move {
                let (sender, mut receivers) = mesh.join().await.expect("join mesh");
                let shard_id = sender.peer_id();
                let (requester, rx) = IntrashardLeaseRenewalRequester::new(Rc::new(sender), shard_id);

                if shard_id == 1 {
                    assert!(rx.is_none(), "only shard 0 may hold a self-delivery channel");
                    isolating_metrics(|| requester.request_renewal());
                    return true;
                }

                // Shard 0: the mesh stream from shard 1 must carry the request.
                let (_src, stream) = receivers.streams().into_iter().next().expect("a stream from shard 1");
                let msg = glommio::timer::timeout(Duration::from_secs(5), async { Ok(stream.recv().await) })
                    .await
                    .expect("shard 1's renewal request never reached shard 0 over the mesh");
                matches!(msg, Some(IntrashardMessages::RenewS3LeaseNow { requesting_shard: 1 }))
            }
        })
        .expect("spawn pool")
        .join_all();

    assert!(
        results.into_iter().all(|r| r.unwrap()),
        "shard 1 must reach shard 0 over the mesh, unchanged by the shard-0 fix"
    );
}
