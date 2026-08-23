//! Optional per-op history recording, Jepsen-style.
//!
//! Each write attempt emits one JSONL record classified `ok` / `fail` /
//! `info`:
//!
//! - `ok` — the server acknowledged the write.
//! - `fail` — the server definitively rejected it (OCC conflict, idempotency
//!   violation, not-leader, busy, validation). The write did NOT commit.
//! - `info` — outcome unknown (timeout, connection loss, replication/fsync
//!   errors whose entries may be requeued and commit later). The checkers
//!   must allow these to appear in the final read or not.
//!
//! Recording is opt-in (`Option<Arc<HistoryRecorder>>`): when off, the bench
//! hot loop pays one branch. When on, serialization happens on the bench
//! task and the line is handed to a dedicated writer thread over a bounded
//! channel — a full channel drops the record and counts it instead of
//! applying backpressure, so chaos-window timing is never distorted by the
//! recorder. Checkers treat a non-zero drop count as "history incomplete"
//! and skip sequence-sensitive verdicts.
//!
//! The chaos final-read phase appends `final-read` records to the same file
//! after the bench finishes (no concurrency with the writer thread).

use celeriant_client_tokio::{ClientError, ServerError, WriteError};
use celeriant_msg::response::responses::WriteResponse;
use celeriant_wal::aggregate_key::AggregateKey;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use tokio::time::Instant;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum HistoryLine {
    Op(OpRecord),
    FinalRead(FinalReadRecord),
    Ryw(RywRecord),
    WatchDelivery(WatchDeliveryRecord),
}

/// u128 ids ride as JSON strings: serde's internally-tagged enum buffering
/// rejects u128 outright ("u128 is not supported"), and a JSON number that
/// size wouldn't survive Value-based readers either.
mod u128_str {
    pub fn serialize<S: serde::Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
        use serde::Deserialize;
        String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpOutcome {
    Ok,
    Fail,
    Info,
}

/// One write attempt. Retries of the same `client_seq` produce multiple
/// records; `t_start_ns`/`t_end_ns` are relative to the recorder's epoch.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpRecord {
    pub process: u32,
    #[serde(with = "u128_str")]
    pub org_id: u128,
    #[serde(with = "u128_str")]
    pub type_id: u128,
    #[serde(with = "u128_str")]
    pub agg_id: u128,
    #[serde(with = "u128_str")]
    pub client_id: u128,
    pub client_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub expected_version: Option<u64>,
    pub outcome: OpOutcome,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub error: Option<String>,
    /// The `max_aggregate_version` the server reported in the ack. `None`
    /// for non-Ok outcomes or when the server reported none (multi-aggregate
    /// writes). For the idempotent bench this equals `client_seq` (one seq =
    /// one batch); for OCC workloads it is the committed version.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub acked_max_aggregate_version: Option<u64>,
    pub t_start_ns: u64,
    pub t_end_ns: u64,
}

/// A read-your-writes probe: immediately after a write ack, read the same
/// aggregate and record whether the acked version was visible.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RywRecord {
    /// Which node served the read (pinned probes). None = un-pinned pool read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Groups multi-node reads of the same probe instant: records sharing
    /// (process, probe_id) observed the same acked write. A violation requires
    /// EVERY successful read in the group to be below the acked version —
    /// the acking node provably has visibility before the ack, so a single
    /// stale replica read is documented behavior, not a bug.
    #[serde(default)]
    pub probe_id: u64,
    pub process: u32,
    #[serde(with = "u128_str")]
    pub org_id: u128,
    #[serde(with = "u128_str")]
    pub type_id: u128,
    #[serde(with = "u128_str")]
    pub agg_id: u128,
    #[serde(with = "u128_str")]
    pub client_id: u128,
    /// The version the write was acknowledged at (= `client_seq` in the
    /// idempotent bench).
    pub acked_max_aggregate_version: u64,
    /// What the subsequent read returned. `None` means the read errored.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub observed_max_aggregate_version: Option<u64>,
    /// Error string from the read, if it errored.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub read_error: Option<String>,
    /// Timestamp (ns from recorder epoch) at which the probe completed.
    pub t_ns: u64,
}

/// A watch event delivered to a long-lived watcher.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WatchDeliveryRecord {
    /// Watcher id (index within the long-lived set).
    pub connection: u32,
    /// Connection epoch: bumped on every (re)connect. The no-dup/no-reorder
    /// contract holds within ONE TCP stream; across a reconnect the server
    /// legally re-delivers the boundary range (invariants.md watch section),
    /// so ordering checks must key on (connection, epoch).
    #[serde(default)]
    pub epoch: u32,
    #[serde(with = "u128_str")]
    pub org_id: u128,
    #[serde(with = "u128_str")]
    pub type_id: u128,
    #[serde(with = "u128_str")]
    pub agg_id: u128,
    pub from_version: u64,
    pub to_version: u64,
    /// Timestamp (ns from recorder epoch) at which this event was received.
    pub t_ns: u64,
}

/// Per-aggregate state read from one node after heal + quiesce.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FinalReadRecord {
    pub node: String,
    #[serde(with = "u128_str")]
    pub org_id: u128,
    #[serde(with = "u128_str")]
    pub type_id: u128,
    #[serde(with = "u128_str")]
    pub agg_id: u128,
    #[serde(with = "u128_str")]
    pub client_id: u128,
    /// Committed batch count on this node; `None` when the read errored.
    pub max_aggregate_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub error: Option<String>,
}

pub struct HistorySummary {
    pub path: PathBuf,
    pub records_written: u64,
    pub records_dropped: u64,
}

pub struct HistoryRecorder {
    tx: SyncSender<String>,
    epoch: Instant,
    dropped: AtomicU64,
    written: AtomicU64,
    path: PathBuf,
    writer: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

const CHANNEL_CAPACITY: usize = 65_536;

impl HistoryRecorder {
    pub fn create(path: &Path) -> std::io::Result<Self> {
        let file = std::fs::File::create(path)?;
        let (tx, rx) = sync_channel::<String>(CHANNEL_CAPACITY);
        let writer = std::thread::spawn(move || {
            let mut out = std::io::BufWriter::new(file);
            for line in rx {
                let _ = out.write_all(line.as_bytes());
                let _ = out.write_all(b"\n");
            }
            let _ = out.flush();
        });
        Ok(Self {
            tx,
            epoch: Instant::now(),
            dropped: AtomicU64::new(0),
            written: AtomicU64::new(0),
            path: path.to_path_buf(),
            writer: std::sync::Mutex::new(Some(writer)),
        })
    }

    /// Record one write attempt. `req_start` is the `Instant` captured just
    /// before the pool call; the end timestamp is taken here.
    /// `acked_max_aggregate_version` on an Ok outcome is the version the
    /// SERVER reported in the ack (single-aggregate writes); `None` for
    /// non-Ok outcomes or if the server reported none.
    pub fn record_op(
        &self,
        process: u32,
        key: &AggregateKey,
        client_id: u128,
        client_seq: u64,
        expected_version: Option<u64>,
        res: &Result<WriteResponse, ClientError>,
        req_start: Instant,
    ) {
        let (outcome, error) = match res {
            Ok(_) => (OpOutcome::Ok, None),
            Err(e) => classify_error(e),
        };
        let acked_max_aggregate_version = match res {
            Ok(r) => r.max_aggregate_version,
            Err(_) => None,
        };
        let record = HistoryLine::Op(OpRecord {
            process,
            org_id: key.org_id,
            type_id: key.aggregate_type_id,
            agg_id: key.aggregate_id,
            client_id,
            client_seq,
            expected_version,
            outcome,
            error,
            acked_max_aggregate_version,
            t_start_ns: req_start.duration_since(self.epoch).as_nanos() as u64,
            t_end_ns: self.epoch.elapsed().as_nanos() as u64,
        });
        self.push(&record);
    }

    /// Record a read-your-writes probe result.
    pub fn record_ryw(
        &self,
        process: u32,
        key: &AggregateKey,
        client_id: u128,
        acked_max_aggregate_version: u64,
        observed: Result<u64, String>,
        node: Option<String>,
        probe_id: u64,
    ) {
        let (observed_max_aggregate_version, read_error) = match observed {
            Ok(v) => (Some(v), None),
            Err(e) => (None, Some(e)),
        };
        let record = HistoryLine::Ryw(RywRecord {
            process,
            node,
            probe_id,
            org_id: key.org_id,
            type_id: key.aggregate_type_id,
            agg_id: key.aggregate_id,
            client_id,
            acked_max_aggregate_version,
            observed_max_aggregate_version,
            read_error,
            t_ns: self.epoch.elapsed().as_nanos() as u64,
        });
        self.push(&record);
    }

    /// Record a watch delivery event.
    pub fn record_watch_delivery(
        &self,
        connection: u32,
        epoch: u32,
        org_id: u128,
        type_id: u128,
        agg_id: u128,
        from_version: u64,
        to_version: u64,
    ) {
        let record = HistoryLine::WatchDelivery(WatchDeliveryRecord {
            connection,
            epoch,
            org_id,
            type_id,
            agg_id,
            from_version,
            to_version,
            t_ns: self.epoch.elapsed().as_nanos() as u64,
        });
        self.push(&record);
    }

    fn push(&self, line: &HistoryLine) {
        let json = match serde_json::to_string(line) {
            Ok(j) => j,
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        match self.tx.try_send(json) {
            Ok(()) => {
                self.written.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Flush and close the file. Call after all bench tasks have joined.
    pub fn finish(self) -> HistorySummary {
        let written = self.written.load(Ordering::Relaxed);
        let dropped = self.dropped.load(Ordering::Relaxed);
        let path = self.path.clone();
        drop(self.tx); // closes the channel; writer thread drains and exits
        if let Ok(mut guard) = self.writer.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
        HistorySummary { path, records_written: written, records_dropped: dropped }
    }
}

/// Server definitively rejected (did not commit) → `fail`. Anything whose
/// commit status is unknowable from the client → `info`.
///
/// Two masking effects force conservatism:
/// - `ReplicationError`, `FsyncError` and friends are info, not fail: the
///   write is fsynced before replication and pending entries are requeued —
///   it may commit later.
/// - The pool retries routing errors across nodes inside ONE logical call: a
///   write can commit on the leader, the connection drop pre-ACK, and the
///   pool's retry surface `NotLeader`/`ServerBusy` from the other node. At
///   this layer those cannot distinguish "never submitted" from "committed
///   then re-routed", so they are info. A `ServerError` response, by
///   contrast, came from a node that processed THIS request — definitive.
pub fn classify_error(e: &ClientError) -> (OpOutcome, Option<String>) {
    let (outcome, label) = match e {
        ClientError::Server(ServerError::Write { kind, .. }) => {
            let outcome = match kind {
                WriteError::EmptyEventsList
                | WriteError::ZeroEventType
                | WriteError::ClientIdempotencyViolation { .. }
                | WriteError::OptimisticConcurrencyViolation { .. }
                | WriteError::FailedToSerialiseDatablocks
                | WriteError::AggregateNotExists
                | WriteError::AggregateRecreateNotAllowed
                // Rejected against an existing in-flight attempt: THIS
                // attempt did not commit (the earlier one, recorded info,
                // may).
                | WriteError::InflightDuplicateWrite { .. }
                // Rejected at the gate, before enqueue means never submitted
                | WriteError::ReplicationBackpressure => OpOutcome::Fail,
                WriteError::ReplicationError
                | WriteError::FsyncError
                | WriteError::CacheAggregateClientError
                | WriteError::AggregateExistsCacheError => OpOutcome::Info,
            };
            (outcome, write_error_label(kind))
        }
        ClientError::NotLeader { .. } => (OpOutcome::Info, "NotLeader".to_string()),
        ClientError::ServerBusy => (OpOutcome::Info, "ServerBusy".to_string()),
        ClientError::IdentityRequired => (OpOutcome::Fail, "IdentityRequired".to_string()),
        ClientError::Server(_) => (OpOutcome::Fail, format!("{e}")),
        ClientError::ConnectionFailed(_) => (OpOutcome::Info, "ConnectionFailed".to_string()),
        ClientError::WireError(_) => (OpOutcome::Info, "WireError".to_string()),
        ClientError::ReadError(_) => (OpOutcome::Info, "ReadError".to_string()),
        ClientError::ProtocolError => (OpOutcome::Info, "ProtocolError".to_string()),
        ClientError::CorrelationMismatch { .. } => {
            (OpOutcome::Info, "CorrelationMismatch".to_string())
        }
        ClientError::ConnectionTimeout => (OpOutcome::Info, "ConnectionTimeout".to_string()),
        ClientError::RequestTimeout => (OpOutcome::Info, "RequestTimeout".to_string()),
        ClientError::IdentityError(_) => (OpOutcome::Info, "IdentityError".to_string()),
    };
    (outcome, Some(label))
}

fn write_error_label(kind: &WriteError) -> String {
    match kind {
        WriteError::EmptyEventsList => "EmptyEventsList",
        WriteError::ZeroEventType => "ZeroEventType",
        WriteError::ClientIdempotencyViolation { .. } => "ClientIdempotencyViolation",
        WriteError::OptimisticConcurrencyViolation { .. } => "OccConflict",
        WriteError::FailedToSerialiseDatablocks => "FailedToSerialiseDatablocks",
        WriteError::AggregateNotExists => "AggregateNotExists",
        WriteError::AggregateRecreateNotAllowed => "AggregateRecreateNotAllowed",
        WriteError::ReplicationError => "ReplicationError",
        WriteError::FsyncError => "FsyncError",
        WriteError::CacheAggregateClientError => "CacheAggregateClientError",
        WriteError::AggregateExistsCacheError => "AggregateExistsCacheError",
        WriteError::InflightDuplicateWrite { .. } => "InflightDuplicateWrite",
        WriteError::ReplicationBackpressure => "ReplicationBackpressure",
    }
    .to_string()
}

/// Append `final-read` records to an existing history file. Runs after the
/// bench recorder has finished — plain synchronous appends, no writer thread.
pub fn append_final_reads(path: &Path, records: &[FinalReadRecord]) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
    let mut buf = String::new();
    for r in records {
        if let Ok(json) = serde_json::to_string(&HistoryLine::FinalRead(r.clone())) {
            buf.push_str(&json);
            buf.push('\n');
        }
    }
    file.write_all(buf.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_record_roundtrips_through_json() {
        let line = HistoryLine::Op(OpRecord {
            process: 123,
            org_id: 1,
            type_id: 1,
            agg_id: u128::MAX, // full range must survive
            client_id: 124,
            client_seq: 1,
            expected_version: None,
            outcome: OpOutcome::Ok,
            error: None,
            acked_max_aggregate_version: Some(1),
            t_start_ns: 1_568_545,
            t_end_ns: 95_666_311,
        });
        let json = serde_json::to_string(&line).expect("serialize");
        let back: HistoryLine = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("deserialize {json}: {e}"));
        match back {
            HistoryLine::Op(op) => {
                assert_eq!(op.agg_id, u128::MAX);
                assert_eq!(op.outcome, OpOutcome::Ok);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn ryw_record_roundtrips_through_json() {
        let line = HistoryLine::Ryw(RywRecord {
            process: 7,
            node: None,
            probe_id: 7,
            org_id: 1,
            type_id: 1,
            agg_id: 99,
            client_id: 8,
            acked_max_aggregate_version: 42,
            observed_max_aggregate_version: Some(42),
            read_error: None,
            t_ns: 100_000,
        });
        let json = serde_json::to_string(&line).expect("serialize");
        let back: HistoryLine = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("deserialize {json}: {e}"));
        match back {
            HistoryLine::Ryw(r) => {
                assert_eq!(r.acked_max_aggregate_version, 42);
                assert_eq!(r.observed_max_aggregate_version, Some(42));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn watch_delivery_record_roundtrips_through_json() {
        let line = HistoryLine::WatchDelivery(WatchDeliveryRecord {
            connection: 3,
            epoch: 0,
            org_id: 1,
            type_id: 1,
            agg_id: 55,
            from_version: 10,
            to_version: 20,
            t_ns: 500_000,
        });
        let json = serde_json::to_string(&line).expect("serialize");
        let back: HistoryLine = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("deserialize {json}: {e}"));
        assert!(matches!(back, HistoryLine::WatchDelivery(_)));
    }

    #[test]
    fn final_read_record_roundtrips_through_json() {
        let line = HistoryLine::FinalRead(FinalReadRecord {
            node: "192.168.88.213".into(),
            org_id: 1,
            type_id: 1,
            agg_id: 499,
            client_id: 500,
            max_aggregate_version: Some(264),
            error: None,
        });
        let json = serde_json::to_string(&line).expect("serialize");
        let back: HistoryLine = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("deserialize {json}: {e}"));
        assert!(matches!(back, HistoryLine::FinalRead(_)));
    }
}

/// Parse a history file back into lines, skipping unparseable rows (counted).
pub fn read_history(path: &Path) -> std::io::Result<(Vec<HistoryLine>, u64)> {
    use std::io::BufRead;
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut lines = Vec::new();
    let mut unparseable = 0u64;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<HistoryLine>(&line) {
            Ok(l) => lines.push(l),
            Err(_) => unparseable += 1,
        }
    }
    Ok((lines, unparseable))
}
