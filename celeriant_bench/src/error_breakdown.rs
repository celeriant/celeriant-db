//! Per-variant tally of the `ClientError`s a bench loop saw.
//!
//! A chaos scenario produces tens of thousands of failures. One stderr line each
//! is unreadable and nothing structured survives into the run JSON. This keeps a
//! fixed-size counter array plus the first message seen per key: recording costs
//! one relaxed `fetch_add`, and a key allocates exactly once (its example text).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use celeriant_client_tokio::{ClientError, ServerError};

pub const KEY_COUNT: usize = 29;

/// The reported error classes. The discriminant is the index into
/// `ErrorBreakdown`'s arrays, so `ALL` must stay in declaration order;
/// `all_is_in_discriminant_order` asserts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum ErrorKey {
    PoolTimeout,
    ConnectionLostAfterSend,
    ConnectionTimeout,
    ConnectionFailed,
    NotLeaderHint,
    NotLeaderNoHint,
    ServerBusy,
    RequestTimeout,
    WireError,
    ReadError,
    ProtocolError,
    CorrelationMismatch,
    IdentityRequired,
    IdentityError,
    DictUnavailable,
    InvalidShardRange,
    ServerRead,
    ServerWrite,
    ServerSchema,
    ServerDelete,
    ServerTrim,
    ServerWatch,
    ServerDetails,
    ServerAuth,
    ServerList,
    ServerReplication,
    ServerShardRouting,
    ServerUnknown,
    /// A `ClientError` variant added since this match was written.
    Unclassified,
}

impl ErrorKey {
    pub const ALL: [ErrorKey; KEY_COUNT] = [
        ErrorKey::PoolTimeout,
        ErrorKey::ConnectionLostAfterSend,
        ErrorKey::ConnectionTimeout,
        ErrorKey::ConnectionFailed,
        ErrorKey::NotLeaderHint,
        ErrorKey::NotLeaderNoHint,
        ErrorKey::ServerBusy,
        ErrorKey::RequestTimeout,
        ErrorKey::WireError,
        ErrorKey::ReadError,
        ErrorKey::ProtocolError,
        ErrorKey::CorrelationMismatch,
        ErrorKey::IdentityRequired,
        ErrorKey::IdentityError,
        ErrorKey::DictUnavailable,
        ErrorKey::InvalidShardRange,
        ErrorKey::ServerRead,
        ErrorKey::ServerWrite,
        ErrorKey::ServerSchema,
        ErrorKey::ServerDelete,
        ErrorKey::ServerTrim,
        ErrorKey::ServerWatch,
        ErrorKey::ServerDetails,
        ErrorKey::ServerAuth,
        ErrorKey::ServerList,
        ErrorKey::ServerReplication,
        ErrorKey::ServerShardRouting,
        ErrorKey::ServerUnknown,
        ErrorKey::Unclassified,
    ];

    /// `ClientError` is `#[non_exhaustive]`, so the compiler no longer catches
    /// a new variant here. It lands in `Unclassified`, named in the run JSON.
    pub fn of(e: &ClientError) -> Self {
        match e {
            ClientError::PoolTimeout { .. } => ErrorKey::PoolTimeout,
            ClientError::ConnectionLostAfterSend(_) => ErrorKey::ConnectionLostAfterSend,
            ClientError::ConnectionTimeout => ErrorKey::ConnectionTimeout,
            ClientError::ConnectionFailed(_) => ErrorKey::ConnectionFailed,
            // Hint presence is the interesting split: a hintless NotLeader ends
            // the routing walk, a hinted one redirects.
            ClientError::NotLeader { leader_address: Some(_), .. } => ErrorKey::NotLeaderHint,
            ClientError::NotLeader { leader_address: None, .. } => ErrorKey::NotLeaderNoHint,
            ClientError::ServerBusy => ErrorKey::ServerBusy,
            ClientError::RequestTimeout => ErrorKey::RequestTimeout,
            ClientError::WireError(_) => ErrorKey::WireError,
            ClientError::ReadError(_) => ErrorKey::ReadError,
            ClientError::ProtocolError => ErrorKey::ProtocolError,
            ClientError::CorrelationMismatch { .. } => ErrorKey::CorrelationMismatch,
            ClientError::IdentityRequired => ErrorKey::IdentityRequired,
            ClientError::IdentityError(_) => ErrorKey::IdentityError,
            ClientError::DictUnavailable { .. } => ErrorKey::DictUnavailable,
            ClientError::InvalidShardRange { .. } => ErrorKey::InvalidShardRange,
            // Server errors collapse to their family; the error code and message
            // survive in the family's first example.
            ClientError::Server(s) => match s {
                ServerError::Read { .. } => ErrorKey::ServerRead,
                ServerError::Write { .. } => ErrorKey::ServerWrite,
                ServerError::Schema { .. } => ErrorKey::ServerSchema,
                ServerError::Delete { .. } => ErrorKey::ServerDelete,
                ServerError::Trim { .. } => ErrorKey::ServerTrim,
                ServerError::Watch { .. } => ErrorKey::ServerWatch,
                ServerError::Details { .. } => ErrorKey::ServerDetails,
                ServerError::Auth { .. } => ErrorKey::ServerAuth,
                ServerError::List { .. } => ErrorKey::ServerList,
                ServerError::Replication { .. } => ErrorKey::ServerReplication,
                ServerError::ShardRouting { .. } => ErrorKey::ServerShardRouting,
                ServerError::Unknown { .. } => ErrorKey::ServerUnknown,
            },
            _ => ErrorKey::Unclassified,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ErrorKey::PoolTimeout => "PoolTimeout",
            ErrorKey::ConnectionLostAfterSend => "ConnectionLostAfterSend",
            ErrorKey::ConnectionTimeout => "ConnectionTimeout",
            ErrorKey::ConnectionFailed => "ConnectionFailed",
            ErrorKey::NotLeaderHint => "NotLeader(hint)",
            ErrorKey::NotLeaderNoHint => "NotLeader(no hint)",
            ErrorKey::ServerBusy => "ServerBusy",
            ErrorKey::RequestTimeout => "RequestTimeout",
            ErrorKey::WireError => "WireError",
            ErrorKey::ReadError => "ReadError",
            ErrorKey::ProtocolError => "ProtocolError",
            ErrorKey::CorrelationMismatch => "CorrelationMismatch",
            ErrorKey::IdentityRequired => "IdentityRequired",
            ErrorKey::IdentityError => "IdentityError",
            ErrorKey::DictUnavailable => "DictUnavailable",
            ErrorKey::InvalidShardRange => "InvalidShardRange",
            ErrorKey::ServerRead => "Server(read)",
            ErrorKey::ServerWrite => "Server(write)",
            ErrorKey::ServerSchema => "Server(schema)",
            ErrorKey::ServerDelete => "Server(delete)",
            ErrorKey::ServerTrim => "Server(trim)",
            ErrorKey::ServerWatch => "Server(watch)",
            ErrorKey::ServerDetails => "Server(details)",
            ErrorKey::ServerAuth => "Server(auth)",
            ErrorKey::ServerList => "Server(list)",
            ErrorKey::ServerReplication => "Server(replication)",
            ErrorKey::ServerShardRouting => "Server(shard_routing)",
            ErrorKey::ServerUnknown => "Server(unknown)",
            ErrorKey::Unclassified => "Unclassified",
        }
    }
}

/// One non-zero key, as it appears in the scenario JSON.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ErrorKindCount {
    pub kind: &'static str,
    pub count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_example: Option<String>,
}

#[derive(Debug)]
pub struct ErrorBreakdown {
    counts: [AtomicU64; KEY_COUNT],
    first: [Mutex<Option<String>>; KEY_COUNT],
}

impl Default for ErrorBreakdown {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            first: std::array::from_fn(|_| Mutex::new(None)),
        }
    }
}

impl ErrorBreakdown {
    /// Exactly one caller per key sees the count go from 0, so the example text
    /// is rendered, and the lock taken, once per key for the whole run.
    pub fn record(&self, e: &ClientError) {
        let i = ErrorKey::of(e) as usize;
        if self.counts[i].fetch_add(1, Ordering::Relaxed) == 0
            && let Ok(mut slot) = self.first[i].lock()
        {
            *slot = Some(e.to_string());
        }
    }

    pub fn total(&self) -> u64 {
        self.counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// Non-zero keys only, heaviest first, ties broken by name so the output is
    /// stable across runs.
    pub fn snapshot(&self) -> Vec<ErrorKindCount> {
        let mut rows: Vec<ErrorKindCount> = ErrorKey::ALL
            .iter()
            .filter_map(|&k| {
                let count = self.counts[k as usize].load(Ordering::Relaxed);
                (count > 0).then(|| ErrorKindCount {
                    kind: k.name(),
                    count,
                    first_example: self.first[k as usize].lock().ok().and_then(|s| s.clone()),
                })
            })
            .collect();
        rows.sort_unstable_by(|a, b| b.count.cmp(&a.count).then_with(|| a.kind.cmp(b.kind)));
        rows
    }
}

/// The one block a bench loop prints instead of a line per failed request.
pub fn print_error_summary(rows: &[ErrorKindCount]) {
    if rows.is_empty() {
        return;
    }
    let total: u64 = rows.iter().map(|r| r.count).sum();
    println!("  Errors by kind — {total} across {} kinds", rows.len());
    for r in rows {
        println!(
            "    {:<24} {:>9}  {}",
            r.kind,
            r.count,
            r.first_example.as_deref().unwrap_or("")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celeriant_client_tokio::server_error::{
        AuthError, DeleteError, DetailsError, ReadError, SchemaError, TrimError, WatchError, WriteError,
    };
    use celeriant_crypto::CryptoError;
    use celeriant_msg::read_wire_data_error::ReadWireDataError;
    use celeriant_wire::network::wire_error::WireError;
    use std::sync::Arc;

    fn io() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset")
    }

    fn server(kind: ServerError) -> ClientError {
        ClientError::Server(kind)
    }

    #[test]
    fn every_variant_lands_on_its_own_key() {
        let cases: Vec<(ClientError, &str)> = vec![
            (ClientError::PoolTimeout { address: "1.2.3.4:9000".into() }, "PoolTimeout"),
            (ClientError::ConnectionLostAfterSend(io()), "ConnectionLostAfterSend"),
            (ClientError::ConnectionTimeout, "ConnectionTimeout"),
            (ClientError::ConnectionFailed(io()), "ConnectionFailed"),
            (
                ClientError::NotLeader { leader_address: Some("1.2.3.4:9000".into()), error_message: String::new() },
                "NotLeader(hint)",
            ),
            (
                ClientError::NotLeader { leader_address: None, error_message: String::new() },
                "NotLeader(no hint)",
            ),
            (ClientError::ServerBusy, "ServerBusy"),
            (ClientError::RequestTimeout, "RequestTimeout"),
            (ClientError::WireError(WireError::UnsupportedProtocol(9)), "WireError"),
            (ClientError::ReadError(ReadWireDataError::UnknownMessageType(7)), "ReadError"),
            (ClientError::ProtocolError, "ProtocolError"),
            (ClientError::CorrelationMismatch { sent: Some(1), received: Some(2) }, "CorrelationMismatch"),
            (ClientError::IdentityRequired, "IdentityRequired"),
            (ClientError::IdentityError(CryptoError::InvalidNonce), "IdentityError"),
            (ClientError::DictUnavailable { sha: "deadbeef".into() }, "DictUnavailable"),
            (ClientError::InvalidShardRange { start_shard: 5, max_shard_hint: 4 }, "InvalidShardRange"),
            (server(ServerError::Read { kind: ReadError::AggregateNotExists, error_message: String::new() }), "Server(read)"),
            (server(ServerError::Write { kind: WriteError::FsyncError, error_message: String::new() }), "Server(write)"),
            (server(ServerError::Schema { kind: SchemaError::Invalid, error_message: String::new() }), "Server(schema)"),
            (server(ServerError::Delete { kind: DeleteError::CacheError, error_message: String::new() }), "Server(delete)"),
            (server(ServerError::Trim { kind: TrimError::CacheError, error_message: String::new() }), "Server(trim)"),
            (server(ServerError::Watch { kind: WatchError::ReadIo, error_message: String::new() }), "Server(watch)"),
            (server(ServerError::Details { kind: DetailsError::CacheError, error_message: String::new() }), "Server(details)"),
            (server(ServerError::Auth { kind: AuthError::InvalidKey, error_message: String::new() }), "Server(auth)"),
            (server(ServerError::List { error_code: 1, error_message: String::new() }), "Server(list)"),
            (server(ServerError::Replication { error_code: 2, error_message: String::new() }), "Server(replication)"),
            (server(ServerError::ShardRouting { error_code: 3, error_message: String::new() }), "Server(shard_routing)"),
            (server(ServerError::Unknown { error_code: 4, error_message: String::new() }), "Server(unknown)"),
        ];

        // Every key but `Unclassified`, which no variant that exists today reaches.
        assert_eq!(cases.len(), KEY_COUNT - 1, "the table must cover every reachable key");
        let breakdown = ErrorBreakdown::default();
        for (err, expected) in &cases {
            assert_eq!(ErrorKey::of(err).name(), *expected);
            breakdown.record(err);
        }
        assert_eq!(breakdown.snapshot().len(), KEY_COUNT - 1, "every key must report once");
    }

    #[test]
    fn all_is_in_discriminant_order() {
        for (i, k) in ErrorKey::ALL.iter().enumerate() {
            assert_eq!(*k as usize, i, "{} is out of order", k.name());
        }
    }

    #[test]
    fn the_first_example_is_kept_and_later_ones_ignored() {
        let breakdown = ErrorBreakdown::default();
        breakdown.record(&ClientError::PoolTimeout { address: "first:1".into() });
        breakdown.record(&ClientError::PoolTimeout { address: "second:2".into() });

        let rows = breakdown.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].count, 2);
        let example = rows[0].first_example.as_deref().unwrap();
        assert!(example.contains("first:1"), "got {example}");
    }

    #[test]
    fn heaviest_key_sorts_first() {
        let breakdown = ErrorBreakdown::default();
        breakdown.record(&ClientError::ConnectionTimeout);
        for _ in 0..3 {
            breakdown.record(&ClientError::ServerBusy);
        }
        let rows = breakdown.snapshot();
        assert_eq!(rows[0].kind, "ServerBusy");
        assert_eq!(rows[0].count, 3);
        assert_eq!(rows[1].kind, "ConnectionTimeout");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_recording_loses_nothing() {
        const TASKS: u64 = 8;
        const PER_TASK: u64 = 2_000;

        let breakdown = Arc::new(ErrorBreakdown::default());
        let mut handles = Vec::new();
        for t in 0..TASKS {
            let breakdown = Arc::clone(&breakdown);
            handles.push(tokio::spawn(async move {
                for _ in 0..PER_TASK {
                    // Two keys so the first-example race is exercised on both.
                    if t % 2 == 0 {
                        breakdown.record(&ClientError::ConnectionTimeout);
                    } else {
                        breakdown.record(&ClientError::RequestTimeout);
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(breakdown.total(), TASKS * PER_TASK);
        let rows = breakdown.snapshot();
        assert_eq!(rows.len(), 2);
        for r in &rows {
            assert_eq!(r.count, TASKS / 2 * PER_TASK);
            assert!(r.first_example.is_some(), "{} lost its example", r.kind);
        }
    }
}
