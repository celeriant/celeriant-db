//! White-box contracts for the post-handshake drain.
//!
//! `tests/accept_bounded_completion.rs` pins the outside-visible property:
//! `ktls_accept` returns in bounded time when the peer's first app-data record
//! is still incomplete. It deliberately asserts nothing about the plaintext of
//! a record that never finishes arriving. These tests cover what it cannot:
//!
//! - the record that DOES complete after one more read — its plaintext must
//!   come back byte-exact across the split, with nothing lost or duplicated;
//! - the record that never completes — a typed error at ~`DRAIN_DEADLINE`,
//!   while the executor keeps running other tasks (a yield, not a spin);
//! - the peer that closes mid-record — a typed EOF error, promptly.
//!
//! The client is a plain `std::net::TcpStream` driving buffered rustls, so the
//! test decides which bytes hit the wire in which `write()`.

use std::cell::Cell;
use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpStream as StdTcpStream};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use rustls_pki_types::ServerName;

use crate::tests::test_tls_configs;
use crate::{DRAIN_DEADLINE, KtlsError, ktls_accept};

/// Outside the executor under test, and generously past `DRAIN_DEADLINE`:
/// anything reaching this is a hang, not slowness.
const WATCHDOG: Duration = Duration::from_secs(20);

/// Long enough that a byte-exactness check across a split is meaningful, and
/// patterned so a duplicated or dropped chunk cannot pass unnoticed.
fn payload(tag: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(tag)).collect()
}

/// What the accept task reports back: the trailing plaintext (or the typed
/// error), plus how many times a co-scheduled task got to run while
/// `ktls_accept` was in flight.
struct AcceptReport {
    outcome: Result<Vec<u8>, KtlsError>,
    ticks: u64,
}

/// Accept exactly one connection on a fresh executor on its own OS thread and
/// run the real `ktls_accept` on it. A second task ticks every millisecond: if
/// the drain ever spins without yielding, the tick count stays at zero and the
/// executor is provably frozen.
///
/// The executor handle is dropped on purpose — a wedged `ktls_accept` must not
/// take libtest's ability to report the failure down with it.
fn spawn_accept_server(server_config: Arc<rustls::ServerConfig>) -> (SocketAddr, Receiver<AcceptReport>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (out_tx, out_rx) = mpsc::channel();

    let handle = glommio::LocalExecutorBuilder::default()
        .name("ktls-drain")
        .spawn(move || async move {
            let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let stream = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);

            let ticks = Rc::new(Cell::new(0u64));
            let ticker = ticks.clone();
            glommio::spawn_local(async move {
                loop {
                    glommio::timer::sleep(Duration::from_millis(1)).await;
                    ticker.set(ticker.get() + 1);
                }
            })
            .detach();

            let outcome = match ktls_accept(stream, server_config).await {
                Ok((_stream, trailing)) => Ok(trailing),
                Err(e) => Err(e),
            };
            let _ = out_tx.send(AcceptReport { outcome, ticks: ticks.get() });
        })
        .expect("spawn server executor");
    drop(handle);

    let addr = addr_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server executor failed to bind");
    (addr, out_rx)
}

/// A client that finishes a real handshake but sends nothing after the
/// ClientHello: the final flight and each app-data record are held in memory
/// so the test controls the framing on the wire.
struct HeldClient {
    sock: StdTcpStream,
    flight: Vec<u8>,
    records: Vec<Vec<u8>>,
}

fn handshake_holding_final_flight(
    addr: SocketAddr,
    client_config: Arc<rustls::ClientConfig>,
    payloads: &[&[u8]],
) -> HeldClient {
    let mut sock = StdTcpStream::connect(addr).expect("connect");
    sock.set_nodelay(true).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    let name = ServerName::try_from("localhost").unwrap();
    let mut conn = rustls::ClientConnection::new(client_config, name).expect("client conn");

    let mut hello = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut hello).unwrap();
    }
    sock.write_all(&hello).unwrap();
    sock.flush().unwrap();

    let mut flight = Vec::new();
    while conn.is_handshaking() {
        let n = conn.read_tls(&mut sock).expect("read_tls");
        assert!(n > 0, "server closed during handshake");
        conn.process_new_packets().expect("process_new_packets");
        while conn.wants_write() {
            conn.write_tls(&mut flight).unwrap();
        }
    }
    assert!(!flight.is_empty(), "client produced no final handshake flight");

    // One record per payload — each is encrypted and framed on its own.
    let records = payloads
        .iter()
        .map(|p| {
            conn.writer().write_all(p).unwrap();
            let mut record = Vec::new();
            while conn.wants_write() {
                conn.write_tls(&mut record).unwrap();
            }
            assert!(record.len() > 5, "expected one framed app-data record");
            record
        })
        .collect();

    HeldClient { sock, flight, records }
}

impl HeldClient {
    fn send(&mut self, bytes: &[u8]) {
        self.sock.write_all(bytes).unwrap();
        self.sock.flush().unwrap();
    }

    /// The app-data records back to back, as they would appear on the wire.
    fn all_records(&self) -> Vec<u8> {
        self.records.concat()
    }
}

fn expect_report(rx: &Receiver<AcceptReport>, what: &str) -> AcceptReport {
    match rx.recv_timeout(WATCHDOG) {
        Ok(report) => report,
        Err(RecvTimeoutError::Timeout) => {
            panic!("ktls_accept did not return within {}s ({what})", WATCHDOG.as_secs())
        }
        Err(RecvTimeoutError::Disconnected) => panic!("server task died without an outcome ({what})"),
    }
}

/// kTLS install is a kernel feature, not the property under test.
fn trailing_or_skip(outcome: Result<Vec<u8>, KtlsError>) -> Option<Vec<u8>> {
    match outcome {
        Ok(trailing) => Some(trailing),
        Err(e @ (KtlsError::SetsockoptFailed(_) | KtlsError::KernelNotSupported)) => {
            eprintln!("kTLS install unavailable on this host, tolerated: {e}");
            None
        }
        Err(e) => panic!("unexpected error from ktls_accept: {e:?}"),
    }
}

/// The case the blind contract cannot check: a first app-data record split
/// across two `write()`s must be reassembled, and its plaintext handed back
/// byte-exact. Every interesting split point is exercised — inside the 5-byte
/// header (length not even known yet), at the header boundary, and inside the
/// body.
#[test]
fn split_record_is_reassembled_byte_exact() {
    let (server_config, client_config) = test_tls_configs();
    let plaintext = payload(0x5a, 400);

    let record_len = {
        let (addr, out_rx) = spawn_accept_server(server_config.clone());
        let client = handshake_holding_final_flight(addr, client_config.clone(), &[&plaintext]);
        let len = client.records[0].len();
        drop(client);
        drop(out_rx);
        len
    };

    for split in [1usize, 3, 5, 6, record_len - 1] {
        let (addr, out_rx) = spawn_accept_server(server_config.clone());
        let mut client = handshake_holding_final_flight(addr, client_config.clone(), &[&plaintext]);
        let record = client.records[0].clone();

        let mut head = client.flight.clone();
        head.extend_from_slice(&record[..split]);
        client.send(&head);
        // Wide enough that the server has certainly consumed the fragment and
        // parked on a read before the rest arrives.
        std::thread::sleep(Duration::from_millis(150));
        client.send(&record[split..]);

        let report = expect_report(&out_rx, "split record");
        if let Some(trailing) = trailing_or_skip(report.outcome) {
            assert_eq!(
                trailing, plaintext,
                "trailing plaintext must survive a record split at byte {split} intact"
            );
        }
        drop(client);
    }
}

/// Two records, the second one split: the first record's plaintext must not be
/// lost while waiting for the second, and must not be replayed once it lands.
#[test]
fn split_second_record_preserves_first_record_bytes() {
    let (server_config, client_config) = test_tls_configs();
    let first = payload(0x11, 300);
    let second = payload(0x22, 200);

    let (addr, out_rx) = spawn_accept_server(server_config);
    let mut client = handshake_holding_final_flight(addr, client_config, &[&first, &second]);

    let wire = client.all_records();
    let split = client.records[0].len() + 7; // first record whole, second cut open
    let mut head = client.flight.clone();
    head.extend_from_slice(&wire[..split]);
    client.send(&head);
    std::thread::sleep(Duration::from_millis(150));
    client.send(&wire[split..]);

    let report = expect_report(&out_rx, "split second record");
    if let Some(trailing) = trailing_or_skip(report.outcome) {
        let mut expected = first.clone();
        expected.extend_from_slice(&second);
        assert_eq!(
            trailing, expected,
            "both records' plaintext must appear once, in order, across the split"
        );
    }
    drop(client);
}

/// A fragment whose remainder never arrives must fail with a typed error at
/// roughly `DRAIN_DEADLINE` — and the executor must stay alive throughout,
/// which is what separates a bounded wait from the busy-spin that froze a
/// shard core in production.
#[test]
fn abandoned_fragment_times_out_while_executor_keeps_running() {
    let (server_config, client_config) = test_tls_configs();
    let plaintext = payload(0x77, 400);

    let (addr, out_rx) = spawn_accept_server(server_config);
    let mut client = handshake_holding_final_flight(addr, client_config, &[&plaintext]);

    let mut head = client.flight.clone();
    head.extend_from_slice(&client.records[0][..6]);
    let started = Instant::now();
    client.send(&head);

    let report = expect_report(&out_rx, "abandoned fragment");
    let elapsed = started.elapsed();

    assert!(
        matches!(report.outcome, Err(KtlsError::TrailingRecordTimeout)),
        "expected TrailingRecordTimeout, got {:?}",
        report.outcome.map(|t| t.len())
    );
    assert!(
        elapsed >= DRAIN_DEADLINE && elapsed < DRAIN_DEADLINE + Duration::from_secs(3),
        "must fail at the deadline, not before or much after: {elapsed:?}"
    );
    // A spinning drain never returns to the scheduler, so a co-scheduled 1ms
    // ticker cannot advance. Half the theoretical ticks is ample slack.
    assert!(
        report.ticks > DRAIN_DEADLINE.as_millis() as u64 / 2,
        "executor was starved during the wait: only {} ticks in {elapsed:?}",
        report.ticks
    );
    drop(client);
}

/// Peer closes with a record half sent: the remainder can never arrive, so the
/// drain must fail immediately rather than sit out the deadline.
#[test]
fn eof_mid_fragment_fails_promptly() {
    let (server_config, client_config) = test_tls_configs();
    let plaintext = payload(0x33, 400);

    let (addr, out_rx) = spawn_accept_server(server_config);
    let mut client = handshake_holding_final_flight(addr, client_config, &[&plaintext]);

    let mut head = client.flight.clone();
    head.extend_from_slice(&client.records[0][..6]);
    let started = Instant::now();
    client.send(&head);
    client.sock.shutdown(Shutdown::Write).unwrap();

    let report = expect_report(&out_rx, "eof mid fragment");
    let elapsed = started.elapsed();

    match report.outcome {
        Err(KtlsError::Io(e)) => assert_eq!(
            e.kind(),
            std::io::ErrorKind::UnexpectedEof,
            "expected UnexpectedEof, got {e:?}"
        ),
        other => panic!("expected a typed EOF error, got {:?}", other.map(|t| t.len())),
    }
    assert!(
        elapsed < DRAIN_DEADLINE,
        "EOF is terminal — the drain must not wait out the deadline: {elapsed:?}"
    );
    drop(client);
}
