// REVIEW-EVIDENCE — adversarial review of the post-handshake drain fix.
// Everything here is evidence for a review claim; nothing under review is modified.

use futures_lite::AsyncReadExt;
use std::io::Write;
use std::net::{SocketAddr, TcpListener as StdTcpListener, TcpStream as StdTcpStream};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use rustls_pki_types::ServerName;

use crate::tests::test_tls_configs;
use crate::{KtlsError, drain_discard, ktls_accept, ktls_connect};

const WATCHDOG: Duration = Duration::from_secs(20);
/// Wide enough that the peer has certainly parked on a read before the rest lands.
const SETTLE: Duration = Duration::from_millis(150);

fn payload(tag: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| (i as u8).wrapping_mul(31).wrapping_add(tag)).collect()
}

struct Report {
    outcome: Result<Vec<u8>, KtlsError>,
}

/// kTLS install is a kernel feature, not the property under test.
fn trailing_or_skip(outcome: Result<Vec<u8>, KtlsError>) -> Option<Vec<u8>> {
    match outcome {
        Ok(t) => Some(t),
        Err(e @ (KtlsError::SetsockoptFailed(_) | KtlsError::KernelNotSupported)) => {
            eprintln!("kTLS install unavailable on this host, tolerated: {e}");
            None
        }
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

fn expect(rx: &Receiver<Report>, what: &str) -> Report {
    match rx.recv_timeout(WATCHDOG) {
        Ok(r) => r,
        Err(RecvTimeoutError::Timeout) => panic!("no outcome within {}s ({what})", WATCHDOG.as_secs()),
        Err(RecvTimeoutError::Disconnected) => panic!("task died without an outcome ({what})"),
    }
}

// ---------------------------------------------------------------------------
// Server-side harness (mirrors the white-box one; that module's helpers are private)
// ---------------------------------------------------------------------------

fn spawn_accept(server_config: Arc<rustls::ServerConfig>) -> (SocketAddr, Receiver<Report>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (out_tx, out_rx) = mpsc::channel();
    let handle = glommio::LocalExecutorBuilder::default()
        .name("review-accept")
        .spawn(move || async move {
            let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let stream = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            let outcome = ktls_accept(stream, server_config).await.map(|(_s, t)| t);
            let _ = out_tx.send(Report { outcome });
        })
        .expect("spawn executor");
    drop(handle);
    let addr = addr_rx.recv_timeout(Duration::from_secs(10)).expect("bind");
    (addr, out_rx)
}

/// Like `spawn_accept`, but after `ktls_accept` returns it keeps reading the
/// stream — now decrypted by the kernel — until `want` total plaintext bytes
/// have been seen or the peer goes quiet. Reports (trailing, kernel-read).
fn spawn_accept_then_read(
    server_config: Arc<rustls::ServerConfig>,
    want: usize,
) -> (SocketAddr, Receiver<Result<(Vec<u8>, Vec<u8>), KtlsError>>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (out_tx, out_rx) = mpsc::channel();
    let handle = glommio::LocalExecutorBuilder::default()
        .name("review-totality")
        .spawn(move || async move {
            let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let stream = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);

            let outcome = match ktls_accept(stream, server_config).await {
                Err(e) => Err(e),
                Ok((mut stream, trailing)) => {
                    let need = want.saturating_sub(trailing.len());
                    let mut kernel = Vec::new();
                    let _ = glommio::timer::timeout(Duration::from_secs(5), async {
                        let mut chunk = vec![0u8; 16 * 1024];
                        while kernel.len() < need {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => kernel.extend_from_slice(&chunk[..n]),
                            }
                        }
                        Ok::<_, glommio::GlommioError<()>>(())
                    })
                    .await;
                    Ok((trailing, kernel))
                }
            };
            let _ = out_tx.send(outcome);
        })
        .expect("spawn executor");
    drop(handle);
    let addr = addr_rx.recv_timeout(Duration::from_secs(10)).expect("bind");
    (addr, out_rx)
}

struct HeldClient {
    sock: StdTcpStream,
    flight: Vec<u8>,
    records: Vec<Vec<u8>>,
    conn: rustls::ClientConnection,
}

fn client_holding_flight(
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

    let records = payloads
        .iter()
        .map(|p| {
            conn.writer().write_all(p).unwrap();
            let mut record = Vec::new();
            while conn.wants_write() {
                conn.write_tls(&mut record).unwrap();
            }
            record
        })
        .collect();

    HeldClient { sock, flight, records, conn }
}

impl HeldClient {
    fn send(&mut self, bytes: &[u8]) {
        self.sock.write_all(bytes).unwrap();
        self.sock.flush().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Claim: a maximum-size TLS record (16384 plaintext = 16406 on the wire) does
// NOT deadlock against the 16KiB incoming buffer. read_into doubles the buffer
// when it fills, so a record larger than the initial window still makes
// progress. Split at the last byte forces the whole record to be resident.
// ---------------------------------------------------------------------------
#[test]
fn max_size_tls_record_split_reassembles_despite_16k_buffer() {
    let (server_config, client_config) = test_tls_configs();
    // rustls' maximum plaintext fragment; the framed record exceeds the 16KiB
    // buffer `drive_handshake_server` starts with.
    let plaintext = payload(0xa5, 16384);

    for split in [5usize, 16000] {
        let (addr, out_rx) = spawn_accept(server_config.clone());
        let mut client = client_holding_flight(addr, client_config.clone(), &[&plaintext]);
        let record = client.records[0].clone();
        assert!(
            record.len() > 16 * 1024,
            "expected a record larger than the initial buffer, got {}",
            record.len()
        );

        let mut head = client.flight.clone();
        head.extend_from_slice(&record[..split]);
        client.send(&head);
        std::thread::sleep(SETTLE);
        client.send(&record[split..]);

        let report = expect(&out_rx, "max-size record");
        if let Some(trailing) = trailing_or_skip(report.outcome) {
            assert_eq!(trailing.len(), plaintext.len(), "length mismatch at split {split}");
            assert_eq!(trailing, plaintext, "max-size record corrupted at split {split}");
        }
        drop(client);
    }
}

// ---------------------------------------------------------------------------
// Claim: a header torn at EVERY interior offset reassembles. The white-box
// tests cover 1 and 3; 2 and 4 complete the set (the record length is not yet
// knowable at any of them).
// ---------------------------------------------------------------------------
#[test]
fn header_torn_at_offsets_two_and_four_reassembles() {
    let (server_config, client_config) = test_tls_configs();
    let plaintext = payload(0xc3, 512);

    for split in [2usize, 4] {
        let (addr, out_rx) = spawn_accept(server_config.clone());
        let mut client = client_holding_flight(addr, client_config.clone(), &[&plaintext]);
        let record = client.records[0].clone();

        let mut head = client.flight.clone();
        head.extend_from_slice(&record[..split]);
        client.send(&head);
        std::thread::sleep(SETTLE);
        client.send(&record[split..]);

        let report = expect(&out_rx, "torn header");
        if let Some(trailing) = trailing_or_skip(report.outcome) {
            assert_eq!(trailing, plaintext, "torn header at byte {split} lost data");
        }
        drop(client);
    }
}

// ---------------------------------------------------------------------------
// TOTALITY CONTRACT — the permanent guard against the drain over-reading.
//
// A split record followed by three MORE complete records, all arriving in one
// later write. The bounded drain reads no further than the split record's last
// byte, so records b/c/d stay in the kernel's receive queue and are decrypted
// by kTLS instead. Where the split falls is an implementation detail; what must
// hold is that the two halves CONCATENATE to everything the peer sent, exactly
// once, in order.
//
// This fails in both directions: if the drain under-reads, the split record is
// unrecoverable (userspace ate its head, the kernel cannot decrypt the rest)
// and the totality breaks; if the drain over-reads, `trailing` grows without a
// bound — the loop-1 defect.
// ---------------------------------------------------------------------------
#[test]
fn drain_and_kernel_together_yield_every_record_exactly_once() {
    let (server_config, client_config) = test_tls_configs();
    let a = payload(0x01, 300);
    let b = payload(0x02, 100);
    let c = payload(0x03, 700);
    let d = payload(0x04, 50);
    let mut expected = a.clone();
    expected.extend_from_slice(&b);
    expected.extend_from_slice(&c);
    expected.extend_from_slice(&d);

    let (addr, out_rx) = spawn_accept_then_read(server_config, expected.len());
    let mut client = client_holding_flight(addr, client_config, &[&a, &b, &c, &d]);
    let wire: Vec<u8> = client.records.concat();
    let split = 9; // first record cut open just past its header

    let mut head = client.flight.clone();
    head.extend_from_slice(&wire[..split]);
    client.send(&head);
    std::thread::sleep(SETTLE);
    // a's remainder AND b, c, d in one write: the drain must take only what it
    // needs to finish a, and leave the rest for the kernel.
    client.send(&wire[split..]);

    let outcome = match out_rx.recv_timeout(WATCHDOG) {
        Ok(o) => o,
        Err(RecvTimeoutError::Timeout) => panic!("no outcome within {}s", WATCHDOG.as_secs()),
        Err(RecvTimeoutError::Disconnected) => panic!("task died without an outcome"),
    };
    let (trailing, kernel) = match outcome {
        Ok(pair) => pair,
        Err(e @ (KtlsError::SetsockoptFailed(_) | KtlsError::KernelNotSupported)) => {
            eprintln!("kTLS install unavailable on this host, tolerated: {e}");
            drop(client);
            return;
        }
        Err(e) => panic!("unexpected error from ktls_accept: {e:?}"),
    };

    eprintln!("split: trailing={} kernel={}", trailing.len(), kernel.len());
    assert!(
        !trailing.is_empty(),
        "the drain must finish the record it cut open — the kernel cannot decrypt a record \
         whose head userspace already consumed"
    );
    let mut whole = trailing.clone();
    whole.extend_from_slice(&kernel);
    assert_eq!(
        whole, expected,
        "every record must appear exactly once, in order, across the userspace/kernel split \
         (trailing={} kernel={})",
        trailing.len(),
        kernel.len()
    );
    drop(client);
}

// ---------------------------------------------------------------------------
// Claim: the CLIENT side of the shared drain reassembles a split record too.
// The server sends its flight plus a half-RTT app-data record fragment in one
// write, so `ktls_connect` reaches its drain with a partial record resident.
// ---------------------------------------------------------------------------
#[test]
fn client_side_split_record_reassembles_byte_exact() {
    let (server_config, client_config) = test_tls_configs();
    let mut server_config = Arc::try_unwrap(server_config).expect("unique config");
    // Lets the server emit app data right after its own Finished, which is what
    // puts a partial record in the client's buffer at handshake completion.
    server_config.send_half_rtt_data = true;
    let server_config = Arc::new(server_config);

    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let plaintext = payload(0x6f, 800);

    let (out_tx, out_rx) = mpsc::channel();
    let handle = glommio::LocalExecutorBuilder::default()
        .name("review-connect")
        .spawn(move || async move {
            let stream = glommio::net::TcpStream::connect(addr).await.unwrap();
            let _ = stream.set_nodelay(true);
            let name = ServerName::try_from("localhost").unwrap();
            let outcome = ktls_connect(stream, client_config, name).await.map(|(_s, t)| t);
            let _ = out_tx.send(Report { outcome });
        })
        .expect("spawn executor");
    drop(handle);

    let (mut sock, _) = listener.accept().unwrap();
    sock.set_nodelay(true).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut conn = rustls::ServerConnection::new(server_config).expect("server conn");

    // Read the ClientHello; buffer the whole server flight instead of sending it.
    let mut flight = Vec::new();
    while flight.is_empty() {
        let n = conn.read_tls(&mut sock).expect("read_tls");
        assert!(n > 0, "client closed during handshake");
        conn.process_new_packets().expect("process_new_packets");
        while conn.wants_write() {
            conn.write_tls(&mut flight).unwrap();
        }
    }

    // Half-RTT application data, framed but not yet sent.
    conn.writer().write_all(&plaintext).unwrap();
    let mut record = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut record).unwrap();
    }
    assert!(record.len() > 5, "server produced no half-RTT app record");

    let split = 7; // record cut open just past its header
    let mut head = flight.clone();
    head.extend_from_slice(&record[..split]);
    sock.write_all(&head).unwrap();
    sock.flush().unwrap();
    std::thread::sleep(SETTLE);
    sock.write_all(&record[split..]).unwrap();
    sock.flush().unwrap();

    let report = expect(&out_rx, "client-side split record");
    if let Some(trailing) = trailing_or_skip(report.outcome) {
        assert_eq!(trailing, plaintext, "client drain lost or corrupted the split record");
    }
    drop(sock);
}

// ---------------------------------------------------------------------------
// Claim (loop 2 contract): `drain_discard` compacts in place and leaves the
// read window — `buf.len()` — untouched; only `filled` moves.
//
// Loop 1 used `Vec::drain`, so every discard permanently shrank the window.
// Since `read_into` only grows the buffer when `filled == buf.len()`, a
// handshake read and consumed in small pieces shrank the window toward zero,
// and at zero `buf[filled..]` is empty: the next read can only return 0, which
// both callers translate into a false `UnexpectedEof`. This test is the
// regression guard for that.
// ---------------------------------------------------------------------------
#[test]
fn discards_keep_the_read_window_stable() {
    const WINDOW: usize = 16 * 1024;
    let mut buf = vec![0u8; WINDOW];
    let mut filled = 0usize;

    // Four windows' worth of traffic read and consumed in pieces too small to
    // ever trip read_into's `filled == buf.len()` growth condition.
    let mut consumed = 0usize;
    while consumed < 4 * WINDOW {
        let chunk = 256;
        for i in 0..chunk {
            buf[filled + i] = (consumed + i) as u8; // stands in for read_into
        }
        filled += chunk;
        drain_discard(&mut buf, &mut filled, chunk);
        consumed += chunk;
        assert_eq!(buf.len(), WINDOW, "read window must not shrink with discards");
    }

    assert_eq!(filled, 0);
    assert!(
        !buf[filled..].is_empty(),
        "read window must stay non-empty — an empty one makes the next read a false EOF"
    );

    // Compaction must also be correct: what survives a discard is the tail,
    // moved to offset 0, unchanged and in order.
    let mut buf = vec![0u8; WINDOW];
    buf[..10].copy_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let mut filled = 10usize;
    drain_discard(&mut buf, &mut filled, 4);
    assert_eq!(filled, 6);
    assert_eq!(&buf[..filled], &[4u8, 5, 6, 7, 8, 9], "surviving bytes must compact to offset 0");
    assert_eq!(buf.len(), WINDOW);
}

// ---------------------------------------------------------------------------
// DEFECT (this test is expected to FAIL): `trailing` has no cap.
//
// The drain exits only when the incoming buffer is EMPTY, and reads more
// whenever the buffer ends mid-record. A peer that streams continuously after
// its Finished leaves a partial record at the tail of nearly every read, so the
// drain keeps reading — and every drained record is appended to `trailing`,
// which is never bounded. The ceiling is DRAIN_DEADLINE x link rate, per
// connection, allocated on the shard executor.
//
// The pre-fix macro could not do this: it never read more bytes, so `trailing`
// was bounded by what was already resident. `read_more_bounded` is what turns a
// bounded accumulation into an unbounded one.
// ---------------------------------------------------------------------------
#[test]
fn streaming_peer_grows_trailing_without_a_cap() {
    /// Generous: the drain only ever needs to finish the record it is stuck on.
    const SANE_TRAILING_CAP: usize = 1024 * 1024;
    /// The peer stops here; the drain would keep going to DRAIN_DEADLINE.
    const PEER_CAP: usize = 64 * 1024 * 1024;

    let (server_config, client_config) = test_tls_configs();
    let (addr, out_rx) = spawn_accept(server_config);
    let seed = payload(0x9e, 16 * 1024);
    let mut client = client_holding_flight(addr, client_config, &[&seed]);

    // Finished + a record cut open: the drain enters its loop, exactly as in
    // production. From here the peer never lets the buffer end on a boundary.
    let record = client.records[0].clone();
    let mut head = client.flight.clone();
    head.extend_from_slice(&record[..7]);
    client.send(&head);
    let mut pending = record[7..].to_vec();

    // One max-size record at a time — rustls' plaintext buffer limit is 64KiB,
    // so each record is framed and drained before the next is queued.
    let chunk = payload(0x9e, 16 * 1024);
    let mut sent = 0usize;
    let started = std::time::Instant::now();
    while sent < PEER_CAP {
        let mut wire = std::mem::take(&mut pending);
        for _ in 0..16 {
            client.conn.writer().write_all(&chunk).unwrap();
            while client.conn.wants_write() {
                client.conn.write_tls(&mut wire).unwrap();
            }
        }
        if client.sock.write_all(&wire).is_err() {
            break;
        }
        sent += chunk.len() * 16;
    }
    let streamed_for = started.elapsed();

    let report = expect(&out_rx, "streaming peer");
    if let Some(trailing) = trailing_or_skip(report.outcome) {
        eprintln!(
            "trailing={} after the peer streamed {sent} bytes in {streamed_for:?}",
            trailing.len()
        );
        assert!(
            trailing.len() <= SANE_TRAILING_CAP,
            "drain buffered {} bytes of peer plaintext into `trailing` (peer streamed {sent} in \
             {streamed_for:?}); nothing bounds it but DRAIN_DEADLINE x link rate",
            trailing.len()
        );
    }
}
