//! Contract: `ktls_accept` completes in bounded time for ANY byte sequence the
//! peer sends, including a first application-data record that arrives split
//! across TCP segments. Bounded completion must hold at the function's own
//! level: the production caller wraps it in `glommio::timer::timeout`, and that
//! timer can never fire if the future never yields.
//!
//! The watchdog lives outside the executor under test. A future that spins
//! without yielding starves every timer on its own executor, so an in-executor
//! timeout would be part of the thing being measured.

use std::io::Write;
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::{KtlsError, ktls_accept};
use rustls::pki_types::ServerName;

/// Outer bound the contract must respect. Generous: a correct implementation
/// returns in milliseconds, so anything approaching this is a hang, not slowness.
const WATCHDOG: Duration = Duration::from_secs(20);

/// Attempts of the poison sequence before declaring the contract upheld. Each
/// attempt is an independent connection; a single unlucky TCP split that hides
/// the defect must not turn the whole test green.
const POISON_ATTEMPTS: usize = 3;

/// Plaintext carried by the client's first application-data record.
const APP_PAYLOAD: &[u8] = b"first-app-record-payload";

// ---------------------------------------------------------------------------
// TLS material
// ---------------------------------------------------------------------------

/// Self-signed CA + node cert, TLS 1.3 only, secret extraction on and session
/// tickets off — the shape `ktls_accept` documents as required.
fn test_tls_configs() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    use rcgen::{CertificateParams, Issuer, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let ca_issuer = Issuer::from_ca_cert_pem(&ca.pem(), ca_key).unwrap();
    let node_key = KeyPair::generate().unwrap();
    let node_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
    let node_cert = node_params.signed_by(&node_key, &ca_issuer).unwrap();

    let ca_der = CertificateDer::from(ca.der().to_vec());
    let node_der = CertificateDer::from(node_cert.der().to_vec());
    let node_key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(node_key.serialize_der()));

    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(ca_der).unwrap();

    let mut server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![node_der], node_key_der)
        .unwrap();
    server_config.enable_secret_extraction = true;
    server_config.send_tls13_tickets = 0;

    let mut client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    client_config.enable_secret_extraction = true;

    (Arc::new(server_config), Arc::new(client_config))
}

// ---------------------------------------------------------------------------
// Server harness: real ktls_accept, on its own executor, on its own OS thread
// ---------------------------------------------------------------------------

type AcceptOutcome = Result<Vec<u8>, KtlsError>;

/// Bind a listener inside a fresh glommio executor on a dedicated OS thread,
/// accept one connection and run the real `ktls_accept` on it.
///
/// The executor's `JoinHandle` is dropped, i.e. the thread is detached: if
/// `ktls_accept` never returns, `join()` would never return either and libtest
/// could not report the failure.
fn spawn_accept_server(server_config: Arc<rustls::ServerConfig>) -> (SocketAddr, Receiver<AcceptOutcome>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (out_tx, out_rx) = mpsc::channel();

    let handle = glommio::LocalExecutorBuilder::default()
        .name("ktls-accept")
        .spawn(move || async move {
            let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let stream = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            let outcome = match ktls_accept(stream, server_config).await {
                Ok((_stream, trailing)) => Ok(trailing),
                Err(e) => Err(e),
            };
            let _ = out_tx.send(outcome);
        })
        .expect("spawn server executor");
    drop(handle); // detached on purpose — see doc comment

    let addr = addr_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("server executor failed to bind");
    (addr, out_rx)
}

// ---------------------------------------------------------------------------
// Client: real rustls TLS 1.3 handshake with byte-level control of the wire
// ---------------------------------------------------------------------------

/// A client that completes a real handshake but writes nothing back to the
/// socket after the ClientHello. The final client flight and the first
/// application-data record are held in memory, so the caller decides exactly
/// which bytes hit the wire and in how many `write()` calls.
struct HeldClient {
    sock: StdTcpStream,
    /// Client's final handshake flight (CCS + Finished), not yet sent.
    flight: Vec<u8>,
    /// The first encrypted application-data record, not yet sent.
    app_record: Vec<u8>,
}

fn handshake_holding_final_flight(
    addr: SocketAddr,
    client_config: Arc<rustls::ClientConfig>,
) -> HeldClient {
    let mut sock = StdTcpStream::connect(addr).expect("connect");
    // Nagle would let the kernel coalesce or delay our writes on its own
    // schedule; the framing must come from us, not from it.
    sock.set_nodelay(true).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10))).unwrap();

    let name = ServerName::try_from("localhost").unwrap();
    let mut conn = rustls::ClientConnection::new(client_config, name).expect("client conn");

    // ClientHello goes out on its own — the server must reach its own flight.
    let mut hello = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut hello).unwrap();
    }
    sock.write_all(&hello).unwrap();
    sock.flush().unwrap();

    // Read the server flight. Everything the client wants to write in response
    // is buffered, never sent — that is what makes the later split deterministic.
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

    // Encrypt the first application-data record, still without sending it.
    conn.writer().write_all(APP_PAYLOAD).unwrap();
    let mut app_record = Vec::new();
    while conn.wants_write() {
        conn.write_tls(&mut app_record).unwrap();
    }
    assert!(app_record.len() > 5, "expected one framed app-data record");

    HeldClient { sock, flight, app_record }
}

impl HeldClient {
    /// One `write()` of the final handshake flight followed by `prefix_len`
    /// bytes of the application-data record. A single small write on loopback
    /// lands in the receive queue as one contiguous chunk, so the server's next
    /// `read()` returns the Finished *and* the record fragment together — the
    /// production situation: handshake complete, partial record in the buffer.
    fn send_flight_with_record_prefix(&mut self, prefix_len: usize) {
        let mut wire = Vec::with_capacity(self.flight.len() + prefix_len);
        wire.extend_from_slice(&self.flight);
        wire.extend_from_slice(&self.app_record[..prefix_len]);
        self.sock.write_all(&wire).unwrap();
        self.sock.flush().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Spin evidence — distinguishes "wedged in a busy loop" from "slow but moving"
// ---------------------------------------------------------------------------

fn clock_ticks_per_sec() -> u64 {
    let v = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if v > 0 { v as u64 } else { 100 }
}

/// (tid/comm, utime+stime in clock ticks) for every thread of this process.
fn thread_cpu_ticks() -> Vec<(String, u64)> {
    let mut out = Vec::new();
    let Ok(dir) = std::fs::read_dir("/proc/self/task") else {
        return out;
    };
    for entry in dir.flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let comm = &stat[open + 1..close];
        let fields: Vec<&str> = stat[close + 2..].split_whitespace().collect();
        if fields.len() < 13 {
            continue;
        }
        let utime: u64 = fields[11].parse().unwrap_or(0);
        let stime: u64 = fields[12].parse().unwrap_or(0);
        out.push((
            format!("{}/{}", entry.file_name().to_string_lossy(), comm),
            utime + stime,
        ));
    }
    out
}

/// Sample per-thread CPU over one wall second and report the busiest threads.
/// A spinning executor shows ~1.0 CPU-seconds per wall second.
fn spin_evidence() -> String {
    let before = thread_cpu_ticks();
    std::thread::sleep(Duration::from_secs(1));
    let after = thread_cpu_ticks();
    let hz = clock_ticks_per_sec() as f64;

    let mut deltas: Vec<(String, f64)> = after
        .into_iter()
        .map(|(name, ticks)| {
            let base = before
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, t)| *t)
                .unwrap_or(0);
            (name, ticks.saturating_sub(base) as f64 / hz)
        })
        .collect();
    deltas.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    deltas.truncate(3);

    let busiest: Vec<String> = deltas
        .iter()
        .map(|(n, s)| format!("{n}={s:.2} cpu-s/wall-s"))
        .collect();
    // Both verdicts break the contract, but they point at different fixes: a
    // spin needs the loop to stop re-parsing the same bytes, a block needs the
    // wait to be bounded.
    let verdict = match deltas.first() {
        Some((_, s)) if *s > 0.5 => "VERDICT: busy-spinning, no yield",
        _ => "VERDICT: blocked, not spinning — still unbounded, but it does yield",
    };
    format!("{verdict}; busiest threads over a 1s window: [{}]", busiest.join(", "))
}

// ---------------------------------------------------------------------------
// Contracts
// ---------------------------------------------------------------------------

/// The defect: at handshake completion the buffer holds the client's Finished
/// plus an unparseable fragment of the first app-data record. The post-handshake
/// drain loop neither reads more bytes nor exits on that fragment, so it spins
/// inside a single poll — 100% CPU, no yield, the shard executor never runs
/// another task again.
#[test]
fn contract_partial_trailing_record_must_not_hang_accept() {
    let (server_config, client_config) = test_tls_configs();

    for attempt in 1..=POISON_ATTEMPTS {
        let (addr, out_rx) = spawn_accept_server(server_config.clone());
        let mut client = handshake_holding_final_flight(addr, client_config.clone());

        // Record header (5 bytes) plus one body byte: the length prefix promises
        // far more than is present, so the record can never be parsed. The rest
        // of the record is withheld for the lifetime of the test.
        client.send_flight_with_record_prefix(6);

        let started = Instant::now();
        match out_rx.recv_timeout(WATCHDOG) {
            Ok(_outcome) => {
                // Bounded completion is the whole contract: Ok or a typed error
                // both satisfy it. Nothing is asserted about the trailing bytes
                // of a record the peer never finished sending.
                eprintln!(
                    "attempt {attempt}: ktls_accept returned in {:?}",
                    started.elapsed()
                );
            }
            Err(RecvTimeoutError::Timeout) => {
                let evidence = spin_evidence();
                panic!(
                    "ktls_accept did not return within {}s with a partial trailing record in \
                     the buffer — the drain loop is spinning; one such connection freezes an \
                     entire shard executor in production. \
                     (attempt {attempt}, no outer timeout can rescue this: the future never \
                     yields, so glommio::timer::timeout in the caller never fires.) {evidence}",
                    WATCHDOG.as_secs()
                );
            }
            Err(RecvTimeoutError::Disconnected) => {
                panic!("server task died without reporting an outcome (attempt {attempt})");
            }
        }
        drop(client);
    }
}

/// A COMPLETE first app-data record riding along with the final handshake flight
/// must come back as trailing bytes, exactly.
#[test]
fn contract_complete_trailing_record_returns_exact_bytes() {
    let (server_config, client_config) = test_tls_configs();
    let (addr, out_rx) = spawn_accept_server(server_config);
    let mut client = handshake_holding_final_flight(addr, client_config);

    let whole_record = client.app_record.len();
    client.send_flight_with_record_prefix(whole_record);

    match out_rx.recv_timeout(WATCHDOG) {
        Ok(Ok(trailing)) => assert_eq!(
            trailing, APP_PAYLOAD,
            "trailing bytes must be exactly the plaintext of the first app-data record"
        ),
        // Kernel TLS may be unavailable; the contract under test is bounded
        // completion, not kernel feature availability.
        Ok(Err(e @ (KtlsError::SetsockoptFailed(_) | KtlsError::KernelNotSupported))) => {
            eprintln!("kTLS install unavailable on this host, tolerated: {e}");
        }
        Ok(Err(e)) => panic!("unexpected error from ktls_accept: {e:?}"),
        Err(RecvTimeoutError::Timeout) => panic!(
            "ktls_accept did not return within {}s with a complete trailing record in the \
             buffer — the drain loop is spinning; one such connection freezes an entire shard \
             executor in production. {}",
            WATCHDOG.as_secs(),
            spin_evidence()
        ),
        Err(RecvTimeoutError::Disconnected) => panic!("server task died without an outcome"),
    }
    drop(client);
}

/// The plain path: handshake, nothing trailing, empty trailing buffer.
#[test]
fn contract_clean_handshake_returns_empty_trailing() {
    let (server_config, client_config) = test_tls_configs();
    let (addr, out_rx) = spawn_accept_server(server_config);
    let mut client = handshake_holding_final_flight(addr, client_config);

    client.send_flight_with_record_prefix(0);

    match out_rx.recv_timeout(WATCHDOG) {
        Ok(Ok(trailing)) => assert!(
            trailing.is_empty(),
            "no application data was sent, trailing must be empty, got {} bytes",
            trailing.len()
        ),
        Ok(Err(e @ (KtlsError::SetsockoptFailed(_) | KtlsError::KernelNotSupported))) => {
            eprintln!("kTLS install unavailable on this host, tolerated: {e}");
        }
        Ok(Err(e)) => panic!("unexpected error from ktls_accept: {e:?}"),
        Err(RecvTimeoutError::Timeout) => panic!(
            "ktls_accept did not return within {}s on a clean handshake with no trailing data. {}",
            WATCHDOG.as_secs(),
            spin_evidence()
        ),
        Err(RecvTimeoutError::Disconnected) => panic!("server task died without an outcome"),
    }
    drop(client);
}
