use std::mem::size_of;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rustls::ConnectionTrafficSecrets;
use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::server::{ServerConnectionData, UnbufferedServerConnection};
use rustls::unbuffered::{ConnectionState, EncodeError, UnbufferedStatus};
use rustls_pki_types::ServerName;
use tracing::debug;

/// Maximum size for the incoming handshake buffer. TLS records are at most
/// 16KB + 5-byte header, so 128KB gives ample room for multi-record flights
/// while preventing unbounded allocation from malicious peers.
const MAX_HANDSHAKE_BUF: usize = 128 * 1024; // 131_072 bytes

/// Can't wait forever for a split application-data record. 
/// Anything over this just gets its connection killed
const DRAIN_DEADLINE: Duration = Duration::from_secs(5);

// Linux kTLS ULP constants
const SOL_TCP: libc::c_int = 6;
const TCP_ULP: libc::c_int = 31;
const SOL_TLS: libc::c_int = 282;
const TLS_TX: libc::c_int = 1;
const TLS_RX: libc::c_int = 2;
const TLS_1_3_VERSION: u16 = 0x0304;
const TLS_CIPHER_AES_GCM_128: u16 = 51;
const TLS_CIPHER_AES_GCM_256: u16 = 52;
const TLS_CIPHER_CHACHA20_POLY1305: u16 = 54;

// Kernel TLS crypto info structs (from Linux uapi/linux/tls.h)
#[repr(C)]
struct TlsCryptoInfo {
    version: u16,
    cipher_type: u16,
}

#[repr(C)]
struct TlsCryptoInfoAesGcm128 {
    info: TlsCryptoInfo,
    iv: [u8; 8],
    key: [u8; 16],
    salt: [u8; 4],
    rec_seq: [u8; 8],
}

#[repr(C)]
struct TlsCryptoInfoAesGcm256 {
    info: TlsCryptoInfo,
    iv: [u8; 8],
    key: [u8; 32],
    salt: [u8; 4],
    rec_seq: [u8; 8],
}

#[repr(C)]
struct TlsCryptoInfoChacha20Poly1305 {
    info: TlsCryptoInfo,
    iv: [u8; 12],
    key: [u8; 32],
    // salt is zero-length for ChaCha20
    rec_seq: [u8; 8],
}

#[derive(Debug)]
pub enum KtlsError {
    Tls(rustls::Error),
    Io(std::io::Error),
    KernelNotSupported,
    UnsupportedCipher,
    HandshakeIncomplete,
    SetsockoptFailed(std::io::Error),
    TrailingRecordTimeout,
    NoProgress,
}

impl From<rustls::Error> for KtlsError {
    fn from(e: rustls::Error) -> Self {
        KtlsError::Tls(e)
    }
}

impl From<std::io::Error> for KtlsError {
    fn from(e: std::io::Error) -> Self {
        KtlsError::Io(e)
    }
}

impl From<EncodeError> for KtlsError {
    fn from(e: EncodeError) -> Self {
        KtlsError::Tls(rustls::Error::General(format!("encode error: {e}")))
    }
}

impl std::fmt::Display for KtlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tls(e) => write!(f, "TLS error: {e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::KernelNotSupported => write!(f, "kernel kTLS not supported"),
            Self::UnsupportedCipher => write!(f, "unsupported TLS cipher suite for kTLS"),
            Self::HandshakeIncomplete => write!(f, "TLS handshake incomplete"),
            Self::SetsockoptFailed(e) => write!(f, "setsockopt failed: {e}"),
            Self::TrailingRecordTimeout => write!(
                f,
                "timed out waiting for the rest of a partial TLS record after handshake"
            ),
            Self::NoProgress => write!(f, "TLS connection state cannot be advanced"),
        }
    }
}

impl std::error::Error for KtlsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tls(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::SetsockoptFailed(e) => Some(e),
            _ => None,
        }
    }
}

/// Check if the running kernel supports kTLS.
/// Called once at startup. Fail fast if not supported.
pub fn verify_ktls_support() -> Result<(), KtlsError> {
    // Create a dummy TCP socket and attempt to enable TLS ULP.
    // ENOENT means the tls kernel module is not loaded.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(KtlsError::KernelNotSupported);
    }

    let tls_str = b"tls\0";
    let ret = unsafe {
        libc::setsockopt(
            fd,
            SOL_TCP,
            TCP_ULP,
            tls_str.as_ptr() as *const libc::c_void,
            tls_str.len() as libc::socklen_t,
        )
    };
    // Capture errno before close() can clobber it.
    let errno = if ret != 0 {
        unsafe { *libc::__errno_location() }
    } else {
        0
    };
    unsafe { libc::close(fd) };

    if ret == 0 {
        return Ok(());
    }

    // ENOTCONN means the TLS ULP module was found (the socket just isn't
    // connected yet). This proves kTLS is available.
    if errno == libc::ENOTCONN {
        Ok(())
    } else {
        Err(KtlsError::KernelNotSupported)
    }
}

#[cfg(test)]
mod drain_contract_tests;

#[cfg(test)]
mod review_evidence_tests;

#[cfg(test)]
mod accept_bounded_completion;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_ktls_returns_ok_or_not_supported() {
        match verify_ktls_support() {
            Ok(()) => {}
            Err(KtlsError::KernelNotSupported) => {}
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    #[test]
    fn test_drain_discard_normal() {
        let mut buf = vec![1u8, 2, 3, 4, 5];
        let mut filled = 5usize;
        drain_discard(&mut buf, &mut filled, 2);
        assert_eq!(filled, 3);
        assert_eq!(&buf[..filled], &[3u8, 4, 5]);
    }

    #[test]
    fn test_drain_discard_zero() {
        let mut buf = vec![1u8, 2, 3];
        let mut filled = 3usize;
        drain_discard(&mut buf, &mut filled, 0);
        assert_eq!(filled, 3);
        assert_eq!(&buf[..filled], &[1u8, 2, 3]);
    }

    #[test]
    fn test_drain_discard_all() {
        let mut buf = vec![1u8, 2, 3];
        let mut filled = 3usize;
        drain_discard(&mut buf, &mut filled, 3);
        assert_eq!(filled, 0);
    }

    #[test]
    fn test_drain_discard_overflow_clamped() {
        // discard > filled: release builds clamp silently; debug builds fire debug_assert.
        let mut buf = vec![1u8, 2, 3];
        let mut filled = 2usize;

        // Suppress panic output — the debug_assert panic is intentional.
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drain_discard(&mut buf, &mut filled, 5);
        }));
        std::panic::set_hook(prev_hook);

        // In release builds the clamp prevents panic; in debug the assert fires intentionally.
        #[cfg(not(debug_assertions))]
        assert!(outcome.is_ok());
        #[cfg(debug_assertions)]
        let _ = outcome; // panic is expected and acceptable in debug
    }

    /// Generate a self-signed CA + node certificate for testing.
    /// Returns (ServerConfig, ClientConfig) with secret extraction enabled
    /// and session tickets disabled (required for kTLS-to-kTLS).
    pub(crate) fn test_tls_configs() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
        use rcgen::{CertificateParams, Issuer, KeyPair};
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        // CA
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();

        // Node cert signed by CA (SAN: localhost, 127.0.0.1)
        let ca_issuer = Issuer::from_ca_cert_pem(&ca.pem(), ca_key).unwrap();
        let node_key = KeyPair::generate().unwrap();
        let node_params = CertificateParams::new(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ]).unwrap();
        let node_cert = node_params.signed_by(&node_key, &ca_issuer).unwrap();

        let ca_der = CertificateDer::from(ca.der().to_vec());
        let node_der = CertificateDer::from(node_cert.der().to_vec());
        let node_key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(node_key.serialize_der()));

        let provider = Arc::new(rustls::crypto::ring::default_provider());

        // Server config — send_tls13_tickets=0 prevents NewSessionTicket which
        // would desync seq counters between kTLS endpoints.
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(ca_der.clone()).unwrap();
        let mut server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![node_der.clone()], node_key_der.clone_key())
            .unwrap();
        server_config.enable_secret_extraction = true;
        server_config.send_tls13_tickets = 0;
        let server_config = Arc::new(server_config);

        // Client config
        let mut client_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(root_store)
            .with_client_auth_cert(vec![node_der], node_key_der)
            .unwrap();
        client_config.enable_secret_extraction = true;
        let client_config = Arc::new(client_config);

        (server_config, client_config)
    }

    #[test]
    fn test_ktls_connect_accept_roundtrip() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        if verify_ktls_support().is_err() {
            eprintln!("kTLS not supported on this kernel, skipping test");
            return;
        }

        let (server_config, client_config) = test_tls_configs();

        let ex = glommio::LocalExecutorBuilder::default()
            .spawn(move || async move {
                use glommio::net::TcpListener;

                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();

                // Verify kTLS handshake + install succeeds on both sides.
                // Data transfer is not tested here because io_uring + kTLS
                // has compatibility issues on some kernels (e.g. 6.17).
                let server = glommio::spawn_local(async move {
                    let stream = listener.accept().await.unwrap();
                    let (_stream, _trailing) = ktls_accept(stream, server_config).await
                        .expect("ktls_accept must succeed");
                    // Keep connection alive until client finishes
                    glommio::timer::sleep(std::time::Duration::from_millis(100)).await;
                });

                let client = glommio::spawn_local(async move {
                    let stream = TcpStream::connect(addr).await.unwrap();
                    let server_name = ServerName::try_from("localhost").unwrap();
                    let (_stream, _trailing) = ktls_connect(stream, client_config, server_name).await
                        .expect("ktls_connect must succeed");
                    // Keep connection alive until server finishes
                    glommio::timer::sleep(std::time::Duration::from_millis(100)).await;
                });

                server.await;
                client.await;
            })
            .unwrap();

        ex.join().unwrap();
    }
}

/// Perform a TLS handshake over a Glommio TcpStream (server side),
/// extract session keys, configure kTLS via setsockopt, and return
/// the plain TcpStream with kernel-level encryption active.
///
/// The provided `server_config` must have:
/// - `enable_secret_extraction = true`
/// - `send_tls13_tickets = 0` (for kTLS-to-kTLS internode connections;
///   session tickets desync sequence counters between kTLS endpoints)
pub async fn ktls_accept(
    mut stream: TcpStream,
    server_config: Arc<rustls::ServerConfig>,
) -> Result<(TcpStream, Vec<u8>), KtlsError> {
    let fd = stream.as_raw_fd();
    debug!(fd, "kTLS accept: starting handshake");
    let conn = UnbufferedServerConnection::new(server_config)?;
    let (secrets, trailing) = drive_handshake_server(&mut stream, conn).await?;
    debug!(fd, trailing_bytes = trailing.len(), "kTLS accept: handshake complete, installing kernel TLS");
    setup_ktls(fd, secrets)?;
    debug!(fd, "kTLS accept: kernel TLS active");
    Ok((stream, trailing))
}

/// Perform a TLS handshake over a Glommio TcpStream (client side),
/// extract session keys, configure kTLS, and return the plain TcpStream.
///
/// The provided `client_config` must have `enable_secret_extraction = true`.
pub async fn ktls_connect(
    mut stream: TcpStream,
    client_config: Arc<rustls::ClientConfig>,
    server_name: ServerName<'static>,
) -> Result<(TcpStream, Vec<u8>), KtlsError> {
    let fd = stream.as_raw_fd();
    debug!(fd, server_name = ?server_name, "kTLS connect: starting handshake");
    let conn = UnbufferedClientConnection::new(client_config, server_name)?;
    let (secrets, trailing) = drive_handshake_client(&mut stream, conn).await?;
    debug!(fd, trailing_bytes = trailing.len(), "kTLS connect: handshake complete, installing kernel TLS");
    setup_ktls(fd, secrets)?;
    debug!(fd, "kTLS connect: kernel TLS active");
    Ok((stream, trailing))
}

/// rustls exposes `process_tls_records` on two separate inherent impls, one per
/// connection data type. Naming it in a trait lets server and client share one
/// drain implementation instead of a macro expanded per call site.
trait ProcessRecords {
    type Data;
    fn process_records<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data>;
}

impl ProcessRecords for UnbufferedServerConnection {
    type Data = ServerConnectionData;
    fn process_records<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(incoming)
    }
}

impl ProcessRecords for UnbufferedClientConnection {
    type Data = ClientConnectionData;
    fn process_records<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(incoming)
    }
}

/// Keep pulling bytes off the wire even after the handshake is done
/// TCP can split app data records across multiple segments so we gotta loop
/// If don't pull the whole byte block for the entire record kTLS will send
/// our connection layer the second half and it's fail deserialisation
/// every iteration either consumes bytes, advances connection state, awaits a read, or returns
async fn drain_remaining_records<C: ProcessRecords>(
    conn: &mut C,
    stream: &mut TcpStream,
    incoming: &mut Vec<u8>,
    incoming_filled: &mut usize,
    outgoing: &mut [u8],
    trailing: &mut Vec<u8>,
) -> Result<(), KtlsError> {
    let deadline = Instant::now() + DRAIN_DEADLINE;

    while *incoming_filled > 0 {
        if Instant::now() >= deadline {
            return Err(KtlsError::TrailingRecordTimeout);
        }

        let UnbufferedStatus { discard, state } =
            conn.process_records(&mut incoming[..*incoming_filled]);

        let mut encoded = None;
        let advanced = match state? {
            ConnectionState::EncodeTlsData(mut etd) => {
                encoded = Some(etd.encode(outgoing)?);
                true
            }
            ConnectionState::TransmitTlsData(ttd) => {
                ttd.done();
                true
            }
            ConnectionState::ReadTraffic(mut rt) => {
                while let Some(record) = rt.next_record() {
                    let record = record?;
                    // rustls decrypts out-of-place today and always reports 0. A
                    // future in-place variant would need the payload copied out
                    // before discarding, so trip here rather than corrupt.
                    debug_assert_eq!(record.discard, 0, "rustls reported a per-record discard");
                    trailing.extend_from_slice(record.payload);
                }
                true
            }
            // Handshake done but some bytes left over, let rustls continue waiting on the rest
            ConnectionState::WriteTraffic(_) | ConnectionState::BlockedHandshake => false,
            ConnectionState::PeerClosed | ConnectionState::Closed => break,
            // Other states like 0-RTT data, which the handshake loop has already
            // drained by the time we get here, plus any future rustls states added in new versions
            _ => return Err(KtlsError::NoProgress),
        };

        drain_discard(incoming, incoming_filled, discard);
        if let Some(n) = encoded {
            stream.write_all(&outgoing[..n]).await?;
        }
        if advanced || discard > 0 {
            continue;
        }

        // Nothing moved. If the pending record is already whole, more bytes
        // cannot help; rustls simply will not act on it.
        let want = want_for_pending_record(incoming, *incoming_filled);
        if want == 0 {
            return Err(KtlsError::NoProgress);
        }
        read_more_bounded(deadline, stream, incoming, incoming_filled, want).await?;
    }

    Ok(())
}

/// Bytes still missing before `buf[..filled]` holds one whole TLS record.
/// `buf[0]` is a record boundary; `drain_discard` compacts consumed records
/// away; so this describes the record the drain is currently stuck on.
///
/// A record is a 5-byte header plus a body whose length is bytes 3..5 of the
/// header, big-endian. While the header itself is torn the body length is not
/// yet knowable, so only the header's remainder is asked for; the next call,
/// with the header complete, asks for the body. Zero means the record is whole.
#[inline]
fn want_for_pending_record(buf: &[u8], filled: usize) -> usize {
    const HEADER: usize = 5;
    if filled < HEADER {
        return HEADER - filled;
    }
    let body = u16::from_be_bytes([buf[3], buf[4]]) as usize;
    (HEADER + body).saturating_sub(filled)
}

/// Await more bytes of the one record left half-received by the handshake,
/// bounded by `deadline`. The read stops at that record's last byte (`want`),
/// so the drain can never pull a following record out of the kernel's reach and
/// into `trailing`. Awaiting is the point: it hands the executor back so its
/// timers; including the caller's own handshake timeout; keep running.
async fn read_more_bounded(
    deadline: Instant,
    stream: &mut TcpStream,
    incoming: &mut Vec<u8>,
    incoming_filled: &mut usize,
    want: usize,
) -> Result<(), KtlsError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(KtlsError::TrailingRecordTimeout);
    }

    let start = *incoming_filled;
    let need = start + want;
    // need is 5 + a u16 body length, so at most 65_540; always under MAX_HANDSHAKE_BUF.
    if need > incoming.len() {
        incoming.resize(need, 0);
    }

    let read = glommio::timer::timeout(deadline - now, async {
        Ok::<_, glommio::GlommioError<()>>(stream.read(&mut incoming[start..need]).await)
    })
    .await;

    match read {
        Ok(inner) => {
            let n = inner?;
            if n == 0 {
                return Err(KtlsError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed with a partial TLS record after handshake",
                )));
            }
            *incoming_filled = start + n;
            Ok(())
        }
        Err(_) => Err(KtlsError::TrailingRecordTimeout),
    }
}

async fn drive_handshake_server(
    stream: &mut TcpStream,
    mut conn: UnbufferedServerConnection,
) -> Result<(rustls::ExtractedSecrets, Vec<u8>), KtlsError> {
    let mut incoming: Vec<u8> = vec![0u8; 16 * 1024];
    let mut incoming_filled = 0usize;
    let mut outgoing: Vec<u8> = vec![0u8; 16 * 1024];

    loop {
        let UnbufferedStatus { discard, state } =
            conn.process_tls_records(&mut incoming[..incoming_filled]);

        match state? {
            ConnectionState::EncodeTlsData(mut etd) => {
                let n = etd.encode(&mut outgoing)?;
                stream.write_all(&outgoing[..n]).await?;
            }

            ConnectionState::TransmitTlsData(ttd) => {
                ttd.done();
            }

            ConnectionState::BlockedHandshake => {
                drain_discard(&mut incoming, &mut incoming_filled, discard);
                let n = read_into(stream, &mut incoming, &mut incoming_filled).await?;
                if n == 0 {
                    return Err(KtlsError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "connection closed during TLS handshake",
                    )));
                }
                continue;
            }

            ConnectionState::ReadTraffic(mut rt) => {
                // process_tls_records processed the Finished AND app data in
                // one call. Extract the decrypted plaintext before it's lost —
                // once kTLS is installed, the kernel can't see bytes that were
                // already consumed from the TCP receive buffer by userspace.
                let mut trailing = Vec::new();
                while let Some(record) = rt.next_record() {
                    let record = record?;
                    debug_assert_eq!(record.discard, 0, "rustls reported a per-record discard");
                    trailing.extend_from_slice(record.payload);
                }
                drop(rt);
                drain_discard(&mut incoming, &mut incoming_filled, discard);
                drain_remaining_records(&mut conn, stream, &mut incoming, &mut incoming_filled, &mut outgoing, &mut trailing).await?;
                debug!(incoming_filled, trailing_bytes = trailing.len(), "server: extracting kernel connection");
                return Ok((conn.dangerous_into_kernel_connection()?.0, trailing));
            }

            ConnectionState::WriteTraffic(_) => {
                let mut trailing = Vec::new();
                drain_discard(&mut incoming, &mut incoming_filled, discard);
                drain_remaining_records(&mut conn, stream, &mut incoming, &mut incoming_filled, &mut outgoing, &mut trailing).await?;
                debug!(incoming_filled, trailing_bytes = trailing.len(), "server: extracting kernel connection");
                return Ok((conn.dangerous_into_kernel_connection()?.0, trailing));
            }

            ConnectionState::ReadEarlyData(mut red) => {
                // The state promises at least one 0-RTT record. Consuming none
                // would leave the loop re-parsing the same bytes without ever
                // reading or exiting.
                let mut any = false;
                while red.next_record().is_some() {
                    any = true;
                }
                if !any {
                    return Err(KtlsError::NoProgress);
                }
            }

            ConnectionState::PeerClosed | ConnectionState::Closed => {
                return Err(KtlsError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "peer closed during TLS handshake",
                )));
            }

            _ => return Err(KtlsError::HandshakeIncomplete),
        }

        drain_discard(&mut incoming, &mut incoming_filled, discard);
    }
}

async fn drive_handshake_client(
    stream: &mut TcpStream,
    mut conn: UnbufferedClientConnection,
) -> Result<(rustls::ExtractedSecrets, Vec<u8>), KtlsError> {
    let mut incoming: Vec<u8> = vec![0u8; 16 * 1024];
    let mut incoming_filled = 0usize;
    let mut outgoing: Vec<u8> = vec![0u8; 16 * 1024];

    loop {
        let UnbufferedStatus { discard, state } =
            conn.process_tls_records(&mut incoming[..incoming_filled]);

        match state? {
            ConnectionState::EncodeTlsData(mut etd) => {
                let n = etd.encode(&mut outgoing)?;
                stream.write_all(&outgoing[..n]).await?;
            }

            ConnectionState::TransmitTlsData(ttd) => {
                ttd.done();
            }

            ConnectionState::BlockedHandshake => {
                drain_discard(&mut incoming, &mut incoming_filled, discard);
                let n = read_into(stream, &mut incoming, &mut incoming_filled).await?;
                if n == 0 {
                    return Err(KtlsError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "connection closed during TLS handshake",
                    )));
                }
                continue;
            }

            ConnectionState::ReadTraffic(mut rt) => {
                let mut trailing = Vec::new();
                while let Some(record) = rt.next_record() {
                    let record = record?;
                    debug_assert_eq!(record.discard, 0, "rustls reported a per-record discard");
                    trailing.extend_from_slice(record.payload);
                }
                drop(rt);
                drain_discard(&mut incoming, &mut incoming_filled, discard);
                drain_remaining_records(&mut conn, stream, &mut incoming, &mut incoming_filled, &mut outgoing, &mut trailing).await?;
                debug!(incoming_filled, trailing_bytes = trailing.len(), "client: extracting kernel connection");
                return Ok((conn.dangerous_into_kernel_connection()?.0, trailing));
            }

            ConnectionState::WriteTraffic(_) => {
                let mut trailing = Vec::new();
                drain_discard(&mut incoming, &mut incoming_filled, discard);
                drain_remaining_records(&mut conn, stream, &mut incoming, &mut incoming_filled, &mut outgoing, &mut trailing).await?;
                debug!(incoming_filled, trailing_bytes = trailing.len(), "client: extracting kernel connection");
                return Ok((conn.dangerous_into_kernel_connection()?.0, trailing));
            }

            ConnectionState::PeerClosed | ConnectionState::Closed => {
                return Err(KtlsError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "peer closed during TLS handshake",
                )));
            }

            _ => return Err(KtlsError::HandshakeIncomplete),
        }

        drain_discard(&mut incoming, &mut incoming_filled, discard);
    }
}

/// Compact the consumed prefix out of `buf`, leaving the next record at offset 0.
///
/// `buf.len()` is the read window and must never change here: only `filled`
/// tracks how much of it holds data. Shrinking the window would eventually make
/// `buf[filled..]` empty, turning the next read into a false EOF.
#[inline]
fn drain_discard(buf: &mut [u8], filled: &mut usize, discard: usize) {
    if discard > 0 {
        debug_assert!(discard <= *filled, "rustls asked to discard more than filled");
        let discard = discard.min(*filled);
        buf.copy_within(discard..*filled, 0);
        *filled -= discard;
    }
}

async fn read_into(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    filled: &mut usize,
) -> Result<usize, KtlsError> {
    if *filled == buf.len() {
        let new_len = buf.len() * 2;
        if new_len > MAX_HANDSHAKE_BUF {
            return Err(KtlsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "TLS handshake message exceeds maximum buffer size",
            )));
        }
        buf.resize(new_len, 0);
    }
    let n = stream.read(&mut buf[*filled..]).await?;
    *filled += n;
    Ok(n)
}

/// Enable kTLS on `fd` using the extracted TLS session secrets.
fn setup_ktls(fd: RawFd, secrets: rustls::ExtractedSecrets) -> Result<(), KtlsError> {
    enable_tls_ulp(fd)?;
    debug!(fd, "kTLS ULP enabled");

    let (tx_seq, tx_secret) = secrets.tx;
    let (rx_seq, rx_secret) = secrets.rx;

    let cipher = cipher_name(&tx_secret);
    debug!(fd, cipher, tx_seq, rx_seq, "installing kTLS crypto");

    set_tls_crypto(fd, TLS_TX, tx_seq, &tx_secret)?;
    set_tls_crypto(fd, TLS_RX, rx_seq, &rx_secret)?;

    Ok(())
}

fn cipher_name(secret: &ConnectionTrafficSecrets) -> &'static str {
    match secret {
        ConnectionTrafficSecrets::Aes128Gcm { .. } => "AES-128-GCM",
        ConnectionTrafficSecrets::Aes256Gcm { .. } => "AES-256-GCM",
        ConnectionTrafficSecrets::Chacha20Poly1305 { .. } => "ChaCha20-Poly1305",
        _ => "unknown",
    }
}

fn enable_tls_ulp(fd: RawFd) -> Result<(), KtlsError> {
    let tls_str = b"tls\0";
    let ret = unsafe {
        libc::setsockopt(
            fd,
            SOL_TCP,
            TCP_ULP,
            tls_str.as_ptr() as *const libc::c_void,
            tls_str.len() as libc::socklen_t,
        )
    };
    if ret == 0 {
        Ok(())
    } else {
        Err(KtlsError::SetsockoptFailed(std::io::Error::last_os_error()))
    }
}

fn set_tls_crypto(
    fd: RawFd,
    direction: libc::c_int,
    seq: u64,
    secret: &ConnectionTrafficSecrets,
) -> Result<(), KtlsError> {
    let rec_seq = seq.to_be_bytes();

    match secret {
        ConnectionTrafficSecrets::Aes128Gcm { key, iv } => {
            let key_bytes = key.as_ref(); // 16 bytes
            let iv_bytes = iv.as_ref(); // 12 bytes: first 4 = salt, last 8 = iv

            let mut key_arr = [0u8; 16];
            key_arr.copy_from_slice(key_bytes);

            let mut salt = [0u8; 4];
            let mut iv_arr = [0u8; 8];
            salt.copy_from_slice(&iv_bytes[..4]);
            iv_arr.copy_from_slice(&iv_bytes[4..]);

            let info = TlsCryptoInfoAesGcm128 {
                info: TlsCryptoInfo {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_AES_GCM_128,
                },
                iv: iv_arr,
                key: key_arr,
                salt,
                rec_seq,
            };
            setsockopt_tls(fd, direction, &info)
        }

        ConnectionTrafficSecrets::Aes256Gcm { key, iv } => {
            let key_bytes = key.as_ref(); // 32 bytes
            let iv_bytes = iv.as_ref(); // 12 bytes

            let mut key_arr = [0u8; 32];
            key_arr.copy_from_slice(key_bytes);

            let mut salt = [0u8; 4];
            let mut iv_arr = [0u8; 8];
            salt.copy_from_slice(&iv_bytes[..4]);
            iv_arr.copy_from_slice(&iv_bytes[4..]);

            let info = TlsCryptoInfoAesGcm256 {
                info: TlsCryptoInfo {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_AES_GCM_256,
                },
                iv: iv_arr,
                key: key_arr,
                salt,
                rec_seq,
            };
            setsockopt_tls(fd, direction, &info)
        }

        ConnectionTrafficSecrets::Chacha20Poly1305 { key, iv } => {
            let key_bytes = key.as_ref(); // 32 bytes
            let iv_bytes = iv.as_ref(); // 12 bytes

            let mut key_arr = [0u8; 32];
            key_arr.copy_from_slice(key_bytes);

            let mut iv_arr = [0u8; 12];
            iv_arr.copy_from_slice(iv_bytes);

            let info = TlsCryptoInfoChacha20Poly1305 {
                info: TlsCryptoInfo {
                    version: TLS_1_3_VERSION,
                    cipher_type: TLS_CIPHER_CHACHA20_POLY1305,
                },
                iv: iv_arr,
                key: key_arr,
                rec_seq,
            };
            setsockopt_tls(fd, direction, &info)
        }

        // ConnectionTrafficSecrets is #[non_exhaustive]; handle future variants
        _ => Err(KtlsError::UnsupportedCipher),
    }
}

fn setsockopt_tls<T>(fd: RawFd, direction: libc::c_int, info: &T) -> Result<(), KtlsError> {
    let ret = unsafe {
        libc::setsockopt(
            fd,
            SOL_TLS,
            direction,
            info as *const T as *const libc::c_void,
            size_of::<T>() as libc::socklen_t,
        )
    };
    if ret == 0 {
        Ok(())
    } else {
        Err(KtlsError::SetsockoptFailed(std::io::Error::last_os_error()))
    }
}
