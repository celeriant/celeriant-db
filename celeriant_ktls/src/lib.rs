use std::mem::size_of;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;

use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rustls::ConnectionTrafficSecrets;
use rustls::client::UnbufferedClientConnection;
use rustls::server::UnbufferedServerConnection;
use rustls::unbuffered::{ConnectionState, EncodeError, UnbufferedStatus};
use rustls_pki_types::ServerName;
use tracing::debug;

/// Maximum size for the incoming handshake buffer. TLS records are at most
/// 16KB + 5-byte header, so 128KB gives ample room for multi-record flights
/// while preventing unbounded allocation from malicious peers.
const MAX_HANDSHAKE_BUF: usize = 128 * 1024; // 131_072 bytes

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

}

/// Perform a TLS handshake over a Glommio TcpStream (server side),
/// extract session keys, configure kTLS via setsockopt, and return
/// the plain TcpStream with kernel-level encryption active.
///
/// The provided `server_config` must have `enable_secret_extraction = true`.
pub async fn ktls_accept(
    mut stream: TcpStream,
    server_config: Arc<rustls::ServerConfig>,
) -> Result<TcpStream, KtlsError> {
    let fd = stream.as_raw_fd();
    debug!(fd, "kTLS accept: starting handshake");
    let conn = UnbufferedServerConnection::new(server_config)?;
    let secrets = drive_handshake_server(&mut stream, conn).await?;
    debug!(fd, "kTLS accept: handshake complete, installing kernel TLS");
    setup_ktls(fd, secrets)?;
    debug!(fd, "kTLS accept: kernel TLS active");
    Ok(stream)
}

/// Perform a TLS handshake over a Glommio TcpStream (client side),
/// extract session keys, configure kTLS, and return the plain TcpStream.
///
/// The provided `client_config` must have `enable_secret_extraction = true`.
pub async fn ktls_connect(
    mut stream: TcpStream,
    client_config: Arc<rustls::ClientConfig>,
    server_name: ServerName<'static>,
) -> Result<TcpStream, KtlsError> {
    let fd = stream.as_raw_fd();
    debug!(fd, server_name = ?server_name, "kTLS connect: starting handshake");
    let conn = UnbufferedClientConnection::new(client_config, server_name)?;
    let secrets = drive_handshake_client(&mut stream, conn).await?;
    debug!(fd, "kTLS connect: handshake complete, installing kernel TLS");
    setup_ktls(fd, secrets)?;
    debug!(fd, "kTLS connect: kernel TLS active");
    Ok(stream)
}

async fn drive_handshake_server(
    stream: &mut TcpStream,
    mut conn: UnbufferedServerConnection,
) -> Result<rustls::ExtractedSecrets, KtlsError> {
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

            ConnectionState::WriteTraffic(_) | ConnectionState::ReadTraffic(_) => {
                return Ok(conn.dangerous_into_kernel_connection()?.0);
            }

            ConnectionState::ReadEarlyData(mut red) => {
                while red.next_record().is_some() {}
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
) -> Result<rustls::ExtractedSecrets, KtlsError> {
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

            ConnectionState::WriteTraffic(_) | ConnectionState::ReadTraffic(_) => {
                return Ok(conn.dangerous_into_kernel_connection()?.0);
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

#[inline]
fn drain_discard(buf: &mut Vec<u8>, filled: &mut usize, discard: usize) {
    if discard > 0 {
        debug_assert!(discard <= *filled, "rustls asked to discard more than filled");
        let discard = discard.min(*filled);
        buf.drain(..discard);
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
