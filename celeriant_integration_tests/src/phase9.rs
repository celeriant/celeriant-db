//! Phase 9 — transport security (TLS/mTLS) and client identity.
//!
//! Oracle: celeriant-docs/docs/operations/tls-mtls.md,
//! concepts/identity.md, reference/wire-protocol.md ("The identify handshake"),
//! reference/error-codes.md (10001-10007).
//!
//! TLS posture is set on the server via `ConfigTlsMode` + `ConfigClientAuth`
//! and the harness `TestPki` (a CA + node/client certs). The client connects
//! with `CeleriantClient::connect_tls` using a `ClientTlsConfig` built from the
//! same CA. Identity is the separate handshake driven by `identify()`.
//!
//! These tests are slower and flakier than the happy path (real TLS handshakes,
//! process spawns). Transient startup/handshake failures that pass on re-run are
//! flakes — see FINDINGS.

use std::time::Duration;

use celeriant_client_tokio::celeriant_client::{CeleriantClient, ClientIdentityConfig};
use celeriant_client_tokio::client_error::ClientError;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::server_error::{AuthError, ServerError};
use celeriant_crypto::Crypto;
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{ConfigClientAuth, ConfigTlsMode, ServerConfig, TestPki, TestServer};

use crate::common::{event, port_for, read_all, R};

const TYPE: u64 = 900;

/// Build a TLS-strict standalone server config with the given client-auth mode,
/// presenting `node_cert`/`node_key` and trusting `ca`.
fn tls_config(
    ca_cert: &std::path::Path,
    node_cert: &std::path::Path,
    node_key: &std::path::Path,
    client_auth: ConfigClientAuth,
) -> ServerConfig {
    ServerConfig {
        standalone: true,
        num_shards: Some(1),
        tls_mode: ConfigTlsMode::Strict,
        tls_ca_cert: Some(ca_cert.to_path_buf()),
        tls_node_cert: Some(node_cert.to_path_buf()),
        tls_node_key: Some(node_key.to_path_buf()),
        tls_client_auth: client_auth,
        ..Default::default()
    }
}

/// 9.1 mTLS `require`: a client presenting a cert signed by the trusted CA
/// connects and writes successfully (tls-mtls "require ... Clients must present
/// a cert signed by the trusted CA").
pub async fn mtls_require_good_client_connects() -> R {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;
    let (client_cert, client_key) = pki.create_client_cert("good")?;

    let port = port_for("p9_mtls_good");
    let cfg = tls_config(&pki.ca_cert_path(), &node_cert, &node_key, ConfigClientAuth::Require);
    let server = TestServer::start_with_config_labeled(port, cfg, "tls-require".into()).await?;

    let tls = pki.build_client_tls_config(&client_cert, &client_key, "localhost")?;
    let mut c = CeleriantClient::connect_tls(server.address(), tls).await?;

    let key = AggregateKey::new(900, 1, 1);
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, ..Default::default() },
    )
    .await?;

    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 {
        return Err(format!("expected 1 batch over mTLS, got {}", batches.len()).into());
    }
    Ok(())
}

/// 9.2 mTLS `require`: a client whose cert is signed by a DIFFERENT, untrusted CA
/// is refused — while a properly CA-signed client connects to the SAME server
/// (positive control, so a refusal can't false-pass from a down server).
/// (tls-mtls "require ... a cert signed by the trusted CA".)
pub async fn mtls_require_untrusted_ca_refused() -> R {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;
    let port = port_for("p9_mtls_untrusted");
    let cfg = tls_config(&pki.ca_cert_path(), &node_cert, &node_key, ConfigClientAuth::Require);
    let server = TestServer::start_with_config_labeled(port, cfg, "tls-require".into()).await?;

    // Positive control: a CA-signed client connects and writes (server is up and
    // accepts valid clients) — rules out a "server down / wrong port" false pass.
    let (good_cert, good_key) = pki.create_client_cert("good")?;
    let good = pki.build_client_tls_config(&good_cert, &good_key, "localhost")?;
    connect_and_probe(server.address(), good)
        .await
        .map_err(|e| format!("positive control failed — a valid mTLS client could not connect: {e}"))?;

    // A cert from a CA the server does NOT trust must be refused (bounded by
    // connect_and_probe's connect timeout, so a hang fails rather than passes).
    let other = TestPki::new()?;
    let (other_cert, other_key) = other.create_client_cert("stranger")?;
    let bad = other.build_client_tls_config(&other_cert, &other_key, "localhost")?;
    match connect_and_probe(server.address(), bad).await {
        Err(_) => Ok(()),
        Ok(()) => Err("require server accepted a client whose cert was signed by an untrusted CA".into()),
    }
}

/// 9.2b mTLS `require`: a client presenting NO client certificate (genuinely
/// anonymous) is refused — the true missing-cert case, distinct from the
/// untrusted-CA case above — with the same valid-client positive control.
/// (tls-mtls "require ... Clients must present a cert".)
pub async fn mtls_require_no_cert_refused() -> R {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;
    let port = port_for("p9_mtls_nocert");
    let cfg = tls_config(&pki.ca_cert_path(), &node_cert, &node_key, ConfigClientAuth::Require);
    let server = TestServer::start_with_config_labeled(port, cfg, "tls-require".into()).await?;

    // Control: CA-signed client connects + writes.
    let (good_cert, good_key) = pki.create_client_cert("good")?;
    let good = pki.build_client_tls_config(&good_cert, &good_key, "localhost")?;
    connect_and_probe(server.address(), good)
        .await
        .map_err(|e| format!("positive control failed — a valid mTLS client could not connect: {e}"))?;

    // No client cert at all (anonymous) must be refused under `require`.
    let anon = build_anonymous_tls(&pki, "localhost")?;
    match connect_and_probe(server.address(), anon).await {
        Err(_) => Ok(()),
        Ok(()) => Err("require server accepted a client presenting NO certificate".into()),
    }
}

/// 9.3 `tls-mode strict` refuses plaintext: a plaintext client cannot talk to a
/// TLS-strict server (tls-mtls "strict (TLS only, plaintext refused)"). A TLS
/// client connects to the same server as a positive control, and the plaintext
/// attempt is bounded so a hang FAILS rather than passing.
pub async fn tls_strict_refuses_plaintext() -> R {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;

    let port = port_for("p9_strict_plain");
    // client-auth none so the ONLY thing under test is plaintext-vs-TLS.
    let cfg = tls_config(&pki.ca_cert_path(), &node_cert, &node_key, ConfigClientAuth::None);
    let server = TestServer::start_with_config_labeled(port, cfg, "tls-strict".into()).await?;

    // Positive control: a TLS client (anonymous, trusts the CA) connects + writes.
    let tls = build_anonymous_tls(&pki, "localhost")?;
    connect_and_probe(server.address(), tls)
        .await
        .map_err(|e| format!("positive control failed — a TLS client could not reach the strict server: {e}"))?;

    // Plaintext must be refused. Bound the attempt: a hang is a failure, not a pass.
    let key = AggregateKey::new(900, 3, 1);
    let res = tokio::time::timeout(Duration::from_secs(10), async {
        let mut c = CeleriantClient::connect(server.address()).await?;
        c.write_events_with(key, vec![event(1, TYPE, 1000, "{}")],
            WriteEventsOptions { allow_create: true, ..Default::default() }).await
    })
    .await;
    match res {
        Err(_) => Err("plaintext write to a TLS-strict server neither completed nor was refused within 10s (hang)".into()),
        Ok(Ok(_)) => Err("TLS-strict server accepted a plaintext write".into()),
        Ok(Err(_)) => Ok(()), // plaintext refused — documented
    }
}

/// 9.4 mTLS `none`: server-authenticated TLS only. A client that trusts the
/// server CA but presents no client cert connects and writes (tls-mtls "none:
/// server-authenticated TLS only; client certs are not requested").
pub async fn tls_client_auth_none_allows_anonymous() -> R {
    let pki = TestPki::new()?;
    let (node_cert, node_key) = pki.create_node_cert("node")?;

    let port = port_for("p9_auth_none");
    let cfg = tls_config(&pki.ca_cert_path(), &node_cert, &node_key, ConfigClientAuth::None);
    let server = TestServer::start_with_config_labeled(port, cfg, "tls-none".into()).await?;

    // Client trusts the CA but presents NO client cert (anonymous). With
    // client-auth=none the server does not request one, so the write succeeds.
    let tls = build_anonymous_tls(&pki, "localhost")?;
    let mut c = CeleriantClient::connect_tls(server.address(), tls).await?;
    let key = AggregateKey::new(900, 4, 1);
    c.write_events_with(
        key.clone(),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, ..Default::default() },
    )
    .await?;
    let batches = read_all(&mut c, &key).await?;
    if batches.len() != 1 {
        return Err("anonymous client over client-auth=none failed to write".into());
    }
    Ok(())
}

/// 9.5 Identity required but absent: with `--require-client-identity`, a client
/// that sends no identity is refused (identity "identity required but not sent";
/// error-codes 10004 IdentifyRequired). Observed via a write attempt without an
/// identify handshake.
pub async fn identity_required_but_absent_rejected() -> R {
    let port = port_for("p9_ident_absent");
    let cfg = ServerConfig {
        standalone: true,
        num_shards: Some(1),
        require_client_identity: true,
        // Identity requires TLS unless this guard is lifted (tls-mtls); we run
        // plaintext for the test and lift the guard for local dev.
        insecure_allow_plaintext_auth: true,
        ..Default::default()
    };
    let server = TestServer::start_with_config_labeled(port, cfg, "require-identity".into()).await?;

    let mut c = CeleriantClient::connect(server.address()).await?;
    // No identify() call. A write must be refused for missing identity.
    let res = c
        .write_events_with(
            AggregateKey::new(900, 5, 1),
            vec![event(1, TYPE, 1000, "{}")],
            WriteEventsOptions { allow_create: true, ..Default::default() },
        )
        .await;
    match res {
        Err(ClientError::IdentityRequired) => Ok(()),
        Err(ClientError::Server(ServerError::Auth { kind: AuthError::Required, .. })) => Ok(()),
        other => Err(format!("expected IdentityRequired (10004), got {other:?}").into()),
    }
}

/// 9.6 Public-key identity is accepted and the client id is derived
/// deterministically from the public key (identity "The client's id is derived
/// deterministically from its public key"; wire-protocol "id is ... SHA-256(DER
/// public key)[..16]"). After a successful identify, a write under the matching
/// client_id is accepted.
pub async fn public_key_identity_accepted_and_deterministic() -> R {
    let port = port_for("p9_ident_pubkey");
    let cfg = ServerConfig {
        standalone: true,
        num_shards: Some(1),
        require_client_identity: true,
        insecure_allow_plaintext_auth: true,
        ..Default::default()
    };
    let server = TestServer::start_with_config_labeled(port, cfg, "require-identity".into()).await?;

    let kp = Crypto::generate_keypair(None)?;
    // The id the docs promise: SHA-256(DER public key)[..16].
    let pub_der = base64_decode(&kp.public_key_base64)?;
    let expected_id = Crypto::generate_short_client_identity(&pub_der);

    let mut c = CeleriantClient::connect(server.address()).await?;
    let identity = ClientIdentityConfig::from_key_pair(kp.public_key_base64.clone(), kp.private_key_base64.clone());
    let returned = c.identify(&identity).await?;
    match returned {
        Some(id) if id == expected_id => {}
        Some(id) => return Err(format!("server returned client id {id}, expected {expected_id} (SHA-256(pubkey)[..16])").into()),
        None => return Err("identify returned no client id for a public-key identity".into()),
    }

    // A write under the identified client id is accepted.
    c.write_events_with(
        AggregateKey::new(900, 6, 1),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, client_id: expected_id, ..Default::default() },
    )
    .await?;
    Ok(())
}

/// 9.7 A signature that does not verify against the presented public key is
/// rejected with IdentifyInvalidSignature (10002) (identity "a signature that
/// does not verify"; error-codes 10002). Forced by signing with a private key
/// that does not match the advertised public key.
pub async fn identity_bad_signature_rejected() -> R {
    let port = port_for("p9_ident_badsig");
    let cfg = ServerConfig {
        standalone: true,
        num_shards: Some(1),
        require_client_identity: true,
        insecure_allow_plaintext_auth: true,
        ..Default::default()
    };
    let server = TestServer::start_with_config_labeled(port, cfg, "require-identity".into()).await?;

    let kp_a = Crypto::generate_keypair(None)?;
    let kp_b = Crypto::generate_keypair(None)?;
    // Advertise A's public key but sign with B's private key → signature is
    // valid RSA but does not verify against A's public key.
    let mismatched = ClientIdentityConfig::from_key_pair(kp_a.public_key_base64, kp_b.private_key_base64);

    let mut c = CeleriantClient::connect(server.address()).await?;
    let res = c.identify(&mismatched).await;
    match res {
        Err(ClientError::Server(ServerError::Auth { kind: AuthError::InvalidSignature, .. })) => Ok(()),
        other => Err(format!("expected IdentifyInvalidSignature (10002), got {other:?}").into()),
    }
}

/// 9.8 A write whose `clientId` does not match the identified client is rejected
/// with IdentifyMismatch (10003) (identity "a clientId in a write that does not
/// match the identified client"; error-codes 10003).
pub async fn identity_clientid_mismatch_rejected() -> R {
    let port = port_for("p9_ident_mismatch");
    let cfg = ServerConfig {
        standalone: true,
        num_shards: Some(1),
        require_client_identity: true,
        insecure_allow_plaintext_auth: true,
        ..Default::default()
    };
    let server = TestServer::start_with_config_labeled(port, cfg, "require-identity".into()).await?;

    let kp = Crypto::generate_keypair(None)?;
    let pub_der = base64_decode(&kp.public_key_base64)?;
    let real_id = Crypto::generate_short_client_identity(&pub_der);

    let mut c = CeleriantClient::connect(server.address()).await?;
    c.identify(&ClientIdentityConfig::from_key_pair(kp.public_key_base64, kp.private_key_base64)).await?;

    // Write claiming a DIFFERENT client_id than the identified one.
    let bogus = real_id.wrapping_add(0xDEAD_BEEF);
    let res = c
        .write_events_with(
            AggregateKey::new(900, 8, 1),
            vec![event(1, TYPE, 1000, "{}")],
            WriteEventsOptions { allow_create: true, client_id: bogus, ..Default::default() },
        )
        .await;
    match res {
        Err(ClientError::Server(ServerError::Auth { kind: AuthError::Mismatch, .. })) => Ok(()),
        other => Err(format!("expected IdentifyMismatch (10003), got {other:?}").into()),
    }
}

// --- helpers -------------------------------------------------------------

fn base64_decode(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use base64::Engine;
    Ok(base64::engine::general_purpose::STANDARD.decode(s)?)
}

/// Connect over TLS and issue one request, so the result reflects whether the
/// server accepted the session (handshake reject surfaces at connect; a request
/// reject surfaces here).
async fn connect_and_probe(
    address: &str,
    tls: celeriant_client_tokio::celeriant_client::ClientTlsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut c = tokio::time::timeout(Duration::from_secs(10), CeleriantClient::connect_tls(address, tls))
        .await
        .map_err(|_| "connect_tls timed out")??;
    c.write_events_with(
        AggregateKey::new(900, 2, 1),
        vec![event(1, TYPE, 1000, "{}")],
        WriteEventsOptions { allow_create: true, ..Default::default() },
    )
    .await?;
    Ok(())
}

/// A client TLS config that trusts the test CA but presents NO client identity
/// certificate (anonymous client). Built directly from rustls so we can omit the
/// client auth cert that `TestPki::build_client_tls_config` always attaches.
fn build_anonymous_tls(
    pki: &TestPki,
    server_name: &str,
) -> Result<celeriant_client_tokio::celeriant_client::ClientTlsConfig, Box<dyn std::error::Error>> {
    use rustls_pki_types::ServerName;
    use std::sync::Arc;

    let ca_pem = std::fs::read(pki.ca_cert_path())?;
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = std::io::BufReader::new(&ca_pem[..]);
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert?)?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let sni = ServerName::try_from(server_name.to_string())?;
    Ok(celeriant_client_tokio::celeriant_client::ClientTlsConfig::new(Arc::new(client_config), sni))
}
