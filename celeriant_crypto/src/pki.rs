use std::fs;
use std::io::BufReader;
use std::net::IpAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, SanType,
};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use time::{Duration, OffsetDateTime};

/// Errors from PKI operations.
#[derive(Debug)]
pub enum PkiError {
    Io(std::io::Error),
    CertGen(rcgen::Error),
    Tls(rustls::Error),
    Verifier(rustls::server::VerifierBuilderError),
    NoPrivateKey(PathBuf),
    NoCertificates(PathBuf),
    /// A host string could not be parsed as a DNS name.
    InvalidDnsName(String),
    /// The requested validity duration overflows the datetime representation.
    InvalidValidity(String),
}

impl From<std::io::Error> for PkiError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<rcgen::Error> for PkiError {
    fn from(e: rcgen::Error) -> Self {
        Self::CertGen(e)
    }
}

impl From<rustls::Error> for PkiError {
    fn from(e: rustls::Error) -> Self {
        Self::Tls(e)
    }
}

impl From<rustls::server::VerifierBuilderError> for PkiError {
    fn from(e: rustls::server::VerifierBuilderError) -> Self {
        Self::Verifier(e)
    }
}

impl std::fmt::Display for PkiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "PKI I/O error: {e}"),
            Self::CertGen(e) => write!(f, "Certificate generation error: {e}"),
            Self::Tls(e) => write!(f, "TLS error: {e}"),
            Self::Verifier(e) => write!(f, "Client verifier error: {e}"),
            Self::NoPrivateKey(p) => write!(f, "No private key found in {:?}", p),
            Self::NoCertificates(p) => write!(f, "No certificates found in {:?}", p),
            Self::InvalidDnsName(s) => write!(f, "Invalid DNS name: {s}"),
            Self::InvalidValidity(s) => write!(f, "Invalid certificate validity: {s}"),
        }
    }
}

impl std::error::Error for PkiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::CertGen(e) => Some(e),
            Self::Tls(e) => Some(e),
            Self::Verifier(e) => Some(e),
            _ => None,
        }
    }
}

/// Controls whether and how the server verifies client certificates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuthMode {
    /// Clients must present a certificate signed by the CA. Reject anonymous clients.
    Require,
    /// Verify the cert if presented, but allow anonymous clients.
    Optional,
    /// Do not request or verify client certificates.
    None,
}

/// Manages X.509 certificate generation, loading, and rustls configuration.
pub struct PkiManager;

impl PkiManager {
    /// Generate a self-signed CA keypair. Writes `ca.crt` + `ca.key` to `ca_dir`.
    ///
    /// Uses ECDSA P-256, `BasicConstraints CA:true pathLen:0`, `KeyUsage keyCertSign cRLSign`.
    pub fn create_ca(ca_dir: &Path, validity_days: u32) -> Result<(), PkiError> {
        fs::create_dir_all(ca_dir)?;

        let key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let (not_before, not_after) = cert_validity_window(validity_days)?;

        let mut params = CertificateParams::new(vec![])?;
        params.distinguished_name.push(DnType::CommonName, "celeriant-cluster-ca");
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.not_before = not_before;
        params.not_after = not_after;

        let cert = params.self_signed(&key_pair)?;
        write_pem_pair(&ca_dir.join("ca.crt"), &ca_dir.join("ca.key"), &cert.pem(), &key_pair.serialize_pem())
    }

    /// Generate a node certificate signed by the CA. Writes `node.crt` + `node.key` to `cert_dir`.
    ///
    /// `hosts` is a list of IP addresses and/or hostnames added as SANs.
    /// EKU: `serverAuth` + `clientAuth`.
    pub fn create_node_cert(
        ca_dir: &Path,
        cert_dir: &Path,
        hosts: &[String],
        validity_days: u32,
    ) -> Result<(), PkiError> {
        fs::create_dir_all(cert_dir)?;

        let ca_issuer = load_ca_issuer(ca_dir)?;
        let node_key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let (not_before, not_after) = cert_validity_window(validity_days)?;

        let mut params = CertificateParams::new(vec![])?;
        params.subject_alt_names = parse_san_types(hosts)?;
        params.distinguished_name.push(DnType::CommonName, "celeriant-node");
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.not_before = not_before;
        params.not_after = not_after;

        let cert = params.signed_by(&node_key_pair, &ca_issuer)?;
        write_pem_pair(
            &cert_dir.join("node.crt"),
            &cert_dir.join("node.key"),
            &cert.pem(),
            &node_key_pair.serialize_pem(),
        )
    }

    /// Generate a client certificate signed by the CA.
    ///
    /// Writes `client-{name}.crt` + `client-{name}.key` to `cert_dir`.
    /// EKU: `clientAuth`. CN: `celeriant-client-{name}`.
    pub fn create_client_cert(
        ca_dir: &Path,
        cert_dir: &Path,
        client_name: &str,
        validity_days: u32,
    ) -> Result<(), PkiError> {
        fs::create_dir_all(cert_dir)?;

        let ca_issuer = load_ca_issuer(ca_dir)?;
        let client_key_pair = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let (not_before, not_after) = cert_validity_window(validity_days)?;

        let cn = format!("celeriant-client-{client_name}");
        let mut params = CertificateParams::new(vec![])?;
        params.distinguished_name.push(DnType::CommonName, &cn);
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.not_before = not_before;
        params.not_after = not_after;

        let cert = params.signed_by(&client_key_pair, &ca_issuer)?;
        write_pem_pair(
            &cert_dir.join(format!("client-{client_name}.crt")),
            &cert_dir.join(format!("client-{client_name}.key")),
            &cert.pem(),
            &client_key_pair.serialize_pem(),
        )
    }

    /// Load CA trust anchors from a PEM file. Supports concatenated CA bundles.
    pub fn load_ca_bundle(ca_cert_path: &Path) -> Result<Vec<CertificateDer<'static>>, PkiError> {
        read_cert_chain(ca_cert_path)
    }

    /// Load a certificate chain + private key from PEM files.
    pub fn load_identity(
        cert_path: &Path,
        key_path: &Path,
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), PkiError> {
        let certs = read_cert_chain(cert_path)?;

        let key_data = fs::read(key_path)?;
        let mut key_reader = BufReader::new(key_data.as_slice());
        let key = rustls_pemfile::private_key(&mut key_reader)?
            .ok_or_else(|| PkiError::NoPrivateKey(key_path.to_owned()))?;

        Ok((certs, key))
    }

    /// Build a rustls `ServerConfig` for TLS 1.3.
    ///
    /// `client_auth` controls whether client certificates are required, optional, or disabled.
    pub fn build_server_config(
        ca_bundle: &[CertificateDer<'static>],
        node_cert_chain: Vec<CertificateDer<'static>>,
        node_key: PrivateKeyDer<'static>,
        client_auth: ClientAuthMode,
    ) -> Result<Arc<rustls::ServerConfig>, PkiError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?;

        let config = match client_auth {
            ClientAuthMode::None => builder
                .with_no_client_auth()
                .with_single_cert(node_cert_chain, node_key)?,
            ClientAuthMode::Require => {
                let root_store = build_root_store(ca_bundle)?;
                let verifier = WebPkiClientVerifier::builder_with_provider(
                    Arc::new(root_store),
                    provider,
                )
                .build()?;
                builder
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(node_cert_chain, node_key)?
            }
            ClientAuthMode::Optional => {
                let root_store = build_root_store(ca_bundle)?;
                let verifier = WebPkiClientVerifier::builder_with_provider(
                    Arc::new(root_store),
                    provider,
                )
                .allow_unauthenticated()
                .build()?;
                builder
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(node_cert_chain, node_key)?
            }
        };

        Ok(Arc::new(config))
    }

    /// Build a rustls `ClientConfig` for TLS 1.3.
    pub fn build_client_config(
        ca_bundle: &[CertificateDer<'static>],
        client_cert_chain: Vec<CertificateDer<'static>>,
        client_key: PrivateKeyDer<'static>,
    ) -> Result<Arc<rustls::ClientConfig>, PkiError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let root_store = build_root_store(ca_bundle)?;

        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(root_store)
            .with_client_auth_cert(client_cert_chain, client_key)?;

        Ok(Arc::new(config))
    }
}

// --- Internal helpers ---

/// Compute not_before (60 s in the past) and not_after for a certificate.
///
/// Returns `Err(PkiError::InvalidValidity)` if `validity_days` overflows the datetime range.
fn cert_validity_window(validity_days: u32) -> Result<(OffsetDateTime, OffsetDateTime), PkiError> {
    let now = OffsetDateTime::now_utc();
    // 60-second skew: overflow is astronomically unlikely; fall back to now (no skew buffer).
    let not_before = now.checked_sub(Duration::seconds(60)).unwrap_or(now);
    let not_after = now
        .checked_add(Duration::days(i64::from(validity_days)))
        .ok_or_else(|| {
            PkiError::InvalidValidity(format!(
                "validity_days={validity_days} overflows the datetime representation"
            ))
        })?;
    Ok((not_before, not_after))
}

/// Write a cert PEM and key PEM to disk, then restrict the key file to 0600.
fn write_pem_pair(
    cert_path: &Path,
    key_path: &Path,
    cert_pem: &str,
    key_pem: &str,
) -> Result<(), PkiError> {
    fs::write(cert_path, cert_pem)?;
    fs::write(key_path, key_pem)?;
    set_key_permissions(key_path)
}

/// Parse PEM file at `path` into a vector of DER certificates.
fn read_cert_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, PkiError> {
    let data = fs::read(path)?;
    let mut reader = BufReader::new(data.as_slice());
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(PkiError::NoCertificates(path.to_owned()));
    }
    Ok(certs)
}

/// Load CA cert and key from `ca_dir`, construct an `Issuer` for signing.
fn load_ca_issuer(ca_dir: &Path) -> Result<Issuer<'static, KeyPair>, PkiError> {
    let ca_cert_pem = fs::read_to_string(ca_dir.join("ca.crt"))?;
    let ca_key_pem = fs::read_to_string(ca_dir.join("ca.key"))?;

    let ca_key_pair = KeyPair::from_pem(&ca_key_pem)?;
    let issuer = Issuer::from_ca_cert_pem(&ca_cert_pem, ca_key_pair)?;

    Ok(issuer)
}

/// Build a `RootCertStore` from a slice of DER certificates.
fn build_root_store(ca_bundle: &[CertificateDer<'static>]) -> Result<RootCertStore, PkiError> {
    let mut store = RootCertStore::empty();
    for cert in ca_bundle {
        store.add(cert.clone())?;
    }
    Ok(store)
}

/// Parse a list of host strings into rcgen `SanType`s.
///
/// Each entry is tried as an IP address first; if that fails it's treated as a DNS name.
fn parse_san_types(hosts: &[String]) -> Result<Vec<SanType>, PkiError> {
    hosts
        .iter()
        .map(|h| {
            if let Ok(ip) = IpAddr::from_str(h) {
                Ok(SanType::IpAddress(ip))
            } else {
                h.clone()
                    .try_into()
                    .map(SanType::DnsName)
                    .map_err(|_| PkiError::InvalidDnsName(h.clone()))
            }
        })
        .collect()
}

/// Set file permissions to 0600 (owner read/write only).
fn set_key_permissions(path: &Path) -> Result<(), PkiError> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    #[test]
    fn test_create_ca() {
        let dir = temp_dir();
        PkiManager::create_ca(dir.path(), 3650).expect("create_ca failed");

        let ca_crt = dir.path().join("ca.crt");
        let ca_key = dir.path().join("ca.key");
        assert!(ca_crt.exists(), "ca.crt not found");
        assert!(ca_key.exists(), "ca.key not found");

        let meta = fs::metadata(&ca_key).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);

        let certs = PkiManager::load_ca_bundle(&ca_crt).unwrap();
        assert_eq!(certs.len(), 1);

        let (_, x509) = x509_parser::parse_x509_certificate(certs[0].as_ref()).unwrap();
        assert!(x509.is_ca(), "cert should be a CA");
        // OID 1.2.840.10045.4.3.2 is ecdsaWithSHA256 (ECDSA P-256)
        assert_eq!(
            x509.signature_algorithm.algorithm.to_id_string(),
            "1.2.840.10045.4.3.2",
            "expected ECDSA P-256 signature algorithm"
        );
    }

    #[test]
    fn test_create_node_cert() {
        let ca_dir = temp_dir();
        let cert_dir = temp_dir();

        PkiManager::create_ca(ca_dir.path(), 3650).unwrap();

        let hosts = vec!["127.0.0.1".to_string(), "localhost".to_string()];
        PkiManager::create_node_cert(ca_dir.path(), cert_dir.path(), &hosts, 90).unwrap();

        let node_crt = cert_dir.path().join("node.crt");
        let node_key = cert_dir.path().join("node.key");
        assert!(node_crt.exists());
        assert!(node_key.exists());

        let certs = PkiManager::load_ca_bundle(&node_crt).unwrap();
        let (_, x509) = x509_parser::parse_x509_certificate(certs[0].as_ref()).unwrap();

        let san_ext = x509.subject_alternative_name().unwrap().unwrap();
        let san_values = &san_ext.value.general_names;
        let has_ip = san_values.iter().any(|n| {
            matches!(n, x509_parser::extensions::GeneralName::IPAddress(b) if *b == [127, 0, 0, 1])
        });
        let has_dns = san_values.iter().any(|n| {
            matches!(n, x509_parser::extensions::GeneralName::DNSName(s) if *s == "localhost")
        });
        assert!(has_ip, "missing IP SAN 127.0.0.1");
        assert!(has_dns, "missing DNS SAN localhost");

        let eku = x509.extended_key_usage().unwrap().unwrap();
        assert!(eku.value.server_auth, "missing serverAuth EKU");
        assert!(eku.value.client_auth, "missing clientAuth EKU");
    }

    #[test]
    fn test_create_client_cert() {
        let ca_dir = temp_dir();
        let cert_dir = temp_dir();

        PkiManager::create_ca(ca_dir.path(), 3650).unwrap();
        PkiManager::create_client_cert(ca_dir.path(), cert_dir.path(), "myapp", 90).unwrap();

        let client_crt = cert_dir.path().join("client-myapp.crt");
        let client_key = cert_dir.path().join("client-myapp.key");
        assert!(client_crt.exists());
        assert!(client_key.exists());

        let certs = PkiManager::load_ca_bundle(&client_crt).unwrap();
        let (_, x509) = x509_parser::parse_x509_certificate(certs[0].as_ref()).unwrap();

        let cn = x509
            .subject()
            .iter_common_name()
            .next()
            .and_then(|attr| attr.as_str().ok())
            .unwrap_or("");
        assert_eq!(cn, "celeriant-client-myapp");

        let eku = x509.extended_key_usage().unwrap().unwrap();
        assert!(eku.value.client_auth, "missing clientAuth EKU");
        assert!(!eku.value.server_auth, "should not have serverAuth EKU");
    }

    #[test]
    fn test_load_ca_bundle() {
        let dir = temp_dir();
        PkiManager::create_ca(dir.path(), 3650).unwrap();

        let bundle = PkiManager::load_ca_bundle(&dir.path().join("ca.crt")).unwrap();
        assert_eq!(bundle.len(), 1);
    }

    #[test]
    fn test_load_identity() {
        let ca_dir = temp_dir();
        let cert_dir = temp_dir();

        PkiManager::create_ca(ca_dir.path(), 3650).unwrap();
        PkiManager::create_node_cert(ca_dir.path(), cert_dir.path(), &["localhost".to_string()], 90).unwrap();

        let (chain, key) = PkiManager::load_identity(
            &cert_dir.path().join("node.crt"),
            &cert_dir.path().join("node.key"),
        )
        .unwrap();
        assert!(!chain.is_empty());
        // ECDSA P-256 keys are stored as PKCS8.
        assert!(matches!(key, PrivateKeyDer::Pkcs8(_)));
    }

    #[test]
    fn test_build_server_config() {
        let ca_dir = temp_dir();
        let cert_dir = temp_dir();

        PkiManager::create_ca(ca_dir.path(), 3650).unwrap();
        PkiManager::create_node_cert(ca_dir.path(), cert_dir.path(), &["localhost".to_string()], 90).unwrap();

        let ca_bundle = PkiManager::load_ca_bundle(&ca_dir.path().join("ca.crt")).unwrap();
        let (chain, key) = PkiManager::load_identity(
            &cert_dir.path().join("node.crt"),
            &cert_dir.path().join("node.key"),
        )
        .unwrap();

        let cfg = PkiManager::build_server_config(&ca_bundle, chain.clone(), key.clone_key(), ClientAuthMode::None);
        assert!(cfg.is_ok(), "build_server_config(None) failed: {:?}", cfg);

        let cfg = PkiManager::build_server_config(&ca_bundle, chain.clone(), key.clone_key(), ClientAuthMode::Require);
        assert!(cfg.is_ok(), "build_server_config(Require) failed: {:?}", cfg);

        let cfg = PkiManager::build_server_config(&ca_bundle, chain, key, ClientAuthMode::Optional);
        assert!(cfg.is_ok(), "build_server_config(Optional) failed: {:?}", cfg);
    }

    #[test]
    fn test_build_client_config() {
        let ca_dir = temp_dir();
        let cert_dir = temp_dir();

        PkiManager::create_ca(ca_dir.path(), 3650).unwrap();
        PkiManager::create_client_cert(ca_dir.path(), cert_dir.path(), "test", 90).unwrap();

        let ca_bundle = PkiManager::load_ca_bundle(&ca_dir.path().join("ca.crt")).unwrap();
        let (chain, key) = PkiManager::load_identity(
            &cert_dir.path().join("client-test.crt"),
            &cert_dir.path().join("client-test.key"),
        )
        .unwrap();

        let cfg = PkiManager::build_client_config(&ca_bundle, chain, key);
        assert!(cfg.is_ok(), "build_client_config failed: {:?}", cfg);
    }

    #[test]
    fn test_ca_bundle_concatenation() {
        let ca1_dir = temp_dir();
        let ca2_dir = temp_dir();

        PkiManager::create_ca(ca1_dir.path(), 3650).unwrap();
        PkiManager::create_ca(ca2_dir.path(), 3650).unwrap();

        let ca1_pem = fs::read_to_string(ca1_dir.path().join("ca.crt")).unwrap();
        let ca2_pem = fs::read_to_string(ca2_dir.path().join("ca.crt")).unwrap();
        let bundle_pem = format!("{ca1_pem}{ca2_pem}");

        let bundle_path = ca1_dir.path().join("bundle.crt");
        fs::write(&bundle_path, bundle_pem).unwrap();

        let bundle = PkiManager::load_ca_bundle(&bundle_path).unwrap();
        assert_eq!(bundle.len(), 2, "expected 2 certs in bundle, got {}", bundle.len());
    }

    #[test]
    fn test_parse_san_types_invalid_dns_name() {
        // Non-ASCII strings are rejected by rcgen's Ia5String validation.
        let hosts = vec!["caf\u{00e9}.example.com".to_string()];
        let result = parse_san_types(&hosts);
        assert!(matches!(result, Err(PkiError::InvalidDnsName(_))));
    }

    #[test]
    fn test_cert_validity_overflow() {
        // u32::MAX days is far beyond the representable datetime range.
        let result = cert_validity_window(u32::MAX);
        assert!(
            matches!(result, Err(PkiError::InvalidValidity(_))),
            "expected InvalidValidity, got {result:?}"
        );
    }

    #[test]
    fn test_cert_validity_normal() {
        // Reasonable values must succeed and produce a sensible window.
        let result = cert_validity_window(90);
        assert!(result.is_ok(), "expected Ok for validity_days=90, got {result:?}");
        let (not_before, not_after) = result.unwrap();
        assert!(not_after > not_before, "not_after must be after not_before");
    }
}
