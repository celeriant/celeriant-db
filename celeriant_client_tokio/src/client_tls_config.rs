use std::path::Path;
use std::sync::Arc;

use celeriant_crypto::pki::{PkiError, PkiManager};
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// A rustls client config bound to the SNI it will present.
/// Build from PEM files with `from_paths`, or from a `rustls::ClientConfig` with `new`.
#[derive(Clone)]
pub struct ClientTlsConfig {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl std::fmt::Debug for ClientTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTlsConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl ClientTlsConfig {
    pub fn new(client_config: Arc<rustls::ClientConfig>, server_name: ServerName<'static>) -> Self {
        Self {
            connector: TlsConnector::from(client_config),
            server_name,
        }
    }

    /// Load PEM files from disk and build a TLS 1.3 client config trusting `ca_cert`.
    /// `identity` supplies the client cert chain and key for mTLS; `None` is an anonymous client.
    pub fn from_paths(
        ca_cert: &Path,
        identity: Option<(&Path, &Path)>,
        server_name: &str,
    ) -> Result<Self, PkiError> {
        let ca_bundle = PkiManager::load_ca_bundle(ca_cert)?;

        let client_config = match identity {
            Some((cert_path, key_path)) => {
                let (chain, key) = PkiManager::load_identity(cert_path, key_path)?;
                PkiManager::build_client_config(&ca_bundle, chain, key)?
            }
            None => PkiManager::build_client_config_no_auth(&ca_bundle)?,
        };

        Ok(Self::new(client_config, parse_sni(server_name)?))
    }

    /// The SNI this config presents.
    pub fn server_name(&self) -> &ServerName<'static> {
        &self.server_name
    }

    /// Begin the TLS handshake over an established socket.
    /// The caller owns the deadline; this only starts it.
    pub fn handshake(&self, tcp: TcpStream) -> tokio_rustls::Connect<TcpStream> {
        self.connector.connect(self.server_name.clone(), tcp)
    }
}

/// Parse a host into the SNI presented during the handshake.
fn parse_sni(server_name: &str) -> Result<ServerName<'static>, PkiError> {
    ServerName::try_from(server_name.to_owned())
        .map_err(|_| PkiError::InvalidDnsName(server_name.to_owned()))
}

/// Host portion of a `"host:port"` address, for use as the TLS SNI.
///
/// `"10.0.0.1:10000"` and `"node.local:10000"` give the part before the last
/// colon; `"[::1]:10000"` gives `"::1"`. A bare host with no port is returned
/// unchanged, so callers may pass either form.
pub fn sni_host(address: &str) -> Result<&str, PkiError> {
    if let Some(rest) = address.strip_prefix('[') {
        return rest
            .split_once(']')
            .map(|(host, _)| host)
            .ok_or_else(|| PkiError::InvalidDnsName(address.to_owned()));
    }
    Ok(address.rsplit_once(':').map_or(address, |(host, _)| host))
}

/// Accepts any server certificate. Testing only
#[cfg(test)]
#[derive(Debug)]
struct AcceptAnyServerCert(Vec<rustls::SignatureScheme>);

#[cfg(test)]
impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.clone()
    }
}

#[cfg(test)]
impl ClientTlsConfig {
    /// A config that trusts every server certificate, for tests that exercise the
    /// transport rather than the trust decision.
    pub(crate) fn dangerous_accept_any(server_name: &str) -> Result<Self, PkiError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let schemes = provider.signature_verification_algorithms.supported_schemes();

        let client_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(schemes)))
            .with_no_client_auth();

        Ok(Self::new(Arc::new(client_config), parse_sni(server_name)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// A CA plus one client identity signed by it.
    struct TestCerts {
        _dir: TempDir,
        ca_cert: PathBuf,
        client_cert: PathBuf,
        client_key: PathBuf,
    }

    fn test_certs() -> TestCerts {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let ca_dir = dir.path().join("ca");
        let client_dir = dir.path().join("client");

        PkiManager::create_ca(&ca_dir, 90).expect("create_ca failed");
        PkiManager::create_client_cert(&ca_dir, &client_dir, "tester", 90)
            .expect("create_client_cert failed");

        TestCerts {
            ca_cert: ca_dir.join("ca.crt"),
            client_cert: client_dir.join("client-tester.crt"),
            client_key: client_dir.join("client-tester.key"),
            _dir: dir,
        }
    }

    #[test]
    fn an_identity_builds_a_mutual_tls_config() {
        let certs = test_certs();

        let tls = ClientTlsConfig::from_paths(
            &certs.ca_cert,
            Some((&certs.client_cert, &certs.client_key)),
            "localhost",
        )
        .expect("from_paths failed");

        assert_eq!(*tls.server_name(), ServerName::try_from("localhost").unwrap());
    }

    #[test]
    fn no_identity_builds_an_anonymous_config() {
        let certs = test_certs();

        ClientTlsConfig::from_paths(&certs.ca_cert, None, "localhost")
            .expect("anonymous from_paths failed");
    }

    #[test]
    fn an_ip_literal_is_accepted_as_a_server_name() {
        let certs = test_certs();

        let tls = ClientTlsConfig::from_paths(&certs.ca_cert, None, "127.0.0.1")
            .expect("ip server name rejected");

        assert_eq!(*tls.server_name(), ServerName::try_from("127.0.0.1").unwrap());
    }

    #[test]
    fn a_malformed_server_name_is_rejected() {
        let certs = test_certs();

        let result = ClientTlsConfig::from_paths(&certs.ca_cert, None, "not a hostname");

        assert!(
            matches!(result, Err(PkiError::InvalidDnsName(ref s)) if s == "not a hostname"),
            "expected InvalidDnsName, got {:?}",
            result.map(|_| ())
        );
    }

    #[test]
    fn a_missing_ca_file_is_rejected() {
        let certs = test_certs();
        let missing = certs.ca_cert.parent().unwrap().join("absent.crt");

        let result = ClientTlsConfig::from_paths(&missing, None, "localhost");

        assert!(
            matches!(result, Err(PkiError::Io(_))),
            "expected Io, got {:?}",
            result.map(|_| ())
        );
    }

    #[test]
    fn a_ca_file_holding_no_certificate_is_rejected() {
        let certs = test_certs();
        let empty = certs.ca_cert.parent().unwrap().join("empty.crt");
        std::fs::write(&empty, b"not pem at all\n").unwrap();

        let result = ClientTlsConfig::from_paths(&empty, None, "localhost");

        assert!(
            matches!(result, Err(PkiError::NoCertificates(_))),
            "expected NoCertificates, got {:?}",
            result.map(|_| ())
        );
    }

    #[test]
    fn sni_host_takes_the_name_before_the_port() {
        assert_eq!(sni_host("10.0.0.1:10000").unwrap(), "10.0.0.1");
        assert_eq!(sni_host("node.cluster.local:10000").unwrap(), "node.cluster.local");
    }

    #[test]
    fn sni_host_unwraps_an_ipv6_literal() {
        assert_eq!(sni_host("[::1]:10000").unwrap(), "::1");
        assert_eq!(sni_host("[fd00::5]:10000").unwrap(), "fd00::5");
    }

    #[test]
    fn sni_host_passes_through_an_address_with_no_port() {
        assert_eq!(sni_host("localhost").unwrap(), "localhost");
    }

    #[test]
    fn sni_host_rejects_an_unclosed_ipv6_bracket() {
        assert!(matches!(
            sni_host("[::1-no-bracket"),
            Err(PkiError::InvalidDnsName(_))
        ));
    }

    #[test]
    fn a_cert_presented_without_its_key_is_rejected() {
        let certs = test_certs();

        let result = ClientTlsConfig::from_paths(
            &certs.ca_cert,
            Some((&certs.client_cert, &certs.client_cert)),
            "localhost",
        );

        assert!(
            matches!(result, Err(PkiError::NoPrivateKey(_))),
            "expected NoPrivateKey, got {:?}",
            result.map(|_| ())
        );
    }
}