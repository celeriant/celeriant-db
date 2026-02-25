use std::cell::Cell;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use celeriant_crypto::pki::{ClientAuthMode, PkiManager};
use tracing::{info, warn};

use crate::sharded::tls_config::{TlsConfig, TlsMode};

/// Watches TLS certificate files for mtime changes and rebuilds TLS configs on demand.
pub struct TlsReloader {
    ca_cert: PathBuf,
    node_cert: PathBuf,
    node_key: PathBuf,
    client_auth: ClientAuthMode,
    tls_mode: TlsMode,
    /// Most recent mtime seen across all three files (oldest wins for change detection).
    last_mtimes: Cell<[SystemTime; 3]>,
}

impl TlsReloader {
    pub fn new(
        ca_cert: PathBuf,
        node_cert: PathBuf,
        node_key: PathBuf,
        client_auth: ClientAuthMode,
        tls_mode: TlsMode,
    ) -> Self {
        let mtimes = read_mtimes(&ca_cert, &node_cert, &node_key);
        Self {
            ca_cert,
            node_cert,
            node_key,
            client_auth,
            tls_mode,
            last_mtimes: Cell::new(mtimes),
        }
    }

    /// Check whether any cert file has changed. If so, rebuild and return the new config.
    ///
    /// Returns `None` if no change was detected or if reload fails (a warning is logged).
    pub fn check_and_reload(&self) -> Option<Arc<TlsConfig>> {
        let current = read_mtimes(&self.ca_cert, &self.node_cert, &self.node_key);
        if current == self.last_mtimes.get() {
            return None;
        }

        info!(
            ca_cert = %self.ca_cert.display(),
            node_cert = %self.node_cert.display(),
            "TLS cert files changed, reloading"
        );

        match self.rebuild() {
            Ok(cfg) => {
                self.last_mtimes.set(current);
                Some(Arc::new(cfg))
            }
            Err(e) => {
                warn!(error = %e, "TLS cert reload failed, keeping existing config");
                None
            }
        }
    }

    fn rebuild(&self) -> Result<TlsConfig, String> {
        let ca_bundle = PkiManager::load_ca_bundle(&self.ca_cert)
            .map_err(|e| format!("load CA bundle: {e}"))?;
        let (cert_chain, node_key) = PkiManager::load_identity(&self.node_cert, &self.node_key)
            .map_err(|e| format!("load identity: {e}"))?;

        let mut server_config = PkiManager::build_server_config(
            &ca_bundle,
            cert_chain.clone(),
            node_key.clone_key(),
            self.client_auth,
        )
        .map_err(|e| format!("build server config: {e}"))?;

        Arc::get_mut(&mut server_config)
            .ok_or("Arc<ServerConfig> already shared")?
            .enable_secret_extraction = true;

        let mut client_config =
            PkiManager::build_client_config(&ca_bundle, cert_chain, node_key)
                .map_err(|e| format!("build client config: {e}"))?;

        Arc::get_mut(&mut client_config)
            .ok_or("Arc<ClientConfig> already shared")?
            .enable_secret_extraction = true;

        Ok(TlsConfig {
            server_config,
            client_config,
            tls_mode: self.tls_mode,
        })
    }
}

/// Read the mtime of each cert file. Uses `UNIX_EPOCH` as a fallback so that
/// an unreadable file does not mask future changes.
fn read_mtimes(ca: &PathBuf, cert: &PathBuf, key: &PathBuf) -> [SystemTime; 3] {
    [mtime(ca), mtime(cert), mtime(key)]
}

fn mtime(path: &PathBuf) -> SystemTime {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH)
}
