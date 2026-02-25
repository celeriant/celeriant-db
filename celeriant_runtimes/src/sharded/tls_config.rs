use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    Strict,
}

pub struct TlsConfig {
    pub server_config: Arc<rustls::ServerConfig>,
    /// Client config for outbound replication connections (node → node).
    pub client_config: Arc<rustls::ClientConfig>,
    pub tls_mode: TlsMode,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("tls_mode", &self.tls_mode)
            .finish_non_exhaustive()
    }
}
