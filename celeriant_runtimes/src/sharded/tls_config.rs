use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    Strict,
}

pub struct TlsConfig {
    /// Server config for the client-facing listener (port 10000).
    /// Trusts tls_ca_cert (the client CA).
    pub client_server_config: Arc<rustls::ServerConfig>,
    /// Server config for the replication listener (port 10001).
    /// Trusts tls_intracluster_ca_cert (or tls_ca_cert if not set).
    pub replication_server_config: Arc<rustls::ServerConfig>,
    /// Client config for outbound node→node replication connections.
    /// Trusts tls_intracluster_ca_cert (or tls_ca_cert if not set).
    pub replication_client_config: Arc<rustls::ClientConfig>,
    pub tls_mode: TlsMode,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("tls_mode", &self.tls_mode)
            .finish_non_exhaustive()
    }
}
