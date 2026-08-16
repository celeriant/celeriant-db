/// Drive the leader-routing failover pattern for a write-class operation.
///
/// `$pool` must be `&CeleriantPool`.
/// `$try_addr` must be a locally-defined `macro_rules!` that accepts a `&str`
/// expression and evaluates to `Result<T, ClientError>` (the single-attempt).
///
/// Pattern:
/// 1. Try the cached leader / primary address.
/// 2. On `NotLeader { Some(addr) }` → update cache, retry once.
/// 3. On `NotLeader { None }` / `ConnectionFailed` → iterate seeds up to
///    `PoolOptions::max_leader_retries` nodes.
macro_rules! leader_route {
    ($pool:expr, $try_addr:ident) => {{
        let pool: &CeleriantPool = $pool;
        let all_addrs = pool.options.all_addresses();
        if all_addrs.is_empty() {
            return Err(err_no_addresses());
        }

        let first_addr = pool.current_or_primary_leader();
        match $try_addr!(&first_addr) {
            Ok(result) => return Ok(result),
            Err(ClientError::NotLeader { leader_address: Some(ref new_addr), .. }) => {
                let new_addr = new_addr.clone();
                pool.update_leader(new_addr.clone());
                return $try_addr!(&new_addr);
            }
            Err(ClientError::NotLeader { leader_address: None, .. }) => {}
            Err(ClientError::ConnectionFailed(_)) => { pool.clear_leader(); }
            Err(ClientError::ConnectionTimeout) => { pool.clear_leader(); }
            // Pooled conn died mid-request: leader probably gone.
            Err(ClientError::WireError(_)) => { pool.clear_leader(); }
            Err(ClientError::ReadError(_)) => { pool.clear_leader(); }
            Err(e @ ClientError::RequestTimeout) => { return Err(e); }
            Err(ClientError::ServerBusy) => {}
            Err(e) => return Err(e),
        }

        let max_retries = pool.options.max_leader_retries;
        let mut retries = 0usize;
        for addr in &all_addrs {
            if retries >= max_retries {
                break;
            }
            if *addr == first_addr {
                continue;
            }
            retries += 1;
            match $try_addr!(addr.as_str()) {
                Ok(result) => {
                    pool.update_leader(addr.clone());
                    return Ok(result);
                }
                Err(ClientError::NotLeader { leader_address: Some(ref new_addr), .. }) => {
                    let new_addr = new_addr.clone();
                    pool.update_leader(new_addr.clone());
                    return $try_addr!(&new_addr);
                }
                Err(ClientError::NotLeader { leader_address: None, .. }) => continue,
                Err(ClientError::ConnectionFailed(_)) => continue,
                Err(ClientError::ConnectionTimeout) => continue,
                Err(ClientError::WireError(_)) => continue,
                Err(ClientError::ReadError(_)) => continue,
                Err(ClientError::RequestTimeout) => continue,
                Err(ClientError::ServerBusy) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(ClientError::ConnectionFailed(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no leader found across known nodes",
        )))
    }};
}

/// Route a read across `read_addresses()` in order (leader-pinned by default,
/// rotated followers on opt-in).
///
/// `$pool` must be `&CeleriantPool`.
/// `$client` is the name to bind the `&mut CeleriantClient` to inside `$body`.
/// `$body` is an expression (typically an `async` method call on `$client`)
/// that returns `Result<_, ClientError>`. The macro expands inline so that
/// async method borrows have no lifetime issues.
///
/// Connection-class failures skip to the next candidate (clearing the leader
/// cache when the pinned leader was the one that failed). In leader-pinned
/// mode `ServerBusy`/`RequestTimeout` return to the caller rather than
/// silently downgrading the read to a follower.
macro_rules! read_route {
    ($pool:expr, $client:ident => $body:expr) => {{
        let pool: &CeleriantPool = $pool;
        let addrs = pool.read_addresses();
        if addrs.is_empty() {
            return Err(err_no_addresses());
        }
        let to_followers = pool.options.route_reads_to_followers;
        for (i, addr) in addrs.iter().enumerate() {
            let pinned_leader = !to_followers && i == 0;
            let node = pool.get_or_create_node(addr);
            match node.get().await {
                Ok(mut conn) => {
                    let $client = conn.client();
                    match $body.await {
                        Ok(resp) => return Ok(resp),
                        Err(ClientError::ConnectionFailed(_)) => {
                            conn.mark_broken();
                            if pinned_leader { pool.clear_leader(); }
                            continue;
                        }
                        Err(ClientError::ConnectionTimeout) => {
                            conn.mark_broken();
                            if pinned_leader { pool.clear_leader(); }
                            continue;
                        }
                        Err(ClientError::WireError(_)) => {
                            conn.mark_broken();
                            if pinned_leader { pool.clear_leader(); }
                            continue;
                        }
                        Err(ClientError::ReadError(_)) => {
                            conn.mark_broken();
                            if pinned_leader { pool.clear_leader(); }
                            continue;
                        }
                        Err(e @ ClientError::RequestTimeout) => {
                            conn.mark_broken();
                            // Last candidate: surface the real error, never
                            // "all unreachable" the node answered.
                            if to_followers && i + 1 < addrs.len() { continue; }
                            return Err(e);
                        }
                        Err(e @ ClientError::ServerBusy) => {
                            if to_followers && i + 1 < addrs.len() { continue; }
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(ClientError::ConnectionFailed(_)) => {
                    if pinned_leader { pool.clear_leader(); }
                    continue;
                }
                Err(ClientError::ConnectionTimeout) => {
                    if pinned_leader { pool.clear_leader(); }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(err_all_unreachable())
    }};
}

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;


use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, ListAggregateTypesRequest, ListAggregatesRequest,
    ListOrgsRequest, ReadRequest, RegisterSchemaRequest, SingleAggregateWrite, TrimStartRequest,
    WatchRequest, WriteRequest,
};
use celeriant_msg::response::responses::{AggregateListItem, AggregateTypeListItem, OrgListItem};
use celeriant_wal::aggregate_type_key::AggregateTypeKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::{
    AggregateDetailsResponse, DeleteResponse, ReadResponse, RegisterSchemaResponse, TrimStartResponse, WriteResponse,
};
use celeriant_wal::aggregate_key::AggregateKey;
use tokio::time::Duration;

use crate::celeriant_client::{CeleriantClient, ClientIdentityConfig, ClientTlsConfig};
use crate::client_error::ClientError;
use crate::client_operations::WriteEventsOptions;
use crate::watch_connection::{WatchConnection, WatchOptions};

// ---------------------------------------------------------------------------
// PoolOptions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PoolOptions {
    /// Primary server address ("host:port")
    pub address: String,
    /// Additional seed addresses for failover and read distribution
    pub seed_addresses: Vec<String>,
    pub tls_config: Option<ClientTlsConfig>,
    pub identity_config: Option<ClientIdentityConfig>,
    /// Maximum connections per node (default: 10)
    pub max_connections_per_node: usize,
    /// Timeout for establishing new connections (default: 5s)
    pub connection_timeout: Duration,
    /// Timeout for individual requests (default: 30s)
    pub request_timeout: Duration,
    /// Max request size in bytes (default: 10 MB)
    pub max_request_size: u64,
    /// Max response size in bytes (default: 64 MB)
    pub max_response_size: u64,
    /// Idle connection lifetime before eviction (default: 25s).
    /// Keep shorter than server's slow_client_timeout (default 30s).
    pub idle_timeout: Duration,
    /// When true, reads and watch subscriptions go to followers instead of the
    /// leader; sheds leader load but gives up read-your-writes. If every
    /// follower fails, the leader serves as last resort (default: false)
    pub route_reads_to_followers: bool,
    /// Maximum number of seed nodes to try during leader failover (default: 3)
    pub max_leader_retries: usize,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            address: String::new(),
            seed_addresses: Vec::new(),
            tls_config: None,
            identity_config: None,
            max_connections_per_node: 10,
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_request_size: 10_000_000,
            max_response_size: 64 * 1024 * 1024,
            idle_timeout: Duration::from_secs(25),
            route_reads_to_followers: false,
            max_leader_retries: 3,
        }
    }
}

impl PoolOptions {
    pub fn new(address: impl Into<String>) -> Self {
        Self { address: address.into(), ..Default::default() }
    }

    pub fn with_seed_addresses(mut self, addrs: Vec<String>) -> Self {
        self.seed_addresses = addrs;
        self
    }

    pub fn with_tls(mut self, tls: ClientTlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    pub fn with_identity(mut self, identity: ClientIdentityConfig) -> Self {
        self.identity_config = Some(identity);
        self
    }

    pub fn with_max_connections(mut self, n: usize) -> Self {
        self.max_connections_per_node = n;
        self
    }

    pub fn with_connection_timeout(mut self, d: Duration) -> Self {
        self.connection_timeout = d;
        self
    }

    pub fn with_request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = d;
        self
    }

    pub fn with_max_request_size(mut self, bytes: u64) -> Self {
        self.max_request_size = bytes;
        self
    }

    pub fn with_max_response_size(mut self, bytes: u64) -> Self {
        self.max_response_size = bytes;
        self
    }

    pub fn with_idle_timeout(mut self, d: Duration) -> Self {
        self.idle_timeout = d;
        self
    }

    pub fn with_route_reads_to_followers(mut self, v: bool) -> Self {
        self.route_reads_to_followers = v;
        self
    }

    pub fn with_max_leader_retries(mut self, n: usize) -> Self {
        self.max_leader_retries = n;
        self
    }

    fn all_addresses(&self) -> Vec<String> {
        let mut addrs = Vec::with_capacity(1 + self.seed_addresses.len());
        if !self.address.is_empty() {
            addrs.push(self.address.clone());
        }
        addrs.extend_from_slice(&self.seed_addresses);
        addrs
    }
}

// ---------------------------------------------------------------------------
// Routing error helpers
// ---------------------------------------------------------------------------

#[inline]
fn err_no_addresses() -> ClientError {
    ClientError::ConnectionFailed(std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "no addresses configured",
    ))
}

#[inline]
fn err_all_unreachable() -> ClientError {
    ClientError::ConnectionFailed(std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "all nodes unreachable",
    ))
}

// ---------------------------------------------------------------------------
// NodePool (internal)
// ---------------------------------------------------------------------------

/// How long to fast-fail after a connection attempt to a node fails.
/// Prevents thousands of tasks from each independently timing out on a dead host.
const CIRCUIT_BREAKER_COOLDOWN: Duration = Duration::from_secs(2);

/// Maximum concurrent TCP connection attempts per node. Limits waste when a node
/// is down (at most this many tasks block on TCP connect before the circuit breaker
/// trips), while still allowing parallel connections to healthy nodes.
const MAX_CONCURRENT_CONNECTS: usize = 32;

struct NodePool {
    address: String,
    connections: Mutex<VecDeque<(CeleriantClient, Instant)>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    options: Arc<PoolOptions>,
    /// Limits concurrent TCP connection attempts. Tasks beyond this limit queue
    /// and re-check the circuit breaker when they wake up.
    connect_semaphore: tokio::sync::Semaphore,
    /// Last time a connection attempt to this node failed. Tasks that arrive
    /// within `CIRCUIT_BREAKER_COOLDOWN` of this timestamp fail immediately
    /// instead of blocking on TCP connect to a potentially dead host.
    last_connect_failure: Mutex<Option<Instant>>,
    /// Shared pool-level dict cache. Allows `create_client` to supply a known sha
    /// and store received bytes without a back-reference to `CeleriantPool`.
    dict_cache: Arc<Mutex<PoolDictCache>>,
}

impl NodePool {
    fn new(address: String, options: Arc<PoolOptions>, dict_cache: Arc<Mutex<PoolDictCache>>) -> Self {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(options.max_connections_per_node));
        Self {
            address,
            connections: Mutex::new(VecDeque::new()),
            semaphore,
            options,
            connect_semaphore: tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTS),
            last_connect_failure: Mutex::new(None),
            dict_cache,
        }
    }

    fn is_circuit_open(&self) -> bool {
        let guard = self.last_connect_failure.lock().unwrap();
        matches!(*guard, Some(failed_at) if failed_at.elapsed() < CIRCUIT_BREAKER_COOLDOWN)
    }

    async fn get(self: &Arc<Self>) -> Result<PooledConnection, ClientError> {
        if self.is_circuit_open() {
            return Err(ClientError::ConnectionFailed(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("circuit breaker open for {}", self.address),
            )));
        }

        // Acquire a permit — enforces max_connections_per_node as a hard cap.
        let permit = tokio::time::timeout(
            self.options.connection_timeout,
            Arc::clone(&self.semaphore).acquire_owned(),
        )
        .await
        .map_err(|_| ClientError::ConnectionTimeout)?
        .expect("semaphore closed unexpectedly");

        // Evict stale connections and pop the first fresh one.
        let reuse = {
            let mut guard = self.connections.lock().unwrap();
            let idle_timeout = self.options.idle_timeout;
            while let Some((_, ts)) = guard.front() {
                if ts.elapsed() >= idle_timeout {
                    guard.pop_front();
                } else {
                    break;
                }
            }
            guard.pop_front().map(|(c, _)| c)
        };

        if let Some(client) = reuse {
            return Ok(PooledConnection {
                client: Some(client),
                broken: false,
                return_to: Arc::clone(self),
                _permit: permit,
            });
        }

        // Limit concurrent TCP connection attempts. Tasks beyond the limit queue
        // and re-check the circuit breaker (which may have tripped) when they wake.
        let _connect_permit = tokio::time::timeout(
            self.options.connection_timeout,
            self.connect_semaphore.acquire(),
        )
        .await
        .map_err(|_| ClientError::ConnectionTimeout)?
        .expect("connect semaphore closed unexpectedly");

        // Re-check circuit breaker — may have tripped while we waited.
        if self.is_circuit_open() {
            return Err(ClientError::ConnectionFailed(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("circuit breaker open for {}", self.address),
            )));
        }

        // Re-check pool — someone ahead of us may have returned a connection.
        {
            let mut guard = self.connections.lock().unwrap();
            if let Some((client, _)) = guard.pop_front() {
                return Ok(PooledConnection {
                    client: Some(client),
                    broken: false,
                    return_to: Arc::clone(self),
                    _permit: permit,
                });
            }
        }

        match Self::create_client(&self.address, &self.options, &self.dict_cache).await {
            Ok(client) => {
                *self.last_connect_failure.lock().unwrap() = None;
                Ok(PooledConnection {
                    client: Some(client),
                    broken: false,
                    return_to: Arc::clone(self),
                    _permit: permit,
                })
            }
            Err(e) => {
                *self.last_connect_failure.lock().unwrap() = Some(Instant::now());
                Err(e)
            }
        }
    }

    fn return_connection(&self, client: CeleriantClient) {
        let mut guard = self.connections.lock().unwrap();
        // Only pool up to max_connections_per_node idle connections.
        if guard.len() < self.options.max_connections_per_node {
            guard.push_back((client, Instant::now()));
        }
        // If over limit, the connection is simply dropped.
    }

    async fn create_client(
        address: &str,
        options: &PoolOptions,
        dict_cache: &Arc<Mutex<PoolDictCache>>,
    ) -> Result<CeleriantClient, ClientError> {
        let mut client = CeleriantClient::connect_with_timeout(
            address,
            Some(options.connection_timeout),
            options.tls_config.clone(),
        )
        .await?;

        client = client
            .with_timeout(options.request_timeout)
            .with_max_request_size(options.max_request_size)
            .with_max_response_size(options.max_response_size);

        if let Some(ref identity) = options.identity_config {
            let known_sha = dict_cache.lock().unwrap().last_sha.clone();
            let dict_cache_ref = Arc::clone(dict_cache);
            client.identify_with_known_sha(identity, known_sha, move |sha| {
                dict_cache_ref.lock().unwrap().cache.get(sha).cloned()
            }).await?;

            // If the client received new dict bytes, store them in the pool cache.
            if let Some(ref d) = client.current_dict {
                let mut guard = dict_cache.lock().unwrap();
                guard.cache.entry(d.sha.clone()).or_insert_with(|| Arc::clone(&d.bytes));
                guard.last_sha = Some(d.sha.clone());
            }
        }

        Ok(client)
    }
}

// ---------------------------------------------------------------------------
// PooledConnection
// ---------------------------------------------------------------------------

/// A borrowed connection from the pool. Returns to the pool on drop unless
/// `mark_broken()` was called, in which case it is discarded.
pub struct PooledConnection {
    client: Option<CeleriantClient>,
    broken: bool,
    return_to: Arc<NodePool>,
    /// Holds the semaphore permit for the lifetime of this connection.
    /// Released automatically on drop, decrementing the in-flight count.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PooledConnection {
    /// Access the underlying client.
    pub fn client(&mut self) -> &mut CeleriantClient {
        self.client.as_mut().expect("client consumed before drop")
    }

    /// Mark this connection as broken — it will be dropped instead of returned to the pool.
    pub fn mark_broken(&mut self) {
        self.broken = true;
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if !self.broken {
                self.return_to.return_connection(client);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// CeleriantPool
// ---------------------------------------------------------------------------

/// Pool-level content-addressed dict cache.
///
/// All connections to the same cluster (or distinct clusters sharing a dict)
/// read from one in-memory copy. `last_sha` is the most-recently-seen cluster
/// dict sha, sent as `known_dict_sha256` on every new connection so the server
/// can skip re-shipping the bytes.
struct PoolDictCache {
    /// sha256 → bytes. Content-addressed so same dict under different names costs nothing.
    cache: HashMap<String, Arc<[u8]>>,
    /// Most recently confirmed dict sha for this pool's cluster.
    last_sha: Option<String>,
}

impl PoolDictCache {
    fn new() -> Self {
        Self { cache: HashMap::new(), last_sha: None }
    }
}

/// Topology-aware connection pool.
///
/// Routes writes to the leader, distributes reads across nodes, handles
/// transparent failover, and manages connection lifecycle (idle eviction,
/// broken connection discard).
///
/// Wrap in `Arc` if shared across tasks — the pool itself is not `Clone`.
pub struct CeleriantPool {
    options: Arc<PoolOptions>,
    nodes: RwLock<HashMap<String, Arc<NodePool>>>,
    leader_address: RwLock<Option<String>>,
    read_counter: AtomicU64,
    /// Pool-level dict cache shared between all node pools in this pool.
    dict_cache: Arc<Mutex<PoolDictCache>>,
}

impl CeleriantPool {
    pub fn new(options: PoolOptions) -> Self {
        let options = Arc::new(options);
        Self {
            options,
            nodes: RwLock::new(HashMap::new()),
            leader_address: RwLock::new(None),
            read_counter: AtomicU64::new(0),
            dict_cache: Arc::new(Mutex::new(PoolDictCache::new())),
        }
    }

    /// Returns the cached dict bytes for `sha`, or `None` if not yet cached.
    pub fn dict_for_sha(&self, sha: &str) -> Option<Arc<[u8]>> {
        self.dict_cache.lock().unwrap().cache.get(sha).cloned()
    }

    /// Insert `bytes` under `sha` and record it as the last-known cluster dict.
    pub fn cache_dict(&self, sha: String, bytes: Arc<[u8]>) {
        let mut guard = self.dict_cache.lock().unwrap();
        guard.cache.insert(sha.clone(), bytes);
        guard.last_sha = Some(sha);
    }

    // --- High-level operations ---

    pub async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ClientError> {
        read_route!(self, c => c.read(request.clone()))
    }

    pub async fn write(&self, request: WriteRequest) -> Result<WriteResponse, ClientError> {
        self.write_leader(request).await
    }

    /// Convenience method: write events to a single aggregate without constructing a `WriteRequest`.
    ///
    /// `client_id` scopes client-seq idempotency — use a stable id per logical writer, never a
    /// fresh random value per call.
    pub async fn write_events(
        &self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
        client_id: u128,
    ) -> Result<WriteResponse, ClientError> {
        self.write_events_with(aggregate_key, events, client_id, WriteEventsOptions::default()).await
    }

    /// Like `write_events` but accepts options to control idempotency, optimistic concurrency, etc.
    pub async fn write_events_with(
        &self,
        aggregate_key: AggregateKey,
        events: Vec<DatablockAggregateEvent>,
        client_id: u128,
        options: WriteEventsOptions,
    ) -> Result<WriteResponse, ClientError> {
        let mut writes = HashMap::new();
        writes.insert(aggregate_key, SingleAggregateWrite {
            events,
            allow_create: options.allow_create,
            expected_version: options.expected_version,
            enforce_client_idempotency: options.enforce_client_idempotency,
        });
        self.write(WriteRequest {
            correlation_id: None,
            client_id,
            user_id: None,
            writes,
        })
        .await
    }

    pub async fn delete(&self, request: DeleteRequest) -> Result<DeleteResponse, ClientError> {
        self.delete_leader(request).await
    }

    pub async fn trim_start(&self, request: TrimStartRequest) -> Result<TrimStartResponse, ClientError> {
        self.trim_start_leader(request).await
    }

    pub async fn aggregate_details(
        &self,
        request: AggregateDetailsRequest,
    ) -> Result<AggregateDetailsResponse, ClientError> {
        read_route!(self, c => c.aggregate_details(request.clone()))
    }

    pub async fn register_schema(
        &self,
        request: RegisterSchemaRequest,
    ) -> Result<RegisterSchemaResponse, ClientError> {
        self.register_schema_leader(request).await
    }

    /// Create a streaming read-all iterator. The returned iterator holds a
    /// pooled connection for its lifetime.
    pub async fn read_all(
        &self,
        aggregate_key: AggregateKey,
        filters: Option<ReadFilters>,
    ) -> Result<PooledReadAllIterator, ClientError> {
        let conn = self.get_connection().await?;
        Ok(PooledReadAllIterator::new(conn, aggregate_key, filters))
    }

    /// Create a streaming list-orgs iterator. The returned iterator holds a
    /// pooled connection for its lifetime.
    pub async fn list_orgs(
        &self,
        options: crate::list_operations::ListOptions,
    ) -> Result<PooledListOrgsIterator, ClientError> {
        let conn = self.get_connection().await?;
        Ok(PooledListOrgsIterator::new(conn, options))
    }

    /// Create a streaming list-aggregate-types iterator. The returned iterator
    /// holds a pooled connection for its lifetime.
    pub async fn list_aggregate_types(
        &self,
        org_id: Option<u128>,
        options: crate::list_operations::ListOptions,
    ) -> Result<PooledListAggregateTypesIterator, ClientError> {
        let conn = self.get_connection().await?;
        Ok(PooledListAggregateTypesIterator::new(conn, org_id, options))
    }

    /// Create a streaming list-aggregates iterator. The returned iterator holds
    /// a pooled connection for its lifetime.
    pub async fn list_aggregates(
        &self,
        org_id: Option<u128>,
        aggregate_type_id: Option<u128>,
        options: crate::list_operations::ListOptions,
    ) -> Result<PooledListAggregatesIterator, ClientError> {
        let conn = self.get_connection().await?;
        Ok(PooledListAggregatesIterator::new(conn, org_id, aggregate_type_id, options))
    }

    // --- Watch ---

    /// Create a dedicated non-pooled WatchConnection.
    ///
    /// Dials the current leader by default; with `route_reads_to_followers`
    /// it dials a follower to keep subscription load off the leader, falling
    /// through to the remaining candidates (leader last) on connect failure.
    /// The pool's TLS and identity configuration are applied to the watch
    /// connection, overriding any values set on `options`. The pool's dict
    /// cache is threaded in so the watch stream can decompress ZstdDict responses.
    pub async fn watch(
        &self,
        request: WatchRequest,
        mut options: WatchOptions,
    ) -> Result<WatchConnection, ClientError> {
        options.tls_config = self.options.tls_config.clone();
        options.identity_config = self.options.identity_config.clone();
        // Without a dial timeout a black-holed node stalls failover for the
        // OS TCP timeout; default to the pool's connection timeout.
        if options.timeout.is_none() {
            options.timeout = Some(self.options.connection_timeout);
        }

        let addrs = self.read_addresses();
        if addrs.is_empty() {
            return Err(err_no_addresses());
        }
        let to_followers = self.options.route_reads_to_followers;
        let known_sha = self.dict_cache.lock().unwrap().last_sha.clone();
        for (i, addr) in addrs.iter().enumerate() {
            let dict_cache = Arc::clone(&self.dict_cache);
            let result = WatchConnection::connect_with_dict(
                addr,
                request.clone(),
                options.clone(),
                known_sha.clone(),
                move |sha| dict_cache.lock().unwrap().cache.get(sha).cloned(),
            ).await;
            match result {
                Ok(conn) => return Ok(conn),
                Err(ClientError::ConnectionFailed(_) | ClientError::ConnectionTimeout)
                    if i < addrs.len() - 1 =>
                {
                    if !to_followers && i == 0 {
                        self.clear_leader();
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(err_all_unreachable())
    }

    // --- Low-level access ---

    /// Borrow a connection from the read-routing candidate set (leader by default).
    pub async fn get_connection(&self) -> Result<PooledConnection, ClientError> {
        let addrs = self.read_addresses();
        if addrs.is_empty() {
            return Err(err_no_addresses());
        }
        let to_followers = self.options.route_reads_to_followers;
        for (i, addr) in addrs.iter().enumerate() {
            let node = self.get_or_create_node(addr);
            match node.get().await {
                Ok(conn) => return Ok(conn),
                Err(ClientError::ConnectionFailed(_)) | Err(ClientError::ConnectionTimeout) => {
                    if !to_followers && i == 0 {
                        self.clear_leader();
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(err_all_unreachable())
    }

    /// Borrow a connection to the current leader.
    pub async fn get_leader_connection(&self) -> Result<PooledConnection, ClientError> {
        let addr = self.current_or_primary_leader();
        let node = self.get_or_create_node(&addr);
        node.get().await
    }

    // --- Per-operation leader routing helpers ---
    //
    // Each method uses an inline macro to stamp out the leader failover pattern:
    // 1. Try cached leader (or primary address).
    // 2. On NotLeader { Some(addr) } → update cache, retry once.
    // 3. On NotLeader { None } / ConnectionFailed → try each seed address.

    async fn write_leader(&self, request: WriteRequest) -> Result<WriteResponse, ClientError> {
        macro_rules! try_addr {
            ($addr:expr) => {{
                let node = self.get_or_create_node($addr);
                match node.get().await {
                    Ok(mut conn) => match conn.client().write(request.clone()).await {
                        Err(ClientError::ConnectionFailed(e)) => { conn.mark_broken(); Err(ClientError::ConnectionFailed(e)) }
                        Err(ClientError::ConnectionTimeout) => { conn.mark_broken(); Err(ClientError::ConnectionTimeout) }
                        Err(ClientError::WireError(e)) => { conn.mark_broken(); Err(ClientError::WireError(e)) }
                        Err(ClientError::ReadError(e)) => { conn.mark_broken(); Err(ClientError::ReadError(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::RequestTimeout)
                    | Err(e @ ClientError::ServerBusy) => Err(e),
                    Err(e) => return Err(e),
                }
            }};
        }
        leader_route!(self, try_addr)
    }

    async fn delete_leader(&self, request: DeleteRequest) -> Result<DeleteResponse, ClientError> {
        macro_rules! try_addr {
            ($addr:expr) => {{
                let node = self.get_or_create_node($addr);
                match node.get().await {
                    Ok(mut conn) => match conn.client().delete(request.clone()).await {
                        Err(ClientError::ConnectionFailed(e)) => { conn.mark_broken(); Err(ClientError::ConnectionFailed(e)) }
                        Err(ClientError::ConnectionTimeout) => { conn.mark_broken(); Err(ClientError::ConnectionTimeout) }
                        Err(ClientError::WireError(e)) => { conn.mark_broken(); Err(ClientError::WireError(e)) }
                        Err(ClientError::ReadError(e)) => { conn.mark_broken(); Err(ClientError::ReadError(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::RequestTimeout)
                    | Err(e @ ClientError::ServerBusy) => Err(e),
                    Err(e) => return Err(e),
                }
            }};
        }
        leader_route!(self, try_addr)
    }

    async fn trim_start_leader(&self, request: TrimStartRequest) -> Result<TrimStartResponse, ClientError> {
        macro_rules! try_addr {
            ($addr:expr) => {{
                let node = self.get_or_create_node($addr);
                match node.get().await {
                    Ok(mut conn) => match conn.client().trim_start(request.clone()).await {
                        Err(ClientError::ConnectionFailed(e)) => { conn.mark_broken(); Err(ClientError::ConnectionFailed(e)) }
                        Err(ClientError::ConnectionTimeout) => { conn.mark_broken(); Err(ClientError::ConnectionTimeout) }
                        Err(ClientError::WireError(e)) => { conn.mark_broken(); Err(ClientError::WireError(e)) }
                        Err(ClientError::ReadError(e)) => { conn.mark_broken(); Err(ClientError::ReadError(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::RequestTimeout)
                    | Err(e @ ClientError::ServerBusy) => Err(e),
                    Err(e) => return Err(e),
                }
            }};
        }
        leader_route!(self, try_addr)
    }

    async fn register_schema_leader(
        &self,
        request: RegisterSchemaRequest,
    ) -> Result<RegisterSchemaResponse, ClientError> {
        macro_rules! try_addr {
            ($addr:expr) => {{
                let node = self.get_or_create_node($addr);
                match node.get().await {
                    Ok(mut conn) => match conn.client().register_schema(request.clone()).await {
                        Err(ClientError::ConnectionFailed(e)) => { conn.mark_broken(); Err(ClientError::ConnectionFailed(e)) }
                        Err(ClientError::ConnectionTimeout) => { conn.mark_broken(); Err(ClientError::ConnectionTimeout) }
                        Err(ClientError::WireError(e)) => { conn.mark_broken(); Err(ClientError::WireError(e)) }
                        Err(ClientError::ReadError(e)) => { conn.mark_broken(); Err(ClientError::ReadError(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::RequestTimeout)
                    | Err(e @ ClientError::ServerBusy) => Err(e),
                    Err(e) => return Err(e),
                }
            }};
        }
        leader_route!(self, try_addr)
    }

    /// Ordered read candidates honoring `route_reads_to_followers`.
    ///
    /// Default: cached leader (or primary) first so reads see their own writes;
    /// the remaining nodes are connection-failure fallback only. Opt-in:
    /// followers rotated to spread load, then the leader LAST; reached only
    /// when every follower failed, so a follower outage degrades to leader
    /// reads instead of a read outage.
    fn read_addresses(&self) -> Vec<String> {
        let all = self.options.all_addresses();
        if !self.options.route_reads_to_followers {
            let leader = self.current_or_primary_leader();
            // Unset primary with seed-only config: nothing to pin yet.
            if leader.is_empty() {
                return all;
            }
            let mut addrs = Vec::with_capacity(all.len() + 1);
            addrs.push(leader);
            for a in all {
                if a != addrs[0] {
                    addrs.push(a);
                }
            }
            return addrs;
        }
        let leader = { self.leader_address.read().unwrap().clone() };
        let Some(leader) = leader else {
            let mut candidates = all;
            if candidates.len() > 1 {
                let start = self.read_counter.fetch_add(1, Ordering::Relaxed) as usize % candidates.len();
                candidates.rotate_left(start);
            }
            return candidates;
        };
        let mut candidates: Vec<String> = all.into_iter().filter(|a| *a != leader).collect();
        if candidates.len() > 1 {
            let start = self.read_counter.fetch_add(1, Ordering::Relaxed) as usize % candidates.len();
            candidates.rotate_left(start);
        }
        candidates.push(leader);
        candidates
    }

    /// Test hook: the address a watch subscription dials first — the leader by
    /// default, a rotating follower on opt-in. `watch()` itself iterates the
    /// full candidate list, so this exists only to pin the routing contract.
    #[cfg(test)]
    fn watch_address(&self) -> String {
        self.read_addresses()
            .into_iter()
            .next()
            .unwrap_or_else(|| self.primary_address())
    }

    fn current_or_primary_leader(&self) -> String {
        self.leader_address
            .read()
            .unwrap()
            .clone()
            .unwrap_or_else(|| self.options.address.clone())
    }

    #[cfg(test)]
    fn primary_address(&self) -> String {
        self.options.address.clone()
    }

    fn update_leader(&self, address: String) {
        *self.leader_address.write().unwrap() = Some(address);
    }

    /// Mark the current leader as suspect. If a cached leader was set, clear it
    /// so the next attempt falls through to the seed retry loop. If no cached
    /// leader was set (i.e., the primary itself failed), pick the first seed
    /// address to avoid retrying the dead primary.
    fn clear_leader(&self) {
        let mut guard = self.leader_address.write().unwrap();
        if guard.is_some() {
            *guard = None;
        } else if let Some(seed) = self.options.seed_addresses.first() {
            *guard = Some(seed.clone());
        }
    }

    fn get_or_create_node(&self, address: &str) -> Arc<NodePool> {
        // Fast path: read lock.
        {
            let guard = self.nodes.read().unwrap();
            if let Some(node) = guard.get(address) {
                return Arc::clone(node);
            }
        }
        // Slow path: write lock.
        let dict_cache = Arc::clone(&self.dict_cache);
        let mut guard = self.nodes.write().unwrap();
        guard
            .entry(address.to_owned())
            .or_insert_with(|| Arc::new(NodePool::new(
                address.to_owned(),
                Arc::clone(&self.options),
                dict_cache,
            )))
            .clone()
    }
}

// ---------------------------------------------------------------------------
// PooledReadAllIterator
// ---------------------------------------------------------------------------

/// Streaming read-all iterator that holds a pooled connection for its lifetime.
///
/// Owns the `PooledConnection` directly and implements pagination inline,
/// avoiding any lifetime or unsafe concerns.
pub struct PooledReadAllIterator {
    conn: PooledConnection,
    aggregate_key: AggregateKey,
    filters: ReadFilters,
    buffer: std::collections::VecDeque<AggregateEventBatch>,
    exhausted: bool,
}

impl PooledReadAllIterator {
    fn new(
        conn: PooledConnection,
        aggregate_key: AggregateKey,
        filters: Option<ReadFilters>,
    ) -> Self {
        Self {
            conn,
            aggregate_key,
            filters: filters.unwrap_or_else(|| ReadFilters::new(1)),
            buffer: std::collections::VecDeque::new(),
            exhausted: false,
        }
    }

    pub async fn next(&mut self) -> Option<Result<AggregateEventBatch, ClientError>> {
        loop {
            if let Some(batch) = self.buffer.pop_front() {
                return Some(Ok(batch));
            }
            if self.exhausted {
                return None;
            }
            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        let request = ReadRequest {
            correlation_id: None,
            aggregate_key: self.aggregate_key.clone(),
            filters: self.filters.clone(),
        };
        let response = self.conn.client().read(request).await?;
        self.buffer.extend(response.event_batches);
        match response.next_aggregate_version {
            Some(next_index) => {
                self.filters.from_aggregate_version = next_index;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub async fn collect(mut self) -> Result<Vec<AggregateEventBatch>, ClientError> {
        let mut results = Vec::new();
        while let Some(batch) = self.next().await {
            results.push(batch?);
        }
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// Shared shard routing helper
// ---------------------------------------------------------------------------

fn is_shard_routing_error(error: &ClientError) -> bool {
    matches!(error, ClientError::Server(crate::server_error::ServerError::ShardRouting { .. }))
}

// ---------------------------------------------------------------------------
// PooledListOrgsIterator
// ---------------------------------------------------------------------------

/// Streaming list-orgs iterator that holds a pooled connection for its lifetime.
pub struct PooledListOrgsIterator {
    conn: PooledConnection,
    shard_cursors: HashMap<u64, Option<u64>>,
    active_shards: VecDeque<u64>,
    max_shard: Option<u64>,
    next_shard_to_try: u64,
    seen: HashSet<u128>,
    buffer: VecDeque<OrgListItem>,
    exhausted: bool,
}

impl PooledListOrgsIterator {
    fn new(conn: PooledConnection, options: crate::list_operations::ListOptions) -> Self {
        let mut active_shards = VecDeque::new();
        let mut shard_cursors = HashMap::new();
        active_shards.push_back(options.start_shard);
        shard_cursors.insert(options.start_shard, None);
        Self {
            conn,
            shard_cursors,
            active_shards,
            max_shard: options.max_shard_hint,
            next_shard_to_try: options.start_shard + 1,
            seen: HashSet::new(),
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    pub async fn next(&mut self) -> Option<Result<OrgListItem, ClientError>> {
        loop {
            while let Some(item) = self.buffer.pop_front() {
                if self.seen.insert(item.org_id) {
                    return Some(Ok(item));
                }
            }
            if self.exhausted {
                return None;
            }
            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        if self.active_shards.is_empty() {
            if !self.try_add_next_shard() {
                return Ok(false);
            }
        }
        let shard_id = match self.active_shards.pop_front() {
            Some(s) => s,
            None => return Ok(false),
        };
        let cursor = self.shard_cursors.get(&shard_id).copied().flatten();
        let request = ClientRequest::ListOrgs(ListOrgsRequest {
            correlation_id: None,
            shard_id,
            cursor,
        });
        match self.conn.client().send_request(&request).await {
            Ok(ClientResponse::ListOrgs(response)) => {
                self.buffer.extend(response.orgs);
                if let Some(next_cursor) = response.next_cursor {
                    self.shard_cursors.insert(shard_id, Some(next_cursor));
                    self.active_shards.push_back(shard_id);
                } else {
                    self.shard_cursors.remove(&shard_id);
                }
                self.try_add_next_shard();
                Ok(true)
            }
            Ok(_) => Err(ClientError::ProtocolError),
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    let last_shard = shard_id.saturating_sub(1);
                    self.active_shards.retain(|s| *s <= last_shard);
                    self.shard_cursors.retain(|s, _| *s <= last_shard);
                    return Ok(!self.active_shards.is_empty() || !self.buffer.is_empty());
                }
                Err(e)
            }
        }
    }

    fn try_add_next_shard(&mut self) -> bool {
        if let Some(max) = self.max_shard {
            if self.next_shard_to_try > max {
                return false;
            }
        }
        if !self.shard_cursors.contains_key(&self.next_shard_to_try) {
            self.shard_cursors.insert(self.next_shard_to_try, None);
            self.active_shards.push_back(self.next_shard_to_try);
            self.next_shard_to_try += 1;
            return true;
        }
        false
    }

    pub async fn collect(mut self) -> Result<Vec<OrgListItem>, ClientError> {
        let mut results = Vec::new();
        while let Some(item) = self.next().await {
            results.push(item?);
        }
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// PooledListAggregateTypesIterator
// ---------------------------------------------------------------------------

/// Streaming list-aggregate-types iterator that holds a pooled connection for
/// its lifetime.
pub struct PooledListAggregateTypesIterator {
    conn: PooledConnection,
    org_id: Option<u128>,
    shard_cursors: HashMap<u64, Option<u64>>,
    active_shards: VecDeque<u64>,
    max_shard: Option<u64>,
    next_shard_to_try: u64,
    seen: HashSet<AggregateTypeKey>,
    buffer: VecDeque<AggregateTypeListItem>,
    exhausted: bool,
}

impl PooledListAggregateTypesIterator {
    fn new(
        conn: PooledConnection,
        org_id: Option<u128>,
        options: crate::list_operations::ListOptions,
    ) -> Self {
        let mut active_shards = VecDeque::new();
        let mut shard_cursors = HashMap::new();
        active_shards.push_back(options.start_shard);
        shard_cursors.insert(options.start_shard, None);
        Self {
            conn,
            org_id,
            shard_cursors,
            active_shards,
            max_shard: options.max_shard_hint,
            next_shard_to_try: options.start_shard + 1,
            seen: HashSet::new(),
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    pub async fn next(&mut self) -> Option<Result<AggregateTypeListItem, ClientError>> {
        loop {
            while let Some(item) = self.buffer.pop_front() {
                let key = AggregateTypeKey::new(item.org_id, item.aggregate_type_id);
                if self.seen.insert(key) {
                    return Some(Ok(item));
                }
            }
            if self.exhausted {
                return None;
            }
            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        if self.active_shards.is_empty() {
            if !self.try_add_next_shard() {
                return Ok(false);
            }
        }
        let shard_id = match self.active_shards.pop_front() {
            Some(s) => s,
            None => return Ok(false),
        };
        let cursor = self.shard_cursors.get(&shard_id).copied().flatten();
        let request = ClientRequest::ListAggregateTypes(ListAggregateTypesRequest {
            correlation_id: None,
            shard_id,
            org_id: self.org_id,
            cursor,
        });
        match self.conn.client().send_request(&request).await {
            Ok(ClientResponse::ListAggregateTypes(response)) => {
                self.buffer.extend(response.aggregate_types);
                if let Some(next_cursor) = response.next_cursor {
                    self.shard_cursors.insert(shard_id, Some(next_cursor));
                    self.active_shards.push_back(shard_id);
                } else {
                    self.shard_cursors.remove(&shard_id);
                }
                self.try_add_next_shard();
                Ok(true)
            }
            Ok(_) => Err(ClientError::ProtocolError),
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    let last_shard = shard_id.saturating_sub(1);
                    self.active_shards.retain(|s| *s <= last_shard);
                    self.shard_cursors.retain(|s, _| *s <= last_shard);
                    return Ok(!self.active_shards.is_empty() || !self.buffer.is_empty());
                }
                Err(e)
            }
        }
    }

    fn try_add_next_shard(&mut self) -> bool {
        if let Some(max) = self.max_shard {
            if self.next_shard_to_try > max {
                return false;
            }
        }
        if !self.shard_cursors.contains_key(&self.next_shard_to_try) {
            self.shard_cursors.insert(self.next_shard_to_try, None);
            self.active_shards.push_back(self.next_shard_to_try);
            self.next_shard_to_try += 1;
            return true;
        }
        false
    }

    pub async fn collect(mut self) -> Result<Vec<AggregateTypeListItem>, ClientError> {
        let mut results = Vec::new();
        while let Some(item) = self.next().await {
            results.push(item?);
        }
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// PooledListAggregatesIterator
// ---------------------------------------------------------------------------

/// Streaming list-aggregates iterator that holds a pooled connection for its
/// lifetime. Merges stats across shards/pages and deduplicates by aggregate key.
pub struct PooledListAggregatesIterator {
    conn: PooledConnection,
    org_id: Option<u128>,
    aggregate_type_id: Option<u128>,
    include_deleted: bool,
    shard_cursors: HashMap<u64, Option<u64>>,
    active_shards: VecDeque<u64>,
    max_shard: Option<u64>,
    next_shard_to_try: u64,
    stats: HashMap<AggregateKey, crate::list_operations::AggregateStats>,
    deleted: HashSet<AggregateKey>,
    order: Vec<AggregateKey>,
    order_pos: usize,
    buffer: VecDeque<AggregateListItem>,
    exhausted: bool,
}

impl PooledListAggregatesIterator {
    fn new(
        conn: PooledConnection,
        org_id: Option<u128>,
        aggregate_type_id: Option<u128>,
        options: crate::list_operations::ListOptions,
    ) -> Self {
        let mut active_shards = VecDeque::new();
        let mut shard_cursors = HashMap::new();
        active_shards.push_back(options.start_shard);
        shard_cursors.insert(options.start_shard, None);
        Self {
            conn,
            org_id,
            aggregate_type_id,
            include_deleted: options.include_deleted,
            shard_cursors,
            active_shards,
            max_shard: options.max_shard_hint,
            next_shard_to_try: options.start_shard + 1,
            stats: HashMap::new(),
            deleted: HashSet::new(),
            order: Vec::new(),
            order_pos: 0,
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    pub async fn next(&mut self) -> Option<Result<crate::list_operations::AggregateStats, ClientError>> {
        loop {
            while let Some(item) = self.buffer.pop_front() {
                let key = AggregateKey::new(item.org_id, item.aggregate_type_id, item.aggregate_id);
                if item.is_deleted {
                    self.deleted.insert(key.clone());
                }
                if let Some(existing) = self.stats.get_mut(&key) {
                    existing.merge(&item);
                    if self.deleted.contains(&key) {
                        existing.is_deleted = true;
                    }
                } else {
                    let mut s = crate::list_operations::AggregateStats::from_item(&item);
                    if self.deleted.contains(&key) {
                        s.is_deleted = true;
                    }
                    self.stats.insert(key.clone(), s);
                    self.order.push(key);
                }
            }

            while self.order_pos < self.order.len() {
                let key = &self.order[self.order_pos];
                self.order_pos += 1;
                if let Some(s) = self.stats.get(key) {
                    if !self.include_deleted && s.is_deleted {
                        continue;
                    }
                    return Some(Ok(s.clone()));
                }
            }

            if self.exhausted {
                return None;
            }

            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        if self.active_shards.is_empty() {
            if !self.try_add_next_shard() {
                return Ok(false);
            }
        }
        let shard_id = match self.active_shards.pop_front() {
            Some(s) => s,
            None => return Ok(false),
        };
        let cursor = self.shard_cursors.get(&shard_id).copied().flatten();
        let request = ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id,
            org_id: self.org_id,
            aggregate_type_id: self.aggregate_type_id,
            cursor,
        });
        match self.conn.client().send_request(&request).await {
            Ok(ClientResponse::ListAggregates(response)) => {
                self.buffer.extend(response.aggregates);
                if let Some(next_cursor) = response.next_cursor {
                    self.shard_cursors.insert(shard_id, Some(next_cursor));
                    self.active_shards.push_back(shard_id);
                } else {
                    self.shard_cursors.remove(&shard_id);
                }
                self.try_add_next_shard();
                Ok(true)
            }
            Ok(_) => Err(ClientError::ProtocolError),
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    let last_shard = shard_id.saturating_sub(1);
                    self.active_shards.retain(|s| *s <= last_shard);
                    self.shard_cursors.retain(|s, _| *s <= last_shard);
                    return Ok(!self.active_shards.is_empty() || !self.buffer.is_empty());
                }
                Err(e)
            }
        }
    }

    fn try_add_next_shard(&mut self) -> bool {
        if let Some(max) = self.max_shard {
            if self.next_shard_to_try > max {
                return false;
            }
        }
        if !self.shard_cursors.contains_key(&self.next_shard_to_try) {
            self.shard_cursors.insert(self.next_shard_to_try, None);
            self.active_shards.push_back(self.next_shard_to_try);
            self.next_shard_to_try += 1;
            return true;
        }
        false
    }

    pub async fn collect(mut self) -> Result<Vec<crate::list_operations::AggregateStats>, ClientError> {
        let mut results = Vec::new();
        while let Some(item) = self.next().await {
            results.push(item?);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_dict_cache_insert_and_lookup() {
        let pool = CeleriantPool::new(PoolOptions::new("127.0.0.1:10000"));
        let bytes: Arc<[u8]> = Arc::from(b"dict bytes" as &[u8]);

        assert!(pool.dict_for_sha("sha1").is_none());

        pool.cache_dict("sha1".to_string(), Arc::clone(&bytes));

        let found = pool.dict_for_sha("sha1").expect("should be cached");
        assert!(Arc::ptr_eq(&found, &bytes));
    }

    #[test]
    fn pool_dict_cache_is_content_addressed() {
        let pool = CeleriantPool::new(PoolOptions::new("127.0.0.1:10000"));
        let bytes: Arc<[u8]> = Arc::from(b"same bytes" as &[u8]);

        pool.cache_dict("sha-a".to_string(), Arc::clone(&bytes));
        pool.cache_dict("sha-b".to_string(), Arc::clone(&bytes));

        // Both shas present.
        assert!(pool.dict_for_sha("sha-a").is_some());
        assert!(pool.dict_for_sha("sha-b").is_some());
    }

    #[test]
    fn pool_dict_cache_reuse_does_not_overwrite_existing_entry() {
        let pool = CeleriantPool::new(PoolOptions::new("127.0.0.1:10000"));
        let bytes: Arc<[u8]> = Arc::from(b"v1 dict" as &[u8]);
        pool.cache_dict("sha1".to_string(), Arc::clone(&bytes));

        // A second insert for the same sha (e.g. a reconnect) must not overwrite.
        // The NodePool uses `entry().or_insert_with()`.
        // Verify the first pointer is still returned.
        let found = pool.dict_for_sha("sha1").unwrap();
        assert!(Arc::ptr_eq(&found, &bytes));
    }

    // Blind-oracle routing tests (session/goal.md contract, authored unseen).
    #[test]
    fn oracle_default_no_leader_primary_first_all_known_once() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into(), "c:1".into()]),
        );
        let addrs = pool.read_addresses();
        assert_eq!(addrs.first().map(String::as_str), Some("p:1"));
        assert_eq!(addrs.len(), 3);
        let uniq: std::collections::HashSet<&String> = addrs.iter().collect();
        assert_eq!(uniq.len(), 3);
        for a in ["p:1", "b:1", "c:1"] {
            assert!(addrs.iter().any(|x| x == a), "missing {a}");
        }
    }

    #[test]
    fn oracle_default_cached_leader_seed_first_primary_later() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into(), "c:1".into()]),
        );
        pool.update_leader("b:1".into());
        let addrs = pool.read_addresses();
        assert_eq!(addrs.first().map(String::as_str), Some("b:1"));
        // primary is still a fallback candidate, just not first
        assert!(addrs[1..].iter().any(|x| x == "p:1"));
        assert_eq!(addrs.len(), 3);
        let uniq: std::collections::HashSet<&String> = addrs.iter().collect();
        assert_eq!(uniq.len(), 3);
    }

    #[test]
    fn oracle_default_order_stable_across_calls() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into(), "c:1".into()]),
        );
        pool.update_leader("c:1".into());
        let first = pool.read_addresses();
        // default mode pins the leader: no rotation between calls
        for _ in 0..5 {
            assert_eq!(pool.read_addresses(), first);
        }
    }

    #[test]
    fn oracle_default_watch_leader_else_primary() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into()]),
        );
        assert_eq!(pool.watch_address(), "p:1");
        pool.update_leader("b:1".into());
        assert_eq!(pool.watch_address(), "b:1");
    }

    #[test]
    fn oracle_default_clear_leader_reverts_to_primary_first() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into(), "c:1".into()]),
        );
        pool.update_leader("b:1".into());
        pool.clear_leader();
        let addrs = pool.read_addresses();
        assert_eq!(addrs.first().map(String::as_str), Some("p:1"));
        assert_eq!(pool.watch_address(), "p:1");
    }

    #[test]
    fn oracle_default_second_update_wins() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into(), "c:1".into()]),
        );
        pool.update_leader("b:1".into());
        pool.update_leader("c:1".into());
        let addrs = pool.read_addresses();
        assert_eq!(addrs.first().map(String::as_str), Some("c:1"));
        assert_eq!(pool.watch_address(), "c:1");
    }

    #[test]
    fn oracle_default_unknown_leader_goes_first_knowns_follow() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into()]),
        );
        pool.update_leader("x:9".into());
        let addrs = pool.read_addresses();
        // cached leader leads even when outside the known set; knowns remain fallbacks
        assert_eq!(addrs.first().map(String::as_str), Some("x:9"));
        for a in ["p:1", "b:1"] {
            assert_eq!(addrs.iter().filter(|x| *x == a).count(), 1, "{a} once");
        }
        assert_eq!(pool.watch_address(), "x:9");
    }

    #[test]
    fn oracle_optin_leader_present_but_last() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into(), "c:1".into()])
                .with_route_reads_to_followers(true),
        );
        pool.update_leader("p:1".into());
        // amendment 2: leader is a candidate again, but only as last resort
        let addrs = pool.read_addresses();
        assert_eq!(addrs.last().map(String::as_str), Some("p:1"));
        assert_eq!(addrs.iter().filter(|x| *x == "p:1").count(), 1);
        for a in ["b:1", "c:1"] {
            assert_eq!(addrs[..addrs.len() - 1].iter().filter(|x| *x == a).count(), 1, "{a} once before leader");
        }
        assert_eq!(addrs.len(), 3);
    }

    #[test]
    fn oracle_optin_no_leader_all_known_candidates() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into(), "c:1".into()])
                .with_route_reads_to_followers(true),
        );
        let addrs = pool.read_addresses();
        assert_eq!(addrs.len(), 3);
        for a in ["p:1", "b:1", "c:1"] {
            assert_eq!(addrs.iter().filter(|x| *x == a).count(), 1, "{a} once");
        }
    }

    #[test]
    fn oracle_optin_rotation_covers_all_followers() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into(), "c:1".into(), "d:1".into()])
                .with_route_reads_to_followers(true),
        );
        pool.update_leader("p:1".into());
        let mut firsts = std::collections::HashSet::new();
        for _ in 0..12 {
            let addrs = pool.read_addresses();
            // leader never leads, always closes the list
            assert_ne!(addrs[0], "p:1");
            assert_eq!(addrs.last().map(String::as_str), Some("p:1"));
            firsts.insert(addrs[0].clone());
        }
        // load spread: every follower must lead the list eventually
        for a in ["b:1", "c:1", "d:1"] {
            assert!(firsts.contains(a), "{a} never first");
        }
    }

    #[test]
    fn oracle_optin_watch_never_leader_and_rotates() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into(), "c:1".into()])
                .with_route_reads_to_followers(true),
        );
        pool.update_leader("p:1".into());
        let mut seen = std::collections::HashSet::new();
        for _ in 0..8 {
            let w = pool.watch_address();
            assert_ne!(w, "p:1", "watch must avoid the leader");
            seen.insert(w);
        }
        assert!(seen.contains("b:1") && seen.contains("c:1"), "watch must rotate followers");
    }

    #[test]
    fn oracle_optin_watch_no_followers_falls_back() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_route_reads_to_followers(true),
        );
        pool.update_leader("p:1".into());
        // amendment 2: no followers means the leader itself is the sole candidate
        assert_eq!(pool.watch_address(), "p:1");
    }

    #[test]
    fn oracle_optin_single_node_leader_read_addresses_safe() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_route_reads_to_followers(true),
        );
        pool.update_leader("p:1".into());
        // amendment 2: single node that is the leader => exactly [leader], never empty
        assert_eq!(pool.read_addresses(), vec!["p:1".to_string()]);
    }

    #[test]
    fn oracle_optin_clear_leader_restores_rotation() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into(), "c:1".into()])
                .with_route_reads_to_followers(true),
        );
        pool.update_leader("b:1".into());
        // while cached, b:1 is pinned to the tail and never leads
        for _ in 0..6 {
            let addrs = pool.read_addresses();
            assert_ne!(addrs[0], "b:1");
            assert_eq!(addrs.last().map(String::as_str), Some("b:1"));
        }
        pool.clear_leader();
        // no leader cached: all nodes rotate, b:1 leads again eventually
        let mut firsts = std::collections::HashSet::new();
        for _ in 0..12 {
            let addrs = pool.read_addresses();
            assert_eq!(addrs.len(), 3);
            firsts.insert(addrs[0].clone());
        }
        assert!(firsts.contains("b:1"), "b:1 never first after clear");
    }

    #[test]
    fn oracle_optin_only_latest_leader_last() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into(), "c:1".into()])
                .with_route_reads_to_followers(true),
        );
        pool.update_leader("b:1".into());
        pool.update_leader("c:1".into());
        let addrs = pool.read_addresses();
        // only the latest leader takes the tail; the prior one rejoins the followers
        assert_eq!(addrs.last().map(String::as_str), Some("c:1"));
        assert_eq!(addrs.iter().filter(|x| *x == "c:1").count(), 1);
        for a in ["b:1", "p:1"] {
            assert_eq!(addrs[..addrs.len() - 1].iter().filter(|x| *x == a).count(), 1, "{a} once before leader");
        }
        assert_eq!(addrs.len(), 3);
    }

    #[test]
    fn oracle_optin_offlist_leader_still_last() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into()])
                .with_route_reads_to_followers(true),
        );
        pool.update_leader("x:9".into());
        // a cached leader outside the seed set is still the last-resort candidate
        let addrs = pool.read_addresses();
        assert_eq!(addrs.last().map(String::as_str), Some("x:9"));
        assert_eq!(addrs.iter().filter(|x| *x == "x:9").count(), 1);
        for a in ["p:1", "b:1"] {
            assert_eq!(addrs[..addrs.len() - 1].iter().filter(|x| *x == a).count(), 1, "{a} once before leader");
        }
        assert_eq!(addrs.len(), 3);
    }

    #[test]
    fn oracle_optin_leader_is_last_resort() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1")
                .with_seed_addresses(vec!["b:1".into(), "c:1".into()])
                .with_route_reads_to_followers(true),
        );
        pool.update_leader("p:1".into());
        // every call: a follower opens the list, the leader closes it
        for _ in 0..8 {
            let addrs = pool.read_addresses();
            assert!(addrs[0] == "b:1" || addrs[0] == "c:1", "got {addrs:?}");
            assert_eq!(addrs.last().map(String::as_str), Some("p:1"));
        }
    }
}
