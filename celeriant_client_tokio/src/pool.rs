/// Drive the leader-routing failover pattern for a write-class operation.
///
/// `$pool` must be `&CeleriantPool`.
/// `$try_addr` must be a locally-defined `macro_rules!` that accepts a `&str`
/// expression and evaluates to `Result<T, ClientError>` (the single-attempt).
///
/// One walk over candidate addresses: the cached leader (or primary) first,
/// then untried seeds. A `NotLeader` hint jumps the walk to the hinted node
/// and updates the cache; if the hinted node fails too, the walk resumes with
/// the seeds instead of aborting. Only a definitive `NotLeader` answer retires
/// an address, so a node whose connection failed can still be reached by a
/// later hint. Hint hops and seed attempts share the `max_leader_retries`
/// budget, so a circular hint chain terminates.
///
/// `PoolTimeout` returns to the caller: the wait was on this client's own
/// semaphores, no node was contacted, and no follower can serve a write.
/// Any post-send failure returns to the caller: the request was flushed, so
/// re-sending it anywhere could duplicate the operation.
/// `RequestTimeout` returns to the caller: the node may have applied the
/// operation, so blindly retrying it elsewhere risks a duplicate write.
macro_rules! leader_route {
    ($pool:expr, $try_addr:ident) => {{
        let pool: &CeleriantPool = $pool;
        let all_addrs = pool.options.all_addresses();
        if all_addrs.is_empty() {
            return Err(err_no_addresses());
        }

        let first_addr = pool.current_or_primary_leader();
        // Bookkeeping stays empty (and unallocated) until a node answers.
        let mut answered: Vec<String> = Vec::new();
        let mut next_hint: Option<String> = None;
        let mut seeds = all_addrs.iter();
        let mut retries = 0usize;
        let mut candidate = first_addr.clone();
        loop {
            let last_err = match $try_addr!(&candidate) {
                Ok(result) => {
                    if candidate != first_addr {
                        pool.update_leader(candidate);
                    }
                    return Ok(result);
                }
                Err(e @ ClientError::PoolTimeout { .. }) => return Err(e),
                // Post-send: the frame is on the wire and either no answer came
                // back (`ConnectionLostAfterSend`) or one came that this client
                // could not read. The node may have applied it, so it is never
                // re-sent. A `MessageTooLarge` request is pre-send but fails the
                // same way on every node, so it returns here too.
                Err(e @ ClientError::ConnectionLostAfterSend(_)) => {
                    pool.routing.post_send_losses.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
                Err(e @ ClientError::WireError(_)) | Err(e @ ClientError::ReadError(_)) => {
                    return Err(e);
                }
                Err(e @ ClientError::RequestTimeout) => return Err(e),
                Err(e @ ClientError::NotLeader { .. }) => {
                    // A definitive answer retires this address first, so a node
                    // that hints at itself is not dialled twice.
                    answered.push(candidate.clone());
                    if let ClientError::NotLeader { leader_address: Some(hint), .. } = &e {
                        // A hint into an open circuit breaker is skipped without
                        // spending a retry: it would fast-fail and starve an
                        // untried seed of the budget.
                        if !answered.iter().any(|a| a == hint) && !pool.is_known_node_down(hint) {
                            next_hint = Some(hint.clone());
                        } else {
                            pool.routing.hints_skipped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    e
                }
                // Pre-send: the node was never reached, so the walk goes on.
                Err(e @ ClientError::ConnectionFailed(_))
                | Err(e @ ClientError::ConnectionTimeout) => {
                    pool.clear_leader();
                    e
                }
                Err(e @ ClientError::ServerBusy) => e,
                Err(e) => return Err(e),
            };
            if retries >= pool.options.max_leader_retries {
                return Err(pool.err_walk_exhausted(&candidate, &last_err));
            }
            retries += 1;
            candidate = match next_hint.take() {
                Some(hint) => {
                    pool.routing.redirects_followed.fetch_add(1, Ordering::Relaxed);
                    pool.update_leader(hint.clone());
                    hint
                }
                None => {
                    match seeds.find(|a| **a != first_addr && !answered.iter().any(|x| x == *a)) {
                        Some(addr) => addr.clone(),
                        None => return Err(pool.err_walk_exhausted(&candidate, &last_err)),
                    }
                }
            };
        }
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
                        Err(e @ ClientError::WireError(WireError::MessageTooLarge { .. })) => {
                            conn.mark_broken();
                            return Err(e);
                        }
                        Err(ClientError::WireError(_)) => {
                            conn.mark_broken();
                            if pinned_leader { pool.clear_leader(); }
                            continue;
                        }
                        Err(ClientError::ReadError(_))
                        // A read is safe to retry, so a lost response is just
                        // another broken candidate.
                        | Err(ClientError::ConnectionLostAfterSend(_)) => {
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
                // A local pool wait doesnt clear cleader
                Err(e @ ClientError::PoolTimeout { .. }) => {
                    if i + 1 < addrs.len() { continue; }
                    return Err(e);
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
use celeriant_wire::network::wire_error::WireError;
use tokio::time::Duration;

use crate::ClientTlsConfig;
use crate::celeriant_client::{CeleriantClient, ClientIdentityConfig};
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
// Stats
// ---------------------------------------------------------------------------

/// Exclusive upper bounds, in milliseconds, of the `NodePool::get()` wait
/// histogram. Anything at or above the last bound lands in the final bucket.
const WAIT_BUCKET_BOUNDS_MS: [u128; 5] = [1, 10, 100, 1_000, 10_000];

pub const WAIT_BUCKETS: usize = WAIT_BUCKET_BOUNDS_MS.len() + 1;

const WAIT_BUCKET_LABELS: [&str; WAIT_BUCKETS] =
    ["<1ms", "<10ms", "<100ms", "<1s", "<10s", ">=10s"];

fn wait_bucket(waited: Duration) -> usize {
    let ms = waited.as_millis();
    WAIT_BUCKET_BOUNDS_MS.iter().position(|bound| ms < *bound).unwrap_or(WAIT_BUCKETS - 1)
}

/// Per-node connection counters, live. Every write is a relaxed increment: a
/// snapshot is an operational read, never a synchronisation point.
#[derive(Default)]
struct NodeCounters {
    attempted: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    timed_out: AtomicU64,
    pool_timeouts_permit: AtomicU64,
    pool_timeouts_connect: AtomicU64,
    circuit_breaker_rejections: AtomicU64,
    pooled_reuse: AtomicU64,
    preflight_retired: AtomicU64,
    wait_buckets: [AtomicU64; WAIT_BUCKETS],
}

impl NodeCounters {
    fn record_wait(&self, waited: Duration) {
        self.wait_buckets[wait_bucket(waited)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ConnectionStats {
        ConnectionStats {
            attempted: self.attempted.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            pool_timeouts_permit: self.pool_timeouts_permit.load(Ordering::Relaxed),
            pool_timeouts_connect: self.pool_timeouts_connect.load(Ordering::Relaxed),
            circuit_breaker_rejections: self.circuit_breaker_rejections.load(Ordering::Relaxed),
            pooled_reuse: self.pooled_reuse.load(Ordering::Relaxed),
            preflight_retired: self.preflight_retired.load(Ordering::Relaxed),
            wait_buckets: std::array::from_fn(|i| self.wait_buckets[i].load(Ordering::Relaxed)),
        }
    }
}

/// Pool-level leader-routing counters, live.
#[derive(Default)]
struct RoutingCounters {
    redirects_followed: AtomicU64,
    hints_skipped: AtomicU64,
    cache_clears: AtomicU64,
    pinned_to_seed: AtomicU64,
    walks_exhausted: AtomicU64,
    post_send_losses: AtomicU64,
}

/// Connection counters for one node, or summed over every node of a pool.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ConnectionStats {
    /// New connections dialled. A `get()` served from the idle list is not an attempt.
    pub attempted: u64,
    pub succeeded: u64,
    /// Pre-send remote failure of the dial or the handshake.
    pub failed: u64,
    /// The subset of pre-send remote failures that were `ConnectionTimeout`.
    pub timed_out: u64,
    /// `PoolTimeout` waiting on this client's own per-node permit gate.
    pub pool_timeouts_permit: u64,
    /// `PoolTimeout` waiting on this client's own 32-wide connect gate.
    pub pool_timeouts_connect: u64,
    pub circuit_breaker_rejections: u64,
    /// `get()` calls served from the idle list.
    pub pooled_reuse: u64,
    /// Idle connections dropped at checkout because the peer had closed them.
    pub preflight_retired: u64,
    /// `get()` entry to return, whatever the outcome, bucketed by `WAIT_BUCKET_LABELS`.
    pub wait_buckets: [u64; WAIT_BUCKETS],
}

impl ConnectionStats {
    pub fn pool_timeouts(&self) -> u64 {
        self.pool_timeouts_permit + self.pool_timeouts_connect
    }

    fn add(&mut self, other: &ConnectionStats) {
        self.attempted += other.attempted;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.timed_out += other.timed_out;
        self.pool_timeouts_permit += other.pool_timeouts_permit;
        self.pool_timeouts_connect += other.pool_timeouts_connect;
        self.circuit_breaker_rejections += other.circuit_breaker_rejections;
        self.pooled_reuse += other.pooled_reuse;
        self.preflight_retired += other.preflight_retired;
        for (slot, n) in self.wait_buckets.iter_mut().zip(other.wait_buckets) {
            *slot += n;
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeStats {
    pub address: String,
    pub stats: ConnectionStats,
}

/// A point-in-time read of a pool's counters. Snapshotting allocates; the
/// counters it reads cost one relaxed increment each on the hot path.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PoolStats {
    /// Summed over every node this pool has dialled.
    pub connections: ConnectionStats,
    /// `NotLeader` hints the walk hopped to.
    pub leader_redirects_followed: u64,
    /// `NotLeader` hints refused: already answered in this walk, or breaker open.
    pub leader_hints_skipped: u64,
    pub leader_cache_clears: u64,
    /// Failures that found no cached leader and pinned `seed_addresses[0]`
    /// instead: a follower, whenever the primary is the leader.
    pub leader_pinned_to_seed: u64,
    /// Leader walks that ran out of candidates.
    pub walks_exhausted: u64,
    /// `ConnectionLostAfterSend` returned to a write caller.
    pub post_send_losses: u64,
    /// Per-address breakdown, sorted by address.
    pub nodes: Vec<NodeStats>,
}

impl PoolStats {
    /// Fold another pool's snapshot in. Rows for the same address are summed.
    /// For bench runs that spread tasks over several pools.
    pub fn merge(&mut self, other: PoolStats) {
        self.connections.add(&other.connections);
        self.leader_redirects_followed += other.leader_redirects_followed;
        self.leader_hints_skipped += other.leader_hints_skipped;
        self.leader_cache_clears += other.leader_cache_clears;
        self.leader_pinned_to_seed += other.leader_pinned_to_seed;
        self.walks_exhausted += other.walks_exhausted;
        self.post_send_losses += other.post_send_losses;
        for node in other.nodes {
            match self.nodes.iter_mut().find(|n| n.address == node.address) {
                Some(existing) => existing.stats.add(&node.stats),
                None => self.nodes.push(node),
            }
        }
        self.nodes.sort_unstable_by(|a, b| a.address.cmp(&b.address));
    }
}

impl std::fmt::Display for PoolStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = &self.connections;
        write!(
            f,
            "Pool stats: connects {}/{} ok, {} failed, {} timed out | reuse {} | \
             pool timeouts {} (permit {}, connect {}) | breaker rejects {} | \
             preflight retired {} | \
             redirects {}, hints skipped {}, cache clears {}, pinned to seed {}, \
             walks exhausted {}, \
             post-send losses {} | wait",
            c.succeeded,
            c.attempted,
            c.failed,
            c.timed_out,
            c.pooled_reuse,
            c.pool_timeouts(),
            c.pool_timeouts_permit,
            c.pool_timeouts_connect,
            c.circuit_breaker_rejections,
            c.preflight_retired,
            self.leader_redirects_followed,
            self.leader_hints_skipped,
            self.leader_cache_clears,
            self.leader_pinned_to_seed,
            self.walks_exhausted,
            self.post_send_losses,
        )?;
        for (label, count) in WAIT_BUCKET_LABELS.iter().zip(&c.wait_buckets) {
            write!(f, " {label}={count}")?;
        }
        Ok(())
    }
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

fn pop_fresh_from<T>(q: &mut VecDeque<(T, Instant)>, idle_timeout: Duration) -> Option<T> {
    while let Some((_, ts)) = q.front() {
        if ts.elapsed() >= idle_timeout {
            q.pop_front();
        } else {
            break;
        }
    }
    q.pop_front().map(|(c, _)| c)
}

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
    counters: NodeCounters,
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
            counters: NodeCounters::default(),
        }
    }

    /// A wait on one of this pool's own semaphores expired. Nothing was dialled
    /// and nothing was sent, so this says nothing about the node's health.
    /// `gate` is the counter for the semaphore that expired.
    fn err_pool_timeout(&self, gate: &AtomicU64) -> ClientError {
        gate.fetch_add(1, Ordering::Relaxed);
        ClientError::PoolTimeout { address: self.address.clone() }
    }

    /// The breaker exists to stop hammering a node that will not take a
    /// connection, so only a handshake that died on the wire arms it. Anything
    /// the node *answered* with (`DictUnavailable`, `IdentityRequired`, a
    /// server error) proves it is up and reachable, and a local
    /// `IdentityError` is our own key material: none of those say the dial
    /// would fail, and fabricating `ConnectionFailed` for them would hide the
    /// real cause behind "circuit breaker open". A variant added later is not a
    /// dial failure until someone says so here.
    fn is_dial_failure(e: &ClientError) -> bool {
        matches!(
            e,
            ClientError::ConnectionFailed(_)
                | ClientError::ConnectionTimeout
                | ClientError::ConnectionLostAfterSend(_)
                | ClientError::WireError(_)
                | ClientError::ReadError(_)
        )
    }

    fn is_circuit_open(&self) -> bool {
        let guard = self.last_connect_failure.lock().unwrap();
        matches!(*guard, Some(failed_at) if failed_at.elapsed() < CIRCUIT_BREAKER_COOLDOWN)
    }

    /// Times the whole wait, entry to a connection in hand or to the error that
    /// ended it, for the price of one `Instant::now()`.
    async fn get(self: &Arc<Self>) -> Result<PooledConnection, ClientError> {
        let entered = Instant::now();
        let result = self.checkout().await;
        self.counters.record_wait(entered.elapsed());
        result
    }

    async fn checkout(self: &Arc<Self>) -> Result<PooledConnection, ClientError> {
        if self.is_circuit_open() {
            self.counters.circuit_breaker_rejections.fetch_add(1, Ordering::Relaxed);
            return Err(ClientError::ConnectionFailed(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("circuit breaker open for {}", self.address),
            )));
        }

        // Acquire a permit: enforces max_connections_per_node as a hard cap.
        let permit = tokio::time::timeout(
            self.options.connection_timeout,
            Arc::clone(&self.semaphore).acquire_owned(),
        )
        .await
        .map_err(|_| self.err_pool_timeout(&self.counters.pool_timeouts_permit))?
        .expect("semaphore closed unexpectedly");

        // Evict stale connections and pop the first live one.
        let reuse = self.pop_live().await;

        if let Some(client) = reuse {
            self.counters.pooled_reuse.fetch_add(1, Ordering::Relaxed);
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
        .map_err(|_| self.err_pool_timeout(&self.counters.pool_timeouts_connect))?
        .expect("connect semaphore closed unexpectedly");

        // Re-check circuit breaker: may have tripped while we waited.
        if self.is_circuit_open() {
            self.counters.circuit_breaker_rejections.fetch_add(1, Ordering::Relaxed);
            return Err(ClientError::ConnectionFailed(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("circuit breaker open for {}", self.address),
            )));
        }

        if let Some(client) = self.pop_live().await {
            self.counters.pooled_reuse.fetch_add(1, Ordering::Relaxed);
            return Ok(PooledConnection {
                client: Some(client),
                broken: false,
                return_to: Arc::clone(self),
                _permit: permit,
            });
        }

        self.counters.attempted.fetch_add(1, Ordering::Relaxed);
        match Self::create_client(&self.address, &self.options, &self.dict_cache).await {
            Ok(client) => {
                self.counters.succeeded.fetch_add(1, Ordering::Relaxed);
                *self.last_connect_failure.lock().unwrap() = None;
                Ok(PooledConnection {
                    client: Some(client),
                    broken: false,
                    return_to: Arc::clone(self),
                    _permit: permit,
                })
            }
            Err(e) => {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                if matches!(e, ClientError::ConnectionTimeout) {
                    self.counters.timed_out.fetch_add(1, Ordering::Relaxed);
                }
                if Self::is_dial_failure(&e) {
                    *self.last_connect_failure.lock().unwrap() = Some(Instant::now());
                }
                Err(e)
            }
        }
    }

    /// The next idle connection the peer has not closed behind our back. The
    /// peek costs one non-blocking syscall per pooled checkout and none on a
    /// freshly dialled socket.
    async fn pop_live(&self) -> Option<CeleriantClient> {
        while let Some(client) = self.pop_fresh() {
            if client.is_peer_live().await {
                return Some(client);
            }
            self.counters.preflight_retired.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    /// Drop every entry idle for at least `idle_timeout` and take the next
    fn pop_fresh(&self) -> Option<CeleriantClient> {
        pop_fresh_from(&mut self.connections.lock().unwrap(), self.options.idle_timeout)
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
            }).await.map_err(|e| match e {
                // The handshake stalled, not the caller's request: nothing of
                // theirs was serialised, so this is a pre-send connect failure.
                ClientError::RequestTimeout => ClientError::ConnectionTimeout,
                other => other,
            })?;

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
/// `mark_broken()` was called or the client's stream is dirty, in which case it
/// is discarded.
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

    /// Mark this connection as broken: it is dropped instead of returned to the pool.
    pub fn mark_broken(&mut self) {
        self.broken = true;
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if !self.broken && !client.is_stream_dirty() {
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
/// Wrap in `Arc` if shared across tasks; the pool itself is not `Clone`.
pub struct CeleriantPool {
    options: Arc<PoolOptions>,
    nodes: RwLock<HashMap<String, Arc<NodePool>>>,
    leader_address: RwLock<Option<String>>,
    read_counter: AtomicU64,
    /// Pool-level dict cache shared between all node pools in this pool.
    dict_cache: Arc<Mutex<PoolDictCache>>,
    routing: RoutingCounters,
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
            routing: RoutingCounters::default(),
        }
    }

    pub fn options(&self) -> &PoolOptions {
        &self.options
    }

    /// Read this pool's counters. Allocates one row per node dialled so far,
    /// so call it out of band: at the end of a run, or on a metrics tick.
    pub fn stats(&self) -> PoolStats {
        let mut nodes: Vec<NodeStats> = self
            .nodes
            .read()
            .unwrap()
            .values()
            .map(|node| NodeStats { address: node.address.clone(), stats: node.counters.snapshot() })
            .collect();
        nodes.sort_unstable_by(|a, b| a.address.cmp(&b.address));

        let mut connections = ConnectionStats::default();
        for node in &nodes {
            connections.add(&node.stats);
        }

        PoolStats {
            connections,
            leader_redirects_followed: self.routing.redirects_followed.load(Ordering::Relaxed),
            leader_hints_skipped: self.routing.hints_skipped.load(Ordering::Relaxed),
            leader_cache_clears: self.routing.cache_clears.load(Ordering::Relaxed),
            leader_pinned_to_seed: self.routing.pinned_to_seed.load(Ordering::Relaxed),
            walks_exhausted: self.routing.walks_exhausted.load(Ordering::Relaxed),
            post_send_losses: self.routing.post_send_losses.load(Ordering::Relaxed),
            nodes,
        }
    }

    /// The leader walk ran out of candidates. Name the node it gave up on and
    /// what that node produced: "no leader found" alone hid every concrete
    /// failure.
    #[cold]
    fn err_walk_exhausted(&self, address: &str, last: &ClientError) -> ClientError {
        self.routing.walks_exhausted.fetch_add(1, Ordering::Relaxed);
        ClientError::ConnectionFailed(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            format!("no leader found; last attempt to {address} failed: {last}"),
        ))
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
    /// `client_id` scopes client-seq idempotency, so use a stable id per logical writer, never a
    /// fresh random value per call. Idempotency enforcement is opt-in: use `write_events_with`
    /// with `enforce_client_idempotency: true` to enable it.
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
        crate::list_operations::check_shard_range(&options)?;
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
        crate::list_operations::check_shard_range(&options)?;
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
        crate::list_operations::check_shard_range(&options)?;
        let conn = self.get_connection().await?;
        Ok(PooledListAggregatesIterator::new(conn, org_id, aggregate_type_id, options))
    }

    // --- Watch ---

    /// Create a dedicated non-pooled WatchConnection.
    ///
    /// Dials the current leader by default; with `route_reads_to_followers`
    /// it dials a follower to keep subscription load off the leader, falling
    /// through to the remaining candidates (leader last) on connect failure.
    /// The pool's TLS, identity, and request/response size configuration are
    /// applied to the watch connection, overriding any values set on `options`.
    /// The pool's dict cache is threaded in so the watch stream can decompress
    /// ZstdDict responses.
    pub async fn watch(
        &self,
        request: WatchRequest,
        mut options: WatchOptions,
    ) -> Result<WatchConnection, ClientError> {
        options.tls_config = self.options.tls_config.clone();
        options.identity_config = self.options.identity_config.clone();
        options.max_request_size = self.options.max_request_size;
        options.max_response_size = self.options.max_response_size;
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
                // Same failover classes as read_route!: a handshake dying with
                // a wire/read error is a broken candidate, not a caller error.
                Err(ClientError::ConnectionFailed(_)
                | ClientError::ConnectionTimeout
                | ClientError::WireError(_)
                | ClientError::ReadError(_))
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
        if addr.is_empty() {
            return Err(err_no_addresses());
        }
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
                        Err(ClientError::ConnectionLostAfterSend(e)) => { conn.mark_broken(); Err(ClientError::ConnectionLostAfterSend(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::PoolTimeout { .. })
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
                        Err(ClientError::ConnectionLostAfterSend(e)) => { conn.mark_broken(); Err(ClientError::ConnectionLostAfterSend(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::PoolTimeout { .. })
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
                        Err(ClientError::ConnectionLostAfterSend(e)) => { conn.mark_broken(); Err(ClientError::ConnectionLostAfterSend(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::PoolTimeout { .. })
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
                        Err(ClientError::ConnectionLostAfterSend(e)) => { conn.mark_broken(); Err(ClientError::ConnectionLostAfterSend(e)) }
                        Err(e @ ClientError::RequestTimeout) => { conn.mark_broken(); return Err(e); }
                        other => other,
                    },
                    Err(e @ ClientError::ConnectionFailed(_))
                    | Err(e @ ClientError::ConnectionTimeout)
                    | Err(e @ ClientError::PoolTimeout { .. })
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

    /// Test hook: the address a watch subscription dials first. The leader by
    /// default, a rotating follower on opt-in. `watch()` itself iterates the
    /// full candidate list, so this exists only to pin the routing contract.
    #[cfg(test)]
    fn watch_address(&self) -> String {
        self.read_addresses()
            .into_iter()
            .next()
            .unwrap_or_else(|| self.primary_address())
    }

    /// The cached leader, else the primary, else the first seed. Empty only
    /// when no addresses are configured at all.
    fn current_or_primary_leader(&self) -> String {
        if let Some(leader) = self.leader_address.read().unwrap().clone() {
            return leader;
        }
        if !self.options.address.is_empty() {
            return self.options.address.clone();
        }
        self.options.seed_addresses.first().cloned().unwrap_or_default()
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
            self.routing.cache_clears.fetch_add(1, Ordering::Relaxed);
            *guard = None;
        } else if let Some(seed) = self.options.seed_addresses.first() {
            // Not a clear: this pins the first seed, which is a follower
            // whenever the primary is the leader. Counted apart so the two
            // are never read as one number.
            self.routing.pinned_to_seed.fetch_add(1, Ordering::Relaxed);
            *guard = Some(seed.clone());
        }
    }

    /// Whether a node we have already dialled is inside its breaker cooldown.
    /// Never inserts: a hint we skip must not grow the node map.
    fn is_known_node_down(&self, address: &str) -> bool {
        self.nodes.read().unwrap().get(address).is_some_and(|n| n.is_circuit_open())
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
        let response = match self.conn.client().read(request).await {
            Ok(r) => r,
            Err(e) => {
                if leaves_connection_dirty(&e) {
                    self.conn.mark_broken();
                    self.exhausted = true;
                }
                return Err(e);
            }
        };
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

/// Whether an error leaves the connection desynchronised and unusable
fn leaves_connection_dirty(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::ConnectionFailed(_)
            | ClientError::ConnectionTimeout
            | ClientError::ConnectionLostAfterSend(_)
            | ClientError::RequestTimeout
            | ClientError::WireError(_)
            | ClientError::ProtocolError
            | ClientError::CorrelationMismatch { .. }
    )
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
            next_shard_to_try: options.start_shard.saturating_add(1),
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
            Ok(_) => {
                self.conn.mark_broken();
                Err(ClientError::ProtocolError)
            }
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    let last_shard = shard_id.saturating_sub(1);
                    self.active_shards.retain(|s| *s <= last_shard);
                    self.shard_cursors.retain(|s, _| *s <= last_shard);
                    return Ok(!self.active_shards.is_empty() || !self.buffer.is_empty());
                }
                if leaves_connection_dirty(&e) {
                    self.conn.mark_broken();
                    self.exhausted = true;
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
            self.next_shard_to_try = self.next_shard_to_try.saturating_add(1);
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
            next_shard_to_try: options.start_shard.saturating_add(1),
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
            Ok(_) => {
                self.conn.mark_broken();
                Err(ClientError::ProtocolError)
            }
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    let last_shard = shard_id.saturating_sub(1);
                    self.active_shards.retain(|s| *s <= last_shard);
                    self.shard_cursors.retain(|s, _| *s <= last_shard);
                    return Ok(!self.active_shards.is_empty() || !self.buffer.is_empty());
                }
                if leaves_connection_dirty(&e) {
                    self.conn.mark_broken();
                    self.exhausted = true;
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
            self.next_shard_to_try = self.next_shard_to_try.saturating_add(1);
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
            next_shard_to_try: options.start_shard.saturating_add(1),
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
            Ok(_) => {
                self.conn.mark_broken();
                Err(ClientError::ProtocolError)
            }
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    let last_shard = shard_id.saturating_sub(1);
                    self.active_shards.retain(|s| *s <= last_shard);
                    self.shard_cursors.retain(|s, _| *s <= last_shard);
                    return Ok(!self.active_shards.is_empty() || !self.buffer.is_empty());
                }
                if leaves_connection_dirty(&e) {
                    self.conn.mark_broken();
                    self.exhausted = true;
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
            self.next_shard_to_try = self.next_shard_to_try.saturating_add(1);
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
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// Accepts and never answers, so a request future parked on the response
    /// read stays parked.
    async fn silent_server() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        addr
    }

    #[tokio::test]
    async fn a_connection_abandoned_mid_request_is_discarded_not_pooled() {
        use celeriant_msg::request::requests::AggregateDetailsRequest;
        use celeriant_wal::aggregate_key::AggregateKey;
        use futures_util::FutureExt;

        let addr = silent_server().await;
        let mut opts = PoolOptions::new(addr.to_string());
        opts.max_connections_per_node = 1;
        let pool = CeleriantPool::new(opts);
        let node = pool.get_or_create_node(&addr.to_string());

        {
            let mut conn = node.get().await.expect("silent server still accepts TCP");
            let req = ClientRequest::AggregateDetails(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: AggregateKey::new(1, 1, 1),
            });
            // One poll: the request reaches the socket, the response read parks.
            // Then the guard is dropped without any error path ever running, so
            // nothing calls mark_broken, which is the whole bug.
            let polled = conn.client().send_request(&req).now_or_never();
            assert!(polled.is_none(), "one poll must park on the response read");
            assert!(!conn.broken, "cancellation raises no error, so nothing marks it broken");
        }

        assert_eq!(
            node.connections.lock().unwrap().len(),
            0,
            "a connection with an undrained response must never reach the free list"
        );
    }

    #[tokio::test]
    async fn a_connection_that_was_never_used_is_returned_to_the_pool() {
        let addr = silent_server().await;
        let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
        let node = pool.get_or_create_node(&addr.to_string());

        drop(node.get().await.expect("silent server still accepts TCP"));

        assert_eq!(
            node.connections.lock().unwrap().len(),
            1,
            "the dirty guard must not retire clean connections"
        );
    }

    /// How the mock server fills the correlation id of the frame it answers with.
    #[derive(Clone, Copy)]
    enum Answer {
        /// What a real server does: the request's own id, echoed back.
        Echo,
        /// Someone else's id, made deterministic: the production desync.
        WrongId,
        /// A server *error* frame carrying someone else's id: the stale
        /// rejection that walked through as a real answer in production.
        WrongIdError,
        /// A `Watch` reply, which carries no correlation id and never will.
        Watch,
        /// A correctly-typed `Read` reply carrying someone else's id, so the
        /// correlation check fires without the response-type check masking it.
        WrongIdRead,
    }

    fn details_response(correlation_id: Option<u128>) -> celeriant_msg::process_client_responses::ClientResponse {
        use celeriant_msg::process_client_responses::ClientResponse;
        use celeriant_msg::response::responses::AggregateDetailsResponse;
        ClientResponse::AggregateDetails(AggregateDetailsResponse {
            correlation_id,
            min_aggregate_version: 0,
            max_aggregate_version: 42,
            max_event_seq: 100,
            is_deleted: false,
            allow_recreate: false,
            allow_sequence_continuation: true,
            last_server_timestamp: 1234,
            last_client_id: 5678,
            last_user_id: None,
        })
    }

    /// Answers every request frame with a scripted response, so a request can
    /// actually run to completion. `silent_server` can only ever show the
    /// cancelled half of the flag's life. The receiver reports the correlation
    /// id seen on each request frame, which is the only black-box view of what
    /// the transport actually put on the wire.
    async fn correlating_server(
        answer: Answer,
    ) -> (std::net::SocketAddr, std::sync::mpsc::Receiver<Option<u128>>) {
        use celeriant_msg::process_client_responses::ClientResponse;
        use celeriant_msg::response::responses::{ErrorResponse, WatchResponse};
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        use celeriant_wire::codec::compression::DictCodec;
        use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
        use tokio_util::compat::TokioAsyncReadCompatExt;

        const MAX: u64 = 64 * 1024 * 1024;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let (seen_tx, seen_rx) = std::sync::mpsc::channel();
        // `DictCodec` holds a `RefCell`, so it is `!Sync` and cannot cross a
        // `tokio::spawn`. Own thread, own current-thread runtime.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                while let Ok((socket, _)) = listener.accept().await {
                    let seen_tx = seen_tx.clone();
                    tokio::task::spawn_local(async move {
                    let mut stream = socket.compat();
                    let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
                    loop {
                        let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else {
                            return;
                        };
                        let Ok(request) =
                            ClientRequest::read_from_header(header, &mut stream, &codec).await
                        else {
                            return;
                        };
                        let asked = request.correlation_id();
                        // Callers may have dropped the receiver; the answer still matters.
                        let _ = seen_tx.send(asked);
                        // Never equal to `asked`, whether or not the client filled one in.
                        let wrong = Some(asked.unwrap_or(0).wrapping_add(0xDEAD));
                        let resp = match answer {
                            Answer::Echo => details_response(asked),
                            Answer::WrongId => details_response(wrong),
                            Answer::WrongIdError => ClientResponse::GenericError(ErrorResponse {
                                correlation_id: wrong,
                                error_code: celeriant_msg::error_codes::SERVER_BUSY,
                                error_message: "server busy".to_string(),
                            }),
                            Answer::Watch => ClientResponse::Watch(WatchResponse { events: Vec::new() }),
                            Answer::WrongIdRead => ClientResponse::Read(
                                celeriant_msg::response::responses::ReadResponse {
                                    correlation_id: wrong,
                                    event_batches: Vec::new(),
                                    next_aggregate_version: None,
                                },
                            ),
                        };
                        if ClientResponse::write_response(
                            &mut stream, &resp, false, &codec, MAX, PROTOCOL_VERSION_V2,
                        ).await.is_err() {
                            return;
                        }
                    }
                    });
                }
            });
        });
        (addr, seen_rx)
    }

    /// A server that answers correctly, which now means echoing the correlation
    /// id back the way the real server does.
    async fn responding_server() -> std::net::SocketAddr {
        correlating_server(Answer::Echo).await.0
    }

    fn details_request_with(correlation_id: Option<u128>) -> ClientRequest {
        use celeriant_msg::request::requests::AggregateDetailsRequest;
        use celeriant_wal::aggregate_key::AggregateKey;
        ClientRequest::AggregateDetails(AggregateDetailsRequest {
            correlation_id,
            aggregate_key: AggregateKey::new(1, 1, 1),
        })
    }

    fn details_request() -> ClientRequest {
        details_request_with(None)
    }

    /// Pins the *clear* site. Without it every request retires its connection and
    /// the pool silently degrades to connect-per-request, which no behavioural
    /// test notices, because the answers stay correct.
    #[tokio::test]
    async fn a_completed_request_leaves_the_connection_poolable() {
        let addr = responding_server().await;
        let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
        let node = pool.get_or_create_node(&addr.to_string());

        {
            let mut conn = node.get().await.unwrap();
            conn.client().send_request(&details_request()).await.expect("mock server answers");
            assert!(!conn.client().is_stream_dirty(), "a fully read response leaves the stream clean");
        }

        assert_eq!(
            node.connections.lock().unwrap().len(),
            1,
            "a completed request must leave the connection in the free list"
        );
    }

    /// The free list must hand back the same connection, not a fresh one.
    #[tokio::test]
    async fn sequential_requests_reuse_one_connection() {
        let addr = responding_server().await;
        let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
        let node = pool.get_or_create_node(&addr.to_string());

        for _ in 0..5 {
            let mut conn = node.get().await.unwrap();
            conn.client().send_request(&details_request()).await.expect("mock server answers");
        }

        assert_eq!(
            node.connections.lock().unwrap().len(),
            1,
            "five sequential requests must share one pooled connection"
        );
    }

    #[tokio::test]
    async fn a_client_refuses_to_send_on_a_stream_it_knows_is_dirty() {
        use celeriant_msg::request::requests::AggregateDetailsRequest;
        use celeriant_wal::aggregate_key::AggregateKey;
        use futures_util::FutureExt;

        let req = ClientRequest::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 1, 1),
        });
        let addr = silent_server().await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        assert!(client.send_request(&req).now_or_never().is_none());
        assert!(client.is_stream_dirty());

        // `Drop` on the pooled guard is not enough on its own: the pooled
        // iterators and `pool.get_connection()` hold one lease across many
        // requests, so the refusal has to happen here too.
        let second = client.send_request(&req).now_or_never();
        assert!(
            matches!(second, Some(Err(ClientError::ProtocolError))),
            "expected a refusal without touching the socket, got {second:?}"
        );
        // The refusal must not clear the flag. A guard that resets on the way out
        // looks like recovery and hands a still-desynced connection back.
        assert!(client.is_stream_dirty(), "the refusal cleared the flag; the connection would be pooled");
    }

    /// A failure that writes zero bytes leaves the stream clean, so the client
    /// must stay usable. Arming the flag before the size check turned a local
    /// rejection into a permanent refusal on a pristine connection.
    #[tokio::test]
    async fn a_request_rejected_before_it_is_written_leaves_the_client_usable() {
        use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
        use celeriant_wal::aggregate_key::AggregateKey;
        use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

        let addr = responding_server().await;
        let mut client = CeleriantClient::connect(&addr.to_string())
            .await
            .unwrap()
            .with_max_request_size(1024);
        client.send_request(&details_request()).await.expect("baseline request");

        let mut writes = HashMap::new();
        writes.insert(
            AggregateKey::new(1, 1, 1),
            SingleAggregateWrite {
                events: vec![DatablockAggregateEvent {
                    client_seq: 0,
                    event_seq: 0,
                    event_id: None,
                    event_timestamp: 0,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(vec![7u8; 8 * 1024]),
                    iv: None,
                }],
                allow_create: true,
                expected_version: Some(0),
                enforce_client_idempotency: false,
            },
        );
        let too_big = ClientRequest::Write(WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes,
        });

        let err = client.send_request(&too_big).await.unwrap_err();
        assert!(matches!(err, ClientError::WireError(_)), "expected a local size rejection, got {err:?}");
        assert!(!client.is_stream_dirty(), "zero bytes were written, so the stream is still clean");
        assert!(
            client.send_request(&details_request()).await.is_ok(),
            "a local rejection must not brick the connection"
        );
    }

    /// A pooled iterator whose connection is unusable must terminate. Returning
    /// the same error for ever gives a log-and-continue caller an I/O-free spin.
    #[tokio::test]
    async fn a_pooled_iterator_terminates_once_its_connection_is_unusable() {
        use celeriant_wal::aggregate_key::AggregateKey;
        use futures_util::FutureExt;

        let addr = silent_server().await;
        let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
        let node = pool.get_or_create_node(&addr.to_string());
        let mut conn = node.get().await.unwrap();

        // Cancel one request mid-flight on the lease the iterator will own.
        assert!(conn.client().send_request(&details_request()).now_or_never().is_none());

        let mut iter = PooledReadAllIterator::new(conn, AggregateKey::new(1, 1, 1), None);
        assert!(
            matches!(iter.next().await, Some(Err(ClientError::ProtocolError))),
            "the first poll must surface the refusal"
        );
        assert!(iter.next().await.is_none(), "the iterator must then terminate");
    }


    /// The response-type check's only new behaviour is retiring the connection.
    /// The `ProtocolError` value itself is already produced by the `_ =>` arms in
    /// `client_operations`. So the retire half is the half worth pinning.
    #[tokio::test]
    async fn a_wrong_variant_response_retires_the_stream() {
        use celeriant_msg::request::read_filters::ReadFilters;
        use celeriant_msg::request::requests::ReadRequest;
        use celeriant_wal::aggregate_key::AggregateKey;
        use futures_util::FutureExt;

        let (addr, _seen) = correlating_server(Answer::Watch).await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        let first = client
            .read(ReadRequest {
                correlation_id: None,
                aggregate_key: AggregateKey::new(1, 1, 1),
                filters: ReadFilters::new(0),
            })
            .await;
        assert!(first.is_err(), "a Watch frame cannot answer a Read: {first:?}");

        // A refusal completes in one poll; a real send would park on the read.
        let second = client.send_request(&details_request()).now_or_never();
        assert!(
            matches!(second, Some(Err(ClientError::ProtocolError))),
            "another request went out on a stream that answered with the wrong variant: {second:?}"
        );
    }

    /// `CorrelationMismatch` is in `leaves_connection_dirty` so one mismatch ends
    /// the iteration. Without it the iterator needs a second round trip through
    /// the dirty-stream refusal to work out that it is finished.
    #[tokio::test]
    async fn a_pooled_iterator_terminates_on_the_first_correlation_mismatch() {
        use celeriant_wal::aggregate_key::AggregateKey;

        let (addr, _seen) = correlating_server(Answer::WrongIdRead).await;
        let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
        let mut iter = pool.read_all(AggregateKey::new(1, 1, 1), None).await.unwrap();

        assert!(
            matches!(iter.next().await, Some(Err(ClientError::CorrelationMismatch { .. }))),
            "the first page must surface the mismatch"
        );
        assert!(
            iter.next().await.is_none(),
            "the iterator must terminate on the mismatch, not spin through a second error"
        );
    }

    /// `send_request` short-circuits its fill when an id is already present, so
    /// only the typed helpers actually exercise `set_correlation_id_if_absent`
    /// against a supplied value, which is the path `celeriant_cli
    /// --correlation-id` depends on.
    #[tokio::test]
    async fn a_typed_helper_never_rewrites_a_caller_supplied_correlation_id() {
        use celeriant_msg::request::requests::AggregateDetailsRequest;
        use celeriant_wal::aggregate_key::AggregateKey;
        const SUPPLIED: u128 = 0xC0FF_EE00_1234_5678;

        let (addr, seen) = correlating_server(Answer::Echo).await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        client
            .aggregate_details(AggregateDetailsRequest {
                correlation_id: Some(SUPPLIED),
                aggregate_key: AggregateKey::new(1, 1, 1),
            })
            .await
            .expect("echoing server answers");

        assert_eq!(
            seen.recv_timeout(std::time::Duration::from_secs(5)).expect("server saw the request"),
            Some(SUPPLIED),
            "the caller's own id was clobbered on the wire"
        );
    }

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

    // Leader-routing contract tests.
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

    #[test]
    fn a_timed_out_or_broken_connection_is_never_returned_to_the_free_list() {
        // The phase-5 cardinality_pressure failure: `read_all` timed out on a
        // cold read, the desynchronised connection went back into the FIFO
        // free list, and the next task read the late response as its own reply.
        // A 99.2% error rate over 12,000 reads followed.
        for e in [
            ClientError::RequestTimeout,
            ClientError::ConnectionTimeout,
            ClientError::ConnectionLostAfterSend(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            ClientError::ProtocolError,
            ClientError::ConnectionFailed(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        ] {
            assert!(leaves_connection_dirty(&e), "{e} must retire the connection");
        }

        // A server error response was read off the wire in full. Retiring the
        // connection for it would churn the pool on ordinary rejections.
        // The body was read in full; only deserialising it failed, so the
        // stream is still in sync and the connection can be reused.
        assert!(!leaves_connection_dirty(&ClientError::ReadError(
            celeriant_msg::read_wire_data_error::ReadWireDataError::UnknownMessageType(9)
        )));
        assert!(!leaves_connection_dirty(&ClientError::ServerBusy));
        assert!(!leaves_connection_dirty(&ClientError::IdentityRequired));
        assert!(!leaves_connection_dirty(&ClientError::NotLeader {
            leader_address: None,
            error_message: String::new(),
        }));
    }

    #[test]
    fn checkout_never_returns_a_connection_past_its_idle_timeout() {
        const IDLE: Duration = Duration::from_secs(25);
        let aged = |secs: u64| Instant::now() - Duration::from_secs(secs);

        // Named by what each row proves, so a failure says which rule broke.
        // The second `pop_front` in `NodePool::get`, the one taken after queueing
        // on the connect semaphore, used to skip the age check
        // entirely, which is row "all stale".
        let cases: [(&str, Vec<(u32, Instant)>, Option<u32>); 5] = [
            ("empty", vec![], None),
            ("all fresh", vec![(1, aged(1)), (2, aged(2))], Some(1)),
            ("all stale", vec![(1, aged(30)), (2, aged(26))], None),
            ("stale prefix", vec![(1, aged(30)), (2, aged(1))], Some(2)),
            // The list is return-ordered, so `idle_timeout` exactly reached is
            // stale: the server's own close is only 5s behind it.
            ("boundary", vec![(1, aged(25)), (2, aged(1))], Some(2)),
        ];

        for (name, entries, expected) in cases {
            let mut q: VecDeque<(u32, Instant)> = entries.into_iter().collect();
            assert_eq!(pop_fresh_from(&mut q, IDLE), expected, "{name}");
            assert!(
                q.iter().all(|(_, ts)| ts.elapsed() < IDLE),
                "{name}: a stale entry survived in the free list"
            );
        }
    }

    // Blind-oracle correlation-id tests (contract authored unseen).
    //
    // Nothing but stream ordering binds a response to its request. When that
    // ordering slips, the client hands back a plausible answer that belongs to
    // someone else. These pin the second line of defence: every response must
    // be checked against the id of the request that was actually sent.

    const RECV: Duration = Duration::from_secs(5);

    /// Both halves of the fill rule. `None` must be filled, because otherwise
    /// nothing binds the response. A supplied id is an application tag that
    /// `celeriant_cli --correlation-id` prints back, so it must survive
    /// the transport byte for byte.
    #[tokio::test]
    async fn the_transport_fills_a_missing_correlation_id_and_never_rewrites_a_supplied_one() {
        const SUPPLIED: u128 = 0xC0FF_EE00_1234_5678;

        let (addr, seen) = correlating_server(Answer::Echo).await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        client.send_request(&details_request_with(None)).await.expect("echoing server answers");
        let filled = seen.recv_timeout(RECV).expect("server saw the first request");
        assert!(
            filled.is_some(),
            "the request went out with no correlation id, so nothing binds its response to it"
        );

        client.send_request(&details_request_with(Some(SUPPLIED))).await.expect("echoing server answers");
        assert_eq!(
            seen.recv_timeout(RECV).expect("server saw the second request"),
            Some(SUPPLIED),
            "the caller's own id was clobbered on the wire"
        );
    }

    /// A constant filler would satisfy every echo check and bind nothing: the
    /// stale frame from the *previous* request carries the same id.
    #[tokio::test]
    async fn each_filled_correlation_id_is_distinct() {
        let (addr, seen) = correlating_server(Answer::Echo).await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        let mut ids = std::collections::HashSet::new();
        for _ in 0..5 {
            client.send_request(&details_request()).await.expect("echoing server answers");
            ids.insert(seen.recv_timeout(RECV).expect("server saw the request"));
        }

        assert_eq!(ids.len(), 5, "filled ids repeat, so a stale frame still matches: {ids:?}");
    }

    /// The production case. A frame answering somebody else's request must
    /// never be returned as this request's answer.
    #[tokio::test]
    async fn a_response_carrying_another_requests_correlation_id_is_an_error() {
        let (addr, _seen) = correlating_server(Answer::WrongId).await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        let result = client.send_request(&details_request()).await;

        assert!(
            result.is_err(),
            "someone else's response was handed back as this request's answer: {result:?}"
        );
    }

    /// The check has to run *ahead of* the point where an error frame becomes a
    /// Rust `Err`. A stale rejection reported as this caller's rejection is the
    /// exact production symptom, and it is indistinguishable from a real one.
    #[tokio::test]
    async fn a_stale_error_frame_is_not_reported_as_this_requests_rejection() {
        let (addr, _seen) = correlating_server(Answer::WrongIdError).await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        let err = client.send_request(&details_request()).await.expect_err("never Ok");

        assert!(
            !matches!(
                err,
                ClientError::ServerBusy
                    | ClientError::IdentityRequired
                    | ClientError::NotLeader { .. }
                    | ClientError::Server(_)
            ),
            "an error frame belonging to a different request was decoded as this one's: {err:?}"
        );
    }

    /// Detecting the desync and then pooling the connection anyway is strictly
    /// worse than not detecting it: the next borrower reads the leftover frame.
    #[tokio::test]
    async fn a_correlation_mismatch_retires_the_connection() {
        use futures_util::FutureExt;

        let (addr, _seen) = correlating_server(Answer::WrongId).await;
        let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
        let node = pool.get_or_create_node(&addr.to_string());

        {
            let mut conn = node.get().await.unwrap();
            let result = conn.client().send_request(&details_request()).await;
            assert!(result.is_err(), "the mismatch must surface as an error: {result:?}");

            // A local refusal completes in one poll; a real send parks on the
            // response read. So `Some(Err(_))` is the only outcome that proves
            // nothing else went out on a stream we no longer trust.
            let second = conn.client().send_request(&details_request()).now_or_never();
            assert!(
                matches!(second, Some(Err(_))),
                "another request went out on a desynchronised stream: {second:?}"
            );
        }

        assert_eq!(
            node.connections.lock().unwrap().len(),
            0,
            "a connection that answered with the wrong correlation id must never be pooled"
        );
    }

    /// `WatchResponse` carries no correlation id and never will, so the check
    /// must let it through, while the request itself still carries one.
    #[tokio::test]
    async fn a_watch_request_works_though_its_response_carries_no_correlation_id() {
        use celeriant_msg::process_client_responses::ClientResponse;
        use celeriant_msg::request::requests::WatchRequest;

        let (addr, seen) = correlating_server(Answer::Watch).await;
        let mut client = CeleriantClient::connect(&addr.to_string()).await.unwrap();

        let request = ClientRequest::Watch(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        });
        let response = client.send_request(&request).await;

        assert!(
            matches!(response, Ok(ClientResponse::Watch(_))),
            "an id-less Watch response must still be delivered: {response:?}"
        );
        assert!(
            seen.recv_timeout(RECV).expect("server saw the watch request").is_some(),
            "a Watch request must still carry a filled correlation id"
        );
    }

    /// A server that acks the watch handshake with an empty `WatchResponse`,
    /// then pushes a frame far larger than the pool's `max_response_size`.
    async fn oversized_watch_server() -> std::net::SocketAddr {
        use celeriant_msg::process_client_responses::ClientResponse;
        use celeriant_msg::response::responses::WatchResponse;
        use celeriant_msg::response::watch_event::WatchResponseEvent;
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        use celeriant_wire::codec::compression::DictCodec;
        use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
        use tokio_util::compat::TokioAsyncReadCompatExt;

        const MAX: u64 = 64 * 1024 * 1024;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                while let Ok((socket, _)) = listener.accept().await {
                    tokio::task::spawn_local(async move {
                        let mut stream = socket.compat();
                        let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
                        let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else {
                            return;
                        };
                        let Ok(_request) =
                            ClientRequest::read_from_header(header, &mut stream, &codec).await
                        else {
                            return;
                        };
                        let ack = ClientResponse::Watch(WatchResponse { events: Vec::new() });
                        if ClientResponse::write_response(
                            &mut stream, &ack, false, &codec, MAX, PROTOCOL_VERSION_V2,
                        ).await.is_err() {
                            return;
                        }
                        let big = ClientResponse::Watch(WatchResponse {
                            events: (0..256u64).map(|i| WatchResponseEvent {
                                org_id: i as u128,
                                aggregate_type_id: 1,
                                aggregate_id: i as u128,
                                operation: 1,
                                from_aggregate_version: Some(i),
                                to_aggregate_version: Some(i + 1),
                                keep_from_aggregate_version: None,
                            }).collect(),
                        });
                        let _ = ClientResponse::write_response(
                            &mut stream, &big, false, &codec, MAX, PROTOCOL_VERSION_V2,
                        ).await;
                    });
                }
            });
        });
        addr
    }

    /// A watch response past the pool's `max_response_size` must be rejected,
    /// not delivered. The pool threads its size caps into the watch connection;
    /// before the fix the watch used a hardcoded 10 MB cap and the pool's knob
    /// was silently ignored.
    #[tokio::test]
    async fn a_watch_response_past_the_pools_max_response_size_is_rejected() {
        use celeriant_msg::request::requests::WatchRequest;

        let addr = oversized_watch_server().await;
        let mut opts = PoolOptions::new(addr.to_string());
        opts.max_response_size = 1024;
        let pool = CeleriantPool::new(opts);

        let request = WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        };

        let mut watch = pool
            .watch(request, WatchOptions::default())
            .await
            .expect("the watch handshake completes on the empty ack");

        match watch.next_timeout(Duration::from_secs(5)).await {
            Ok(Some(_)) => panic!("an oversized watch response must be rejected, not delivered"),
            Ok(None) => panic!("the oversized frame must arrive, not time out"),
            Err(err) => assert!(
                matches!(err, ClientError::ReadError(_)),
                "expected ReadError from the oversized watch response, got {err:?}"
            ),
        }
    }

    /// Constructing a pooled iterator at the top of the shard range must not
    /// overflow the shard cursor.
    #[tokio::test]
    async fn pooled_list_iterators_accept_max_start_shard_without_overflow() {
        let addr = silent_server().await;
        let pool = CeleriantPool::new(PoolOptions::new(addr.to_string()));
        let node = pool.get_or_create_node(&addr.to_string());

        let opts = crate::list_operations::ListOptions {
            start_shard: u64::MAX,
            ..Default::default()
        };

        let conn = node.get().await.unwrap();
        let mut it = PooledListOrgsIterator::new(conn, opts.clone());
        assert!(!it.try_add_next_shard(), "the cursor must saturate, not wrap");
        let conn = node.get().await.unwrap();
        let mut it = PooledListAggregateTypesIterator::new(conn, Some(1), opts.clone());
        assert!(!it.try_add_next_shard(), "the cursor must saturate, not wrap");
        let conn = node.get().await.unwrap();
        let mut it = PooledListAggregatesIterator::new(conn, Some(1), Some(1), opts.clone());
        assert!(!it.try_add_next_shard(), "the cursor must saturate, not wrap");
    }

    /// The pooled list path must refuse an impossible shard range where the
    /// direct path does, before it costs a connection.
    #[tokio::test]
    async fn a_pooled_impossible_shard_range_never_reaches_a_connection() {
        let primary = write_server(WriteAnswer::Ok).await;
        let pool = CeleriantPool::new(PoolOptions::new(primary.to_string()));
        let opts = crate::list_operations::ListOptions {
            start_shard: 4,
            max_shard_hint: Some(3),
            ..Default::default()
        };

        for outcome in [
            pool.list_orgs(opts.clone()).await.map(|_| ()),
            pool.list_aggregate_types(Some(1), opts.clone()).await.map(|_| ()),
            pool.list_aggregates(Some(1), Some(1), opts.clone()).await.map(|_| ()),
        ] {
            match outcome {
                Err(ClientError::InvalidShardRange { start_shard: 4, max_shard_hint: 3 }) => {}
                Err(other) => panic!("expected InvalidShardRange, got {other:?}"),
                Ok(()) => panic!("an impossible range must not yield a silent empty stream"),
            }
        }

        let c = pool.stats().connections;
        assert_eq!(
            (c.attempted, c.pooled_reuse),
            (0, 0),
            "a rejected range must not cost a connection"
        );
    }

    /// Accepts, reads the request so the client's write completes, then closes.
    /// The client's response read hits EOF, surfacing a `ReadError` rather than
    /// a clean `ConnectionFailed`.
    async fn read_error_server() -> std::net::SocketAddr {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        use celeriant_wire::codec::compression::DictCodec;
        use celeriant_wire::network::wire_header::WireHeader;
        use tokio_util::compat::TokioAsyncReadCompatExt;

        const MAX: u64 = 64 * 1024 * 1024;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                while let Ok((socket, _)) = listener.accept().await {
                    tokio::task::spawn_local(async move {
                        let mut stream = socket.compat();
                        let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
                        let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else {
                            return;
                        };
                        let Ok(_request) =
                            ClientRequest::read_from_header(header, &mut stream, &codec).await
                        else {
                            return;
                        };
                        // Drop the socket: the client's read sees EOF.
                    });
                }
            });
        });
        addr
    }

    /// A watch whose handshake dies with a `ReadError` on the pinned leader must
    /// fail over to the next candidate, mirroring `read_route!`, instead of
    /// surfacing the wire error immediately.
    #[tokio::test]
    async fn watch_fails_over_to_next_node_on_read_error() {
        use celeriant_msg::request::requests::WatchRequest;

        let bad = read_error_server().await;
        let (good, _seen) = correlating_server(Answer::Watch).await;

        let pool = CeleriantPool::new(
            PoolOptions::new(bad.to_string()).with_seed_addresses(vec![good.to_string()]),
        );

        let request = WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: None,
        };

        let result = pool.watch(request, WatchOptions::default()).await;
        assert!(result.is_ok(), "watch must fail over to the healthy node");
        assert_eq!(result.unwrap().address(), good.to_string());
    }

    /// With no primary configured, leader routing must target the first seed,
    /// never an empty address.
    #[test]
    fn oracle_seed_only_current_or_primary_leader_falls_back_to_seed() {
        let pool = CeleriantPool::new(
            PoolOptions::default().with_seed_addresses(vec!["b:1".into(), "c:1".into()]),
        );
        assert_eq!(pool.current_or_primary_leader(), "b:1");
        assert_eq!(pool.read_addresses(), vec!["b:1".to_string(), "c:1".to_string()]);
    }

    /// A server that answers every request with a `Read` response whose body is
    /// larger than the client's `max_response_size`, so the client's own
    /// `WireHeader::from_reader` guard trips before any failover logic runs.
    async fn oversized_read_server() -> std::net::SocketAddr {
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        use celeriant_wire::codec::compression::DictCodec;
        use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
        use tokio_util::compat::TokioAsyncReadCompatExt;

        const MAX: u64 = 64 * 1024 * 1024;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                while let Ok((socket, _)) = listener.accept().await {
                    tokio::task::spawn_local(async move {
                        let mut stream = socket.compat();
                        let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
                        loop {
                            let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else {
                                return;
                            };
                            let Ok(request) =
                                ClientRequest::read_from_header(header, &mut stream, &codec).await
                            else {
                                return;
                            };
                            let asked = request.correlation_id();
                            let resp = ClientResponse::Read(ReadResponse {
                                correlation_id: asked,
                                event_batches: vec![AggregateEventBatch {
                                    aggregate_version: 1,
                                    client_id: 1,
                                    user_id: None,
                                    server_timestamp: 0,
                                    events: vec![DatablockAggregateEvent {
                                        client_seq: 0,
                                        event_seq: 0,
                                        event_id: None,
                                        event_timestamp: 0,
                                        event_type_major: 1,
                                        event_type_minor: 0,
                                        event_value: Arc::new(vec![7u8; 8 * 1024]),
                                        iv: None,
                                    }],
                                }],
                                next_aggregate_version: None,
                            });
                            if ClientResponse::write_response(
                                &mut stream, &resp, false, &codec, MAX, PROTOCOL_VERSION_V2,
                            ).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });
        });
        addr
    }

    /// A response exceeding `max_response_size` is a deterministic client-side
    /// rejection, not a connection fault. `read_route!` must surface the
    /// `MessageTooLarge` to the caller instead of failing every candidate and
    /// reporting "all nodes unreachable".
    #[tokio::test]
    async fn an_oversized_read_response_surfaces_message_too_large_not_unreachable() {
        let addr = oversized_read_server().await;
        let mut opts = PoolOptions::new(addr.to_string());
        opts.max_response_size = 1024;
        let pool = CeleriantPool::new(opts);

        let err = pool
            .read(ReadRequest {
                correlation_id: None,
                aggregate_key: AggregateKey::new(1, 1, 1),
                filters: ReadFilters::new(0),
            })
            .await
            .expect_err("an oversized response must error");

        assert!(
            matches!(err, ClientError::WireError(WireError::MessageTooLarge { .. })),
            "expected MessageTooLarge, got {err:?}"
        );
    }

    /// How a mock write server answers each `Write` request it receives.
    #[derive(Clone)]
    enum WriteAnswer {
        /// Redirect the client to `leader_address` via a `NotLeader` error.
        NotLeader(String),
        /// Commit the write and return a `WriteResponse`.
        Ok,
        /// Read the request in full, then close without answering: the write
        /// may or may not have been applied.
        ReadThenClose,
        /// Answer with a correctly framed fixed-size Write response whose body
        /// is corrupt: the answer arrives, the client cannot read it.
        CorruptBody,
        /// Answer a `Read` request. Lets one fake stand in for a read node.
        ReadOk,
        /// Answer the first request on a session, then read the second and
        /// close without answering it.
        OkThenCloseOnSecond,
        /// Refuse a schema registration as a non-leader (error 2027) with a
        /// leader hint, the way a follower answers `RegisterSchema`.
        SchemaNotLeader(String),
        /// Accept a schema registration.
        SchemaOk,
    }

    /// A server that answers `Write` requests with a scripted response, echoing
    /// the request's correlation id so the client's correlation check passes.
    async fn write_server(answer: WriteAnswer) -> std::net::SocketAddr {
        write_server_at("127.0.0.1:0", answer, Arc::new(AtomicUsize::new(0))).await
    }

    /// A `write_server` whose accepted sockets a test can close, the way a
    /// server restart closes the connections a client still holds pooled.
    struct ClosableServer {
        addr: std::net::SocketAddr,
        requests: Arc<AtomicUsize>,
        close: tokio::sync::watch::Sender<bool>,
        closed: std::sync::mpsc::Receiver<()>,
    }

    impl ClosableServer {
        /// Close every idle session and return once the FIN is on its way.
        fn close_sessions(&self) {
            self.close.send(true).unwrap();
            self.closed.recv().unwrap();
        }
    }

    async fn closable_write_server(answer: WriteAnswer) -> ClosableServer {
        let requests = Arc::new(AtomicUsize::new(0));
        let (close, close_rx) = tokio::sync::watch::channel(false);
        let (closed_tx, closed) = std::sync::mpsc::channel();
        let addr = write_server_inner(
            "127.0.0.1:0",
            answer,
            Arc::clone(&requests),
            Some((close_rx, closed_tx)),
        )
        .await;
        ClosableServer { addr, requests, close, closed }
    }

    /// `write_server` bound to a chosen address, counting the requests it
    /// served so a test can prove a node was, or was not, asked.
    async fn write_server_at(
        bind: &str,
        answer: WriteAnswer,
        requests: Arc<AtomicUsize>,
    ) -> std::net::SocketAddr {
        write_server_inner(bind, answer, requests, None).await
    }

    type CloseSignal = (tokio::sync::watch::Receiver<bool>, std::sync::mpsc::Sender<()>);

    async fn write_server_inner(
        bind: &str,
        answer: WriteAnswer,
        requests: Arc<AtomicUsize>,
        close: Option<CloseSignal>,
    ) -> std::net::SocketAddr {
        use celeriant_msg::process_client_responses::ClientResponse;
        use celeriant_msg::response::responses::{ErrorResponse, WriteResponse};
        use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
        use celeriant_wire::codec::compression::DictCodec;
        use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V2, WireHeader};
        use tokio_util::compat::TokioAsyncReadCompatExt;

        const MAX: u64 = 64 * 1024 * 1024;
        let listener = std::net::TcpListener::bind(bind).unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        // `DictCodec` holds a `RefCell`, so it is `!Sync`; own thread, own runtime.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                while let Ok((socket, _)) = listener.accept().await {
                    let answer = answer.clone();
                    let requests = Arc::clone(&requests);
                    let mut close = close.clone();
                    // Only a close signalled from now on is ours to obey.
                    if let Some((closing, _)) = close.as_mut() {
                        closing.mark_unchanged();
                    }
                    tokio::task::spawn_local(async move {
                        let mut stream = socket.compat();
                        let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
                        let mut served = 0usize;
                        'session: loop {
                            let header = match close.as_mut() {
                                Some((closing, _)) => tokio::select! {
                                    h = WireHeader::from_reader(&mut stream, MAX) => h,
                                    _ = closing.changed() => break 'session,
                                },
                                None => WireHeader::from_reader(&mut stream, MAX).await,
                            };
                            let Ok(header) = header else {
                                return;
                            };
                            let Ok(request) =
                                ClientRequest::read_from_header(header, &mut stream, &codec).await
                            else {
                                return;
                            };
                            let asked = request.correlation_id();
                            requests.fetch_add(1, Ordering::SeqCst);
                            served += 1;
                            if matches!(answer, WriteAnswer::ReadThenClose)
                                || (matches!(answer, WriteAnswer::OkThenCloseOnSecond) && served > 1)
                            {
                                return;
                            }
                            if matches!(answer, WriteAnswer::CorruptBody) {
                                use futures_util::AsyncWriteExt as _;
                                // 17-byte header: version, message type (Write),
                                // compressed len, uncompressed len, compression.
                                let mut frame = Vec::with_capacity(18);
                                frame.extend_from_slice(&PROTOCOL_VERSION_V2.to_le_bytes());
                                frame.extend_from_slice(&3u32.to_le_bytes());
                                frame.extend_from_slice(&1u32.to_le_bytes());
                                frame.extend_from_slice(&1u32.to_le_bytes());
                                frame.push(0);
                                frame.push(2); // invalid `Option` discriminant
                                let _ = stream.write_all(&frame).await;
                                continue;
                            }
                            let resp = match &answer {
                                WriteAnswer::NotLeader(leader) => {
                                    ClientResponse::GenericError(ErrorResponse {
                                        correlation_id: asked,
                                        error_code: celeriant_msg::error_codes::WRITE_NOT_LEADER,
                                        error_message: format!("{{\"leader_address\":\"{leader}\"}}"),
                                    })
                                }
                                WriteAnswer::ReadOk => ClientResponse::Read(
                                    celeriant_msg::response::responses::ReadResponse {
                                        correlation_id: asked,
                                        event_batches: Vec::new(),
                                        next_aggregate_version: None,
                                    },
                                ),
                                WriteAnswer::Ok
                                | WriteAnswer::ReadThenClose
                                | WriteAnswer::OkThenCloseOnSecond
                                | WriteAnswer::CorruptBody => ClientResponse::Write(WriteResponse {
                                    correlation_id: asked,
                                    max_aggregate_version: Some(1),
                                }),
                                WriteAnswer::SchemaNotLeader(leader) => {
                                    ClientResponse::GenericError(ErrorResponse {
                                        correlation_id: asked,
                                        error_code:
                                            celeriant_msg::error_codes::REGISTER_SCHEMA_CANNOT_ACCEPT_WRITES,
                                        error_message: format!("{{\"leader_address\":\"{leader}\"}}"),
                                    })
                                }
                                WriteAnswer::SchemaOk => ClientResponse::RegisterSchema(
                                    celeriant_msg::response::responses::RegisterSchemaResponse {
                                        correlation_id: asked,
                                    },
                                ),
                            };
                            if ClientResponse::write_response(
                                &mut stream, &resp, false, &codec, MAX, PROTOCOL_VERSION_V2,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                        // Asked to close: drop the socket, then report, so the
                        // caller knows the FIN is out before it acts.
                        drop(stream);
                        if let Some((_, closed)) = close {
                            let _ = closed.send(());
                        }
                    });
                }
            });
        });
        addr
    }

    fn empty_read_request() -> ReadRequest {
        ReadRequest {
            correlation_id: None,
            aggregate_key: AggregateKey::new(1, 1, 1),
            filters: ReadFilters::new(0),
        }
    }

    fn empty_write_request() -> celeriant_msg::request::requests::WriteRequest {
        celeriant_msg::request::requests::WriteRequest {
            correlation_id: None,
            client_id: 1,
            user_id: None,
            writes: HashMap::new(),
        }
    }

    /// A stale `NotLeader{Some(addr)}` hint that points at a dead node must not
    /// abort the write: the hinted retry fails, then the seed loop is walked and
    /// the write lands on a reachable seed.
    #[tokio::test]
    async fn a_stale_leader_hint_falls_through_to_seed_failover() {
        // A dead address: bind, capture, drop, so the hinted retry gets a reset.
        let stale = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };

        let primary = write_server(WriteAnswer::NotLeader(stale)).await;
        let seed = write_server(WriteAnswer::Ok).await;

        let pool = CeleriantPool::new(
            PoolOptions::new(primary.to_string()).with_seed_addresses(vec![seed.to_string()]),
        );

        let result = pool.write(empty_write_request()).await;

        assert!(
            result.is_ok(),
            "a stale leader hint must degrade to seed failover, got {result:?}"
        );
    }

    /// A follower answers `RegisterSchema` with error 2027 plus a leader hint,
    /// the same shape as the NotLeader errors. The pool must follow that hint
    /// like any other leader redirect instead of surfacing a schema error.
    #[tokio::test]
    async fn schema_registration_on_a_follower_redirects_to_the_leader() {
        use celeriant_wal::schema_key::SchemaKey;

        let leader = write_server(WriteAnswer::SchemaOk).await;
        let follower = write_server(WriteAnswer::SchemaNotLeader(leader.to_string())).await;

        let pool = CeleriantPool::new(PoolOptions::new(follower.to_string()));

        let result = pool
            .register_schema(RegisterSchemaRequest {
                correlation_id: None,
                client_id: 1,
                user_id: None,
                schema_key: SchemaKey::new(1, 1, 1, 0),
                schema_type: 0,
                schema: "{}".to_string(),
            })
            .await;

        assert!(
            result.is_ok(),
            "schema registration must follow the 2027 leader hint, got {result:?}"
        );
        assert_eq!(
            pool.leader_address.read().unwrap().clone(),
            Some(leader.to_string()),
            "the redirect must update the cached leader"
        );
    }

    /// The write-path analog of the oversized-read case: a request past
    /// `max_request_size` fails identically on every node, so `leader_route!`
    /// must surface the `MessageTooLarge` instead of "no leader found".
    #[tokio::test]
    async fn an_oversized_write_request_surfaces_message_too_large_not_no_leader() {
        use celeriant_msg::request::requests::SingleAggregateWrite;

        let primary = write_server(WriteAnswer::Ok).await;
        let mut opts = PoolOptions::new(primary.to_string());
        opts.max_request_size = 1024;
        let pool = CeleriantPool::new(opts);

        let mut request = empty_write_request();
        request.writes.insert(
            AggregateKey::new(1, 1, 1),
            SingleAggregateWrite {
                events: vec![DatablockAggregateEvent {
                    client_seq: 0,
                    event_seq: 0,
                    event_id: None,
                    event_timestamp: 0,
                    event_type_major: 1,
                    event_type_minor: 0,
                    event_value: Arc::new(vec![7u8; 8 * 1024]),
                    iv: None,
                }],
                allow_create: true,
                expected_version: None,
                enforce_client_idempotency: false,
            },
        );

        let err = pool.write(request).await.expect_err("an oversized request must error");
        assert!(
            matches!(err, ClientError::WireError(WireError::MessageTooLarge { .. })),
            "expected MessageTooLarge, got {err:?}"
        );
    }

    /// A redirect chain (primary hints at node2, node2 hints at node3, node3 is
    /// the leader) is followed hop by hop instead of abandoning the second hint.
    #[tokio::test]
    async fn a_leader_hint_chain_is_followed_to_the_leader() {
        let leader = write_server(WriteAnswer::Ok).await;
        let middle = write_server(WriteAnswer::NotLeader(leader.to_string())).await;
        let primary = write_server(WriteAnswer::NotLeader(middle.to_string())).await;

        let pool = CeleriantPool::new(PoolOptions::new(primary.to_string()));

        let result = pool.write(empty_write_request()).await;

        assert!(result.is_ok(), "a two-hop leader hint chain must land, got {result:?}");
        assert_eq!(
            pool.leader_address.read().unwrap().clone(),
            Some(leader.to_string()),
            "the cache must hold the node that answered"
        );
    }

    fn counter() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    fn count(c: &Arc<AtomicUsize>) -> usize {
        c.load(Ordering::SeqCst)
    }

    /// A port nothing listens on: a dial is refused, so the node fails
    /// pre-send. The address can be handed to `write_server_at` later.
    fn reserved_address() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    }

    /// The leader took the request and died before answering: it may
    /// have applied it. Re-sending it to the seed could duplicate the write, so
    /// the caller is told the outcome is unknown and nothing else is dialled.
    #[tokio::test]
    async fn a_request_lost_after_it_was_sent_is_never_re_sent_elsewhere() {
        let (leader_hits, seed_hits) = (counter(), counter());
        let leader =
            write_server_at("127.0.0.1:0", WriteAnswer::ReadThenClose, leader_hits.clone()).await;
        let seed = write_server_at(
            "127.0.0.1:0",
            WriteAnswer::NotLeader(leader.to_string()),
            seed_hits.clone(),
        )
        .await;

        let pool = CeleriantPool::new(
            PoolOptions::new(leader.to_string()).with_seed_addresses(vec![seed.to_string()]),
        );

        let err = pool
            .write(empty_write_request())
            .await
            .expect_err("a lost response is not a success");

        assert!(
            matches!(err, ClientError::ConnectionLostAfterSend(_)),
            "expected ConnectionLostAfterSend, got {err:?}"
        );
        assert_eq!(count(&leader_hits), 1, "the leader must be asked exactly once");
        assert_eq!(count(&seed_hits), 0, "a request that may have landed is never re-sent");
    }

    /// A server that closed a pooled connection while it sat idle must not
    /// cost the caller a write: writing to that socket still succeeds locally
    /// and only the read sees EOF, which is unknown-outcome and never retried.
    /// The checkout preflights the socket and dials a fresh one instead.
    #[tokio::test]
    async fn a_connection_closed_while_idle_is_retired_at_checkout() {
        let leader = closable_write_server(WriteAnswer::Ok).await;
        let pool = CeleriantPool::new(PoolOptions::new(leader.addr.to_string()));

        pool.write(empty_write_request()).await.expect("the first write must land");
        leader.close_sessions();

        let second = pool.write(empty_write_request()).await;

        assert!(second.is_ok(), "a closed idle connection must not fail a write, got {second:?}");
        assert_eq!(
            leader.requests.load(Ordering::SeqCst),
            2,
            "the leader must have served both writes, each exactly once"
        );
        assert_eq!(
            pool.stats().connections.preflight_retired,
            1,
            "the dead connection must be counted as retired at checkout"
        );
    }

    /// The honest residual: a connection that passes the preflight and is closed
    /// while the request is in flight is still an unknown outcome.
    #[tokio::test]
    async fn a_connection_closed_after_the_request_is_read_stays_unknown() {
        let leader = write_server(WriteAnswer::OkThenCloseOnSecond).await;
        let pool = CeleriantPool::new(PoolOptions::new(leader.to_string()));

        pool.write(empty_write_request()).await.expect("the first write must land");

        let err = pool
            .write(empty_write_request())
            .await
            .expect_err("the second answer never comes");

        assert!(
            matches!(err, ClientError::ConnectionLostAfterSend(_)),
            "expected ConnectionLostAfterSend, got {err:?}"
        );
    }

    /// A hint naming a node whose breaker is open is skipped for free. When
    /// each follower hints back at the dead primary, charging a retry for every
    /// re-entry burns the budget before the live leader is ever reached.
    #[tokio::test]
    async fn a_stale_hint_must_not_starve_an_untried_seed() {
        let primary = reserved_address();
        let live = counter();
        let s1 = write_server(WriteAnswer::NotLeader(primary.clone())).await;
        let s2 = write_server(WriteAnswer::NotLeader(primary.clone())).await;
        let s3 = write_server_at("127.0.0.1:0", WriteAnswer::Ok, live.clone()).await;

        let pool = CeleriantPool::new(PoolOptions::new(primary).with_seed_addresses(vec![
            s1.to_string(),
            s2.to_string(),
            s3.to_string(),
        ]));

        let result = pool.write(empty_write_request()).await;

        assert_eq!(count(&live), 1, "the live leader must be reached");
        assert!(result.is_ok(), "a live leader in the seed list must serve the write, got {result:?}");
    }

    /// An address is never re-entered, even when the hint names the answering
    /// node itself, which is what a follower with a stale election view reports.
    #[tokio::test]
    async fn a_node_that_hints_at_itself_must_not_be_re_entered() {
        let hits = counter();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        write_server_at(&addr, WriteAnswer::NotLeader(addr.clone()), hits.clone()).await;

        let pool = CeleriantPool::new(PoolOptions::new(addr));

        pool.write(empty_write_request())
            .await
            .expect_err("a node that redirects to itself cannot serve the write");

        assert_eq!(count(&hits), 1, "a definitive NotLeader must retire the address");
    }

    /// A stalled identify handshake is this node failing before the
    /// caller's write was ever serialised. Reporting it as `RequestTimeout`
    /// would tell the caller an untouched write may have been applied.
    #[tokio::test]
    async fn a_stalled_handshake_is_not_an_ambiguous_request_timeout() {
        let deaf = silent_server().await;

        let pool = CeleriantPool::new(
            PoolOptions::new(deaf.to_string())
                .with_identity(ClientIdentityConfig::from_api_key("k"))
                .with_request_timeout(Duration::from_millis(300)),
        );

        let err = pool
            .write(empty_write_request())
            .await
            .expect_err("the handshake never completes");

        assert!(!matches!(err, ClientError::RequestTimeout), "got {err:?}");
        assert!(
            err.to_string().contains("Connection timeout"),
            "a pre-send handshake stall is a connect failure: {err}"
        );
    }

    /// The answer arrived but could not be read, so the node may have
    /// applied the write. The error keeps its type and nothing is re-sent.
    #[tokio::test]
    async fn an_answer_that_cannot_be_decoded_is_never_re_sent() {
        let (leader_hits, seed_hits) = (counter(), counter());
        let leader =
            write_server_at("127.0.0.1:0", WriteAnswer::CorruptBody, leader_hits.clone()).await;
        let seed = write_server_at(
            "127.0.0.1:0",
            WriteAnswer::NotLeader(leader.to_string()),
            seed_hits.clone(),
        )
        .await;

        let pool = CeleriantPool::new(
            PoolOptions::new(leader.to_string()).with_seed_addresses(vec![seed.to_string()]),
        );

        let err = pool
            .write(empty_write_request())
            .await
            .expect_err("an unreadable answer is not a success");

        assert!(matches!(err, ClientError::ReadError(_)), "expected a typed ReadError, got {err:?}");
        assert_eq!(count(&leader_hits), 1, "the leader must be asked exactly once");
        assert_eq!(count(&seed_hits), 0, "a write that may have landed is never re-sent");
    }

    /// A read is not the leader's to refuse: when the pinned leader's pool
    /// is saturated the read moves on, and the leader stays cached.
    #[tokio::test]
    async fn a_saturated_leader_pool_falls_through_to_the_next_read_candidate() {
        let leader = write_server(WriteAnswer::ReadOk).await;
        let follower = write_server(WriteAnswer::ReadOk).await;

        let mut opts = PoolOptions::new(leader.to_string())
            .with_seed_addresses(vec![follower.to_string()]);
        opts.max_connections_per_node = 1;
        opts.connection_timeout = Duration::from_millis(50);
        let pool = CeleriantPool::new(opts);

        let _held = pool.get_leader_connection().await.expect("the leader must accept");

        let result = pool.read(empty_read_request()).await;

        assert!(result.is_ok(), "a follower must serve the read, got {result:?}");
        assert!(pool.leader_address.read().unwrap().is_none(), "the cache must be untouched");
    }

    /// A read whose answer is lost is safe to retry, so the next
    /// candidate serves it instead of failing the caller.
    #[tokio::test]
    async fn a_read_whose_answer_is_lost_fails_over_to_the_next_candidate() {
        let leader = write_server(WriteAnswer::ReadThenClose).await;
        let follower = write_server(WriteAnswer::ReadOk).await;

        let pool = CeleriantPool::new(
            PoolOptions::new(leader.to_string()).with_seed_addresses(vec![follower.to_string()]),
        );

        let result = pool.read(empty_read_request()).await;

        assert!(result.is_ok(), "a lost read answer must fail over, got {result:?}");
    }

    /// A pre-send refusal still walks, but the seed's hint back to the refusing
    /// leader is skipped while its breaker is open, so the walk ends naming the
    /// last address it tried and the answer it gave.
    #[tokio::test]
    async fn a_hint_into_an_open_breaker_is_skipped_and_the_walk_reports_its_last_try() {
        let seed_hits = counter();
        let leader = reserved_address();
        let seed =
            write_server_at("127.0.0.1:0", WriteAnswer::NotLeader(leader.clone()), seed_hits.clone())
                .await;

        let pool = CeleriantPool::new(
            PoolOptions::new(leader.clone()).with_seed_addresses(vec![seed.to_string()]),
        );

        let err = pool.write(empty_write_request()).await.expect_err("the leader is down");
        let text = err.to_string();

        assert!(text.contains(&seed.to_string()), "the error must name the last node tried: {text}");
        assert!(text.contains(&leader), "the error must carry the answer it got: {text}");
        assert_eq!(count(&seed_hits), 1, "the seed answers once; it cannot serve the write");
    }

    /// The breaker is a cooldown, not a verdict: once it lapses the
    /// next write reaches the leader through the same walk.
    #[tokio::test]
    async fn the_walk_reaches_the_leader_again_once_the_breaker_lapses() {
        let leader_addr = reserved_address();
        let seed = write_server(WriteAnswer::NotLeader(leader_addr.clone())).await;
        let pool = CeleriantPool::new(
            PoolOptions::new(leader_addr.clone()).with_seed_addresses(vec![seed.to_string()]),
        );

        pool.write(empty_write_request()).await.expect_err("the leader is down");

        let leader_hits = counter();
        write_server_at(&leader_addr, WriteAnswer::Ok, leader_hits.clone()).await;
        tokio::time::sleep(CIRCUIT_BREAKER_COOLDOWN + Duration::from_millis(200)).await;

        pool.write(empty_write_request()).await.expect("the leader is back");
        assert_eq!(count(&leader_hits), 1, "the recovered leader must serve the write");
        // The cache only names a node that differs from the primary, so what
        // must hold is that routing points back at the leader.
        assert_eq!(pool.current_or_primary_leader(), leader_addr, "routing must have recovered");
    }

    /// A local pool wait is not a node fault: no byte was sent, so the write
    /// returns at once with the address it waited on, the leader cache is
    /// untouched, and no seed is dialled (a follower cannot serve a write).
    #[tokio::test]
    async fn a_local_pool_wait_returns_pool_timeout_without_walking_the_seeds() {
        let primary = write_server(WriteAnswer::Ok).await;
        // Bound but never accepted: a dial would still be completed by the
        // kernel backlog, so a pending accept proves the seed was contacted.
        let seed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        seed.set_nonblocking(true).unwrap();

        let mut opts = PoolOptions::new(primary.to_string())
            .with_seed_addresses(vec![seed.local_addr().unwrap().to_string()]);
        opts.max_connections_per_node = 1;
        opts.connection_timeout = Duration::from_millis(50);
        let pool = CeleriantPool::new(opts);

        // Hold the node's only permit for the duration of the write.
        let _held = pool.get_leader_connection().await.expect("primary must accept");

        let err = pool
            .write(empty_write_request())
            .await
            .expect_err("a saturated node pool must not block forever");

        match err {
            ClientError::PoolTimeout { ref address, .. } => {
                assert_eq!(address, &primary.to_string())
            }
            other => panic!("expected PoolTimeout, got {other:?}"),
        }
        assert!(pool.leader_address.read().unwrap().is_none(), "the cache must be untouched");
        assert!(
            matches!(seed.accept(), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
            "the seed must never be dialled"
        );
    }

    /// The two numbers the stats exist to separate: a cold connect and a
    /// pooled reuse. The second write must dial nothing.
    #[tokio::test]
    async fn stats_separate_a_cold_connect_from_a_pooled_reuse() {
        let primary = write_server(WriteAnswer::Ok).await;
        let pool = CeleriantPool::new(PoolOptions::new(primary.to_string()));

        pool.write(empty_write_request()).await.expect("the leader answers");
        let c = pool.stats().connections;
        assert_eq!((c.attempted, c.succeeded, c.pooled_reuse), (1, 1, 0), "first write must dial");

        pool.write(empty_write_request()).await.expect("the leader answers");
        let c = pool.stats().connections;
        assert_eq!((c.attempted, c.pooled_reuse), (1, 1), "second write must reuse the connection");
    }

    /// A followed hint is the hop that the old code refused to make.
    #[tokio::test]
    async fn stats_count_a_followed_leader_redirect() {
        let leader = write_server(WriteAnswer::Ok).await;
        let primary = write_server(WriteAnswer::NotLeader(leader.to_string())).await;
        let pool = CeleriantPool::new(PoolOptions::new(primary.to_string()));

        pool.write(empty_write_request()).await.expect("the hint leads to the leader");

        assert_eq!(pool.stats().leader_redirects_followed, 1);
    }

    /// A local pool wait is counted on the permit gate, and the histogram
    /// bucket it lands in is the one the `connection_timeout` predicts.
    #[tokio::test]
    async fn stats_count_a_pool_timeout_and_its_wait_bucket() {
        let primary = write_server(WriteAnswer::Ok).await;
        let mut opts = PoolOptions::new(primary.to_string());
        opts.max_connections_per_node = 1;
        opts.connection_timeout = Duration::from_millis(50);
        let pool = CeleriantPool::new(opts);

        let _held = pool.get_leader_connection().await.expect("primary must accept");
        // That checkout timed its own wait. On a loaded machine it can land in
        // the same bucket, so only the delta across the write is the evidence.
        let before = pool.stats().connections.wait_buckets;

        pool.write(empty_write_request()).await.expect_err("the only permit is held");

        let c = pool.stats().connections;
        assert_eq!((c.pool_timeouts(), c.pool_timeouts_permit), (1, 1));
        let bucket = wait_bucket(Duration::from_millis(50));
        assert_eq!(
            c.wait_buckets[bucket] - before[bucket], 1,
            "the 50ms wait must land in its own bucket: {before:?} -> {:?}", c.wait_buckets
        );
    }

    /// With no cached leader, `clear_leader` pins `seed_addresses[0]`, which is
    /// follower-pinning, not a cache clear.
    #[tokio::test]
    async fn stats_separate_a_cache_clear_from_a_pin_to_the_first_seed() {
        let pool = CeleriantPool::new(
            PoolOptions::new("p:1").with_seed_addresses(vec!["b:1".into()]),
        );

        pool.clear_leader();
        pool.clear_leader();

        let stats = pool.stats();
        assert_eq!((stats.leader_pinned_to_seed, stats.leader_cache_clears), (1, 1));
    }

    /// A hint naming a node inside its breaker cooldown is refused, and the
    /// refusal is the number that explains a walk ending early.
    #[tokio::test]
    async fn stats_count_a_hint_refused_by_an_open_breaker() {
        let leader = reserved_address();
        let seed = write_server(WriteAnswer::NotLeader(leader.clone())).await;
        let pool = CeleriantPool::new(
            PoolOptions::new(leader).with_seed_addresses(vec![seed.to_string()]),
        );

        pool.write(empty_write_request()).await.expect_err("the leader is down");

        let stats = pool.stats();
        assert_eq!(stats.leader_hints_skipped, 1);
        assert_eq!(stats.leader_redirects_followed, 0, "a refused hint is not a hop");
        assert_eq!(stats.walks_exhausted, 1);
    }

    /// An exhausted walk reports where it gave up. "no leader found across
    /// known nodes" on its own hid the concrete failure of every attempt.
    #[tokio::test]
    async fn an_exhausted_walk_names_the_last_address_and_its_error() {
        let dead = || {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        let (primary, seed) = (dead(), dead());

        let pool = CeleriantPool::new(
            PoolOptions::new(primary).with_seed_addresses(vec![seed.clone()]),
        );

        let err = pool.write(empty_write_request()).await.expect_err("both nodes are dead");
        let text = err.to_string();
        assert!(text.contains(&seed), "the last address tried must be named: {text}");
        assert!(
            !text.ends_with("no leader found across known nodes"),
            "the concrete error must survive: {text}"
        );
    }
}

