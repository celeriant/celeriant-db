use std::{cell::Cell, collections::HashSet, fs, path::{Path, PathBuf}, sync::Arc};

use celeriant_client_tokio::{
    CeleriantPool, PoolOptions, WriteEventsOptions,
    celeriant_client::{ClientIdentityConfig, ClientTlsConfig},
    list_operations::ListOptions,
    watch_connection::WatchOptions,
};
use celeriant_crypto::{pki::PkiManager, Crypto};
use celeriant_msg::request::{
    read_filters::ReadFilters,
    requests::*,
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    schema_key::SchemaKey,
};
use directories::ProjectDirs;
use rustls::pki_types::ServerName;
use tokio::time::Duration;

use crate::cli::Cli;
use crate::tui::settings::{IdentityMode, Settings};
use crate::utils::{extract_host, format_timestamp, format_u128_uuid, parse_u128};

#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Home,
    Connect,
    Settings,
    AggregateContext,
    EnterAggregate,
    ReadEvents,
    WriteEvent,
    TrimStart,
    Watch,
    OrgWatch,
    List,
    RegisterSchema,
    Help,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InputMode {
    Normal,
    Editing,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PendingAction {
    Delete,
    Trim,
}

#[derive(Debug, Clone)]
pub struct AggregateContext {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub info: Option<AggregateContextInfo>,
}

#[derive(Debug, Clone)]
pub struct AggregateContextInfo {
    pub min_batch: u64,
    pub max_batch: u64,
    pub max_event_index: u64,
    pub is_deleted: bool,
}

pub struct App {
    pub settings: Settings,
    pub server_address: String,
    pub pool: Option<CeleriantPool>,
    pub data_root: PathBuf,
    pub is_active: bool,
    pub screen: Screen,
    pub previous_screen: Option<Screen>,
    pub input_mode: InputMode,
    pub should_quit: bool,

    // Status and messages
    pub status_message: String,
    pub last_error: Option<String>,

    // Current context
    pub selected_org: Option<u128>,
    pub aggregate_context: Option<AggregateContext>,

    // Menu selection
    pub menu_index: usize,

    // Input fields
    pub input_fields: Vec<InputField>,
    pub input_field_index: usize,

    // Results display
    pub result_output: Vec<String>,
    pub result_scroll: usize,

    // Read events state
    pub read_from_index: String,
    pub read_to_index: String,

    // Write event state
    pub client_id: u128,
    pub write_event_type: String,
    pub write_data: String,

    // Trim start state
    pub trim_keep_from: String,

    // Watch state
    pub watch_event_types: String,
    pub watch_latency_ms: String,
    pub watch_active: bool,
    pub watch_events: Vec<String>,
    pub watch_scroll: usize,
    pub watch_receiver: Option<tokio::sync::mpsc::UnboundedReceiver<WatchUpdate>>,
    pub watch_cancel: Option<tokio::sync::oneshot::Sender<()>>,

    // Org Watch state
    pub org_watch_org_id: String,
    pub org_watch_aggregate_types: String,
    pub org_watch_event_types: String,
    pub org_watch_latency_ms: String,

    // List state
    pub list_org_id: String,
    pub list_aggregate_type: String,
    pub list_results: Vec<String>,
    pub list_scroll: usize,
    /// Parallel to list_results: maps result-line index to (org_id, aggregate_type_id, aggregate_id).
    /// Only populated when listing aggregates (both org and type are set).
    pub list_selectable: Vec<Option<(u128, u128, u128)>>,

    // RegisterSchema state
    pub register_schema_event_type_major: String,
    pub register_schema_event_type_minor: String,
    pub register_schema_type: String,
    pub register_schema_value: String,

    // Confirmation state for destructive operations
    pub pending_action: Option<PendingAction>,
    pub confirm_input: String,

    // Scroll metrics — set by the draw loop, consumed by event handlers
    pub visible_height: Cell<usize>,

    // Help screen scroll
    pub help_scroll: usize,

    // Settings screen scroll
    pub settings_scroll: usize,
}

#[derive(Debug)]
pub enum WatchUpdate {
    Event(Vec<String>),
    Heartbeat,
    Error(String),
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct InputField {
    pub label: String,
    pub value: String,
    pub placeholder: String,
}

impl InputField {
    pub fn new(label: &str, placeholder: &str) -> Self {
        Self {
            label: label.to_string(),
            value: String::new(),
            placeholder: placeholder.to_string(),
        }
    }

    pub fn effective_value(&self) -> &str {
        if self.value.is_empty() { &self.placeholder } else { &self.value }
    }

    pub fn with_value(label: &str, value: &str) -> Self {
        Self {
            label: label.to_string(),
            value: value.to_string(),
            placeholder: String::new(),
        }
    }
}

impl App {
    pub fn new(cli: &Cli) -> anyhow::Result<Self> {
        let mut settings = Settings::load();

        // Apply CLI flag overrides (CLI > settings file > defaults).
        // Server: use CLI value unless it's still the compiled-in default.
        if cli.server != "127.0.0.1:10000" {
            settings.connection.server = cli.server.clone();
        }
        if cli.tls {
            settings.tls.enabled = true;
        }
        if let Some(ref p) = cli.ca_cert {
            settings.tls.ca_cert = p.to_string_lossy().into_owned();
        }
        if let Some(ref p) = cli.client_cert {
            settings.tls.client_cert = p.to_string_lossy().into_owned();
        }
        if let Some(ref p) = cli.client_key {
            settings.tls.client_key = p.to_string_lossy().into_owned();
        }
        if let Some(ref name) = cli.server_name {
            settings.tls.server_name = name.clone();
        }
        if let Some(ref key) = cli.api_key {
            settings.auth.api_key = key.clone();
        }
        if let Some(ref p) = cli.public_key {
            settings.identity.public_key = p.to_string_lossy().into_owned();
            settings.identity.mode = IdentityMode::Custom;
        }
        if let Some(ref p) = cli.private_key {
            settings.identity.private_key = p.to_string_lossy().into_owned();
        }

        let server_address = settings.connection.server.clone();

        // Get OS-appropriate data directory
        let data_root = ProjectDirs::from("com", "celeriant", "celeriant_cli")
            .map(|dirs| dirs.data_dir().to_path_buf())
            .unwrap_or_else(|| {
                eprintln!("Warning: Could not determine user data directory, using fallback");
                PathBuf::from(".celeriant_cli")
            });

        // Load or generate persistent client ID from keypair
        let client_id = Crypto::load_or_generate_node_id(&data_root)
            .map_err(|e| anyhow::anyhow!("Failed to initialize client ID: {e}"))?;

        let welcome = format!("Welcome to Celeriant CLI. Server: {}", server_address);
        Ok(Self {
            settings,
            server_address,
            pool: None,
            data_root,
            is_active: false,
            screen: Screen::Home,
            previous_screen: None,
            input_mode: InputMode::Normal,
            should_quit: false,
            status_message: welcome,
            last_error: None,
            selected_org: None,
            aggregate_context: None,
            menu_index: 0,
            input_fields: Vec::new(),
            input_field_index: 0,
            result_output: Vec::new(),
            result_scroll: 0,
            read_from_index: "1".to_string(),
            read_to_index: String::new(),
            client_id,
            write_event_type: "1".to_string(),
            write_data: String::new(),
            trim_keep_from: String::new(),

            // Watch state
            watch_event_types: "1".to_string(),  // Default to WRITE events
            watch_latency_ms: "100".to_string(),
            watch_active: false,
            watch_events: Vec::new(),
            watch_scroll: 0,
            watch_receiver: None,
            watch_cancel: None,

            // Org Watch state
            org_watch_org_id: "1".to_string(),
            org_watch_aggregate_types: String::new(),
            org_watch_event_types: "1".to_string(),
            org_watch_latency_ms: "100".to_string(),

            // List state
            list_org_id: String::new(),
            list_aggregate_type: String::new(),
            list_results: Vec::new(),
            list_scroll: 0,
            list_selectable: Vec::new(),

            // RegisterSchema state
            register_schema_event_type_major: "1".to_string(),
            register_schema_event_type_minor: "0".to_string(),
            register_schema_type: "json".to_string(),
            register_schema_value: String::new(),

            pending_action: None,
            confirm_input: String::new(),
            visible_height: Cell::new(20),
            help_scroll: 0,
            settings_scroll: 0,
        })
    }

    pub fn setup_enter_aggregate_fields(&mut self) {
        self.input_fields = vec![
            InputField::new("Organisation ID", "1"),
            InputField::new("Aggregate Type ID", "1"),
            InputField::new("Aggregate ID", "1"),
        ];
        self.input_field_index = 0;
    }

    pub fn setup_connect_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("Server Address", &self.server_address),
        ];
        self.input_field_index = 0;
    }

    pub fn setup_read_events_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("From Batch Index", &self.read_from_index),
            InputField::with_value("To Batch Index (optional)", &self.read_to_index),
        ];
        self.input_field_index = 0;
        self.result_output.clear();
    }

    pub fn setup_write_event_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("Event Type", &self.write_event_type),
            InputField::with_value("Event Data (JSON/text or file path)", &self.write_data),
        ];
        self.input_field_index = 0;
        self.result_output.clear();
    }

    pub fn setup_trim_start_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("Keep From Batch Index", &self.trim_keep_from),
        ];
        self.input_field_index = 0;
    }

    pub fn setup_register_schema_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("Event Type Major", &self.register_schema_event_type_major),
            InputField::with_value("Event Type Minor", &self.register_schema_event_type_minor),
            InputField::with_value("Schema Type (json/avro)", &self.register_schema_type),
            InputField::with_value("Schema (string or file path)", &self.register_schema_value),
        ];
        self.input_field_index = 0;
        self.result_output.clear();
    }

    pub fn setup_settings_fields(&mut self) {
        let s = &self.settings;
        self.input_fields = vec![
            // Connection section
            InputField::with_value("[Connection] Server", &s.connection.server),
            InputField::with_value("[Connection] Seed Addresses (comma-separated)", &s.connection.seed_addresses.join(",")),
            // TLS section
            InputField::with_value("[TLS] Enabled (true/false)", if s.tls.enabled { "true" } else { "false" }),
            InputField::with_value("[TLS] CA Cert Path", &s.tls.ca_cert),
            InputField::with_value("[TLS] Client Cert Path", &s.tls.client_cert),
            InputField::with_value("[TLS] Client Key Path", &s.tls.client_key),
            InputField::with_value("[TLS] Server Name", &s.tls.server_name),
            // Auth section
            InputField::with_value("[Auth] API Key", &s.auth.api_key),
            // Identity section
            InputField::with_value("[Identity] Mode (auto/custom/none)", s.identity.mode.label()),
            InputField::with_value("[Identity] Public Key Path", &s.identity.public_key),
            InputField::with_value("[Identity] Private Key Path", &s.identity.private_key),
            // Pool section
            InputField::with_value("[Pool] Max Connections Per Node", &s.pool.max_connections_per_node.to_string()),
            InputField::with_value("[Pool] Connection Timeout (ms)", &s.pool.connection_timeout_ms.to_string()),
            InputField::with_value("[Pool] Request Timeout (ms)", &s.pool.request_timeout_ms.to_string()),
            // Routing section
            InputField::with_value("[Routing] Route Reads to Followers (true/false)", if s.routing.route_reads_to_followers { "true" } else { "false" }),
            InputField::with_value("[Routing] Max Leader Retries", &s.routing.max_leader_retries.to_string()),
        ];
        self.input_field_index = 0;
        self.settings_scroll = 0;
    }

    pub fn sync_settings_from_fields(&mut self) {
        if self.input_fields.len() < 16 {
            return;
        }
        let s = &mut self.settings;

        s.connection.server = self.input_fields[0].value.clone();
        s.connection.seed_addresses = self.input_fields[1].value
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();

        s.tls.enabled = self.input_fields[2].value.trim().eq_ignore_ascii_case("true");
        s.tls.ca_cert = self.input_fields[3].value.clone();
        s.tls.client_cert = self.input_fields[4].value.clone();
        s.tls.client_key = self.input_fields[5].value.clone();
        s.tls.server_name = self.input_fields[6].value.clone();

        s.auth.api_key = self.input_fields[7].value.clone();

        s.identity.mode = match self.input_fields[8].value.trim().to_lowercase().as_str() {
            "custom" => IdentityMode::Custom,
            "none" => IdentityMode::None,
            _ => IdentityMode::Auto,
        };
        s.identity.public_key = self.input_fields[9].value.clone();
        s.identity.private_key = self.input_fields[10].value.clone();

        s.pool.max_connections_per_node = self.input_fields[11].value.trim().parse().unwrap_or(s.pool.max_connections_per_node);
        s.pool.connection_timeout_ms = self.input_fields[12].value.trim().parse().unwrap_or(s.pool.connection_timeout_ms);
        s.pool.request_timeout_ms = self.input_fields[13].value.trim().parse().unwrap_or(s.pool.request_timeout_ms);

        s.routing.route_reads_to_followers = self.input_fields[14].value.trim().eq_ignore_ascii_case("true");
        s.routing.max_leader_retries = self.input_fields[15].value.trim().parse().unwrap_or(s.routing.max_leader_retries);

        // Keep server_address in sync
        self.server_address = self.settings.connection.server.clone();
    }

    /// Copy `input_fields` values back to the concrete App fields for the current screen.
    /// Call this immediately before executing an operation.
    pub fn sync_fields_to_state(&mut self) {
        match self.screen {
            Screen::Connect => {
                if let Some(f) = self.input_fields.first() {
                    self.server_address = f.value.clone();
                    self.settings.connection.server = f.value.clone();
                }
            }
            Screen::ReadEvents => {
                if self.input_fields.len() >= 2 {
                    self.read_from_index = self.input_fields[0].value.clone();
                    self.read_to_index = self.input_fields[1].value.clone();
                }
            }
            Screen::WriteEvent => {
                if self.input_fields.len() >= 2 {
                    self.write_event_type = self.input_fields[0].value.clone();
                    self.write_data = self.input_fields[1].value.clone();
                }
            }
            Screen::TrimStart => {
                if let Some(f) = self.input_fields.first() {
                    self.trim_keep_from = f.value.clone();
                }
            }
            Screen::Watch => {
                if self.input_fields.len() >= 2 {
                    self.watch_event_types = self.input_fields[0].value.clone();
                    self.watch_latency_ms = self.input_fields[1].value.clone();
                }
            }
            Screen::OrgWatch => {
                if self.input_fields.len() >= 4 {
                    self.org_watch_org_id = self.input_fields[0].value.clone();
                    self.org_watch_aggregate_types = self.input_fields[1].value.clone();
                    self.org_watch_event_types = self.input_fields[2].value.clone();
                    self.org_watch_latency_ms = self.input_fields[3].value.clone();
                }
            }
            Screen::List => {
                if self.input_fields.len() >= 2 {
                    self.list_org_id = self.input_fields[0].value.clone();
                    self.list_aggregate_type = self.input_fields[1].value.clone();
                }
            }
            Screen::RegisterSchema => {
                if self.input_fields.len() >= 4 {
                    self.register_schema_event_type_major = self.input_fields[0].value.clone();
                    self.register_schema_event_type_minor = self.input_fields[1].value.clone();
                    self.register_schema_type = self.input_fields[2].value.clone();
                    self.register_schema_value = self.input_fields[3].value.clone();
                }
            }
            Screen::Settings => {
                self.sync_settings_from_fields();
            }
            _ => {}
        }
    }

    pub async fn navigate_to_aggregate_from_input(&mut self) -> Result<(), String> {
        if self.input_fields.len() < 3 {
            return Err("Invalid input fields".to_string());
        }

        let org_id = parse_u128(self.input_fields[0].effective_value())
            .map_err(|_| "Invalid Organisation ID (use number or UUID)")?;
        let aggregate_type_id = parse_u128(self.input_fields[1].effective_value())
            .map_err(|_| "Invalid Aggregate Type ID (use number or UUID)")?;
        let aggregate_id = parse_u128(self.input_fields[2].effective_value())
            .map_err(|_| "Invalid Aggregate ID (use number or UUID)")?;

        self.aggregate_context = Some(AggregateContext {
            org_id,
            aggregate_type_id,
            aggregate_id,
            info: None,
        });

        // Check if aggregate exists
        if let Err(e) = self.check_aggregate_exists().await {
            self.set_error(&e);
        }

        self.go_to_screen(Screen::AggregateContext);
        Ok(())
    }

    /// Navigate to AggregateContext from the current list_scroll position.
    /// Returns true if navigation occurred.
    pub async fn navigate_to_aggregate_from_list(&mut self) -> bool {
        let idx = self.list_scroll;
        let selectable = self.list_selectable.get(idx).copied().flatten();
        if let Some((org_id, aggregate_type_id, aggregate_id)) = selectable {
            self.aggregate_context = Some(AggregateContext {
                org_id,
                aggregate_type_id,
                aggregate_id,
                info: None,
            });
            if let Err(e) = self.check_aggregate_exists().await {
                self.set_error(&e);
            }
            self.go_to_screen(Screen::AggregateContext);
            true
        } else {
            false
        }
    }

    pub fn is_connected(&self) -> bool {
        self.is_active
    }

    pub fn set_status(&mut self, msg: &str) {
        self.status_message = msg.to_string();
        self.last_error = None;
    }

    pub fn set_error(&mut self, msg: &str) {
        self.last_error = Some(msg.to_string());
        self.status_message = format!("Error: {}", msg);
    }

    fn stop_watch_if_leaving(&mut self) {
        if matches!(self.screen, Screen::Watch | Screen::OrgWatch) && self.watch_active {
            self.stop_watch();
        }
    }

    pub fn go_to_screen(&mut self, screen: Screen) {
        self.stop_watch_if_leaving();
        self.previous_screen = Some(self.screen.clone());
        self.screen = screen;
        self.menu_index = 0;
        self.input_mode = InputMode::Normal;
    }

    pub fn go_back(&mut self) {
        self.stop_watch_if_leaving();
        if let Some(prev) = self.previous_screen.take() {
            // Refresh connect fields when returning to Connect so the settings summary reflects
            // any changes made on the Settings screen.
            if prev == Screen::Connect {
                self.setup_connect_fields();
            }
            self.screen = prev;
        } else {
            self.screen = Screen::Home;
        }
        self.menu_index = 0;
        self.input_mode = InputMode::Normal;
    }

    pub async fn connect(&mut self) -> Result<(), String> {
        self.set_status(&format!("Connecting to {}...", self.server_address));

        let options = self.build_pool_options()?;
        let pool = CeleriantPool::new(options);

        // Test connectivity with a real round-trip — list orgs page 0
        let mut iter = pool
            .list_orgs(ListOptions::default())
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;

        // Drain the first result (or None) just to confirm the server is reachable.
        let _ = iter.next().await;

        self.pool = Some(pool);
        self.is_active = true;
        let tls_info = if self.settings.tls.enabled { "TLS: on" } else { "TLS: off" };
        let identity_info = format!("Identity: {}", self.settings.identity.mode.label());
        self.set_status(&format!(
            "Connected to {} ({}, {})",
            self.server_address, tls_info, identity_info
        ));
        Ok(())
    }

    fn build_pool_options(&self) -> Result<PoolOptions, String> {
        let s = &self.settings;
        let mut opts = PoolOptions::new(&self.server_address);

        // Seed addresses
        if !s.connection.seed_addresses.is_empty() {
            opts = opts.with_seed_addresses(s.connection.seed_addresses.clone());
        }

        // TLS
        if s.tls.enabled {
            let tls = self.build_tls_config()?;
            opts = opts.with_tls(tls);
        }

        // Identity
        if let Some(identity) = self.build_identity_config()? {
            opts = opts.with_identity(identity);
        }

        // Pool sizing and timeouts
        opts = opts
            .with_max_connections(s.pool.max_connections_per_node as usize)
            .with_connection_timeout(Duration::from_millis(s.pool.connection_timeout_ms))
            .with_request_timeout(Duration::from_millis(s.pool.request_timeout_ms));

        // Routing
        opts = opts
            .with_route_reads_to_followers(s.routing.route_reads_to_followers)
            .with_max_leader_retries(s.routing.max_leader_retries as usize);

        Ok(opts)
    }

    fn build_tls_config(&self) -> Result<ClientTlsConfig, String> {
        let s = &self.settings.tls;

        if s.ca_cert.is_empty() {
            return Err("TLS is enabled but ca_cert is not configured".to_string());
        }

        let ca_bundle = PkiManager::load_ca_bundle(Path::new(&s.ca_cert))
            .map_err(|e| format!("Failed to load CA certificate '{}': {}", s.ca_cert, e))?;

        let client_config = if !s.client_cert.is_empty() && !s.client_key.is_empty() {
            let (chain, key) = PkiManager::load_identity(
                Path::new(&s.client_cert),
                Path::new(&s.client_key),
            )
            .map_err(|e| format!("Failed to load client identity '{}': {}", s.client_cert, e))?;
            PkiManager::build_client_config(&ca_bundle, chain, key)
                .map_err(|e| format!("Failed to build TLS client config: {}", e))?
        } else {
            PkiManager::build_client_config_no_auth(&ca_bundle)
                .map_err(|e| format!("Failed to build TLS client config: {}", e))?
        };

        let host = if !s.server_name.is_empty() {
            &s.server_name
        } else {
            extract_host(&self.server_address)
        };

        let server_name = ServerName::try_from(host.to_owned())
            .map_err(|_| format!("Invalid TLS server name: {host}"))?;

        Ok(ClientTlsConfig::new(client_config, server_name))
    }

    fn build_identity_config(&self) -> Result<Option<ClientIdentityConfig>, String> {
        let s = &self.settings;
        let api_key = if s.auth.api_key.is_empty() { None } else { Some(s.auth.api_key.clone()) };

        match s.identity.mode {
            IdentityMode::None => {
                // API-key-only auth (no keypair identity)
                Ok(api_key.map(|key| ClientIdentityConfig {
                    public_key: None,
                    private_key: None,
                    api_key: Some(key),
                }))
            }
            IdentityMode::Custom => {
                if s.identity.public_key.is_empty() || s.identity.private_key.is_empty() {
                    return Err("Identity mode is 'custom' but public_key or private_key is not configured".to_string());
                }
                let pub_key = fs::read_to_string(&s.identity.public_key)
                    .map_err(|e| format!("Failed to read public key '{}': {e}", s.identity.public_key))?;
                let priv_key = fs::read_to_string(&s.identity.private_key)
                    .map_err(|e| format!("Failed to read private key '{}': {e}", s.identity.private_key))?;
                Ok(Some(ClientIdentityConfig {
                    public_key: Some(pub_key.trim().to_string()),
                    private_key: Some(priv_key.trim().to_string()),
                    api_key,
                }))
            }
            IdentityMode::Auto => {
                let pub_path = self.data_root.join("public_key");
                let priv_path = self.data_root.join("private_key");
                if !pub_path.exists() || !priv_path.exists() {
                    return Ok(api_key.map(|key| ClientIdentityConfig {
                        public_key: None,
                        private_key: None,
                        api_key: Some(key),
                    }));
                }
                let pub_key = fs::read_to_string(&pub_path)
                    .map_err(|e| format!("Failed to read auto public key: {e}"))?;
                let priv_key = fs::read_to_string(&priv_path)
                    .map_err(|e| format!("Failed to read auto private key: {e}"))?;
                Ok(Some(ClientIdentityConfig {
                    public_key: Some(pub_key.trim().to_string()),
                    private_key: Some(priv_key.trim().to_string()),
                    api_key,
                }))
            }
        }
    }

    pub async fn disconnect(&mut self) {
        self.pool = None;
        self.is_active = false;
        self.selected_org = None;
        self.aggregate_context = None;
        self.set_status("Disconnected");
    }

    pub async fn check_aggregate_exists(&mut self) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_mut().ok_or("No aggregate selected")?;

        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        match pool.aggregate_details(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key: key,
        }).await {
            Ok(res) => {
                ctx.info = Some(AggregateContextInfo {
                    min_batch: res.min_event_batch_index,
                    max_batch: res.max_event_batch_index,
                    max_event_index: res.max_event_index,
                    is_deleted: res.is_deleted,
                });
                self.set_status("Aggregate info loaded");
                Ok(())
            }
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }

    pub async fn read_events(&mut self) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;

        let from: u64 = self.read_from_index.parse().unwrap_or(1);
        let to: Option<u64> = self.read_to_index.parse().ok();

        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let mut filters = ReadFilters::new(from);
        if let Some(to_idx) = to {
            filters = filters.to_event_batch_index(to_idx);
        }

        let res = pool.read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters,
        }).await.map_err(|e| format!("Request failed: {}", e))?;

        self.result_output.clear();
        self.result_output.push(format!("Read {} event batches", res.event_batches.len()));
        if let Some(next) = res.next_event_batch_index {
            self.result_output.push(format!("Next batch index: {}", next));
        }
        self.result_output.push(String::new());

        for batch in &res.event_batches {
            self.result_output.push(format!(
                "━━━ Batch {} ━━━ {} ━━━",
                batch.event_batch_index,
                crate::utils::format_timestamp(batch.server_timestamp)
            ));
            self.result_output.push(format!(
                "Client: {} | User: {:?} | Events: {}",
                batch.client_id,
                batch.user_id,
                batch.events.len()
            ));

            for event in &batch.events {
                let data_str = String::from_utf8_lossy(&event.event_value);
                self.result_output.push(format!(
                    "  [{}] Type: {} | Index: {} | Time: {}",
                    humansize::format_size(event.event_value.len(), humansize::BINARY),
                    event.event_type_major,
                    event.event_index,
                    format_timestamp(event.event_timestamp)
                ));
                let lines: Vec<&str> = data_str.lines().collect();
                for line in lines.iter().take(5) {
                    self.result_output.push(format!("      {}", line));
                }
                if lines.len() > 5 {
                    self.result_output.push("      ...".to_string());
                }
            }
            self.result_output.push(String::new());
        }

        self.result_scroll = 0;
        self.set_status(&format!("Read {} batches", res.event_batches.len()));
        Ok(())
    }

    pub async fn write_event(&mut self) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;

        let event_type: u64 = self.write_event_type.parse().map_err(|_| "Invalid event type")?;

        if self.write_data.is_empty() {
            return Err("Event data cannot be empty".to_string());
        }

        let data = if Path::new(&self.write_data).exists() {
            std::fs::read(&self.write_data)
                .map_err(|e| format!("Failed to read file: {}", e))?
        } else {
            self.write_data.clone().into_bytes()
        };

        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let event = DatablockAggregateEvent {
            event_type_major: event_type,
            event_type_minor: 0,
            client_event_index: 0,
            event_timestamp: chrono::Utc::now().timestamp_millis() as u64,
            event_value: Arc::new(data),
            event_index: 0,
            event_id: None,
            iv: None,
        };

        let resp = pool.write_events_with(key, vec![event], WriteEventsOptions {
                client_id: self.client_id,
                ..Default::default()
            })
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        self.result_output.clear();
        self.result_output.push("Write successful!".to_string());
        if let Some(cid) = resp.correlation_id {
            self.result_output.push(format!("Correlation ID: {}", format_u128_uuid(cid)));
        }
        self.set_status("Event written successfully");
        Ok(())
    }

    pub async fn delete_aggregate(&mut self) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;

        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);

        let mut deletes = std::collections::HashMap::new();
        deletes.insert(key, SingleAggregateDelete {
            allow_recreate: true,
            allow_index_continuation: true,
            expected_event_batch_index: None,
        });

        pool.delete(DeleteRequest {
            correlation_id: None,
            client_id: self.client_id,
            user_id: None,
            deletes,
        })
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

        self.set_status("Aggregate deleted");
        self.aggregate_context = None;
        Ok(())
    }

    pub async fn trim_aggregate(&mut self) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;

        let keep_from: u64 = self.trim_keep_from.parse()
            .map_err(|_| "Invalid batch index")?;

        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);

        pool.trim_start(TrimStartRequest {
            correlation_id: None,
            aggregate_key: key,
            keep_from_event_batch_index: keep_from,
            client_id: self.client_id,
            user_id: None,
        })
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

        self.set_status(&format!("Trimmed events before batch {}", keep_from));
        Ok(())
    }

    pub async fn register_schema(&mut self) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;

        let event_type_major: u64 = self.register_schema_event_type_major.parse()
            .map_err(|_| "Invalid event type major (must be a number)")?;
        let event_type_minor: u64 = self.register_schema_event_type_minor.parse()
            .map_err(|_| "Invalid event type minor (must be a number)")?;

        let schema_type: u8 = match self.register_schema_type.to_lowercase().as_str() {
            "json" | "json schema" => 0,
            "avro" => 1,
            other => return Err(format!("Unknown schema type '{}' — use 'json' or 'avro'", other)),
        };

        let schema = if Path::new(&self.register_schema_value).exists() {
            std::fs::read_to_string(&self.register_schema_value)
                .map_err(|e| format!("Failed to read schema file: {}", e))?
        } else {
            self.register_schema_value.clone()
        };

        if schema.is_empty() {
            return Err("Schema cannot be empty".to_string());
        }

        let schema_key = SchemaKey::new(
            ctx.org_id,
            ctx.aggregate_type_id,
            event_type_major,
            event_type_minor,
        );

        pool.register_schema(RegisterSchemaRequest {
            correlation_id: None,
            client_id: self.client_id,
            user_id: None,
            schema_key,
            schema_type,
            schema,
        })
        .await
        .map_err(|e| format!("Request failed: {}", e))?;

        self.result_output.clear();
        self.result_output.push("Schema registered successfully.".to_string());
        self.set_status("Schema registered");
        Ok(())
    }

    pub fn get_home_menu_items(&self) -> Vec<(&str, &str)> {
        if self.is_connected() {
            vec![
                ("Enter Aggregate", "Go directly to an aggregate by ID"),
                ("List", "List orgs, types, or aggregates"),
                ("Organisation Watch", "Watch events across an organisation"),
                ("Disconnect", "Disconnect from server"),
                ("Help", "Show keyboard shortcuts"),
                ("Quit", "Exit the application"),
            ]
        } else {
            vec![
                ("Connect", "Connect to the server"),
                ("Change Server", "Change server address"),
                ("Help", "Show keyboard shortcuts"),
                ("Quit", "Exit the application"),
            ]
        }
    }

    pub fn setup_list_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("Organisation ID (empty=list orgs)", &self.list_org_id),
            InputField::with_value("Aggregate Type (empty=list types)", &self.list_aggregate_type),
        ];
        self.input_field_index = 0;
        self.list_results.clear();
        self.list_scroll = 0;
    }

    pub async fn execute_list(&mut self) -> Result<(), String> {
        let pool = self.pool.as_ref().ok_or("Not connected")?;

        let options = ListOptions::default();
        self.list_results.clear();
        self.list_selectable.clear();

        let org_id: Option<u128> = if self.list_org_id.trim().is_empty() {
            None
        } else {
            Some(parse_u128(self.list_org_id.trim()).map_err(|_| "Invalid Organisation ID (use number or UUID)")?)
        };

        let agg_type: Option<u128> = if self.list_aggregate_type.trim().is_empty() {
            None
        } else {
            Some(parse_u128(self.list_aggregate_type.trim()).map_err(|_| "Invalid Aggregate Type (use number or UUID)")?)
        };

        match (org_id, agg_type) {
            (None, _) => {
                self.list_results.push("━━━ Organisations ━━━".to_string());
                self.list_results.push(String::new());

                let mut iter = pool
                    .list_orgs(options)
                    .await
                    .map_err(|e| format!("Request failed: {}", e))?;

                let mut count = 0;
                while let Some(result) = iter.next().await {
                    match result {
                        Ok(org) => {
                            self.list_results.push(format!("  Org: {}", format_u128_uuid(org.org_id)));
                            count += 1;
                        }
                        Err(e) => {
                            self.list_results.push(format!("  Error: {}", e));
                            break;
                        }
                    }
                }
                self.list_results.push(String::new());
                self.list_results.push(format!("Total: {} organisations", count));
                self.set_status(&format!("Listed {} organisations", count));
            }
            (Some(org), None) => {
                self.list_results.push(format!("━━━ Aggregate Types for Org {} ━━━", format_u128_uuid(org)));
                self.list_results.push(String::new());

                let mut iter = pool
                    .list_aggregate_types(Some(org), options)
                    .await
                    .map_err(|e| format!("Request failed: {}", e))?;

                let mut count = 0;
                while let Some(result) = iter.next().await {
                    match result {
                        Ok(agg_type) => {
                            self.list_results.push(format!(
                                "  Type: {} (Org: {})",
                                format_u128_uuid(agg_type.aggregate_type_id),
                                format_u128_uuid(agg_type.org_id)
                            ));
                            count += 1;
                        }
                        Err(e) => {
                            self.list_results.push(format!("  Error: {}", e));
                            break;
                        }
                    }
                }
                self.list_results.push(String::new());
                self.list_results.push(format!("Total: {} aggregate types", count));
                self.set_status(&format!("Listed {} aggregate types", count));
            }
            (Some(org), Some(agg_type)) => {
                self.list_results.push(format!(
                    "━━━ Aggregates for Org {} Type {} ━━━",
                    format_u128_uuid(org),
                    format_u128_uuid(agg_type)
                ));
                self.list_selectable.push(None); // header line
                self.list_results.push(String::new());
                self.list_selectable.push(None); // blank line

                let mut iter = pool
                    .list_aggregates(Some(org), Some(agg_type), options)
                    .await
                    .map_err(|e| format!("Request failed: {}", e))?;

                let mut count = 0;
                while let Some(result) = iter.next().await {
                    match result {
                        Ok(agg) => {
                            let deleted_marker = if agg.is_deleted { " [DELETED]" } else { "" };
                            let last_updated = crate::utils::format_timestamp(agg.max_server_timestamp);
                            let events = agg.max_event_index.saturating_add(1);
                            self.list_results.push(format!(
                                "  [{}] - size {} | {} batches, {} events | updated {}{}",
                                format_u128_uuid(agg.aggregate_id),
                                humansize::format_size(agg.compressed_size, humansize::BINARY),
                                agg.event_batch_count,
                                events,
                                last_updated,
                                deleted_marker
                            ));
                            self.list_selectable.push(Some((org, agg_type, agg.aggregate_id)));
                            count += 1;
                        }
                        Err(e) => {
                            self.list_results.push(format!("  Error: {}", e));
                            self.list_selectable.push(None);
                            break;
                        }
                    }
                }
                self.list_results.push(String::new());
                self.list_selectable.push(None);
                self.list_results.push(format!("Total: {} aggregates (Enter to navigate)", count));
                self.list_selectable.push(None);
                self.set_status(&format!("Listed {} aggregates — press Enter on a row to navigate", count));
            }
        }

        self.list_scroll = 0;
        Ok(())
    }

    pub fn setup_watch_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("Event Types (0-5, comma-separated)", &self.watch_event_types),
            InputField::with_value("Latency (ms)", &self.watch_latency_ms),
        ];
        self.input_field_index = 0;
        self.watch_events.clear();
        self.watch_scroll = 0;
    }

    pub async fn start_watch(&mut self) -> Result<(), String> {
        if self.watch_active {
            return Err("Watch already active".to_string());
        }

        let pool = self.pool.as_ref().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;

        let event_types_set: HashSet<u8> = self.watch_event_types
            .split(',')
            .filter_map(|s| s.trim().parse::<u8>().ok())
            .filter(|&t| t <= 5)
            .collect();

        if event_types_set.is_empty() {
            return Err("At least one valid event type required (0-5)".to_string());
        }

        let latency_ms: Option<u64> = if self.watch_latency_ms.is_empty() {
            None
        } else {
            Some(self.watch_latency_ms.parse().map_err(|_| "Invalid latency")?)
        };

        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);

        let request = WatchRequest {
            correlation_id: None,
            requested_latency_ms: latency_ms,
            shard_id: None,
            orgs: Some(HashSet::from([key.org_id])),
            aggregate_types: Some(HashSet::from([key.aggregate_type_id])),
            aggregates: Some(HashSet::from([key.aggregate_id])),
            operation_types: Some(event_types_set.clone()),
        };

        let conn = pool
            .watch(request, WatchOptions::default())
            .await
            .map_err(|e| format!("Watch connection failed: {}", e))?;

        let mut init_lines = vec![
            "Starting watch on aggregate...".to_string(),
            format!("Event types: {:?}", event_types_set),
            String::new(),
        ];
        self.launch_watch_task(conn, &mut init_lines);
        self.set_status("Watch started");
        Ok(())
    }

    pub fn stop_watch(&mut self) {
        if let Some(cancel) = self.watch_cancel.take() {
            let _ = cancel.send(());
        }
        self.watch_receiver = None;
        self.watch_active = false;
        self.set_status("Watch stopped");
    }

    pub fn poll_watch_events(&mut self) {
        if let Some(ref mut rx) = self.watch_receiver {
            while let Ok(update) = rx.try_recv() {
                match update {
                    WatchUpdate::Event(lines) => {
                        for line in lines {
                            self.watch_events.push(line);
                        }
                        // Keep last 500 lines
                        if self.watch_events.len() > 500 {
                            self.watch_events.drain(0..self.watch_events.len() - 500);
                        }
                        // Auto-scroll to bottom
                        self.watch_scroll = self.watch_events.len().saturating_sub(self.visible_height.get());
                    }
                    WatchUpdate::Heartbeat => {
                        self.watch_events.push(format!("♥ Heartbeat at {}", chrono::Local::now().format("%H:%M:%S")));
                    }
                    WatchUpdate::Error(e) => {
                        self.watch_events.push(format!("⚠ Error: {}", e));
                        self.watch_active = false;
                    }
                    WatchUpdate::Disconnected => {
                        self.watch_events.push("Connection closed".to_string());
                        self.watch_active = false;
                    }
                }
            }
        }
    }

    pub fn get_aggregate_menu_items(&self) -> Vec<(&str, &str)> {
        vec![
            ("Refresh Info", "Check aggregate exists and get info"),
            ("Read Events", "Read event batches from aggregate"),
            ("Write Event", "Write a new event to aggregate"),
            ("Watch", "Watch for real-time events"),
            ("Trim Start", "Remove old events from start"),
            ("Register Schema", "Register an event schema for validation"),
            ("Delete", "Delete the entire aggregate"),
            ("Back", "Return to previous screen"),
        ]
    }

    pub fn setup_org_watch_fields(&mut self) {
        self.input_fields = vec![
            InputField::with_value("Organisation ID", &self.org_watch_org_id),
            InputField::with_value("Aggregate Types (comma-separated, optional)", &self.org_watch_aggregate_types),
            InputField::with_value("Event Types (0-5, comma-separated)", &self.org_watch_event_types),
            InputField::with_value("Latency (ms)", &self.org_watch_latency_ms),
        ];
        self.input_field_index = 0;
        self.watch_events.clear();
        self.watch_scroll = 0;
    }

    pub async fn start_org_watch(&mut self) -> Result<(), String> {
        if self.watch_active {
            return Err("Watch already active".to_string());
        }

        let pool = self.pool.as_ref().ok_or("Not connected")?;

        let org_id: Option<u128> = {
            let s = self.org_watch_org_id.trim();
            if s.is_empty() { None } else { Some(s.parse().map_err(|_| "Invalid Organisation ID")?) }
        };

        let aggregate_types: Option<HashSet<u128>> = {
            if self.org_watch_aggregate_types.trim().is_empty() {
                None
            } else {
                let types: HashSet<u128> = self.org_watch_aggregate_types
                    .split(',')
                    .filter_map(|s| s.trim().parse::<u128>().ok())
                    .collect();
                if types.is_empty() { None } else { Some(types) }
            }
        };

        let event_types: Option<HashSet<u8>> = {
            let types: HashSet<u8> = self.org_watch_event_types
                .split(',')
                .filter_map(|s| s.trim().parse::<u8>().ok())
                .filter(|&t| t <= 5)
                .collect();
            if types.is_empty() { None } else { Some(types) }
        };

        let latency_ms: Option<u64> = if self.org_watch_latency_ms.is_empty() {
            None
        } else {
            Some(self.org_watch_latency_ms.parse().map_err(|_| "Invalid latency")?)
        };

        let request = WatchRequest {
            correlation_id: None,
            requested_latency_ms: latency_ms,
            shard_id: None,
            orgs: org_id.map(|id| HashSet::from([id])),
            aggregate_types: aggregate_types.clone(),
            aggregates: None,
            operation_types: event_types.clone(),
        };

        let conn = pool
            .watch(request, WatchOptions::default())
            .await
            .map_err(|e| format!("Watch connection failed: {}", e))?;

        let mut init_lines = vec![
            "Starting organisation watch...".to_string(),
            format!("Organisation: {:?}", org_id),
        ];
        match aggregate_types {
            Some(ref types) => init_lines.push(format!("Aggregate types: {:?}", types.iter().collect::<Vec<_>>())),
            None => init_lines.push("Aggregate types: all".to_string()),
        }
        init_lines.push(format!("Event types: {:?}", event_types));
        init_lines.push(String::new());

        self.launch_watch_task(conn, &mut init_lines);
        self.set_status("Organisation watch started");
        Ok(())
    }

    /// Set up channels, populate initial watch_events lines, and spawn the background task.
    fn launch_watch_task(
        &mut self,
        conn: celeriant_client_tokio::WatchConnection,
        init_lines: &mut Vec<String>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();

        self.watch_receiver = Some(rx);
        self.watch_cancel = Some(cancel_tx);
        self.watch_active = true;
        self.watch_events.clear();
        self.watch_events.append(init_lines);

        tokio::spawn(async move {
            watch_task(conn, tx, cancel_rx).await;
        });
    }

}

fn event_type_name(event_type: u8) -> &'static str {
    match event_type {
        0 => "DELETE",
        1 => "WRITE",
        2 => "READ",
        3 => "TRIM_START",
        4 => "DETAILS",
        5 => "CREATE",
        _ => "UNKNOWN",
    }
}

fn format_watch_events(
    watch_response: celeriant_msg::response::responses::WatchResponse,
) -> Option<Vec<String>> {
    if watch_response.events.is_empty() {
        return None;
    }
    let mut lines = Vec::new();

    for event in &watch_response.events {
        lines.push(format!(
            "━━━ Org: {} | Type: {} | Agg: {} ━━━",
            format_u128_uuid(event.org_id),
            format_u128_uuid(event.aggregate_type_id),
            format_u128_uuid(event.aggregate_id)
        ));

        lines.push(format!(
            "  Event: {} ({})",
            event_type_name(event.operation),
            event.operation
        ));

        if let Some(from) = event.from_event_batch_index {
            lines.push(format!("    From batch: {}", from));
        }
        if let Some(to) = event.to_event_batch_index {
            lines.push(format!("    To batch: {}", to));
        }
        if let Some(keep_from) = event.keep_from_event_batch_index {
            lines.push(format!("    Keep from batch: {}", keep_from));
        }
        lines.push(String::new());
    }
    Some(lines)
}

async fn watch_task(
    mut conn: celeriant_client_tokio::WatchConnection,
    tx: tokio::sync::mpsc::UnboundedSender<WatchUpdate>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) {
    let _ = tx.send(WatchUpdate::Event(vec!["Watch started".to_string()]));

    loop {
        tokio::select! {
            _ = &mut cancel_rx => {
                let _ = tx.send(WatchUpdate::Event(vec!["Watch cancelled by user".to_string()]));
                return;
            }
            result = conn.next() => {
                match result {
                    Ok(response) => match format_watch_events(response) {
                        Some(lines) => { let _ = tx.send(WatchUpdate::Event(lines)); }
                        None => { let _ = tx.send(WatchUpdate::Heartbeat); }
                    },
                    Err(e) => {
                        let _ = tx.send(WatchUpdate::Error(format!("Watch error: {}", e)));
                        break;
                    }
                }
            }
        }
    }

    let _ = tx.send(WatchUpdate::Disconnected);
}
