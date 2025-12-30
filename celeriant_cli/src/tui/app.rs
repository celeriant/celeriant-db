use std::{collections::{HashMap, HashSet}, path::Path, sync::Arc};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_crypto::Crypto;
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::{
        read_filters::ReadFilters,
        requests::*,
    },
};
use celeriant_wal::{
    aggregate_key::AggregateKey, compression_type::CompressionType, datablocks::datablock_aggregate_event::DatablockAggregateEvent
};
use directories::ProjectDirs;

use crate::utils::format_timestamp;

#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Home,
    Connect,
    AggregateContext,
    EnterAggregate,
    ReadEvents,
    WriteEvent,
    TrimStart,
    Watch, 
    OrgWatch,
    Help,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InputMode {
    Normal,
    Editing,
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
}

pub struct App {
    pub server_address: String,
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
    pub _placeholder: String,
}

impl InputField {
    pub fn new(label: &str, placeholder: &str) -> Self {
        Self {
            label: label.to_string(),
            value: String::new(),
            _placeholder: placeholder.to_string(),
        }
    }
    
    pub fn with_value(label: &str, value: &str) -> Self {
        Self {
            label: label.to_string(),
            value: value.to_string(),
            _placeholder: String::new(),
        }
    }
}

impl App {
    pub fn new(server_address: String) -> Self {
        // Get OS-appropriate data directory
        let data_root = ProjectDirs::from("com", "celeriant", "celeriant_cli")
            .map(|dirs| dirs.data_dir().to_path_buf())
            .unwrap_or_else(|| {
                eprintln!("Warning: Could not determine user data directory, using fallback");
                std::path::PathBuf::from(".celeriant_cli")
            });

        // Load or generate persistent client ID from keypair
        let client_id = match Crypto::load_or_generate_node_id(&data_root) {
            Ok(id) => {
                eprintln!("Client ID: {} (keys stored in: {})", id, data_root.display());
                id
            }
            Err(e) => {
                eprintln!("Failed to initialize client ID: {}", e);
                std::process::exit(1);
            }
        };

        Self {
            server_address: server_address.clone(),
            is_active: false,
            screen: Screen::Home,
            previous_screen: None,
            input_mode: InputMode::Normal,
            should_quit: false,
            status_message: format!("Welcome to Celeriant CLI. Server: {}", server_address),
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
        }
    }
    
    pub fn setup_enter_aggregate_fields(&mut self) {
        self.input_fields = vec![
            InputField::new("Organisation ID", "1"),
            InputField::new("Aggregate Type ID", "1"),
            InputField::new("Aggregate ID", "1"),
        ];
        self.input_field_index = 0;
    }
    
    pub async fn navigate_to_aggregate_from_input(&mut self) -> Result<(), String> {
        if self.input_fields.len() < 3 {
            return Err("Invalid input fields".to_string());
        }
        
        let org_id: u128 = self.input_fields[0].value.parse()
            .map_err(|_| "Invalid Organisation ID")?;
        let aggregate_type_id: u128 = self.input_fields[1].value.parse()
            .map_err(|_| "Invalid Aggregate Type ID")?;
        let aggregate_id: u128 = self.input_fields[2].value.parse()
            .map_err(|_| "Invalid Aggregate ID")?;
        
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
    
    pub fn go_to_screen(&mut self, screen: Screen) {
        // Stop watch if leaving watch screen
        if self.screen == Screen::Watch && self.watch_active {
            self.stop_watch();
        }
        
        self.previous_screen = Some(self.screen.clone());
        self.screen = screen;
        self.menu_index = 0;
        self.input_mode = InputMode::Normal;
    }
    
    pub fn go_back(&mut self) {
        // Stop watch if active when navigating away
        if self.screen == Screen::Watch && self.watch_active {
            self.stop_watch();
        }
        
        if let Some(prev) = self.previous_screen.take() {
            self.screen = prev;
            self.menu_index = 0;
        } else {
            self.screen = Screen::Home;
        }
        self.input_mode = InputMode::Normal;
    }
    
    pub async fn connect(&mut self) -> Result<(), String> {
        self.set_status(&format!("Testing connection to {}...", self.server_address));
        
        // Just test connectivity - connection will be dropped after this
        match CeleriantClient::connect(&self.server_address).await {
            Ok(_client) => {
                // Connection successful, client drops here
                self.is_active = true;
                self.set_status(&format!("Ready to use {}", self.server_address));
                Ok(())
            }
            Err(e) => {
                let msg = format!("Connection failed: {}", e);
                self.set_error(&msg);
                Err(msg)
            }
        }
    }
    
    pub async fn disconnect(&mut self) {
        self.is_active = false;
        self.selected_org = None;
        self.aggregate_context = None;
        self.set_status("Disconnected");
    }
    
    pub async fn check_aggregate_exists(&mut self) -> Result<(), String> {
        let ctx = self.aggregate_context.as_mut().ok_or("No aggregate selected")?;
        
        let mut client = CeleriantClient::connect(&self.server_address)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;
        
        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let request = Request::Exists(ExistsRequest {
            correlation_id: None,
            aggregate_key: key,
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::Exists(res)) => {
                ctx.info = Some(AggregateContextInfo {
                    min_batch: res.min_event_batch_index,
                });
                self.set_status("Aggregate info loaded");
                Ok(())
            }
            Ok(Response::GenericError(e)) => {
                ctx.info = None;
                Err(format!("Error {}: {}", e.error_code, e.error_message))
            }
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
    
    pub async fn read_events(&mut self) -> Result<(), String> {
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
        let mut client = CeleriantClient::connect(&self.server_address)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;
        
        let from: u64 = self.read_from_index.parse().unwrap_or(1);
        let to: Option<u64> = if self.read_to_index.is_empty() {
            None
        } else {
            self.read_to_index.parse().ok()
        };
        
        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let mut filters = ReadFilters::new(from);
        if let Some(to_idx) = to {
            filters = filters.to_event_batch_index(to_idx);
        }
        
        let request = Request::Read(ReadRequest {
            correlation_id: None,
            aggregate_key: key,
            filters,
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::Read(res)) => {
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
                    
                    for (_i, event) in batch.events.iter().enumerate() {
                        let data_str = String::from_utf8_lossy(&event.event_value);
                        self.result_output.push(format!(
                            "  [{}] Type: {} | Index: {} | Time: {}",
                            humansize::format_size(event.event_value.len(), humansize::BINARY), event.event_type_major, event.event_index, format_timestamp(event.event_timestamp)
                        ));
                        // Split data into lines for display
                        for line in data_str.lines().take(5) {
                            self.result_output.push(format!("      {}", line));
                        }
                        if data_str.lines().count() > 5 {
                            self.result_output.push("      ...".to_string());
                        }
                    }
                    self.result_output.push(String::new());
                }
                
                self.result_scroll = 0;
                self.set_status(&format!("Read {} batches", res.event_batches.len()));
                Ok(())
            }
            Ok(Response::GenericError(e)) => {
                Err(format!("Error {}: {}", e.error_code, e.error_message))
            }
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
        
    pub async fn write_event(&mut self) -> Result<(), String> {
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
        let mut client = CeleriantClient::connect(&self.server_address)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;
        
        let event_type: u64 = self.write_event_type.parse().map_err(|_| "Invalid event type")?;
        
        if self.write_data.is_empty() {
            return Err("Event data cannot be empty".to_string());
        }
        
        // Check if the input is a file path and read from file if it exists
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

        let mut writes = HashMap::new();
        writes.insert(key, SingleAggregateWrite {
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
            compression_type: CompressionType::None,
        });
        
        let request = Request::Write(WriteRequest {
            correlation_id: None,
            client_id: self.client_id,  // Use app's client_id
            user_id: None,
            writes,
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::Write(_res)) => {
                self.result_output.clear();
                self.result_output.push("Write successful!".to_string());
                
                // Don't clear write_data - allow multiple writes
                self.set_status("Event written successfully");
                Ok(())
            }
            Ok(Response::GenericError(e)) => {
                Err(format!("Error {}: {}", e.error_code, e.error_message))
            }
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
    
    pub async fn delete_aggregate(&mut self) -> Result<(), String> {
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
        let mut client = CeleriantClient::connect(&self.server_address)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;
        
        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let request = Request::Delete(DeleteRequest {
            correlation_id: None,
            aggregate_key: key,
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::Delete(_)) => {
                self.set_status("Aggregate deleted");
                self.aggregate_context = None;
                Ok(())
            }
            Ok(Response::GenericError(e)) => {
                Err(format!("Error {}: {}", e.error_code, e.error_message))
            }
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
    
    pub async fn trim_aggregate(&mut self) -> Result<(), String> {
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
        let mut client = CeleriantClient::connect(&self.server_address)
            .await
            .map_err(|e| format!("Connection failed: {}", e))?;
        
        let keep_from: u64 = self.trim_keep_from.parse()
            .map_err(|_| "Invalid batch index")?;
        
        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let request = Request::TrimStart(TrimStartRequest {
            correlation_id: None,
            aggregate_key: key,
            keep_from_event_batch_index: keep_from,
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::TrimStart(_)) => {
                self.set_status(&format!("Trimmed events before batch {}", keep_from));
                Ok(())
            }
            Ok(Response::GenericError(e)) => {
                Err(format!("Error {}: {}", e.error_code, e.error_message))
            }
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
    
    pub fn get_home_menu_items(&self) -> Vec<(&str, &str)> {
        if self.is_connected() {
            vec![
                ("Enter Aggregate", "Go directly to an aggregate by ID"),
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
        
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
        // Parse event types
        let event_types: Vec<u8> = self.watch_event_types
            .split(',')
            .filter_map(|s| s.trim().parse::<u8>().ok())
            .filter(|&t| t <= 5)
            .collect();
        
        if event_types.is_empty() {
            return Err("At least one valid event type required (0-5)".to_string());
        }
        
        let latency_ms: Option<u64> = if self.watch_latency_ms.is_empty() {
            None
        } else {
            Some(self.watch_latency_ms.parse().map_err(|_| "Invalid latency")?)
        };
        
        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let client_id = self.client_id;
        let server_address = self.server_address.clone();
        
        // Create channels
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        
        self.watch_receiver = Some(rx);
        self.watch_cancel = Some(cancel_tx);
        self.watch_active = true;
        self.watch_events.clear();
        self.watch_events.push(format!("Starting watch on aggregate..."));
        self.watch_events.push(format!("Event types: {:?}", event_types));
        self.watch_events.push(format!("Excluding client: {}", client_id));
        self.watch_events.push(String::new());
            
        let event_types: Option<std::collections::HashSet<u8>> = {
            let types: std::collections::HashSet<u8> = self.watch_event_types
                .split(',')
                .filter_map(|s| s.trim().parse::<u8>().ok())
                .filter(|&t| t <= 5)
                .collect();
            
            if types.is_empty() {
                None  // None means all event types
            } else {
                Some(types)
            }
        };
        
        // Spawn the watch task
        tokio::spawn(async move {
            watch_task(
                server_address,
                key,
                event_types,
                latency_ms,
                tx,
                cancel_rx,
            ).await;
        });
        
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
                        self.watch_scroll = self.watch_events.len().saturating_sub(15);
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
            ("Watch", "Watch for real-time events"),  // Add this
            ("Trim Start", "Remove old events from start"),
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
        
        // Parse org ID
        let org_id: Option<u128> = {
            let s = self.org_watch_org_id.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.parse().map_err(|_| "Invalid Organisation ID")?)
            }
        };
        
        // Parse aggregate types (optional)
        let aggregate_types: Option<HashSet<u128>> = {
            if self.org_watch_aggregate_types.trim().is_empty() {
                None
            } else {
                let types: HashSet<u128> = self
                    .org_watch_aggregate_types
                    .split(',')
                    .filter_map(|s| s.trim().parse::<u128>().ok())
                    .collect();

                if types.is_empty() { None } else { Some(types) }
            }
        };
        
        // Parse event types
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
        
        let server_address = self.server_address.clone();
        
        // Create channels
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        
        self.watch_receiver = Some(rx);
        self.watch_cancel = Some(cancel_tx);
        self.watch_active = true;
        self.watch_events.clear();
        self.watch_events.push(format!("Starting organisation watch..."));
        self.watch_events.push(format!("Organisation: {:?}", org_id));
        if let Some(ref types) = aggregate_types {
            self.watch_events.push(format!("Aggregate types: {:?}", types.iter().map(|t| t).collect::<Vec<_>>()));
        } else {
            self.watch_events.push("Aggregate types: all".to_string());
        }
        self.watch_events.push(format!("Event types: {:?}", event_types));
        self.watch_events.push(String::new());
        
        let orgs = match org_id {
            Some(id) => {
                let mut set = HashSet::new();
                set.insert(id);
                Some(set)
            }
            None => None,
        };
        
        // Spawn the watch task
        tokio::spawn(async move {
            org_watch_task(
                server_address,
                orgs,
                aggregate_types,
                event_types,
                latency_ms,
                tx,
                cancel_rx,
            ).await;
        });
        
        self.set_status("Organisation watch started");
        Ok(())
    }

}

async fn watch_task(
    server_address: String,
    aggregate_key: AggregateKey,
    event_types: Option<std::collections::HashSet<u8>>,
    latency_ms: Option<u64>,
    tx: tokio::sync::mpsc::UnboundedSender<WatchUpdate>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) {
    use celeriant_client_tokio::celeriant_client::CeleriantClient;
    use celeriant_msg::process_requests::Request;
    use celeriant_msg::process_responses::Response;
    use celeriant_msg::request::requests::WatchRequest;
    use celeriant_wal::compression_type::CompressionType;
    
    // Connect to server
    let mut client = match CeleriantClient::connect(&server_address).await {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(WatchUpdate::Error(format!("Connection failed: {}", e)));
            return;
        }
    };
    
    let mut orgs = HashSet::new();
    orgs.insert(aggregate_key.org_id);
    let mut types = HashSet::new();
    types.insert(aggregate_key.aggregate_type_id);
    let mut aggregates = HashSet::new();
    aggregates.insert(aggregate_key.aggregate_id);
    
    let request = Request::Watch(WatchRequest {
        operation_types: event_types,
        correlation_id: None,
        requested_latency_ms: latency_ms,
        orgs: Some(orgs),
        aggregate_types: Some(types),
        aggregates: Some(aggregates),
    });
    
    let _ = tx.send(WatchUpdate::Event(vec!["Sending watch request...".to_string()]));
    
    // Send initial watch request - the server will keep the connection open
    // and send responses as events occur
    loop {
        tokio::select! {
            _ = &mut cancel_rx => {
                let _ = tx.send(WatchUpdate::Event(vec!["Watch cancelled by user".to_string()]));
                break;
            }
            result = client.send_request(&request, CompressionType::None) => {
                match result {
                    Ok(Response::Watch(watch_response)) => {
                        if watch_response.events.is_none() {
                            let _ = tx.send(WatchUpdate::Heartbeat);
                        } else if let Some(events_by_aggregate) = watch_response.events {
                            let mut lines = Vec::new();
                            
                            for (aggregate_key, events_by_type) in events_by_aggregate {
                                lines.push(format!("━━━ Aggregate: {}:{} ━━━", 
                                    aggregate_key.aggregate_type_id, aggregate_key.aggregate_id));
                                
                                for (event_type, maybe_event) in events_by_type {
                                    let event_type_name = match event_type {
                                        0 => "DELETE",
                                        1 => "WRITE",
                                        2 => "READ",
                                        3 => "TRIM_START",
                                        4 => "EXISTS",
                                        5 => "CREATE",
                                        _ => "UNKNOWN",
                                    };
                                    
                                    lines.push(format!("  Event: {} ({})", event_type_name, event_type));
                                    
                                    if let Some(event) = maybe_event {
                                        if let Some(from) = event.from_event_batch_index {
                                            lines.push(format!("    From batch: {}", from));
                                        }
                                        if let Some(to) = event.to_event_batch_index {
                                            lines.push(format!("    To batch: {}", to));
                                        }
                                        if let Some(keep_from) = event.keep_from_event_batch_index {
                                            lines.push(format!("    Keep from batch: {}", keep_from));
                                        }
                                    }
                                }
                                lines.push(String::new());
                            }
                            let _ = tx.send(WatchUpdate::Event(lines));
                        }
                    }
                    Ok(Response::GenericError(e)) => {
                        let _ = tx.send(WatchUpdate::Error(format!("{}: {}", e.error_code, e.error_message)));
                        break;
                    }
                    Ok(_) => {
                        let _ = tx.send(WatchUpdate::Error("Unexpected response type".to_string()));
                    }
                    Err(e) => {
                        let _ = tx.send(WatchUpdate::Error(format!("Request error: {}", e)));
                        break;
                    }
                }
            }
        }
    }
    
    let _ = tx.send(WatchUpdate::Disconnected);
}

async fn org_watch_task(
    server_address: String,
    orgs: Option<HashSet<u128>>,
    aggregate_types: Option<HashSet<u128>>,
    event_types: Option<HashSet<u8>>,
    latency_ms: Option<u64>,
    tx: tokio::sync::mpsc::UnboundedSender<WatchUpdate>,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
) {
    use celeriant_client_tokio::celeriant_client::CeleriantClient;
    use celeriant_msg::process_requests::Request;
    use celeriant_msg::process_responses::Response;
    use celeriant_msg::request::requests::WatchRequest;
    use celeriant_wal::compression_type::CompressionType;
    
    // Connect to server
    let mut client = match CeleriantClient::connect(&server_address).await {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(WatchUpdate::Error(format!("Connection failed: {}", e)));
            return;
        }
    };
    
    let request = Request::Watch(WatchRequest {
        operation_types: event_types,
        correlation_id: None,
        requested_latency_ms: latency_ms,
        orgs: orgs,
        aggregate_types: aggregate_types,
        aggregates: None,
    });
    
    let _ = tx.send(WatchUpdate::Event(vec!["Sending watch request...".to_string()]));
    
    loop {
        tokio::select! {
            _ = &mut cancel_rx => {
                let _ = tx.send(WatchUpdate::Event(vec!["Watch cancelled by user".to_string()]));
                break;
            }
            result = client.send_request(&request, CompressionType::None) => {
                match result {
                    Ok(Response::Watch(watch_response)) => {
                        if watch_response.events.is_none() {
                            let _ = tx.send(WatchUpdate::Heartbeat);
                        } else if let Some(events_by_aggregate) = watch_response.events {
                            let mut lines = Vec::new();
                            
                            for (aggregate_key, events_by_type) in events_by_aggregate {
                                lines.push(format!("━━━ Org: {} | Type: {} | Agg: {} ━━━", 
                                    aggregate_key.org_id,
                                    aggregate_key.aggregate_type_id, 
                                    aggregate_key.aggregate_id));
                                
                                for (event_type, maybe_event) in events_by_type {
                                    let event_type_name = match event_type {
                                        0 => "DELETE",
                                        1 => "WRITE",
                                        2 => "READ",
                                        3 => "TRIM_START",
                                        4 => "EXISTS",
                                        5 => "CREATE",
                                        _ => "UNKNOWN",
                                    };
                                    
                                    lines.push(format!("  Event: {} ({})", event_type_name, event_type));
                                    
                                    if let Some(event) = maybe_event {
                                        if let Some(from) = event.from_event_batch_index {
                                            lines.push(format!("    From batch: {}", from));
                                        }
                                        if let Some(to) = event.to_event_batch_index {
                                            lines.push(format!("    To batch: {}", to));
                                        }
                                        if let Some(keep_from) = event.keep_from_event_batch_index {
                                            lines.push(format!("    Keep from batch: {}", keep_from));
                                        }
                                    }
                                }
                                lines.push(String::new());
                            }
                            let _ = tx.send(WatchUpdate::Event(lines));
                        }
                    }
                    Ok(Response::GenericError(e)) => {
                        let _ = tx.send(WatchUpdate::Error(format!("{}: {}", e.error_code, e.error_message)));
                        break;
                    }
                    Ok(_) => {
                        let _ = tx.send(WatchUpdate::Error("Unexpected response type".to_string()));
                    }
                    Err(e) => {
                        let _ = tx.send(WatchUpdate::Error(format!("Request error: {}", e)));
                        break;
                    }
                }
            }
        }
    }
    
    let _ = tx.send(WatchUpdate::Disconnected);
}