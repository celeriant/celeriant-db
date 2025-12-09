use std::{path::Path, sync::Arc};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_crypto::Crypto;
use celeriant_msg::{
    process_requests::Request,
    process_responses::Response,
    request::{
        directory_filters::DirectoryFilters,
        read_filters::ReadFilters,
        requests::*,
    },
    response::{aggregate_info::AggregateInfo, organisation_info::OrganisationInfo},
};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    compression_type::CompressionType,
    wal::event_item::EventItem,
};
use directories::ProjectDirs;

#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Home,
    Connect,
    Organisations,
    Aggregates,
    AggregateContext,
    EnterAggregate,
    ReadEvents,
    WriteEvent,
    TrimStart,  // Add this
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
    pub max_batch: u64,
}

pub struct App {
    pub server_address: String,
    pub client: Option<CeleriantClient>,
    pub screen: Screen,
    pub previous_screen: Option<Screen>,
    pub input_mode: InputMode,
    pub should_quit: bool,
    
    // Status and messages
    pub status_message: String,
    pub last_error: Option<String>,
    
    // List states
    pub organisations: Vec<OrganisationInfo>,
    pub org_list_index: usize,
    pub aggregates: Vec<AggregateInfo>,
    pub agg_list_index: usize,
    
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
            client: None,
            screen: Screen::Home,
            previous_screen: None,
            input_mode: InputMode::Normal,
            should_quit: false,
            status_message: format!("Welcome to Celeriant CLI. Server: {}", server_address),
            last_error: None,
            organisations: Vec::new(),
            org_list_index: 0,
            aggregates: Vec::new(),
            agg_list_index: 0,
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
        self.client.is_some()
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
        self.previous_screen = Some(self.screen.clone());
        self.screen = screen;
        self.menu_index = 0;
        self.input_mode = InputMode::Normal;
    }
    
    pub fn go_back(&mut self) {
        if let Some(prev) = self.previous_screen.take() {
            self.screen = prev;
            self.menu_index = 0;
        } else {
            self.screen = Screen::Home;
        }
        self.input_mode = InputMode::Normal;
    }
    
    pub async fn connect(&mut self) -> Result<(), String> {
        self.set_status(&format!("Connecting to {}...", self.server_address));
        
        match CeleriantClient::connect(&self.server_address).await {
            Ok(client) => {
                self.client = Some(client);
                self.set_status(&format!("Connected to {}", self.server_address));
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
        self.client = None;
        self.organisations.clear();
        self.aggregates.clear();
        self.selected_org = None;
        self.aggregate_context = None;
        self.set_status("Disconnected");
    }
    
    pub async fn load_organisations(&mut self) -> Result<(), String> {
        let client = self.client.as_mut().ok_or("Not connected")?;
        
        let request = Request::ListOrganisations(ListOrganisationsRequest {
            correlation_id: None,
            filters: DirectoryFilters::default(),
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::ListOrganisations(res)) => {
                self.organisations = res.organisations;
                self.org_list_index = 0;
                self.set_status(&format!("Loaded {} organisations", self.organisations.len()));
                Ok(())
            }
            Ok(Response::GenericError(e)) => {
                Err(format!("Error {}: {}", e.error_code, e.error_message))
            }
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
    
    pub async fn load_aggregates(&mut self) -> Result<(), String> {
        let client = self.client.as_mut().ok_or("Not connected")?;
        let org_id = self.selected_org.ok_or("No organisation selected")?;
        
        let request = Request::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            org_id,
            aggregate_type_id: None,
            filters: DirectoryFilters::default(),
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::ListAggregates(res)) => {
                self.aggregates = res.aggregates;
                self.agg_list_index = 0;
                self.set_status(&format!("Loaded {} aggregates", self.aggregates.len()));
                Ok(())
            }
            Ok(Response::GenericError(e)) => {
                Err(format!("Error {}: {}", e.error_code, e.error_message))
            }
            Ok(_) => Err("Unexpected response".to_string()),
            Err(e) => Err(format!("Request failed: {}", e)),
        }
    }
    
    pub async fn check_aggregate_exists(&mut self) -> Result<(), String> {
        let client = self.client.as_mut().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_mut().ok_or("No aggregate selected")?;
        
        let key = AggregateKey::new(ctx.org_id, ctx.aggregate_type_id, ctx.aggregate_id);
        let request = Request::Exists(ExistsRequest {
            correlation_id: None,
            aggregate_key: key,
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::Exists(res)) => {
                ctx.info = Some(AggregateContextInfo {
                    min_batch: res.min_event_batch_index,
                    max_batch: res.max_event_batch_index,
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
        let client = self.client.as_mut().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
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
                    
                    for (i, event) in batch.events.iter().enumerate() {
                        let data_str = String::from_utf8_lossy(&event.event_value);
                        self.result_output.push(format!(
                            "  [{}] Type: {} | Index: {} | Time: {}",
                            i, event.event_type_major, event.client_event_index, event.event_timestamp
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
        let client = self.client.as_mut().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
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
        
        let event = EventItem {
            event_type_major: event_type,
            event_type_minor: 0,
            client_event_index: 0,
            event_timestamp: chrono::Utc::now().timestamp_millis() as u64,
            event_value: Arc::new(data),
            event_index: 0,
            event_id: None,
            iv: None,
        };
        
        let request = Request::Write(WriteRequest {
            correlation_id: None,
            aggregate_key: key,
            client_id: self.client_id,  // Use app's client_id
            user_id: None,
            events: vec![event],
            allow_create: true,
            expected_event_batch_index: None,
            enforce_client_idempotency: false,
            durable_write_with_delay_us: None,
            compression_type: CompressionType::None,
        });
        
        match client.send_request(&request, CompressionType::None).await {
            Ok(Response::Write(res)) => {
                self.result_output.clear();
                self.result_output.push("Write successful!".to_string());
                self.result_output.push(format!("Batch index: {}", res.event_batch_index));
                self.result_output.push(format!("Start event index: {}", res.start_event_index));
                self.result_output.push(format!("Server timestamp: {}", crate::utils::format_timestamp(res.server_timestamp)));
                self.result_output.push(format!("Compressed size: {}", humansize::format_size(res.compressed_size, humansize::BINARY)));
                self.result_output.push(format!("Node ID: {}", res.node_id));
                self.result_output.push(format!("CRC: 0x{:08X}", res.events_crc));
                
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
        let client = self.client.as_mut().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
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
        let client = self.client.as_mut().ok_or("Not connected")?;
        let ctx = self.aggregate_context.as_ref().ok_or("No aggregate selected")?;
        
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
                ("Browse Organisations", "View and navigate organisations"),
                ("Enter Aggregate", "Go directly to an aggregate by ID"),
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
    
    pub fn get_aggregate_menu_items(&self) -> Vec<(&str, &str)> {
        vec![
            ("Refresh Info", "Check aggregate exists and get info"),
            ("Read Events", "Read event batches from aggregate"),
            ("Write Event", "Write a new event to aggregate"),
            ("Trim Start", "Remove old events from start"),
            ("Delete", "Delete the entire aggregate"),
            ("Back", "Return to previous screen"),
        ]
    }
}