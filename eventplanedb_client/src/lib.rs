use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::form_urlencoded;

use std::time;
use tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuthData {
    pub public_key: String,
    pub nonce: String,
    pub sign: String,
    pub bearer_token: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServerEvent {
    pub ed: i64, // date event added
    pub tp: u64, // Event type
    pub iv: Option<Vec<Option<String>>>,
    pub vi: Option<Vec<i64>>,            // Array of i64 values
    pub vu: Option<Vec<u64>>,            // Array of u64 values
    pub vf: Option<Vec<f32>>,            // Array of f32 values
    pub vd: Option<Vec<f64>>,            // Array of f64 values
    pub vb: Option<Vec<bool>>,           // Array of boolean values
    pub sv: Option<Vec<Option<String>>>, // Array of optional strings
    pub by: Option<Vec<Option<String>>>, // Array of optional base64 encoded byte arrays
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServerEventBatch {
    pub si: i64,            // Incremented id from server as a long
    pub sd: i64,            // Server event batch time UTC
    pub ci: String,         // Which client created this event batch
    pub ui: Option<String>, // User who created this event batch
    pub ev: Vec<ServerEvent>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum AccessLevel {
    Owner = 0,
    Contributor = 1,
    Viewer = 2,
    None = 3,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ShareResponse {
    pub share_key: String,
    pub share_event: ServerEventBatch,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DisableAccessResponse {
    pub event_batches: Vec<ServerEventBatch>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CatchupResult {
    pub event_batches: Vec<ServerEventBatch>,
    pub next_server_id: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WriteResponse {
    pub server_id: i64,
    pub event_batches: Vec<ServerEventBatch>,
}

#[derive(Debug, Clone)]
pub struct ErrorForbidden {
    pub message: String,
}

impl ErrorForbidden {
    pub fn new(message: String) -> Self {
        ErrorForbidden { message }
    }
}

impl std::fmt::Display for ErrorForbidden {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ErrorForbidden: {}", self.message)
    }
}

impl std::error::Error for ErrorForbidden {}

#[derive(Debug, Clone)]
pub struct ErrorNotExist {
    pub message: String,
}

impl ErrorNotExist {
    pub fn new(message: String) -> Self {
        ErrorNotExist { message }
    }
}

impl std::fmt::Display for ErrorNotExist {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ErrorNotExist: {}", self.message)
    }
}

impl std::error::Error for ErrorNotExist {}

fn build_headers(auth_data: &AuthData) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/json"));
    headers.insert("X-Public-Key", reqwest::header::HeaderValue::from_str(&auth_data.public_key).unwrap());
    headers.insert("X-Nonce", reqwest::header::HeaderValue::from_str(&auth_data.nonce).unwrap());
    headers.insert("X-Signature", reqwest::header::HeaderValue::from_str(&auth_data.sign).unwrap());

    if let Some(bearer_token) = &auth_data.bearer_token {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", bearer_token)).unwrap(),
        );
    }

    headers
}

async fn get_fetch_options(auth_data: &AuthData) -> Result<reqwest::Client, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder().default_headers(build_headers(auth_data)).build()?;
    Ok(client)
}

async fn post_fetch_options(auth_data: &AuthData, body: Option<String>) -> Result<(reqwest::Client, Option<String>), Box<dyn std::error::Error>> {
    sleep(time::Duration::from_millis(50)).await;
    let client = reqwest::Client::builder().default_headers(build_headers(auth_data)).build()?;
    Ok((client, body))
}

/**
 * Generate a short client identity from a public key
 * @param publicKey Public key bytes to hash
 * @returns Base64url encoded short identity (first 16 bytes of SHA256 hash)
 */
fn url_friendly_base64(input: &str) -> String {
    let hash = Sha256::digest(input.as_bytes());
    let base64_encoded = base64::encode(hash[0..16].to_vec());

    // Base64url encode (URL-safe, no padding)
    base64_encoded.replace("+", "-").replace("/", "_").replace("=", "")
}

pub struct EventPlaneDBClient {}

impl EventPlaneDBClient {
    /**
     * Creates a new aggregate or adds events to an existing aggregate
     */
    pub async fn write_events(
        base_url: &str,
        auth_data: &AuthData,
        aggregate_id: &str,
        create_if_not_exist: bool,
        events_not_on_server: Vec<ServerEvent>,
    ) -> Result<WriteResponse, Box<dyn std::error::Error>> {
        let mut url_params = Vec::new();
        if create_if_not_exist {
            url_params.push(("create_if_not_exist", create_if_not_exist.to_string()));
        }

        let url_params_str = form_urlencoded::Serializer::new(String::new()).extend_pairs(url_params).finish();

        let url = format!("{}/api/v1/aggregate/{}/write?{}", base_url, aggregate_id, url_params_str);

        // Function to send a batch of events
        async fn send_batch(url: &str, auth_data: &AuthData, events: Vec<ServerEvent>) -> Result<Option<WriteResponse>, Box<dyn std::error::Error>> {
            let (client, body) = post_fetch_options(auth_data, Some(serde_json::to_string(&events)?)).await?;

            let response = client.post(url).body(body.unwrap()).send().await?;

            if response.status() == reqwest::StatusCode::FORBIDDEN {
                return Err(Box::new(ErrorForbidden::new("Forbidden".to_string())));
            }
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(Box::new(ErrorNotExist::new("Not Found".to_string())));
            }
            if response.status() == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
                return Ok(None);
            }

            let result: WriteResponse = response.json().await?;
            Ok(Some(result))
        }

        // Recursive function to handle batch splitting
        async fn process_batch(url: &str, auth_data: &AuthData, events: Vec<ServerEvent>) -> Result<Vec<WriteResponse>, Box<dyn std::error::Error>> {
            if events.is_empty() {
                return Ok(vec![]);
            }

            // Box the recursive async function to allow dynamic size
            Box::pin(async move {
                // Try sending the current batch
                let result = send_batch(url, auth_data, events.clone()).await?;

                // If successful, return the result
                if let Some(result) = result {
                    return Ok(vec![result]);
                }

                // If we got a 413, split the batch in half and try again
                if events.len() == 1 {
                    // Can't split further, this event is just too large
                    return Err("Event too large to send, even as a single item".into());
                }

                let midpoint = events.len() / 2;
                let first_half = events[0..midpoint].to_vec();
                let second_half = events[midpoint..].to_vec();

                // Process each half and combine results
                let first_results = process_batch(url, auth_data, first_half).await?;
                let second_results = process_batch(url, auth_data, second_half).await?;

                let mut combined_results = first_results;
                combined_results.extend(second_results);
                Ok(combined_results)
            })
            .await
        }

        // Start the recursive process
        let results = process_batch(&url, auth_data, events_not_on_server).await?;

        // Combine all results (if multiple batches were sent)
        if results.len() == 1 {
            return Ok(results[0].clone());
        } else {
            // Merge multiple WriteResponse objects
            let combined = results.into_iter().reduce(|combined, current| WriteResponse {
                server_id: current.server_id, // Keep the server_id from the latest response
                event_batches: {
                    let mut evs = combined.event_batches;
                    evs.extend(current.event_batches);
                    evs
                }, // Concatenate all event_batches arrays
            });

            match combined {
                Some(c) => Ok(c),
                None => Ok(WriteResponse {
                    server_id: 0,
                    event_batches: vec![],
                }),
            }
        }
    }

    /**
     * Reads events from an aggregate
     */
    pub async fn read_events(
        base_url: &str,
        auth_data: &AuthData,
        aggregate_id: &str,
        from_server_id: i64,
        share_id_hash: Option<String>,
        include_own_events: bool,
    ) -> Result<Vec<ServerEventBatch>, Box<dyn std::error::Error>> {
        let mut all_event_batches: Vec<ServerEventBatch> = Vec::new();
        let mut current_from_server_id = from_server_id;

        loop {
            let mut url_params = Vec::new();
            url_params.push(("from_server_id", current_from_server_id.to_string()));
            if let Some(share_id_hash) = &share_id_hash {
                url_params.push(("share_id", url_friendly_base64(share_id_hash)));
            }
            if include_own_events {
                url_params.push(("own_events", include_own_events.to_string()));
            }

            let url_params_str = form_urlencoded::Serializer::new(String::new()).extend_pairs(url_params).finish();

            let url = format!("{}/api/v1/aggregate/{}/read?{}", base_url, aggregate_id, url_params_str);

            let client = get_fetch_options(auth_data).await?;
            let response = client.get(&url).send().await?;

            if response.status() == reqwest::StatusCode::FORBIDDEN {
                return Err(Box::new(ErrorForbidden::new("Forbidden".to_string())));
            }
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(Box::new(ErrorNotExist::new("Not Found".to_string())));
            }

            let events_from_server: CatchupResult = response.json().await?;
            all_event_batches.extend(events_from_server.event_batches);

            match events_from_server.next_server_id {
                Some(next_server_id) => current_from_server_id = next_server_id,
                None => break,
            }
        }

        Ok(all_event_batches)
    }

    /**
     * Creates a share link for an aggregate
     */
    pub async fn share(
        base_url: &str,
        auth_data: &AuthData,
        aggregate_id: &str,
        access_level: AccessLevel,
        single_use: bool,
        iv: Option<String>,
        description: Option<String>,
        expires_on: i64,
    ) -> Result<ShareResponse, Box<dyn std::error::Error>> {
        let share_body = serde_json::json!({
            "access_level": access_level as i32,
            "is_single_use": single_use,
            "expires_on": expires_on,
            "description": description,
            "iv": iv,
        });

        let url = format!("{}/api/v1/aggregate/{}/share", base_url, aggregate_id);
        let (client, body) = post_fetch_options(auth_data, Some(share_body.to_string())).await?;
        let response = client.post(&url).body(body.unwrap()).send().await?;

        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(Box::new(ErrorForbidden::new("Forbidden".to_string())));
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Box::new(ErrorNotExist::new("Not Found".to_string())));
        }

        let result: ShareResponse = response.json().await?;
        Ok(result)
    }

    /**
     * Disables a share link
     */
    pub async fn disable_share(
        base_url: &str,
        auth_data: &AuthData,
        aggregate_id: &str,
        share_id_hash: &str,
    ) -> Result<DisableAccessResponse, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/api/v1/aggregate/{}/disableshare/{}",
            base_url,
            aggregate_id,
            url_friendly_base64(share_id_hash)
        );
        let (client, _body) = post_fetch_options(auth_data, None).await?;
        let response = client.post(&url).send().await?;

        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(Box::new(ErrorForbidden::new("Forbidden".to_string())));
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Box::new(ErrorNotExist::new("Not Found".to_string())));
        }

        let result: DisableAccessResponse = response.json().await?;
        Ok(result)
    }

    /**
     * Disables a user's access to an aggregate
     */
    pub async fn disable_client(
        base_url: &str,
        auth_data: &AuthData,
        aggregate_id: &str,
        client_id_hash: &str,
    ) -> Result<DisableAccessResponse, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/api/v1/aggregate/{}/disableclient/{}",
            base_url,
            aggregate_id,
            url_friendly_base64(client_id_hash)
        );
        let (client, _body) = post_fetch_options(auth_data, None).await?;
        let response = client.post(&url).send().await?;

        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(Box::new(ErrorForbidden::new("Forbidden".to_string())));
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Box::new(ErrorNotExist::new("Not Found".to_string())));
        }

        let result: DisableAccessResponse = response.json().await?;
        Ok(result)
    }

    /**
     * Disables a user's access to an aggregate
     */
    pub async fn disable_user(
        base_url: &str,
        auth_data: &AuthData,
        aggregate_id: &str,
        user_id_hash: &str,
    ) -> Result<DisableAccessResponse, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/api/v1/aggregate/{}/disableuser/{}",
            base_url,
            aggregate_id,
            url_friendly_base64(user_id_hash)
        );
        let (client, _body) = post_fetch_options(auth_data, None).await?;
        let response = client.post(&url).send().await?;

        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(Box::new(ErrorForbidden::new("Forbidden".to_string())));
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Box::new(ErrorNotExist::new("Not Found".to_string())));
        }

        let result: DisableAccessResponse = response.json().await?;
        Ok(result)
    }

    /**
     * Deletes an aggregate
     */
    pub async fn delete_project(base_url: &str, auth_data: &AuthData, aggregate_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/api/v1/aggregate/{}/delete", base_url, aggregate_id);
        let (client, _body) = post_fetch_options(auth_data, None).await?;
        let response = client.post(&url).send().await?;

        if response.status() == reqwest::StatusCode::FORBIDDEN {
            return Err(Box::new(ErrorForbidden::new("Forbidden".to_string())));
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(Box::new(ErrorNotExist::new("Not Found".to_string())));
        }

        Ok(())
    }

    /**
     * Creates a server-sent events connection for realtime updates from an aggregate
     * @param baseUrl Base URL of the EventPlaneDB server
     * @param authData Authentication data
     * @param aggregateId The ID of the aggregate to subscribe to
     * @param messageHandler Callback function that will be called when new events are received
     * @returns EventSource object that can be used to manage the connection
     */
    pub async fn create_realtime_connection(
        base_url: &str,
        auth_data: &AuthData,
        aggregate_id: &str,
        message_handler: impl Fn(String) -> (),
        error_handler: Option<impl Fn(reqwest::Error) -> ()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let endpoint_url = format!("{}/api/v1/aggregate/{}/subscribe", base_url, aggregate_id);

        let mut query_params = Vec::new();
        query_params.push(("public_key", auth_data.public_key.clone()));
        query_params.push(("nonce", auth_data.nonce.clone()));
        query_params.push(("signature", auth_data.sign.clone()));

        if let Some(bearer_token) = &auth_data.bearer_token {
            query_params.push(("token", bearer_token.clone()));
        }

        let query_params_str = form_urlencoded::Serializer::new(String::new()).extend_pairs(query_params).finish();

        let url = format!("{}?{}", endpoint_url, query_params_str);

        let client = reqwest::Client::new();
        let mut response = client.get(&url).send().await?;

        if response.status() != reqwest::StatusCode::OK {
            return Err(format!("Failed to connect: {}", response.status()).into());
        }

        while let Some(chunk) = response.chunk().await? {
            match String::from_utf8(chunk.to_vec()) {
                Ok(message) => {
                    message_handler(message);
                }
                Err(e) => {
                    eprintln!("Error decoding message: {}", e);
                }
            }
        }

        Ok(())
    }

    /**
     * Closes a realtime connection
     * @param eventSource The EventSource connection to close
     */
    pub fn close_realtime_connection() {
        //No op - implemented in browser.
    }
}
