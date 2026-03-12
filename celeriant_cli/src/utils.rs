use base64::Engine;
use celeriant_msg::process_client_responses::ClientResponse;
use chrono::{TimeZone, Utc};
use uuid::Uuid;

pub fn extract_host(address: &str) -> &str {
    address.split(':').next().unwrap_or(address)
}

pub fn parse_u128(s: &str) -> Result<u128, String> {
    if s.contains('-') {
        Uuid::parse_str(s)
            .map(|u| u.as_u128())
            .map_err(|e| format!("invalid UUID: {e}"))
    } else if s.chars().all(|c| c.is_ascii_digit()) {
        s.parse::<u128>().map_err(|e| format!("invalid number: {e}"))
    } else {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| format!("invalid base64: {e}"))?;
        if bytes.len() != 16 {
            return Err(format!("base64 must decode to 16 bytes, got {}", bytes.len()));
        }
        Ok(u128::from_be_bytes(bytes.try_into().unwrap()))
    }
}

pub fn format_u128_uuid(val: u128) -> String {
    Uuid::from_u128(val).to_string()
}

pub fn format_timestamp(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        return "N/A".to_string();
    }

    let secs = (timestamp_ms / 1000) as i64;
    let nsecs = ((timestamp_ms % 1000) * 1_000_000) as u32;

    match Utc.timestamp_opt(secs, nsecs) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => format!("{}ms", timestamp_ms),
    }
}

pub fn format_response(response: &ClientResponse) -> String {
    match response {
        ClientResponse::AggregateDetails(r) => format!("Exists: batches from {}",
            r.min_event_batch_index),
        ClientResponse::Read(r) => format!("Read: {} batches", r.event_batches.len()),
        ClientResponse::Write(_) => "Write: success".to_string(),
        ClientResponse::TrimStart(_) => "TrimStart: success".to_string(),
        ClientResponse::Delete(_) => "Delete: success".to_string(),
        ClientResponse::ProtocolError(_) => "ProtocolError".to_string(),
        ClientResponse::GenericError(r) => format!("Error {}: {}", r.error_code, r.error_message),
        ClientResponse::Watch(watch_response) => format!("Watch: {} events",
            watch_response.events.len()),
        ClientResponse::ListOrgs(r) => format!("ListOrgs: {} orgs", r.orgs.len()),
        ClientResponse::ListAggregateTypes(r) => format!("ListAggregateTypes: {} types", r.aggregate_types.len()),
        ClientResponse::ListAggregates(r) => format!("ListAggregates: {} aggregates", r.aggregates.len()),
        ClientResponse::RegisterSchema(_) => "RegisterSchema: success".to_string(),
    }
}