use celeriant_msg::process_responses::Response;
use chrono::{TimeZone, Utc};

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

pub fn format_response(response: &Response) -> String {
    match response {
        Response::ListOrganisations(r) => format!("ListOrganisations: {} orgs", r.organisations.len()),
        Response::ListAggregates(r) => format!("ListAggregates: {} aggregates", r.aggregates.len()),
        Response::Exists(r) => format!("Exists: batches from {}", 
            r.min_event_batch_index),
        Response::Read(r) => format!("Read: {} batches", r.event_batches.len()),
        Response::Write(r) => format!("Write: batch {}", r.event_batch_index),
        Response::TrimStart(_) => "TrimStart: success".to_string(),
        Response::Delete(_) => "Delete: success".to_string(),
        Response::ProtocolError(_) => "ProtocolError".to_string(),
        Response::GenericError(r) => format!("Error {}: {}", r.error_code, r.error_message),
        Response::Watch(watch_response) => format!("Watch: {}-{:?}", 
            watch_response.is_heartbeat, watch_response.events.as_ref().map(|f| f.len())),
    }
}