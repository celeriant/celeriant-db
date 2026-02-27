use celeriant_msg::process_client_responses::ClientResponse;
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
        ClientResponse::Watch(watch_response) => format!("Watch: {}-{:?}",
            watch_response.events.is_none(), watch_response.events.as_ref().map(|f| f.len())),
        ClientResponse::ListOrgs(r) => format!("ListOrgs: {} orgs", r.orgs.len()),
        ClientResponse::ListAggregateTypes(r) => format!("ListAggregateTypes: {} types", r.aggregate_types.len()),
        ClientResponse::ListAggregates(r) => format!("ListAggregates: {} aggregates", r.aggregates.len()),
    }
}