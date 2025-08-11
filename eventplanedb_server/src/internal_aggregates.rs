use base64::{Engine as _, engine::general_purpose};

pub fn client_aggregate_name(client_id: u128) -> String {
    let client_id_bytes = client_id.to_le_bytes();
    let client_id_base64 = general_purpose::STANDARD.encode(client_id_bytes);
    format!("__client_{client_id_base64}")
}

pub fn user_aggregate_name(user_id: &str) -> String {
    format!("__user_{user_id}")
}

pub fn org_aggregate_name(org_id: &str) -> String {
    format!("__org_{org_id}")
}

pub fn is_internal_aggregate(aggregate_id: &str) -> bool {
    aggregate_id.starts_with("__")
}
