use eventplanedb_storage::event_item::EventItem;

use crate::access_level::AccessLevel;

pub enum AggregateEventType {
    ShareLinkCreated = 43,
    UserAccessUpdated = 45,
    ShareLinkDisabled = 46,
}

pub fn event_user_access_updated(
    server_time: u64,
    user_id: Option<&str>,
    org_id: Option<&str>,
    access_level: AccessLevel,
    client_id: Option<u128>,
    share_id: Option<u128>,
    aggregate_id: Option<&str>,
) -> EventItem {
    EventItem {
        local_index: 0,
        event_date: server_time,
        event_type: AggregateEventType::UserAccessUpdated as u64,
        int_values: None,
        uint_values: Some(vec![access_level as u64]),
        f32_values: None,
        f64_values: None,
        bool_values: None,
        string_values: Some(vec![
            user_id.map(|s| s.to_string()),
            org_id.map(|s| s.to_string()),
            aggregate_id.map(|s| s.to_string()),
        ]),
        iv_arrays: None,
        byte_arrays: Some(vec![
            client_id.map(|id| id.to_le_bytes().to_vec()),
            share_id.map(|id| id.to_le_bytes().to_vec()),
        ]),
    }
}
