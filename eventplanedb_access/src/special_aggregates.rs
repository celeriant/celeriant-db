use eventplanedb_storage::event_item::EventItem;

use crate::{access_level::AccessLevel, aggregate_event_type::event_user_access_updated};

pub struct SpecialAggregates {
    pub client_id: Option<u128>,
    pub user_id: Option<String>,
    pub org_id: Option<String>,
    pub user_aggregate: Vec<EventItem>,
    pub client_aggregate: Vec<EventItem>,
}

impl SpecialAggregates {
    pub fn new(client_id: Option<u128>, user_id: Option<String>, org_id: Option<String>) -> Self {
        SpecialAggregates {
            client_id,
            user_id,
            org_id,
            user_aggregate: Vec::new(),
            client_aggregate: Vec::new(),
        }
    }

    pub fn client_removed_from_aggregate(&mut self, aggregate_id: &str, server_time: u64) {
        if self.client_id.is_some() {
            let event_item = event_user_access_updated(server_time, None, None, AccessLevel::None, self.client_id, None, Some(aggregate_id));
            self.client_aggregate.push(event_item);
        }
    }

    pub fn user_removed_from_aggregate(&mut self, aggregate_id: &str, server_time: u64) {
        let event_item = event_user_access_updated(
            server_time,
            self.user_id.as_deref(),
            self.org_id.as_deref(),
            AccessLevel::None,
            self.client_id,
            None,
            Some(aggregate_id),
        );
        self.user_aggregate.push(event_item);
    }

    pub fn permission_updated_on_aggregate(&mut self, aggregate_id: &str, access_level: AccessLevel, share_link_used: Option<u128>, server_time: u64) {
        if self.client_id.is_some() && self.user_id.is_none() {
            let event_item = event_user_access_updated(server_time, None, None, access_level, self.client_id, share_link_used, Some(aggregate_id));
            self.client_aggregate.push(event_item);
        }

        if self.user_id.is_some() {
            let event_item = event_user_access_updated(
                server_time,
                self.user_id.as_deref(),
                self.org_id.as_deref(),
                access_level,
                self.client_id,
                share_link_used,
                Some(aggregate_id),
            );
            self.user_aggregate.push(event_item);
        }
    }
}
