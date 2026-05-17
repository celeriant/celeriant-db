use std::collections::{HashMap, HashSet, VecDeque};

use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::{
    ListAggregateTypesRequest, ListAggregatesRequest, ListOrgsRequest,
};
use celeriant_msg::response::responses::{AggregateListItem, AggregateTypeListItem, OrgListItem};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::aggregate_type_key::AggregateTypeKey;

use crate::celeriant_client::CeleriantClient;
use crate::client_error::ClientError;

/// Options for list operations
#[derive(Debug, Clone)]
pub struct ListOptions {
    /// Include deleted aggregates in results (default: false, only for list_aggregates)
    pub include_deleted: bool,
    /// Starting shard hint - useful if you know your shard range (default: 0)
    pub start_shard: u64,
    /// Max shard hint - if known, avoids discovery overhead (default: None, will discover)
    pub max_shard_hint: Option<u64>,
}

/// Helper to detect shard routing errors (indicates invalid shard_id)
fn is_shard_routing_error(error: &ClientError) -> bool {
    matches!(error, ClientError::Server(crate::server_error::ServerError::ShardRouting { .. }))
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            include_deleted: false,
            start_shard: 0,
            max_shard_hint: None,
        }
    }
}

/// Streaming iterator for listing organizations across all shards
///
/// Automatically handles pagination, shard discovery, and deduplication.
/// Drop the iterator to cancel the operation.
pub struct ListOrgsIterator<'a> {
    client: &'a mut CeleriantClient,
    shard_cursors: HashMap<u64, Option<u64>>,
    active_shards: VecDeque<u64>,
    max_shard: Option<u64>,
    next_shard_to_try: u64,
    seen: HashSet<u128>,
    buffer: VecDeque<OrgListItem>,
    exhausted: bool,
}

impl<'a> ListOrgsIterator<'a> {
    pub fn new(client: &'a mut CeleriantClient, options: ListOptions) -> Self {
        let mut active_shards = VecDeque::new();
        let mut shard_cursors = HashMap::new();

        active_shards.push_back(options.start_shard);
        shard_cursors.insert(options.start_shard, None);

        Self {
            client,
            shard_cursors,
            active_shards,
            max_shard: options.max_shard_hint,
            next_shard_to_try: options.start_shard + 1,
            seen: HashSet::new(),
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    /// Get the next organization, or None if exhausted
    pub async fn next(&mut self) -> Option<Result<OrgListItem, ClientError>> {
        loop {
            while let Some(item) = self.buffer.pop_front() {
                if self.seen.insert(item.org_id) {
                    return Some(Ok(item));
                }
            }

            if self.exhausted {
                return None;
            }

            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        if self.active_shards.is_empty() {
            if !self.try_add_next_shard() {
                return Ok(false);
            }
        }

        let shard_id = match self.active_shards.pop_front() {
            Some(s) => s,
            None => return Ok(false),
        };

        let cursor = self.shard_cursors.get(&shard_id).copied().flatten();

        let request = ClientRequest::ListOrgs(ListOrgsRequest {
            correlation_id: None,
            shard_id,
            cursor,
        });

        match self.client.send_request(&request).await {
            Ok(ClientResponse::ListOrgs(response)) => {
                self.buffer.extend(response.orgs);

                if let Some(next_cursor) = response.next_cursor {
                    self.shard_cursors.insert(shard_id, Some(next_cursor));
                    self.active_shards.push_back(shard_id);
                } else {
                    self.shard_cursors.remove(&shard_id);
                }

                self.try_add_next_shard();

                Ok(true)
            }
            Ok(_) => Err(ClientError::ProtocolError),
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    if self.active_shards.is_empty() && self.buffer.is_empty() {
                        return Ok(false);
                    }
                    return Ok(!self.buffer.is_empty());
                }
                Err(e)
            }
        }
    }

    fn try_add_next_shard(&mut self) -> bool {
        if let Some(max) = self.max_shard {
            if self.next_shard_to_try > max {
                return false;
            }
        }

        if !self.shard_cursors.contains_key(&self.next_shard_to_try) {
            self.shard_cursors.insert(self.next_shard_to_try, None);
            self.active_shards.push_back(self.next_shard_to_try);
            self.next_shard_to_try += 1;
            return true;
        }

        false
    }

    /// Collect all remaining items into a Vec
    pub async fn collect(mut self) -> Result<Vec<OrgListItem>, ClientError> {
        let mut results = Vec::new();
        while let Some(item) = self.next().await {
            results.push(item?);
        }
        Ok(results)
    }
}

/// Streaming iterator for listing aggregate types across all shards
pub struct ListAggregateTypesIterator<'a> {
    client: &'a mut CeleriantClient,
    org_id: Option<u128>,
    shard_cursors: HashMap<u64, Option<u64>>,
    active_shards: VecDeque<u64>,
    max_shard: Option<u64>,
    next_shard_to_try: u64,
    seen: HashSet<AggregateTypeKey>,
    buffer: VecDeque<AggregateTypeListItem>,
    exhausted: bool,
}

impl<'a> ListAggregateTypesIterator<'a> {
    pub fn new(
        client: &'a mut CeleriantClient,
        org_id: Option<u128>,
        options: ListOptions,
    ) -> Self {
        let mut active_shards = VecDeque::new();
        let mut shard_cursors = HashMap::new();
        active_shards.push_back(options.start_shard);
        shard_cursors.insert(options.start_shard, None);

        Self {
            client,
            org_id,
            shard_cursors,
            active_shards,
            max_shard: options.max_shard_hint,
            next_shard_to_try: options.start_shard + 1,
            seen: HashSet::new(),
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    pub async fn next(&mut self) -> Option<Result<AggregateTypeListItem, ClientError>> {
        loop {
            while let Some(item) = self.buffer.pop_front() {
                let key = AggregateTypeKey::new(item.org_id, item.aggregate_type_id);
                if self.seen.insert(key) {
                    return Some(Ok(item));
                }
            }

            if self.exhausted {
                return None;
            }

            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        if self.active_shards.is_empty() {
            if !self.try_add_next_shard() {
                return Ok(false);
            }
        }

        let shard_id = match self.active_shards.pop_front() {
            Some(s) => s,
            None => return Ok(false),
        };

        let cursor = self.shard_cursors.get(&shard_id).copied().flatten();

        let request = ClientRequest::ListAggregateTypes(ListAggregateTypesRequest {
            correlation_id: None,
            shard_id,
            org_id: self.org_id,
            cursor,
        });

        match self.client.send_request(&request).await {
            Ok(ClientResponse::ListAggregateTypes(response)) => {
                self.buffer.extend(response.aggregate_types);

                if let Some(next_cursor) = response.next_cursor {
                    self.shard_cursors.insert(shard_id, Some(next_cursor));
                    self.active_shards.push_back(shard_id);
                } else {
                    self.shard_cursors.remove(&shard_id);
                }

                self.try_add_next_shard();
                Ok(true)
            }
            Ok(_) => Err(ClientError::ProtocolError),
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    if self.active_shards.is_empty() && self.buffer.is_empty() {
                        return Ok(false);
                    }
                    return Ok(!self.buffer.is_empty());
                }
                Err(e)
            }
        }
    }

    fn try_add_next_shard(&mut self) -> bool {
        if let Some(max) = self.max_shard {
            if self.next_shard_to_try > max {
                return false;
            }
        }
        if !self.shard_cursors.contains_key(&self.next_shard_to_try) {
            self.shard_cursors.insert(self.next_shard_to_try, None);
            self.active_shards.push_back(self.next_shard_to_try);
            self.next_shard_to_try += 1;
            return true;
        }
        false
    }

    pub async fn collect(mut self) -> Result<Vec<AggregateTypeListItem>, ClientError> {
        let mut results = Vec::new();
        while let Some(item) = self.next().await {
            results.push(item?);
        }
        Ok(results)
    }
}

/// Accumulated stats for an aggregate across pages
#[derive(Debug, Clone)]
pub struct AggregateStats {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub is_deleted: bool,
    pub event_batch_count: u64,
    pub min_event_timestamp: u64,
    pub max_event_timestamp: u64,
    pub min_server_timestamp: u64,
    pub max_server_timestamp: u64,
    pub min_aggregate_version: u64,
    pub max_aggregate_version: u64,
    pub min_event_seq: u64,
    pub max_event_seq: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
}

impl AggregateStats {
    pub(crate) fn from_item(item: &AggregateListItem) -> Self {
        Self {
            org_id: item.org_id,
            aggregate_type_id: item.aggregate_type_id,
            aggregate_id: item.aggregate_id,
            is_deleted: item.is_deleted,
            event_batch_count: item.event_batch_count,
            min_event_timestamp: item.min_event_timestamp,
            max_event_timestamp: item.max_event_timestamp,
            min_server_timestamp: item.min_server_timestamp,
            max_server_timestamp: item.max_server_timestamp,
            min_aggregate_version: item.min_aggregate_version,
            max_aggregate_version: item.max_aggregate_version,
            min_event_seq: item.min_event_seq,
            max_event_seq: item.max_event_seq,
            compressed_size: item.compressed_size,
            uncompressed_size: item.uncompressed_size,
        }
    }

    pub(crate) fn merge(&mut self, item: &AggregateListItem) {
        self.is_deleted = self.is_deleted || item.is_deleted;
        self.event_batch_count += item.event_batch_count;
        if item.min_event_timestamp > 0 {
            self.min_event_timestamp = if self.min_event_timestamp == 0 {
                item.min_event_timestamp
            } else {
                self.min_event_timestamp.min(item.min_event_timestamp)
            };
        }
        if item.min_server_timestamp > 0 {
            self.min_server_timestamp = if self.min_server_timestamp == 0 {
                item.min_server_timestamp
            } else {
                self.min_server_timestamp.min(item.min_server_timestamp)
            };
        }
        if item.min_aggregate_version > 0 {
            self.min_aggregate_version = if self.min_aggregate_version == 0 {
                item.min_aggregate_version
            } else {
                self.min_aggregate_version.min(item.min_aggregate_version)
            };
        }
        if item.min_event_seq > 0 {
            self.min_event_seq = if self.min_event_seq == 0 {
                item.min_event_seq
            } else {
                self.min_event_seq.min(item.min_event_seq)
            };
        }
        self.max_event_timestamp = self.max_event_timestamp.max(item.max_event_timestamp);
        self.max_server_timestamp = self.max_server_timestamp.max(item.max_server_timestamp);
        self.max_aggregate_version = self.max_aggregate_version.max(item.max_aggregate_version);
        self.max_event_seq = self.max_event_seq.max(item.max_event_seq);
        self.compressed_size += item.compressed_size;
        self.uncompressed_size += item.uncompressed_size;
    }
}

/// Streaming iterator for listing aggregates across all shards
pub struct ListAggregatesIterator<'a> {
    client: &'a mut CeleriantClient,
    org_id: Option<u128>,
    aggregate_type_id: Option<u128>,
    include_deleted: bool,
    shard_cursors: HashMap<u64, Option<u64>>,
    active_shards: VecDeque<u64>,
    max_shard: Option<u64>,
    next_shard_to_try: u64,
    stats: HashMap<AggregateKey, AggregateStats>,
    deleted: HashSet<AggregateKey>,
    order: Vec<AggregateKey>,
    order_pos: usize,
    buffer: VecDeque<AggregateListItem>,
    exhausted: bool,
}

impl<'a> ListAggregatesIterator<'a> {
    pub fn new(
        client: &'a mut CeleriantClient,
        org_id: Option<u128>,
        aggregate_type_id: Option<u128>,
        options: ListOptions,
    ) -> Self {
        let mut active_shards = VecDeque::new();
        let mut shard_cursors = HashMap::new();
        active_shards.push_back(options.start_shard);
        shard_cursors.insert(options.start_shard, None);

        Self {
            client,
            org_id,
            aggregate_type_id,
            include_deleted: options.include_deleted,
            shard_cursors,
            active_shards,
            max_shard: options.max_shard_hint,
            next_shard_to_try: options.start_shard + 1,
            stats: HashMap::new(),
            deleted: HashSet::new(),
            order: Vec::new(),
            order_pos: 0,
            buffer: VecDeque::new(),
            exhausted: false,
        }
    }

    /// Get the next aggregate with accumulated stats, or None if exhausted
    pub async fn next(&mut self) -> Option<Result<AggregateStats, ClientError>> {
        loop {
            while let Some(item) = self.buffer.pop_front() {
                let key = AggregateKey::new(item.org_id, item.aggregate_type_id, item.aggregate_id);

                if item.is_deleted {
                    self.deleted.insert(key.clone());
                }

                if let Some(existing) = self.stats.get_mut(&key) {
                    existing.merge(&item);
                    if self.deleted.contains(&key) {
                        existing.is_deleted = true;
                    }
                } else {
                    let mut stats = AggregateStats::from_item(&item);
                    if self.deleted.contains(&key) {
                        stats.is_deleted = true;
                    }
                    self.stats.insert(key.clone(), stats);
                    self.order.push(key);
                }
            }

            while self.order_pos < self.order.len() {
                let key = &self.order[self.order_pos];
                self.order_pos += 1;

                if let Some(stats) = self.stats.get(key) {
                    if !self.include_deleted && stats.is_deleted {
                        continue;
                    }
                    return Some(Ok(stats.clone()));
                }
            }

            if self.exhausted {
                return None;
            }

            match self.fetch_next_page().await {
                Ok(true) => continue,
                Ok(false) => {
                    self.exhausted = true;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }

    async fn fetch_next_page(&mut self) -> Result<bool, ClientError> {
        if self.active_shards.is_empty() {
            if !self.try_add_next_shard() {
                return Ok(false);
            }
        }

        let shard_id = match self.active_shards.pop_front() {
            Some(s) => s,
            None => return Ok(false),
        };

        let cursor = self.shard_cursors.get(&shard_id).copied().flatten();

        let request = ClientRequest::ListAggregates(ListAggregatesRequest {
            correlation_id: None,
            shard_id,
            org_id: self.org_id,
            aggregate_type_id: self.aggregate_type_id,
            cursor,
        });

        match self.client.send_request(&request).await {
            Ok(ClientResponse::ListAggregates(response)) => {
                self.buffer.extend(response.aggregates);

                if let Some(next_cursor) = response.next_cursor {
                    self.shard_cursors.insert(shard_id, Some(next_cursor));
                    self.active_shards.push_back(shard_id);
                } else {
                    self.shard_cursors.remove(&shard_id);
                }

                self.try_add_next_shard();
                Ok(true)
            }
            Ok(_) => Err(ClientError::ProtocolError),
            Err(e) => {
                if cursor.is_none() && self.max_shard.is_none() && is_shard_routing_error(&e) {
                    self.max_shard = Some(shard_id.saturating_sub(1));
                    self.shard_cursors.remove(&shard_id);
                    if self.active_shards.is_empty() && self.buffer.is_empty() {
                        return Ok(false);
                    }
                    return Ok(!self.buffer.is_empty());
                }
                Err(e)
            }
        }
    }

    fn try_add_next_shard(&mut self) -> bool {
        if let Some(max) = self.max_shard {
            if self.next_shard_to_try > max {
                return false;
            }
        }
        if !self.shard_cursors.contains_key(&self.next_shard_to_try) {
            self.shard_cursors.insert(self.next_shard_to_try, None);
            self.active_shards.push_back(self.next_shard_to_try);
            self.next_shard_to_try += 1;
            return true;
        }
        false
    }

    /// Collect all remaining items into a Vec with fully merged stats
    pub async fn collect(mut self) -> Result<Vec<AggregateStats>, ClientError> {
        let mut results = Vec::new();
        while let Some(item) = self.next().await {
            results.push(item?);
        }
        Ok(results)
    }
}
