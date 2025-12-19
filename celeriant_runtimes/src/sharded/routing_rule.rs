use celeriant_wal::aggregate_key::AggregateKey;



/// Determines which field of the aggregate key is used for shard routing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RoutingRule {
    /// Route by org_id - all aggregates for an org go to the same shard
    OrgId,
    /// Route by aggregate_type_id - all aggregates of a type go to the same shard
    AggregateTypeId,
    /// Route by aggregate_id (default) - individual aggregates are distributed
    #[default]
    AggregateId,
}

impl std::str::FromStr for RoutingRule {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "org_id" | "org" => Ok(RoutingRule::OrgId),
            "aggregate_type_id" | "aggregate_type" | "type" => Ok(RoutingRule::AggregateTypeId),
            "aggregate_id" | "aggregate" => Ok(RoutingRule::AggregateId),
            _ => Err(format!(
                "Invalid routing rule '{}'. Valid options: org_id, aggregate_type_id, aggregate_id",
                s
            )),
        }
    }
}

impl std::fmt::Display for RoutingRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingRule::OrgId => write!(f, "org_id"),
            RoutingRule::AggregateTypeId => write!(f, "aggregate_type_id"),
            RoutingRule::AggregateId => write!(f, "aggregate_id"),
        }
    }
}

impl RoutingRule {
    /// Returns the routing ID based on the specified routing rule.
    pub fn routing_id_for_rule(&self, aggregate_key: AggregateKey) -> u128 {
        match self {
            RoutingRule::OrgId => aggregate_key.org_id,
            RoutingRule::AggregateTypeId => aggregate_key.aggregate_type_id,
            RoutingRule::AggregateId => aggregate_key.aggregate_id,
        }
    }
}