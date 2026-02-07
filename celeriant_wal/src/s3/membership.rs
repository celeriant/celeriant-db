use bincode::{Decode, Encode};

/// Information about a single node in the cluster.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: u128,
    pub client_address: String,
    pub replication_address: String,
}

impl NodeInfo {
    pub fn new(
        node_id: u128,
        client_address: String,
        replication_address: String,
    ) -> Self {
        Self {
            node_id,
            client_address,
            replication_address,
        }
    }
}

/// Cluster membership state stored in S3 at `cluster/membership.bin`.
#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq)]
pub struct Membership {
    pub nodes: [Option<NodeInfo>; 2],
}

impl Membership {
    pub fn empty() -> Self {
        Self {
            nodes: [None, None],
        }
    }

    /// Register a node, updating its existing slot or taking an empty one.
    pub fn register(&mut self, info: NodeInfo) {
        // Update existing slot for this node_id
        for slot in &mut self.nodes {
            if slot.as_ref().map(|n| n.node_id) == Some(info.node_id) {
                *slot = Some(info);
                return;
            }
        }
        // Take the first empty slot
        for slot in &mut self.nodes {
            if slot.is_none() {
                *slot = Some(info);
                return;
            }
        }
    }

    pub fn is_fully_replicated(&self) -> bool {
        self.nodes.iter().all(|n| n.is_some())
    }

    pub fn node_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_some()).count()
    }

    /// Remove a node by node_id (e.g. clearing a stale peer after takeover).
    pub fn deregister(&mut self, node_id: u128) {
        for slot in &mut self.nodes {
            if slot.as_ref().map(|n| n.node_id) == Some(node_id) {
                *slot = None;
                return;
            }
        }
    }

    /// Get the node that is not the specified node_id.
    pub fn peer_of(&self, node_id: u128) -> Option<&NodeInfo> {
        self.nodes
            .iter()
            .filter_map(|n| n.as_ref())
            .find(|n| n.node_id != node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_lifecycle() {
        let mut membership = Membership::empty();
        assert!(!membership.is_fully_replicated());
        assert_eq!(membership.node_count(), 0);

        membership.register(NodeInfo::new(1, "a:10000".into(), "a:10001".into()));
        assert_eq!(membership.node_count(), 1);
        assert!(!membership.is_fully_replicated());

        membership.register(NodeInfo::new(2, "b:10000".into(), "b:10001".into()));
        assert_eq!(membership.node_count(), 2);
        assert!(membership.is_fully_replicated());

        // peer_of returns the other node
        assert_eq!(membership.peer_of(1).unwrap().node_id, 2);
        assert_eq!(membership.peer_of(2).unwrap().node_id, 1);

        // peer_of with unknown ID returns first node (no special handling)
        assert_eq!(membership.peer_of(99).unwrap().node_id, 1);
    }

    #[test]
    fn test_register_updates_existing_slot() {
        let mut membership = Membership::empty();
        membership.register(NodeInfo::new(1, "old:10000".into(), "old:10001".into()));
        membership.register(NodeInfo::new(1, "new:10000".into(), "new:10001".into()));

        assert_eq!(membership.node_count(), 1);
        assert_eq!(membership.nodes[0].as_ref().unwrap().client_address, "new:10000");
    }

    #[test]
    fn test_deregister_clears_slot() {
        let mut membership = Membership::empty();
        membership.register(NodeInfo::new(1, "a:1".into(), "a:2".into()));
        membership.register(NodeInfo::new(2, "b:1".into(), "b:2".into()));

        membership.deregister(1);
        assert_eq!(membership.node_count(), 1);
        assert!(membership.nodes[0].is_none());
        assert_eq!(membership.nodes[1].as_ref().unwrap().node_id, 2);

        // Deregistering a non-existent node is a no-op
        membership.deregister(99);
        assert_eq!(membership.node_count(), 1);
    }

    #[test]
    fn test_register_does_not_overflow() {
        let mut membership = Membership::empty();
        membership.register(NodeInfo::new(1, "a:1".into(), "a:2".into()));
        membership.register(NodeInfo::new(2, "b:1".into(), "b:2".into()));
        // Third node has no slot — silently dropped (two-node cluster)
        membership.register(NodeInfo::new(3, "c:1".into(), "c:2".into()));
        assert_eq!(membership.node_count(), 2);
        // Node 3 was dropped, so slots still contain nodes 1 and 2
        assert_eq!(membership.nodes[0].as_ref().unwrap().node_id, 1);
        assert_eq!(membership.nodes[1].as_ref().unwrap().node_id, 2);
    }
}
