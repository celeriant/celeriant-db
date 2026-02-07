#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Leader { lease_index: u64 },
    Follower { leader_lease_index: u64 },
    Fenced,
    Standalone,
}

impl NodeStatus {
    pub fn is_leader(&self) -> bool {
        matches!(self, NodeStatus::Leader { .. })
    }

    pub fn is_follower(&self) -> bool {
        matches!(self, NodeStatus::Follower { .. })
    }

    pub fn is_fenced(&self) -> bool {
        matches!(self, NodeStatus::Fenced)
    }

    pub fn lease_index(&self) -> Option<u64> {
        match self {
            NodeStatus::Leader { lease_index } => Some(*lease_index),
            NodeStatus::Standalone => Some(0),
            NodeStatus::Follower { .. } | NodeStatus::Fenced => None,
        }
    }

    pub fn lease_index_for_logging(&self) -> u64 {
        match self {
            NodeStatus::Leader { lease_index } => *lease_index,
            NodeStatus::Follower { leader_lease_index } => *leader_lease_index,
            NodeStatus::Standalone => 0,
            NodeStatus::Fenced => 0,
        }
    }

    pub fn is_valid_transition_to(&self, new: &NodeStatus) -> bool {
        use NodeStatus::*;
        match (self, new) {
            // Any state can go to Fenced (emergency stop)
            (_, Fenced) => true,
            // Fenced can go to Leader or Follower (after S3 race)
            (Fenced, Leader { .. }) | (Fenced, Follower { .. }) => true,
            // Standalone is initial state, can go to Leader or Follower
            (Standalone, Leader { .. }) | (Standalone, Follower { .. }) => true,
            // Follower can update to Follower with different lease_index
            (Follower { .. }, Follower { .. }) => true,
            // Leader can update to Leader with different lease_index (renewal)
            (Leader { .. }, Leader { .. }) => true,
            // Leader can't go directly to Follower (must fence first)
            (Leader { .. }, Follower { .. }) => false,
            // Follower can't go directly to Leader (must fence first)
            (Follower { .. }, Leader { .. }) => false,
            // Can't transition back to Standalone
            (_, Standalone) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_helpers() {
        let status = NodeStatus::Leader { lease_index: 5 };
        assert!(status.is_leader());
        assert!(!status.is_follower());
        assert!(!status.is_fenced());
        assert_eq!(status.lease_index(), Some(5));
    }

    #[test]
    fn follower_helpers() {
        let status = NodeStatus::Follower { leader_lease_index: 3 };
        assert!(!status.is_leader());
        assert!(status.is_follower());
        assert!(!status.is_fenced());
        assert_eq!(status.lease_index(), None);
    }

    #[test]
    fn fenced_helpers() {
        let status = NodeStatus::Fenced;
        assert!(!status.is_leader());
        assert!(!status.is_follower());
        assert!(status.is_fenced());
        assert_eq!(status.lease_index(), None);
    }

    #[test]
    fn standalone_helpers() {
        let status = NodeStatus::Standalone;
        assert!(!status.is_leader());
        assert!(!status.is_follower());
        assert!(!status.is_fenced());
        assert_eq!(status.lease_index(), Some(0));
    }

    #[test]
    fn valid_transitions_from_standalone() {
        let standalone = NodeStatus::Standalone;
        assert!(standalone.is_valid_transition_to(&NodeStatus::Leader { lease_index: 1 }));
        assert!(standalone.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 1 }));
        assert!(standalone.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!standalone.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn valid_transitions_from_leader() {
        let leader = NodeStatus::Leader { lease_index: 5 };
        assert!(leader.is_valid_transition_to(&NodeStatus::Leader { lease_index: 6 }));
        assert!(leader.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!leader.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 6 }));
        assert!(!leader.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn valid_transitions_from_follower() {
        let follower = NodeStatus::Follower { leader_lease_index: 3 };
        assert!(follower.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 4 }));
        assert!(follower.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!follower.is_valid_transition_to(&NodeStatus::Leader { lease_index: 5 }));
        assert!(!follower.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn valid_transitions_from_fenced() {
        let fenced = NodeStatus::Fenced;
        assert!(fenced.is_valid_transition_to(&NodeStatus::Leader { lease_index: 1 }));
        assert!(fenced.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 1 }));
        assert!(fenced.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!fenced.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn lease_index_for_logging() {
        assert_eq!(NodeStatus::Leader { lease_index: 42 }.lease_index_for_logging(), 42);
        assert_eq!(NodeStatus::Follower { leader_lease_index: 17 }.lease_index_for_logging(), 17);
        assert_eq!(NodeStatus::Standalone.lease_index_for_logging(), 0);
        assert_eq!(NodeStatus::Fenced.lease_index_for_logging(), 0);
    }
}
