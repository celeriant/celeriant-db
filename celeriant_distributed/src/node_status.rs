#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Leader { lease_index: u64 },
    Follower { leader_lease_index: u64 },
    /// Runtime kick: follower is catching up from S3, rejects TCP replication
    FollowerCatchingUp { leader_lease_index: u64 },
    /// Runtime kick: caught up, waiting for leader to accept re-join
    FollowerCaughtUp { leader_lease_index: u64 },
    /// Boot-time S3 catchup, before election. TTL-exempt.
    BootCatchup,
    Fenced,
    Standalone,
}

impl NodeStatus {
    pub fn is_leader(&self) -> bool {
        matches!(self, NodeStatus::Leader { .. })
    }

    /// Only normal followers accepting TCP replication.
    /// FollowerCatchingUp and FollowerCaughtUp are NOT included.
    pub fn is_follower(&self) -> bool {
        matches!(self, NodeStatus::Follower { .. })
    }

    pub fn is_fenced(&self) -> bool {
        matches!(self, NodeStatus::Fenced)
    }
    
    pub fn is_standalone(&self) -> bool {
        matches!(self, NodeStatus::Standalone)
    }

    pub fn is_catching_up(&self) -> bool {
        matches!(self, NodeStatus::BootCatchup | NodeStatus::FollowerCatchingUp { .. })
    }

    pub fn lease_index(&self) -> Option<u64> {
        match self {
            NodeStatus::Leader { lease_index } => Some(*lease_index),
            NodeStatus::Standalone => Some(0),
            _ => None,
        }
    }

    pub fn lease_index_for_logging(&self) -> u64 {
        match self {
            NodeStatus::Leader { lease_index } => *lease_index,
            NodeStatus::Follower { leader_lease_index }
            | NodeStatus::FollowerCatchingUp { leader_lease_index }
            | NodeStatus::FollowerCaughtUp { leader_lease_index } => *leader_lease_index,
            NodeStatus::Standalone | NodeStatus::Fenced | NodeStatus::BootCatchup => 0,
        }
    }

    pub fn is_valid_transition_to(&self, new: &NodeStatus) -> bool {
        use NodeStatus::*;
        match (self, new) {
            // Any state can go to Fenced (emergency stop)
            (_, Fenced) => true,

            // Fenced can go to Leader, Follower, or BootCatchup
            (Fenced, Leader { .. }) | (Fenced, Follower { .. }) | (Fenced, BootCatchup) => true,

            // BootCatchup completes into Leader or Follower (after election)
            (BootCatchup, Leader { .. }) | (BootCatchup, Follower { .. }) => true,

            // Standalone is initial state, can go to Leader or Follower
            (Standalone, Leader { .. }) | (Standalone, Follower { .. }) => true,

            // Follower can update lease_index or enter catchup (runtime kick)
            (Follower { .. }, Follower { .. }) => true,
            (Follower { .. }, FollowerCatchingUp { .. }) => true,

            // Catchup progression: CatchingUp -> CaughtUp -> Follower
            (FollowerCatchingUp { .. }, FollowerCaughtUp { .. }) => true,
            (FollowerCaughtUp { .. }, Follower { .. }) => true,

            // Leader can update lease_index (renewal)
            (Leader { .. }, Leader { .. }) => true,

            // Everything else is invalid
            _ => false,
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
        assert!(!status.is_catching_up());
        assert_eq!(status.lease_index(), Some(5));
    }

    #[test]
    fn follower_helpers() {
        let status = NodeStatus::Follower { leader_lease_index: 3 };
        assert!(!status.is_leader());
        assert!(status.is_follower());
        assert!(!status.is_fenced());
        assert!(!status.is_catching_up());
        assert_eq!(status.lease_index(), None);
    }

    #[test]
    fn follower_catching_up_helpers() {
        let status = NodeStatus::FollowerCatchingUp { leader_lease_index: 3 };
        assert!(!status.is_leader());
        assert!(!status.is_follower());
        assert!(!status.is_fenced());
        assert!(status.is_catching_up());
        assert_eq!(status.lease_index(), None);
        assert_eq!(status.lease_index_for_logging(), 3);
    }

    #[test]
    fn follower_caught_up_helpers() {
        let status = NodeStatus::FollowerCaughtUp { leader_lease_index: 3 };
        assert!(!status.is_leader());
        assert!(!status.is_follower());
        assert!(!status.is_fenced());
        assert!(!status.is_catching_up());
        assert_eq!(status.lease_index(), None);
        assert_eq!(status.lease_index_for_logging(), 3);
    }

    #[test]
    fn boot_catchup_helpers() {
        let status = NodeStatus::BootCatchup;
        assert!(!status.is_leader());
        assert!(!status.is_follower());
        assert!(!status.is_fenced());
        assert!(status.is_catching_up());
        assert_eq!(status.lease_index(), None);
        assert_eq!(status.lease_index_for_logging(), 0);
    }

    #[test]
    fn fenced_helpers() {
        let status = NodeStatus::Fenced;
        assert!(!status.is_leader());
        assert!(!status.is_follower());
        assert!(status.is_fenced());
        assert!(!status.is_catching_up());
        assert_eq!(status.lease_index(), None);
    }

    #[test]
    fn standalone_helpers() {
        let status = NodeStatus::Standalone;
        assert!(!status.is_leader());
        assert!(!status.is_follower());
        assert!(!status.is_fenced());
        assert!(!status.is_catching_up());
        assert_eq!(status.lease_index(), Some(0));
    }

    #[test]
    fn valid_transitions_from_standalone() {
        let standalone = NodeStatus::Standalone;
        assert!(standalone.is_valid_transition_to(&NodeStatus::Leader { lease_index: 1 }));
        assert!(standalone.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 1 }));
        assert!(standalone.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!standalone.is_valid_transition_to(&NodeStatus::Standalone));
        assert!(!standalone.is_valid_transition_to(&NodeStatus::BootCatchup));
    }

    #[test]
    fn valid_transitions_from_leader() {
        let leader = NodeStatus::Leader { lease_index: 5 };
        assert!(leader.is_valid_transition_to(&NodeStatus::Leader { lease_index: 6 }));
        assert!(leader.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!leader.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 6 }));
        assert!(!leader.is_valid_transition_to(&NodeStatus::FollowerCatchingUp { leader_lease_index: 6 }));
        assert!(!leader.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn valid_transitions_from_follower() {
        let follower = NodeStatus::Follower { leader_lease_index: 3 };
        assert!(follower.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 4 }));
        assert!(follower.is_valid_transition_to(&NodeStatus::FollowerCatchingUp { leader_lease_index: 3 }));
        assert!(follower.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!follower.is_valid_transition_to(&NodeStatus::Leader { lease_index: 5 }));
        assert!(!follower.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn valid_transitions_from_follower_catching_up() {
        let catching_up = NodeStatus::FollowerCatchingUp { leader_lease_index: 3 };
        assert!(catching_up.is_valid_transition_to(&NodeStatus::FollowerCaughtUp { leader_lease_index: 3 }));
        assert!(catching_up.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!catching_up.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 3 }));
        assert!(!catching_up.is_valid_transition_to(&NodeStatus::Leader { lease_index: 5 }));
    }

    #[test]
    fn valid_transitions_from_follower_caught_up() {
        let caught_up = NodeStatus::FollowerCaughtUp { leader_lease_index: 3 };
        assert!(caught_up.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 3 }));
        assert!(caught_up.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!caught_up.is_valid_transition_to(&NodeStatus::FollowerCatchingUp { leader_lease_index: 3 }));
        assert!(!caught_up.is_valid_transition_to(&NodeStatus::Leader { lease_index: 5 }));
    }

    #[test]
    fn valid_transitions_from_fenced() {
        let fenced = NodeStatus::Fenced;
        assert!(fenced.is_valid_transition_to(&NodeStatus::Leader { lease_index: 1 }));
        assert!(fenced.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 1 }));
        assert!(fenced.is_valid_transition_to(&NodeStatus::BootCatchup));
        assert!(fenced.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!fenced.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn valid_transitions_from_boot_catchup() {
        let boot = NodeStatus::BootCatchup;
        assert!(boot.is_valid_transition_to(&NodeStatus::Leader { lease_index: 1 }));
        assert!(boot.is_valid_transition_to(&NodeStatus::Follower { leader_lease_index: 1 }));
        assert!(boot.is_valid_transition_to(&NodeStatus::Fenced));
        assert!(!boot.is_valid_transition_to(&NodeStatus::FollowerCatchingUp { leader_lease_index: 1 }));
        assert!(!boot.is_valid_transition_to(&NodeStatus::Standalone));
    }

    #[test]
    fn lease_index_for_logging() {
        assert_eq!(NodeStatus::Leader { lease_index: 42 }.lease_index_for_logging(), 42);
        assert_eq!(NodeStatus::Follower { leader_lease_index: 17 }.lease_index_for_logging(), 17);
        assert_eq!(NodeStatus::FollowerCatchingUp { leader_lease_index: 17 }.lease_index_for_logging(), 17);
        assert_eq!(NodeStatus::FollowerCaughtUp { leader_lease_index: 17 }.lease_index_for_logging(), 17);
        assert_eq!(NodeStatus::Standalone.lease_index_for_logging(), 0);
        assert_eq!(NodeStatus::Fenced.lease_index_for_logging(), 0);
        assert_eq!(NodeStatus::BootCatchup.lease_index_for_logging(), 0);
    }
}
