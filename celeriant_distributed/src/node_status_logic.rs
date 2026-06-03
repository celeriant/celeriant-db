use crate::node_status::NodeStatus;

/// Extends the follower lease TTL: TTL is never reduced by a heartbeat.
/// invariants.md:61 — `new_expiry = max(current_expiry, leader_timestamp_ms + heartbeat_lease_duration)`.
pub fn compute_new_ttl(current_expiry_ms: u64, leader_timestamp_ms: u64, heartbeat_lease_duration_ms: u64) -> u64 {
    current_expiry_ms.max(leader_timestamp_ms + heartbeat_lease_duration_ms)
}

/// Outcome of a kick request, computed purely from current node status.
#[derive(Debug, PartialEq, Eq)]
pub enum KickOutcome {
    Transition(NodeStatus),
    AlreadyCatchingUp,
    NotAFollower,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PostCatchupAction {
    BootWaitThenReevaluate { wait_ms: u64 },
    StayFollower { leader_lease_epoch: u64, lease_expires_at_ms: u64 },
    ChallengeViaCAS,
}

/// Pure decision function for the post-catchup action. Boot catchup enforces a wait
/// in case leader is depending on existing heartbeat loop
pub fn decide_post_catchup_action(
    current_status: NodeStatus,
    lease_expires_at_ms: u64,
    now_ms: u64,
    heartbeat_lease_duration_ms: u64,
) -> PostCatchupAction {
    let lease_alive = lease_expires_at_ms > now_ms;
    if lease_alive {
        let leader_lease_epoch = match current_status {
            NodeStatus::FollowerCatchingUp { leader_lease_epoch } => leader_lease_epoch,
            _ => 0,
        };
        return PostCatchupAction::StayFollower {
            leader_lease_epoch,
            lease_expires_at_ms,
        };
    }

    if current_status == NodeStatus::BootCatchup && lease_expires_at_ms == 0 {
        return PostCatchupAction::BootWaitThenReevaluate {
            wait_ms: heartbeat_lease_duration_ms,
        };
    }

    // Expired lease and not in bootcatchup, try become leader
    PostCatchupAction::ChallengeViaCAS
}

/// Determine what a kick does to a node
pub fn kick_transition(current_status: NodeStatus) -> KickOutcome {
    match current_status {
        NodeStatus::Follower { leader_lease_epoch } => {
            KickOutcome::Transition(NodeStatus::FollowerCatchingUp { leader_lease_epoch })
        }
        NodeStatus::FollowerCatchingUp { .. } => KickOutcome::AlreadyCatchingUp,
        NodeStatus::Leader { .. }
        | NodeStatus::Standalone
        | NodeStatus::BootCatchup
        | NodeStatus::Fenced => KickOutcome::NotAFollower,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_new_ttl_max_semantics_loop() {
        // TTL is never reduced by a heartbeat
        let cases: &[(u64, u64, u64, u64)] = &[
            (100, 200, 50, 250),  // future heartbeat extends
            (500, 100, 50, 500),  // past heartbeat does NOT reduce
            (300, 200, 100, 300), // equal: max selects current
            (300, 200, 101, 301), // just-barely-extends
        ];
        for &(current, leader_ts, lease_dur, expected) in cases {
            assert_eq!(compute_new_ttl(current, leader_ts, lease_dur), expected);
        }
    }

    #[test]
    fn compute_new_ttl_with_zero_lease_duration() {
        // leader_ts ahead of current_expiry with lease_dur=0 still applies max
        assert_eq!(compute_new_ttl(100, 200, 0), 200);
    }

    #[test]
    fn kick_transition_per_status_loop() {
        // KickFollower only transitions Follower→FollowerCatchingUp; idempotent on FollowerCatchingUp; NotAFollower for all others. (invariants.md:199-202)
        let cases: &[(NodeStatus, KickOutcome)] = &[
            (
                NodeStatus::Follower { leader_lease_epoch: 3 },
                KickOutcome::Transition(NodeStatus::FollowerCatchingUp { leader_lease_epoch: 3 }),
            ),
            (
                NodeStatus::FollowerCatchingUp { leader_lease_epoch: 3 },
                KickOutcome::AlreadyCatchingUp,
            ),
            (NodeStatus::Leader { lease_epoch: 5 }, KickOutcome::NotAFollower),
            (NodeStatus::Standalone, KickOutcome::NotAFollower),
            (NodeStatus::BootCatchup, KickOutcome::NotAFollower),
            (NodeStatus::Fenced, KickOutcome::NotAFollower),
        ];
        for (status, expected) in cases {
            assert_eq!(kick_transition(*status), *expected, "{status:?}");
        }
    }

    #[test]
    fn post_catchup_follower_catching_up_with_alive_ttl_stays_follower() {
        // INVARIANT (heartbeat-liveness gate): if local TTL alive, don't challenge.
        let action = decide_post_catchup_action(
            NodeStatus::FollowerCatchingUp { leader_lease_epoch: 7 },
            25_000,
            10_000,
            1_500,
        );
        assert_eq!(
            action,
            PostCatchupAction::StayFollower { leader_lease_epoch: 7, lease_expires_at_ms: 25_000 }
        );
    }

    #[test]
    fn post_catchup_follower_catching_up_with_expired_ttl_challenges_cas() {
        // INVARIANT: TTL expired (no recent heartbeat) → CAS to determine role.
        let action = decide_post_catchup_action(
            NodeStatus::FollowerCatchingUp { leader_lease_epoch: 7 },
            25_000,
            25_001,
            1_500,
        );
        assert_eq!(action, PostCatchupAction::ChallengeViaCAS);
    }

    #[test]
    fn post_catchup_boot_catchup_with_zero_lease_waits() {
        // INVARIANT (boot wait): no heartbeat history → wait heartbeat_lease_duration first.
        let action = decide_post_catchup_action(NodeStatus::BootCatchup, 0, 100_000, 1_500);
        assert_eq!(action, PostCatchupAction::BootWaitThenReevaluate { wait_ms: 1_500 });
    }

    #[test]
    fn post_catchup_boot_catchup_after_wait_no_heartbeat_challenges_cas() {
        // After the boot wait sleeps, caller re-evaluates. With any non-zero (and expired)
        // lease_expires_at_ms, proceed to CAS — guarding against the boot-grace branch firing
        // again on retry.
        let action = decide_post_catchup_action(NodeStatus::BootCatchup, 1, 100_000, 1_500);
        assert_eq!(action, PostCatchupAction::ChallengeViaCAS);
    }

    #[test]
    fn post_catchup_lease_at_boundary_now_eq_expiry_challenges() {
        // INVARIANT: TTL is "alive" iff strictly greater than now (matches is_lease_expired
        // semantics: now == expires_at_ms is expired).
        let action = decide_post_catchup_action(
            NodeStatus::FollowerCatchingUp { leader_lease_epoch: 5 },
            10_000,
            10_000,
            1_500,
        );
        assert_eq!(action, PostCatchupAction::ChallengeViaCAS);
    }

    #[test]
    fn post_catchup_lease_one_ms_in_future_stays_follower() {
        // Boundary: lease_expires_at_ms = now + 1 → alive → StayFollower.
        let action = decide_post_catchup_action(
            NodeStatus::FollowerCatchingUp { leader_lease_epoch: 5 },
            10_001,
            10_000,
            1_500,
        );
        assert_eq!(
            action,
            PostCatchupAction::StayFollower { leader_lease_epoch: 5, lease_expires_at_ms: 10_001 }
        );
    }
}
