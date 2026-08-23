use crate::fd_limits::*;

#[test]
fn required_nofile_matches_formula_for_representative_config() {
    let required = required_nofile(8, 1000, PROCESS_BASELINE_FDS, FD_HEADROOM_MARGIN).unwrap();
    assert_eq!(required, 8 * 2002 + 4 * 8 + PROCESS_BASELINE_FDS + FD_HEADROOM_MARGIN);
    assert_eq!(required, 16432);
}

#[test]
fn required_nofile_with_zero_shards_is_baseline_plus_margin() {
    assert_eq!(required_nofile(0, 1000, 256, 128), Some(384));
}

#[test]
fn required_nofile_with_zero_max_open_files_still_charges_active_segment_and_executors() {
    assert_eq!(required_nofile(8, 0, 256, 128), Some(16 + 32 + 384));
}

#[test]
fn required_nofile_overflows_on_max_open_files_increment() {
    assert_eq!(required_nofile(1, u64::MAX, 0, 0), None);
}

#[test]
fn required_nofile_overflows_on_segment_doubling() {
    assert_eq!(required_nofile(1, u64::MAX / 2, 0, 0), None);
}

#[test]
fn required_nofile_overflows_on_shard_multiplication() {
    assert_eq!(required_nofile(u64::MAX, 1, 0, 0), None);
}

#[test]
fn required_nofile_overflows_on_executor_multiplication() {
    assert_eq!(required_nofile(u64::MAX / 2, 0, 0, 0), None);
}

#[test]
fn required_nofile_overflows_on_baseline_addition() {
    assert_eq!(required_nofile(1, (u64::MAX - 5) / 2 - 1, 4, 0), None);
}

#[test]
fn required_nofile_overflows_on_margin_addition() {
    assert_eq!(required_nofile(0, 0, u64::MAX, 1), None);
}

#[test]
fn required_nofile_saturates_exactly_at_u64_max_without_overflow() {
    assert_eq!(required_nofile(1, (u64::MAX - 5) / 2 - 1, 0, 0), Some(u64::MAX - 1));
    assert_eq!(required_nofile(0, 0, u64::MAX - 1, 1), Some(u64::MAX));
}

#[test]
fn plan_is_sufficient_when_soft_equals_required() {
    assert_eq!(plan_nofile(1024, 4096, 1024), NofilePlan::Sufficient);
}

#[test]
fn plan_is_sufficient_when_soft_above_required() {
    assert_eq!(plan_nofile(4096, 4096, 1024), NofilePlan::Sufficient);
}

#[test]
fn plan_raises_to_hard_when_soft_below_required() {
    assert_eq!(plan_nofile(1023, 4096, 1024), NofilePlan::Raise { to: 4096 });
}

#[test]
fn plan_raises_when_required_equals_hard() {
    assert_eq!(plan_nofile(1023, 1024, 1024), NofilePlan::Raise { to: 1024 });
}

#[test]
fn plan_exceeds_hard_when_required_is_one_above_hard() {
    assert_eq!(plan_nofile(1023, 1024, 1025), NofilePlan::ExceedsHard);
}

#[test]
fn plan_exceeds_hard_when_soft_equals_hard_below_required() {
    assert_eq!(plan_nofile(1024, 1024, 2048), NofilePlan::ExceedsHard);
}

#[test]
fn plan_raises_to_infinity_hard() {
    assert_eq!(plan_nofile(1024, u64::MAX, 65536), NofilePlan::Raise { to: u64::MAX });
}

#[test]
fn plan_is_sufficient_when_required_is_zero() {
    assert_eq!(plan_nofile(0, 0, 0), NofilePlan::Sufficient);
}

#[test]
fn plan_exceeds_hard_when_required_is_u64_max_and_hard_is_finite() {
    assert_eq!(plan_nofile(1024, u64::MAX - 1, u64::MAX), NofilePlan::ExceedsHard);
}

#[test]
fn nofile_insufficient_display_names_every_number() {
    let err = FdLimitError::NofileInsufficient {
        required: 100003,
        soft: 100019,
        hard: 100043,
        num_shards: 100057,
        max_open_files: 100069,
        baseline: 100103,
        margin: 100109,
    };
    let msg = err.to_string();
    for n in ["100003", "100019", "100043", "100057", "100069", "100103", "100109"] {
        assert!(msg.contains(n), "missing {n} in {msg}");
    }
}

#[test]
fn nofile_overflow_display_names_shards_and_max_open_files() {
    let msg = FdLimitError::NofileOverflow { num_shards: 15485863, max_open_files: 32452843 }
        .to_string();
    assert!(msg.contains("15485863"), "{msg}");
    assert!(msg.contains("32452843"), "{msg}");
}

#[test]
fn nofile_raise_failed_display_names_target_and_source() {
    let msg = FdLimitError::NofileRaiseFailed {
        target: 611953,
        source: "operation not permitted".to_string(),
    }
    .to_string();
    assert!(msg.contains("611953"), "{msg}");
    assert!(msg.contains("operation not permitted"), "{msg}");
}

#[test]
fn limit_query_display_names_resource_and_source() {
    let msg =
        FdLimitError::LimitQuery { resource: "NOFILE", source: "bad file descriptor".to_string() }
            .to_string();
    assert!(msg.contains("NOFILE"), "{msg}");
    assert!(msg.contains("bad file descriptor"), "{msg}");
    assert!(!msg.contains("RLIMIT_RLIMIT_"), "{msg}");
}

#[test]
fn fd_limit_error_is_a_std_error() {
    fn assert_error<E: std::error::Error>(_: &E) {}
    assert_error(&FdLimitError::NofileOverflow { num_shards: 3, max_open_files: 5 });
}

#[test]
fn constants_match_documented_values() {
    assert_eq!(PROCESS_BASELINE_FDS, 256);
    assert_eq!(FD_HEADROOM_MARGIN, 128);
}

#[test]
fn ensure_fd_headroom_returns_self_consistent_headroom_for_minimal_config() {
    let headroom = ensure_fd_headroom(1, 1).expect("minimal config must fit process limits");
    let expected = required_nofile(1, 1, PROCESS_BASELINE_FDS, FD_HEADROOM_MARGIN).unwrap();
    assert_eq!(headroom.required, expected);
    assert!(headroom.soft >= headroom.required, "{headroom:?}");
    assert!(headroom.soft <= headroom.hard, "{headroom:?}");
    assert!(headroom.soft_before <= headroom.soft, "{headroom:?}");
}
