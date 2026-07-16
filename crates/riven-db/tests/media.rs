use riven_db::repo::media::cooldown_for_failed_attempts;

// Must mirror `FAILED_ATTEMPTS_COOLDOWN_SQL`'s tiers exactly — this is the
// job-level retry's only backoff signal, since it doesn't go through
// `get_pending_items_for_retry`'s SQL filter.

#[test]
fn base_tier_under_two_failures() {
    assert_eq!(
        cooldown_for_failed_attempts(0),
        chrono::Duration::minutes(30)
    );
    assert_eq!(
        cooldown_for_failed_attempts(1),
        chrono::Duration::minutes(30)
    );
}

#[test]
fn escalates_at_two_failures() {
    assert_eq!(cooldown_for_failed_attempts(2), chrono::Duration::hours(2));
    assert_eq!(cooldown_for_failed_attempts(4), chrono::Duration::hours(2));
}

#[test]
fn escalates_at_five_failures() {
    assert_eq!(cooldown_for_failed_attempts(5), chrono::Duration::hours(6));
    assert_eq!(cooldown_for_failed_attempts(9), chrono::Duration::hours(6));
}

#[test]
fn escalates_at_ten_failures() {
    assert_eq!(
        cooldown_for_failed_attempts(10),
        chrono::Duration::hours(24)
    );
    assert_eq!(
        cooldown_for_failed_attempts(1000),
        chrono::Duration::hours(24)
    );
}
