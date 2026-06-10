use super::{should_deliver, sign};

#[test]
fn empty_filter_delivers_everything() {
    assert!(should_deliver(&[], "riven.media-item.download.success"));
}

#[test]
fn non_empty_filter_is_an_allowlist() {
    let filter = vec![
        "riven.media-item.download.success".to_string(),
        "riven.media-item.download.error".to_string(),
    ];
    assert!(should_deliver(&filter, "riven.media-item.download.error"));
    assert!(!should_deliver(&filter, "riven.media-item.scrape.success"));
}

#[test]
fn sign_matches_rfc4231_test_case_2() {
    // RFC 4231, Test Case 2 — HMAC-SHA256("Jefe", "what do ya want for nothing?").
    let sig = sign("Jefe", b"what do ya want for nothing?");
    assert_eq!(
        sig,
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
}

#[test]
fn sign_is_deterministic_for_same_input() {
    let body = br#"{"id":"abc","event":"riven.media-item.download.success"}"#;
    assert_eq!(sign("secret", body), sign("secret", body));
    assert_ne!(sign("secret", body), sign("other", body));
}
