use riven_rank::parse;

#[test]
fn test_hardcoded() {
    let data = parse("Movie.Title.HC.1080p.WEBRip");
    assert!(data.hardcoded);
}
