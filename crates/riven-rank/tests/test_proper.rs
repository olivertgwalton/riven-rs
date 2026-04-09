use riven_rank::parse;

#[test]
fn test_proper() {
    let data = parse("Movie.Title.PROPER.1080p.BluRay");
    assert!(data.proper);
}

#[test]
fn test_real_proper() {
    let data = parse("Movie.Title.REAL.PROPER.1080p.BluRay");
    assert!(data.proper);
}
