use riven_rank::parse;

#[test]
fn test_repack() {
    let data = parse("Movie.Title.REPACK.1080p.BluRay");
    assert!(data.repack);
}

#[test]
fn test_rerip() {
    let data = parse("Movie.Title.RERIP.1080p.BluRay");
    assert!(data.repack);
}
