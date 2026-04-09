use riven_rank::parse;

#[test]
fn test_dubbed() {
    let data = parse("Movie.Title.DUBBED.720p.BluRay");
    assert!(data.dubbed);
}

#[test]
fn test_dubbed_dual_audio() {
    let data = parse("Movie.Title.Dual.Audio.720p.BluRay");
    assert!(data.dubbed);
}

#[test]
fn test_dubbed_multi() {
    let data = parse("Movie.Title.MULTI.720p.BluRay");
    assert!(data.dubbed);
}
