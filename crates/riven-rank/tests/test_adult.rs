use riven_rank::parse;

#[test]
fn test_adult_xxx() {
    let data = parse("Movie.Title.XXX.720p");
    assert!(data.adult);
}

#[test]
fn test_not_adult() {
    let data = parse("Movie.Title.2023.1080p.BluRay");
    assert!(!data.adult);
}
