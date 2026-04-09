use riven_rank::parse;

#[test]
fn test_converted() {
    let data = parse("Movie.Title.CONVERT.1080p");
    assert!(data.converted);
}
