use riven_rank::parse;

#[test]
fn test_retail() {
    let data = parse("Movie.Title.Retail.1080p.BluRay");
    assert!(data.retail);
}
