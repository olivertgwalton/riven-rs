use riven_rank::parse;

#[test]
fn test_date_ymd() {
    let data = parse("Show.Title.2023.05.15.720p.WEB-DL");
    assert_eq!(data.date, Some("2023-05-15".into()));
}

#[test]
fn test_date_dmy() {
    let data = parse("Show.Title.15.05.2023.720p.WEB-DL");
    assert_eq!(data.date, Some("2023-05-15".into()));
}

#[test]
fn test_date_compact() {
    let data = parse("Show.Title.20230515.720p.WEB-DL");
    assert_eq!(data.date, Some("2023-05-15".into()));
}
