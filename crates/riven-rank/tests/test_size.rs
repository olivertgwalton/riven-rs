use riven_rank::parse;

#[test]
fn test_size_gb() {
    let data = parse("Movie.Title.2023.1080p.BluRay.1.5GB");
    assert_eq!(data.size, Some("1.5GB".into()));
}

#[test]
fn test_size_mb() {
    let data = parse("Movie.Title.720p.350MB");
    assert_eq!(data.size, Some("350MB".into()));
}

#[test]
fn test_size_detected() {
    let data = parse("The.Mandalorian.S01E06.4K.HDR.2160p 4.42GB");
    assert_eq!(data.size, Some("4.42GB".to_string()));
}

#[test]
fn test_size_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.size, None);
}
