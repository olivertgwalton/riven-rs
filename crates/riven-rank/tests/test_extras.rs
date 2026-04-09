use riven_rank::parse;

#[test]
fn test_extras_nced() {
    let data = parse("[Group] Anime Title NCED [1080p].mkv");
    assert!(data.extras.contains(&"NCED".to_string()));
}

#[test]
fn test_extras_ncop() {
    let data = parse("[Group] Anime Title NCOP [1080p].mkv");
    assert!(data.extras.contains(&"NCOP".to_string()));
}

#[test]
fn test_extras_ova() {
    let data = parse("[Group] Anime Title OVA [1080p].mkv");
    assert!(data.extras.contains(&"OVA".to_string()));
}

#[test]
fn test_extras_trailer() {
    let data = parse("Movie.Title.2023.Trailer.1080p");
    assert!(
        data.extras
            .iter()
            .any(|e| e.to_lowercase().contains("trailer"))
    );
}

#[test]
fn test_extras_sample() {
    let data = parse("Movie.Title.2023.Sample.1080p");
    assert!(
        data.extras
            .iter()
            .any(|e| e.to_lowercase().contains("sample"))
    );
}
