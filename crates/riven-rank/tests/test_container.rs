use riven_rank::parse;

#[test]
fn test_extension_mkv() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP.mkv");
    assert_eq!(data.extension, Some("mkv".into()));
}

#[test]
fn test_extension_mp4() {
    let data = parse("Movie.Title.mp4");
    assert_eq!(data.extension, Some("mp4".into()));
}

#[test]
fn test_extension_srt() {
    let data = parse("Movie.Title.srt");
    assert_eq!(data.extension, Some("srt".into()));
}

#[test]
fn test_container_mkv() {
    let data = parse("Movie.Title.MKV.1080p.BluRay");
    assert_eq!(data.container, Some("mkv".into()));
}

#[test]
fn test_extension_avi() {
    let data = parse("Movie.Title.2023.DivX-GROUP.avi");
    assert_eq!(data.extension, Some("avi".to_string()));
}

#[test]
fn test_extension_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.extension, None);
}
