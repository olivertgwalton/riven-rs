use riven_rank::parse;

#[test]
fn test_complete_keyword() {
    let data = parse("Show.Title.Complete.Series.720p.BluRay");
    assert!(data.complete);
}

#[test]
fn test_complete_year_range() {
    let data = parse("Show Title 2010-2015 DVDRip");
    assert!(data.complete);
}

#[test]
fn test_complete_collection() {
    let data = parse("Movie.Title.Trilogy.1080p.BluRay");
    assert!(data.complete);
}

#[test]
fn test_complete_box_set() {
    let data = parse("Show Title Complete Box Set DVDRip");
    assert!(data.complete);
}

#[test]
fn test_complete_series() {
    let data = parse("Grimm.INTEGRAL.MULTI.COMPLETE.BLURAY-BMTH");
    assert!(data.complete);
}

#[test]
fn test_ptt_collection_avatar_full_series() {
    let data = parse("Avatar: The Last Airbender Full Series 720p");
    assert_eq!(data.parsed_title, "Avatar: The Last Airbender");
    assert!(data.complete);
}

#[test]
fn test_ptt_collection_ninja_collection_title_not_complete() {
    let data = parse("[Erai-raws] Ninja Collection - 05 [720p][Multiple Subtitle].mkv");
    assert_eq!(data.parsed_title, "Ninja Collection");
    assert!(!data.complete);
}

#[test]
fn test_ptt_collection_vinland_saga_complete_title() {
    let data = parse("[Judas] Vinland Saga (Season 2) [1080p][HEVC x265 10bit][Multi-Subs]");
    assert_eq!(data.parsed_title, "Vinland Saga");
    assert!(data.complete);
}
