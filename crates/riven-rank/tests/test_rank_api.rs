use std::collections::HashMap;

use riven_rank::rank::check_fetch;
use riven_rank::{RankSettings, rank_torrent};

#[test]
fn test_rank_torrent_skips_similarity_when_correct_title_missing() {
    let settings = RankSettings::default().prepare();

    let ranked = rank_torrent(
        "Movie.Title.2023.1080p.BluRay.x264-GROUP",
        "c08a9ee8ce3a5c2c08865e2b05406273cabc97e7",
        "",
        &HashMap::new(),
        &settings,
    )
    .expect("empty correct_title should not trigger similarity failure");

    assert_eq!(ranked.lev_ratio, 0.0);
}

#[test]
fn test_check_fetch_required_short_circuits_like_rtn() {
    let settings = RankSettings {
        require: vec!["MustFetch".into()],
        exclude: vec!["1080p".into()],
        ..RankSettings::default()
    }
    .prepare();

    let data = riven_rank::parse("Movie.Title.MustFetch.1080p.BluRay.x264-GROUP");
    let (fetch, failed) = check_fetch(&data, &settings);

    assert!(fetch);
    assert!(failed.is_empty());
}
