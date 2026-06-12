use std::collections::HashMap;

use riven_rank::rank::{check_fetch, scores::get_rank};
use riven_rank::{QualityProfile, RankSettings, RankingModel, parse, rank_torrent};

#[test]
fn test_rank_torrent_skips_similarity_when_correct_title_missing() {
    let settings = RankSettings::default().prepare();

    let ranked = rank_torrent(
        "Movie.Title.2023.1080p.BluRay.x264-GROUP",
        "c08a9ee8ce3a5c2c08865e2b05406273cabc97e7",
        "",
        None,
        &HashMap::new(),
        &settings,
    )
    .expect("empty correct_title should not trigger similarity failure");

    assert_eq!(ranked.lev_ratio, 0.0);
}

#[test]
fn test_rank_torrent_accepts_release_tagged_with_item_country() {
    let settings = RankSettings::default().prepare();

    // "Top Gear UK" scores 0.84 against "Top Gear" — below the 0.85
    // threshold — but the UK tag matches the item's own country (TVDB
    // alpha-3 "gbr"), so similarity is also computed with the tag stripped.
    let ranked = rank_torrent(
        "Top.Gear.UK.S05.1080p.WEB.H264-GROUP",
        "c08a9ee8ce3a5c2c08865e2b05406273cabc97e7",
        "Top Gear",
        Some("gbr"),
        &HashMap::new(),
        &settings,
    )
    .expect("UK-tagged release should pass similarity for a GB item");

    assert_eq!(ranked.lev_ratio, 1.0);
}

#[test]
fn test_rank_torrent_rejects_release_tagged_with_other_country() {
    let settings = RankSettings::default().prepare();

    // The US tag does not match the GB item, so no stripping happens and
    // "Top Gear US" fails similarity against "Top Gear".
    let result = rank_torrent(
        "Top.Gear.US.S05.1080p.WEB.H264-GROUP",
        "c08a9ee8ce3a5c2c08865e2b05406273cabc97e7",
        "Top Gear",
        Some("gbr"),
        &HashMap::new(),
        &settings,
    );

    assert!(matches!(
        result,
        Err(riven_rank::rank::RankError::TitleSimilarity { .. })
    ));
}

#[test]
fn test_rank_torrent_without_item_country_keeps_strict_similarity() {
    let settings = RankSettings::default().prepare();

    let result = rank_torrent(
        "Top.Gear.UK.S05.1080p.WEB.H264-GROUP",
        "c08a9ee8ce3a5c2c08865e2b05406273cabc97e7",
        "Top Gear",
        None,
        &HashMap::new(),
        &settings,
    );

    assert!(matches!(
        result,
        Err(riven_rank::rank::RankError::TitleSimilarity { .. })
    ));
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

#[test]
fn test_ultra_hd_profile_prefers_premium_4k_disc_release() {
    let settings = QualityProfile::UltraHd.base_settings().prepare();
    let model = RankingModel::default();

    let premium = parse("Movie.Title.2024.2160p.BluRay.REMUX.DV.HDR.HEVC.TrueHD.Atmos-GROUP");
    let fallback = parse("Movie.Title.2024.2160p.WEB-DL.SDR.HEVC.DDP5.1-GROUP");

    let (premium_fetch, premium_failed) = check_fetch(&premium, &settings);
    let (fallback_fetch, fallback_failed) = check_fetch(&fallback, &settings);
    let (premium_rank, _) = get_rank(&premium, &settings, &model);
    let (fallback_rank, _) = get_rank(&fallback, &settings, &model);

    assert!(
        premium_fetch,
        "premium release rejected: {premium_failed:?}"
    );
    assert!(
        fallback_fetch,
        "fallback release rejected: {fallback_failed:?}"
    );
    assert!(premium_rank > fallback_rank);
}

#[test]
fn test_hd_profile_prefers_remux_over_hdtv_fallback() {
    let settings = QualityProfile::Hd.base_settings().prepare();
    let model = RankingModel::default();

    let remux = parse("Movie.Title.2024.1080p.BluRay.REMUX.SDR.AVC.TrueHD.7.1-GROUP");
    let hdtv = parse("Movie.Title.2024.1080p.HDTV.SDR.AVC.AAC2.0-GROUP");

    let (remux_fetch, remux_failed) = check_fetch(&remux, &settings);
    let (hdtv_fetch, hdtv_failed) = check_fetch(&hdtv, &settings);
    let (remux_rank, _) = get_rank(&remux, &settings, &model);
    let (hdtv_rank, _) = get_rank(&hdtv, &settings, &model);

    assert!(remux_fetch, "remux rejected: {remux_failed:?}");
    assert!(hdtv_fetch, "hdtv fallback rejected: {hdtv_failed:?}");
    assert!(remux_rank > hdtv_rank);
}

#[test]
fn test_standard_profile_prefers_good_720p_source_over_480p_rip() {
    let settings = QualityProfile::Standard.base_settings().prepare();
    let model = RankingModel::default();

    let preferred = parse("Movie.Title.2024.720p.WEB-DL.SDR.AVC.DDP5.1-GROUP");
    let fallback = parse("Movie.Title.2024.480p.HDRip.SDR.AVC.MP3-GROUP");

    let (preferred_fetch, preferred_failed) = check_fetch(&preferred, &settings);
    let (fallback_fetch, fallback_failed) = check_fetch(&fallback, &settings);
    let (preferred_rank, _) = get_rank(&preferred, &settings, &model);
    let (fallback_rank, _) = get_rank(&fallback, &settings, &model);

    assert!(
        preferred_fetch,
        "preferred standard release rejected: {preferred_failed:?}"
    );
    assert!(
        fallback_fetch,
        "standard fallback rejected: {fallback_failed:?}"
    );
    assert!(preferred_rank > fallback_rank);
}
