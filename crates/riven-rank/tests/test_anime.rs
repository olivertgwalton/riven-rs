use riven_rank::parse;

#[test]
fn test_anime_by_episode_code() {
    let data = parse("[SubsPlease] Anime Title - 01 [5E46AC39].mkv");
    assert!(data.anime);
}

#[test]
fn test_anime_by_group() {
    let data = parse("[HorribleSubs] Anime Title - 01 [1080p].mkv");
    assert!(data.anime);
}

#[test]
fn test_not_anime() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert!(!data.anime);
}
