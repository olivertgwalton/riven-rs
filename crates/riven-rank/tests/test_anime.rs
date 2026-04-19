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

#[test]
fn test_ptt_missing_anime_groups_are_detected() {
    for title in [
        "Sword.Art.Online.Alternative.S01.v2.1080p.Blu-Ray.10-Bit.Dual-Audio.LPCM.x265-iAHD",
        "[GHOST] Anime Title - 01 [1080p]",
        "[EDGE] Anime Title - 01 [1080p]",
        "Anime.Title.01.1080p-Soldado",
        "Anime.Title.01.1080p-E-D",
    ] {
        let data = parse(title);
        assert!(data.anime, "failed for {title}");
    }
}

#[test]
fn test_short_anime_group_words_do_not_mark_plain_movies() {
    let data = parse("The MC 2024 1080p WEB-DL x264-GROUP");
    assert!(!data.anime);

    let data = parse("Ghost.Story.2024.1080p.WEB-DL.x264-GROUP");
    assert!(!data.anime);
}
