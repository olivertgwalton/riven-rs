use riven_rank::parse;

#[test]
fn test_episode_s01e05() {
    let data = parse("Show.Title.S01E05.720p.WEB-DL");
    assert_eq!(data.episodes, vec![5]);
}

#[test]
fn test_episode_range_e01_e03() {
    let data = parse("Show.Title.S01E01-E03.720p.WEB-DL");
    assert_eq!(data.episodes, vec![1, 2, 3]);
}

#[test]
fn test_episode_standalone_e10() {
    let data = parse("Show.Title.E10.720p.WEB-DL");
    assert_eq!(data.episodes, vec![10]);
}

#[test]
fn test_episode_word() {
    let data = parse("Show Title Episode 15 720p WEB-DL");
    assert_eq!(data.episodes, vec![15]);
}

#[test]
fn test_episode_crossref() {
    let data = parse("Show.Title.2x05.720p");
    assert_eq!(data.episodes, vec![5]);
}

#[test]
fn test_episode_consecutive_s01e01e02e03() {
    let data = parse("Show.Title.S01E01E02E03.720p.WEB-DL");
    assert_eq!(data.episodes, vec![1, 2, 3]);
}

#[test]
fn test_episode_of_pattern() {
    let data = parse("Show Title (16 of 26) 720p WEB-DL");
    assert_eq!(data.episodes, vec![16]);
}

#[test]
fn test_episode_code_hex() {
    let data = parse("[SubsPlease] Anime Title - 01 [5E46AC39].mkv");
    assert_eq!(data.episode_code, Some("5E46AC39".into()));
}

#[test]
fn test_episode_code_numeric() {
    let data = parse("[Group] Anime Title - 01 (12345678).mkv");
    assert_eq!(data.episode_code, Some("12345678".into()));
}

#[test]
fn test_episode_code_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay");
    assert_eq!(data.episode_code, None);
}

#[test]
fn test_episode_standalone_e01() {
    let data = parse("The Boys S04E01 E02 E03 4k to 1080p AMZN WEBrip x265 DDP5 1 D0c");
    assert_eq!(data.seasons, vec![4]);
    assert!(data.episodes.contains(&1));
}

#[test]
fn test_ptt_episode_corpus_sample() {
    let cases = [
        (
            "Pokemon Black & White E10 - E17 [CW] AVI",
            (10..=17).collect::<Vec<_>>(),
        ),
        (
            "Marvel's.Agents.of.S.H.I.E.L.D.S02E01-03.Shadows.1080p.WEB-DL.DD5.1",
            vec![1, 2, 3],
        ),
        ("The Office S07E25+E26 Search Committee.mp4", vec![25, 26]),
        (
            "The Simpsons E1-200 1080p BluRay x265 HEVC 10bit AAC 5.1 Tigole",
            (1..=200).collect::<Vec<_>>(),
        ),
        ("[Eng Sub] Rebirth Ep #36 [8CF3ADFA].mkv", vec![36]),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.episodes, expected, "{input}");
    }
}
