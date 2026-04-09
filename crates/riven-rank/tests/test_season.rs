use riven_rank::parse;

#[test]
fn test_season_s01() {
    let data = parse("Show.Title.S01E01.720p.WEB-DL");
    assert_eq!(data.seasons, vec![1]);
}

#[test]
fn test_season_s01_s02_range() {
    let data = parse("Show.Title.S01-S03.720p.WEB-DL");
    assert_eq!(data.seasons, vec![1, 2, 3]);
}

#[test]
fn test_season_word() {
    let data = parse("Show Title Season 3 720p WEB-DL");
    assert_eq!(data.seasons, vec![3]);
}

#[test]
fn test_season_ordinal() {
    let data = parse("Show Title 2nd Season 720p WEB-DL");
    assert_eq!(data.seasons, vec![2]);
}

#[test]
fn test_season_crossref() {
    let data = parse("Show.Title.2x05.720p");
    assert_eq!(data.seasons, vec![2]);
}

#[test]
fn test_season_multiple() {
    let data = parse("Show Title Seasons 1,2,3 720p WEB-DL");
    assert!(data.seasons.contains(&1));
    assert!(data.seasons.contains(&2));
    assert!(data.seasons.contains(&3));
}

#[test]
fn test_season_dot_separated() {
    // "Season.1" with dot separator — was broken before fix
    let data = parse("Mr.Robot.Season.1.Complete.720p.WEB-DL");
    assert_eq!(data.parsed_title, "Mr Robot");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.quality, Some("WEB-DL".into()));
}

#[test]
fn test_season_se_episode_standard() {
    let data = parse("Mr.Robot.S01E01.720p.BluRay.x264-REWARD");
    assert_eq!(data.parsed_title, "Mr Robot");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![1]);
    assert_eq!(data.quality, Some("BluRay".into()));
}

#[test]
fn test_season_pack_s_format() {
    let data = parse("Mr.Robot.S04.1080p.WEB-DL.DD5.1.H264-RARBG");
    assert_eq!(data.parsed_title, "Mr Robot");
    assert_eq!(data.seasons, vec![4]);
    assert_eq!(data.quality, Some("WEB-DL".into()));
}

#[test]
fn test_season_s01e01() {
    let data = parse("Show.Title.S01E01.720p.HDTV.x264-GROUP");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![1]);
}

#[test]
fn test_season_s03e17() {
    let data = parse("Gotham S03E17 XviD-AFG");
    assert_eq!(data.seasons, vec![3]);
    assert_eq!(data.episodes, vec![17]);
}

#[test]
fn test_season_s02e01() {
    let data = parse("The Vet Life S02E01 Dunk-A-Doctor 1080p ANPL WEB-DL AAC2 0 H 264-RTN");
    assert_eq!(data.seasons, vec![2]);
    assert_eq!(data.episodes, vec![1]);
}

#[test]
fn test_season_s07e04() {
    let data = parse("The Blacklist S07E04 (1080p AMZN WEB-DL x265 HEVC 10bit EAC-3 5.1)[Bandi]");
    assert_eq!(data.seasons, vec![7]);
    assert_eq!(data.episodes, vec![4]);
}

#[test]
fn test_season_word_format() {
    let data = parse("Show Title Season 3 Episode 5 720p");
    assert_eq!(data.seasons, vec![3]);
    assert_eq!(data.episodes, vec![5]);
}

#[test]
fn test_season_s01_complete() {
    let data = parse("Monk.S01.1080p.AMZN.WEBRip.DDP2.0.x264-AJP69[rartv]");
    assert_eq!(data.seasons, vec![1]);
    assert!(data.episodes.is_empty());
}

#[test]
fn test_season_s08e01() {
    let data = parse("S.W.A.T.2017.S08E01.720p.HDTV.x264-SYNCOPY[TGx]");
    assert_eq!(data.seasons, vec![8]);
    assert_eq!(data.episodes, vec![1]);
}

#[test]
fn test_season_s09e13() {
    let data = parse("Pawn.Stars.S09E13.1080p.HEVC.x265-MeGusta");
    assert_eq!(data.seasons, vec![9]);
    assert_eq!(data.episodes, vec![13]);
}

#[test]
fn test_season_s02e07() {
    let data = parse(
        "Game of Thrones - S02E07 - A Man Without Honor [2160p] [HDR] [5.1, 7.1, 5.1] [ger, eng, eng] [Vio].mkv",
    );
    assert_eq!(data.seasons, vec![2]);
    assert_eq!(data.episodes, vec![7]);
}

#[test]
fn test_ptt_season_corpus_sample() {
    let cases = [
        (
            "24 Season 1-8 Complete with Subtitles",
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        ),
        ("The Expanse Complete Seasons 01 & 02 1080p", vec![1, 2]),
        ("Naruto Shippuden Season 1:11", vec![1]),
        (
            "Doctor Who S01--S07--Complete with holiday episodes",
            vec![1, 2, 3, 4, 5, 6, 7],
        ),
        (
            "One.Piece.S004E111.Dash.For.a.Miracle!.Alabasta.Animal.Land!.1080p.NF.WEB-DL.DDP2.0.x264-KQRM.mkv",
            vec![4],
        ),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.seasons, expected, "{input}");
    }
}
