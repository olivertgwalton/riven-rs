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

#[test]
fn test_ptt_extras_corpus_sample_direct() {
    let cases: [(&str, &[&str]); 4] = [
        (
            "Madame Web 2024 1080p WEBRip 1400MB DD 5.1 x264 Sample-GalaxyRG[TGx]",
            &["Sample"],
        ),
        (
            "Madame Web Sample 2024 1080p WEBRip 1400MB DD 5.1 x264-GalaxyRG[TGx]",
            &[],
        ),
        (
            "Madame Web Sample 1080p WEBRip 1400MB DD 5.1 x264-GalaxyRG[TGx]",
            &["Sample"],
        ),
        (
            "AVATAR.Featurette.Creating.the.World.of.Pandora.1080p.H264.ITA.AC3.ENGAAC.PappaMux.mkv",
            &["Featurette"],
        ),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        let expected = expected.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert_eq!(data.extras, expected, "{input}");
    }
}
