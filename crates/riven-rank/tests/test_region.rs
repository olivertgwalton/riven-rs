use riven_rank::parse;

#[test]
fn test_region_r1() {
    let data = parse("Movie.Title.R1.DVDRip");
    assert_eq!(data.region, Some("R1".into()));
}

#[test]
fn test_region_pal() {
    let data = parse("Movie.Title.PAL.DVD");
    assert_eq!(data.region, Some("PAL".into()));
}

#[test]
fn test_region_ntsc() {
    let data = parse("Movie.Title.NTSC.DVD");
    assert_eq!(data.region, Some("NTSC".into()));
}

#[test]
fn test_ptt_region_corpus_sample_direct() {
    let cases = [
        ("Welcome to New York 2014 R5 XviD AC3-SUPERFAST", Some("R5")),
        (
            "[Coalgirls]_Code_Geass_R2_06_(1920x1080_Blu-ray_FLAC)_[F8C7FE25].mkv",
            Some("R2"),
        ),
        (
            "[JySzE] Naruto [v2] [R2J] [VFR] [Dual Audio] [Complete] [Extras] [x264]",
            Some("R2J"),
        ),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.region.as_deref(), expected, "{raw}");
    }
}
