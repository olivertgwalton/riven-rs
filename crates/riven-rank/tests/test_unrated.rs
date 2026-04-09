use riven_rank::parse;

#[test]
fn test_unrated() {
    let data = parse("Movie.Title.UNRATED.1080p.BluRay");
    assert!(data.unrated);
}

#[test]
fn test_ptt_unrated_corpus_sample_direct() {
    let cases = [
        (
            "Identity.Thief.2013.Vostfr.UNRATED.BluRay.720p.DTS.x264-Nenuko",
            true,
            false,
        ),
        (
            "Charlie.les.filles.lui.disent.merci.2007.UNCENSORED.TRUEFRENCH.DVDRiP.AC3.Libe",
            false,
            true,
        ),
        (
            "Have I Got News For You S53E02 EXTENDED 720p HDTV x264-QPEL",
            false,
            false,
        ),
    ];

    for (raw, expected_unrated, expected_uncensored) in cases {
        let data = parse(raw);
        assert_eq!(data.unrated, expected_unrated, "{raw}");
        assert_eq!(data.uncensored, expected_uncensored, "{raw}");
    }
}
