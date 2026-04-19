use riven_rank::parse;

#[test]
fn test_date_ymd() {
    let data = parse("Show.Title.2023.05.15.720p.WEB-DL");
    assert_eq!(data.date, Some("2023-05-15".into()));
}

#[test]
fn test_date_dmy() {
    let data = parse("Show.Title.15.05.2023.720p.WEB-DL");
    assert_eq!(data.date, Some("2023-05-15".into()));
}

#[test]
fn test_date_compact() {
    let data = parse("Show.Title.20230515.720p.WEB-DL");
    assert_eq!(data.date, Some("2023-05-15".into()));
}

#[test]
fn test_ptt_date_corpus_sample_direct() {
    let cases = [
        (
            "Indias Best Dramebaaz 2 Ep 19 (13 Feb 2016) HDTV x264-AquoTube",
            Some("2016-02-13"),
        ),
        (
            "SIX.S01E05.400p.229mb.hdtv.x264-][ Collateral ][ 16-Feb-2017 mp4",
            Some("2017-02-16"),
        ),
        (
            "WWE Smackdown - 11/21/17 - 21st November 2017 - Full Show",
            Some("2017-11-21"),
        ),
        (
            "WWE RAW 9th Dec 2019 WEBRip h264-TJ [TJET]",
            Some("2019-12-09"),
        ),
        ("wwf.raw.is.war.18.09.00.avi", Some("2000-09-18")),
        (
            "Dorcel.25.09.03.Joe.Blogs.XXX.2160p.MP4-P2P",
            Some("2025-09-03"),
        ),
        (
            "11 22 63 - Temporada 1 [HDTV][Cap.103][Español Castellano]",
            None,
        ),
        ("September 30 1955 1977 1080p BluRay", None),
        ("11-11-11.2011.1080p.BluRay.x264.DTS-FGT", None),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.date.as_deref(), expected, "{input}");
    }
}
