use riven_rank::parse;

#[test]
fn test_resolution_1080p_bluray() {
    let data = parse("Annabelle.2014.1080p.PROPER.HC.WEBRip.x264.AAC.2.0-RARBG");
    assert_eq!(data.resolution, "1080p");
}

#[test]
fn test_resolution_720p_uppercase() {
    let data = parse("UFC 187 PPV 720P HDTV X264-KYR");
    assert_eq!(data.resolution, "720p");
}

#[test]
fn test_resolution_2160p_from_uhd_4k() {
    let data = parse("The Smurfs 2 2013 COMPLETE FULL BLURAY UHD (4K) - IPT EXCLUSIVE");
    assert_eq!(data.resolution, "2160p");
}

#[test]
fn test_resolution_2160p_explicit() {
    let data = parse("Joker.2019.2160p.4K.BluRay.x265.10bit.HDR.AAC5.1");
    assert_eq!(data.resolution, "2160p");
}

#[test]
fn test_resolution_2160p_from_3840_width() {
    let data =
        parse("[Beatrice-Raws] Evangelion 3.333 You Can (Not) Redo [BDRip 3840x1632 HEVC TrueHD]");
    assert_eq!(data.resolution, "2160p");
}

#[test]
fn test_resolution_1080p_from_1920_width() {
    let data = parse(
        "[Erai-raws] Evangelion 3.0 You Can (Not) Redo - Movie [1920x960][Multiple Subtitle].mkv",
    );
    assert_eq!(data.resolution, "1080p");
}

#[test]
fn test_resolution_720p_from_1280_width() {
    let data = parse("[JacobSwaggedUp] Kizumonogatari I: Tekketsu-hen (BD 1280x544) [MP4 Movie]");
    assert_eq!(data.resolution, "720p");
}

#[test]
fn test_resolution_unknown_when_absent() {
    let data = parse("Some.Random.Title.Without.Resolution-GROUP");
    assert_eq!(data.resolution, "unknown");
}

#[test]
fn test_resolution_480p() {
    let data = parse("Movie.Title.2010.480p.DVDRip.x264-GROUP");
    assert_eq!(data.resolution, "480p");
}

#[test]
fn test_resolution_576p() {
    let data = parse("Movie.Title.576p.BRRip.x264");
    assert_eq!(data.resolution, "576p");
}

#[test]
fn test_resolution_last_generic_match_wins() {
    let data = parse("The Boys S04E01 E02 E03 4k to 1080p AMZN WEBrip x265 DDP5 1 D0c");
    assert_eq!(data.resolution, "1080p");
}

#[test]
fn test_resolution_qhd_1440p() {
    let data = parse("Movie.Title.QHD.WEB-DL.x264");
    assert_eq!(data.resolution, "1440p");
}

#[test]
fn test_resolution_fhd_1080p() {
    let data = parse("Movie.Title.Full.HD.BluRay");
    assert_eq!(data.resolution, "1080p");
}

#[test]
fn test_resolution_240p() {
    let data = parse("Movie.Title.240p.WEB");
    assert_eq!(data.resolution, "240p");
}

#[test]
fn test_resolution_360p() {
    let data = parse("Movie.Title.360p.WEB");
    assert_eq!(data.resolution, "360p");
}

#[test]
fn test_resolution_2160p_bd() {
    let data = parse("Movie BD 2160p HEVC");
    assert_eq!(data.resolution, "2160p");
}

#[test]
fn test_resolution_1080p_with_4k_remastered() {
    let data = parse("Batman Returns 1992 4K Remastered BluRay 1080p DTS AC3 x264-MgB");
    assert_eq!(data.resolution, "1080p");
}

#[test]
fn test_resolution_720p_last_bracket_match() {
    let data = parse("Life After People (2008) [1080P.BLURAY] [720p] [BluRay] [YTS.MX]");
    assert_eq!(data.resolution, "720p");
}

#[test]
fn test_resolution_1080p_standard() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.resolution, "1080p");
}

#[test]
fn test_resolution_2160p_from_4k() {
    let data = parse("Movie.Title.2023.4K.BluRay.x265-GROUP");
    assert_eq!(data.resolution, "2160p");
}

#[test]
fn test_resolution_720p_standard() {
    let data = parse("Movie.Title.2023.720p.WEB-DL.x264-GROUP");
    assert_eq!(data.resolution, "720p");
}

#[test]
fn test_ptt_resolution_corpus_sample_direct_more() {
    let cases = [
        ("UFC 187 PPV 720i HDTV X264-KYR", "720i"),
        (
            "IT Chapter Two.2019.7200p.AMZN WEB-DL.H264.[Eng Hin Tam Tel]DDP 5.1.MSubs.D0T.Telly",
            "720p",
        ),
        ("Dumbo (1941) BRRip XvidHD 10800p-NPW", "1080p"),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.resolution, expected, "{raw}");
    }
}
