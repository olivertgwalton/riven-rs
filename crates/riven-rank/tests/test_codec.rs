use riven_rank::parse;

#[test]
fn test_codec_avc() {
    let data = parse("Movie.Title.x264.BluRay");
    assert_eq!(data.codec, Some("avc".into()));
}

#[test]
fn test_codec_avc_h264() {
    let data = parse("Movie.Title.h264.BluRay");
    assert_eq!(data.codec, Some("avc".into()));
}

#[test]
fn test_codec_hevc() {
    let data = parse("Movie.Title.x265.BluRay");
    assert_eq!(data.codec, Some("hevc".into()));
}

#[test]
fn test_codec_hevc_h265() {
    let data = parse("Movie.Title.H.265.BluRay");
    assert_eq!(data.codec, Some("hevc".into()));
}

#[test]
fn test_codec_hevc_10() {
    let data = parse("Movie.Title.HEVC10.BluRay");
    assert_eq!(data.codec, Some("hevc".into()));
}

#[test]
fn test_codec_xvid() {
    let data = parse("Movie.Title.XviD.DVDRip");
    assert_eq!(data.codec, Some("xvid".into()));
}

#[test]
fn test_codec_divx() {
    let data = parse("Movie.Title.DivX.DVDRip");
    assert_eq!(data.codec, Some("xvid".into()));
}

#[test]
fn test_codec_av1() {
    let data = parse("Movie.Title.AV1.WEB-DL");
    assert_eq!(data.codec, Some("av1".into()));
}

#[test]
fn test_codec_mpeg() {
    let data = parse("Movie.Title.mpeg2.DVD");
    assert_eq!(data.codec, Some("mpeg".into()));
}

#[test]
fn test_codec_avc_x264() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.codec, Some("avc".to_string()));
}

#[test]
fn test_codec_avc_h264_space() {
    let data = parse("Movie.Title.2023.1080p.H264.WEB-DL-GROUP");
    assert_eq!(data.codec, Some("avc".to_string()));
}

#[test]
fn test_codec_avc_h264_dot() {
    let data = parse("Movie.Title.2023.1080p.H.264.WEB-DL-GROUP");
    assert_eq!(data.codec, Some("avc".to_string()));
}

#[test]
fn test_codec_hevc_x265() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay.DDP5.1.x265.10bit-GalaxyRG265[TGx]");
    assert_eq!(data.codec, Some("hevc".to_string()));
}

#[test]
fn test_codec_none_when_absent() {
    let data = parse("Movie.Title.2023.1080p.BluRay-GROUP");
    assert_eq!(data.codec, None);
}

#[test]
fn test_codec_divx_as_xvid() {
    let data = parse("Movie.Title.DivX-GROUP");
    assert_eq!(data.codec, Some("xvid".to_string()));
}

#[test]
fn test_ptt_codec_corpus_sample() {
    let cases = [
        (
            "Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2",
            Some("hevc"),
            None,
        ),
        (
            "doctor_who_2005.8x12.death_in_heaven.720p_hdtv_x264-fov",
            Some("avc"),
            None,
        ),
        (
            "The Vet Life S02E01 Dunk-A-Doctor 1080p ANPL WEB-DL AAC2 0 H 264-RTN",
            Some("avc"),
            None,
        ),
        ("Gotham S03E17 XviD-AFG", Some("xvid"), None),
        (
            "Jimmy Kimmel 2017 05 03 720p HDTV DD5 1 MPEG2-CTL",
            Some("mpeg"),
            None,
        ),
        (
            "[Anime Time] Re Zero kara Hajimeru Isekai Seikatsu (Season 2 Part 1) [1080p][HEVC10bit x265][Multi Sub]",
            Some("hevc"),
            Some("10bit"),
        ),
        (
            "[naiyas] Fate Stay Night - Unlimited Blade Works Movie [BD 1080P HEVC10 QAACx2 Dual Audio]",
            Some("hevc"),
            Some("10bit"),
        ),
        ("[DB]_Bleach_264_[012073FE].avi", None, None),
        ("[DB]_Bleach_265_[B4A04EC9].avi", None, None),
        (
            "Mad.Max.Fury.Road.2015.1080p.BluRay.DDP5.1.x265.10bit-GalaxyRG265[TGx]",
            Some("hevc"),
            Some("10bit"),
        ),
    ];

    for (raw, expected_codec, expected_bit_depth) in cases {
        let data = parse(raw);
        assert_eq!(data.codec.as_deref(), expected_codec, "{raw}");
        assert_eq!(data.bit_depth.as_deref(), expected_bit_depth, "{raw}");
    }
}
