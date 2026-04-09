use riven_rank::parse;

#[test]
fn test_group_dash() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.group, Some("GROUP".into()));
}

#[test]
fn test_group_bracket() {
    let data = parse("[SubsPlease] Anime Title - 01.mkv");
    assert_eq!(data.group, Some("SubsPlease".into()));
}

#[test]
fn test_group_not_codec() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x265");
    // x265 should not be detected as group
    assert_ne!(data.group, Some("x265".into()));
}

#[test]
fn test_group_dash_format() {
    let data = parse("Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2");
    assert_eq!(data.group, Some("HD2".to_string()));
}

#[test]
fn test_group_dash_complex() {
    let data = parse("The Vet Life S02E01 Dunk-A-Doctor 1080p ANPL WEB-DL AAC2 0 H 264-RTN");
    assert_eq!(data.group, Some("RTN".to_string()));
}

#[test]
fn test_group_dash_with_extension() {
    let data = parse("Monk.S01E01E02.1080p.WEB-DL.DD2.0.x264-AJP69.mkv");
    assert_eq!(data.group, Some("AJP69".to_string()));
}

#[test]
fn test_group_framestor() {
    let data =
        parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert_eq!(data.group, Some("FraMeSToR".to_string()));
}

#[test]
fn test_group_bracket_format() {
    let data =
        parse("[Beatrice-Raws] Evangelion 3.333 You Can (Not) Redo [BDRip 3840x1632 HEVC TrueHD]");
    assert_eq!(data.group, Some("Beatrice-Raws".to_string()));
}

#[test]
fn test_group_megusta() {
    let data = parse("Pawn.Stars.S09E13.1080p.HEVC.x265-MeGusta");
    assert_eq!(data.group, Some("MeGusta".to_string()));
}

#[test]
fn test_group_smurf() {
    let data = parse("Bullet.Train.2022.2160p.AMZN.WEB-DL.x265.10bit.HDR10Plus.DDP5.1-SMURF");
    assert_eq!(data.group, Some("SMURF".to_string()));
}

#[test]
fn test_group_fgt() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.group, Some("FGT".to_string()));
}

#[test]
fn test_ptt_group_corpus_sample() {
    let cases = [
        (
            "Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2",
            Some("HD2"),
        ),
        ("Gold 2016 1080p BluRay DTS-HD MA 5 1 x264-HDH", Some("HDH")),
        ("[AnimeRG] One Punch Man - 09 [720p].mkv", Some("AnimeRG")),
        (
            "[ Torrent9.cz ] The.InBetween.S01E10.FiNAL.HDTV.XviD-EXTREME.avi",
            Some("EXTREME"),
        ),
        ("Power (2014) - S02E03.mp4", None),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.group.as_deref(), expected, "{input}");
    }
}

#[test]
fn test_ptt_group_corpus_sample_direct_more() {
    let cases = [
        (
            "The.Expanse.S05E02.720p.WEB.x264-Worldmkv.mkv",
            Some("Worldmkv"),
        ),
        (
            "The.Expanse.S05E02.PROPER.720p.WEB.h264-KOGi[rartv]",
            Some("KOGi"),
        ),
        (
            "The.Expanse.S05E02.1080p.AMZN.WEB.DDP5.1.x264-NTb[eztv.re].mp4",
            Some("NTb"),
        ),
        (
            "Western - L'homme qui n'a pas d'étoile-1955.Multi.DVD9",
            None,
        ),
        ("3-Nen D-Gumi Glass no Kamen - 13", None),
        (
            "[KNK E MMS Fansubs] Nisekoi - 20 Final [PT-BR].mkv",
            Some("KNK E MMS Fansubs"),
        ),
        (
            "[HD-ELITE.NET] -  The.Art.Of.The.Steal.2014.DVDRip.XviD.Dual.Aud",
            None,
        ),
        ("the-x-files-502.mkv", None),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.group.as_deref(), expected, "{raw}");
    }
}
