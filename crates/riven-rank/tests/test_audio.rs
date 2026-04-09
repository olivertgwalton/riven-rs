use riven_rank::parse;

#[test]
fn test_audio_dts_lossless() {
    let data = parse("Movie.Title.DTS-HD.MA.5.1.BluRay");
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
}

#[test]
fn test_audio_dts_x() {
    let data = parse("Movie.Title.DTS-X.BluRay");
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
}

#[test]
fn test_audio_dts_lossy() {
    let data = parse("Movie.Title.DTS.BluRay");
    assert!(data.audio.contains(&"DTS Lossy".to_string()));
}

#[test]
fn test_audio_atmos() {
    let data = parse("Movie.Title.Atmos.BluRay");
    assert!(data.audio.contains(&"Atmos".to_string()));
}

#[test]
fn test_audio_truehd() {
    let data = parse("Movie.Title.TrueHD.BluRay");
    assert!(data.audio.contains(&"TrueHD".to_string()));
}

#[test]
fn test_audio_flac() {
    let data = parse("Movie.Title.FLAC.BluRay");
    assert!(data.audio.contains(&"FLAC".to_string()));
}

#[test]
fn test_audio_dd_plus() {
    let data = parse("Movie.Title.DDP5.1.WEB-DL");
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_dd_plus_eac3() {
    let data = parse("Movie.Title.EAC3.WEB-DL");
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_dolby_digital() {
    let data = parse("Movie.Title.AC3.BluRay");
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
}

#[test]
fn test_audio_aac() {
    let data = parse("Movie.Title.AAC.WEB-DL");
    assert!(data.audio.contains(&"AAC".to_string()));
}

#[test]
fn test_audio_pcm() {
    let data = parse("Movie.Title.LPCM.BluRay");
    assert!(data.audio.contains(&"PCM".to_string()));
}

#[test]
fn test_audio_opus() {
    let data = parse("Movie.Title.OPUS.WEB");
    assert!(data.audio.contains(&"OPUS".to_string()));
}

#[test]
fn test_audio_mp3() {
    let data = parse("Movie.Title.MP3.DVDRip");
    assert!(data.audio.contains(&"MP3".to_string()));
}

#[test]
fn test_audio_hq_clean() {
    let data = parse("Movie.Title.HQ.Clean.Audio");
    assert!(data.audio.contains(&"HQ Clean Audio".to_string()));
}

#[test]
fn test_audio_dts_lossless_dts_hd_ma() {
    let data = parse("Gold 2016 1080p BluRay DTS-HD MA 5 1 x264-HDH");
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
}

#[test]
fn test_audio_dolby_digital_ac3() {
    let data = parse("A Dog's Purpose 2016 BDRip 720p X265 Ac3-GANJAMAN");
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
}

#[test]
fn test_audio_none_when_absent() {
    let data = parse("Detroit.2017.BDRip.MD.GERMAN.x264-SPECTRE");
    assert!(data.audio.is_empty());
}

#[test]
fn test_audio_dolby_digital_plus_eac3() {
    let data = parse("The Blacklist S07E04 (1080p AMZN WEB-DL x265 HEVC 10bit EAC-3 5.1)[Bandi]");
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_dolby_digital_plus_eac3_variant() {
    let data = parse("Condor.S01E03.1080p.WEB-DL.x265.10bit.EAC3.6.0-Qman[UTR].mkv");
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_atmos_truehd() {
    let data =
        parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert!(data.audio.contains(&"Atmos".to_string()));
    assert!(data.audio.contains(&"TrueHD".to_string()));
}

#[test]
fn test_audio_ddp_from_ddp() {
    let data = parse("Monk.S01.1080p.AMZN.WEBRip.DDP2.0.x264-AJP69[rartv]");
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_dts_lossless_with_title() {
    let data =
        parse("The Shawshank Redemption 1994.MULTi.1080p.Blu-ray.DTS-HDMA.5.1.HEVC-DDR[EtHD]");
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
}

#[test]
fn test_audio_dts_lossless_oppenheimer() {
    let data = parse("Oppenheimer.2023.BluRay.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR.mkv");
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
}

#[test]
fn test_audio_dts_lossy_hr() {
    let data = parse("Sleepy.Hollow.1999.BluRay.1080p.2Audio.DTS-HD.HR.5.1.x265.10bit-ALT");
    assert!(data.audio.contains(&"DTS Lossy".to_string()));
}

#[test]
fn test_audio_dts_lossy_and_dolby_digital() {
    let data = parse("Indiana Jones and the Last Crusade 1989 BluRay 1080p DTS AC3 x264-MgB");
    assert!(data.audio.contains(&"DTS Lossy".to_string()));
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
}

#[test]
fn test_audio_dolby_digital_from_ac3_dash() {
    let data = parse("Retroactive 1997 BluRay 1080p AC-3 HEVC-d3g");
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
}

#[test]
fn test_audio_dd_plus_from_ddp_number() {
    let data = parse("Movie.Title.2023.1080p.DDP5.1.x265-GROUP");
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_dolby_digital_plus_ddp51() {
    let data = parse(
        "Madame Web (2024) 1080p HINDI ENGLISH 10bit AMZN WEBRip DDP5 1 x265 HEVC - PSA Shadow",
    );
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_dolby_digital_from_ac3_standalone() {
    let data = parse("Movie.Title.2023.1080p.BluRay.AC3-GROUP");
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
}

#[test]
fn test_ptt_audio_corpus_sample() {
    let cases = [
        (
            "Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2",
            vec!["DTS Lossy"],
        ),
        (
            "Gold 2016 1080p BluRay DTS-HD MA 5 1 x264-HDH",
            vec!["DTS Lossless"],
        ),
        (
            "Monk.S01E01E02.1080p.WEB-DL.DD2.0.x264-AJP69.mkv",
            vec!["Dolby Digital"],
        ),
        (
            "Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR",
            vec!["Atmos", "TrueHD"],
        ),
        (
            "The Flash 2023 WEBRip 1080p DTS DD+ 5.1 Atmos x264-MgB",
            vec!["DTS Lossy", "Atmos", "Dolby Digital Plus"],
        ),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(
            data.audio,
            expected.iter().map(|v| v.to_string()).collect::<Vec<_>>(),
            "{input}"
        );
    }
}
