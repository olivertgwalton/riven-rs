use riven_rank::parse;

#[test]
fn test_quality_bluray() {
    let data = parse("Rogue One 2016 1080p BluRay x264.DTS-JYK");
    assert_eq!(data.quality, Some("BluRay".into()));
}

#[test]
fn test_quality_bluray_remux() {
    let data = parse("Avengers.Endgame.2019.2160p.UHD.BluRay.REMUX.HDR.HEVC.Atmos-EPSiLON");
    assert_eq!(data.quality, Some("BluRay REMUX".into()));
}

#[test]
fn test_quality_remux_bd() {
    let data = parse("Movie.Title.BD-REMUX.1080p.DTS-HD.MA");
    assert_eq!(data.quality, Some("BluRay REMUX".into()));
}

#[test]
fn test_quality_web_dl() {
    let data = parse("Movie.Title.2023.WEB-DL.1080p.x264");
    assert_eq!(data.quality, Some("WEB-DL".into()));
}

#[test]
fn test_quality_webrip() {
    let data = parse("Movie.Title.2023.WEBRip.720p.x264");
    assert_eq!(data.quality, Some("WEBRip".into()));
}

#[test]
fn test_quality_hdtv() {
    let data = parse("Movie.Title.HDTV.x264");
    assert_eq!(data.quality, Some("HDTV".into()));
}

#[test]
fn test_quality_dvdrip() {
    let data = parse("Movie.Title.DVDRip.x264");
    assert_eq!(data.quality, Some("DVDRip".into()));
}

#[test]
fn test_quality_cam() {
    let data = parse("Movie.Title.2023.CAM.x264");
    assert_eq!(data.quality, Some("CAM".into()));
}

#[test]
fn test_quality_telesync() {
    let data = parse("Movie.Title.2023.TS.x264");
    assert_eq!(data.quality, Some("TeleSync".into()));
}

#[test]
fn test_quality_telecine() {
    let data = parse("Movie.Title.2023.TC.x264");
    assert_eq!(data.quality, Some("TeleCine".into()));
}

#[test]
fn test_quality_screener() {
    let data = parse("Movie.Title.2023.SCR.x264");
    assert_eq!(data.quality, Some("SCR".into()));
}

#[test]
fn test_quality_bdrip() {
    let data = parse("Movie.Title.BDRip.x264");
    assert_eq!(data.quality, Some("BDRip".into()));
}

#[test]
fn test_quality_brrip() {
    let data = parse("Color.Of.Night.Unrated.DC.VostFR.BRrip.x264");
    assert_eq!(data.quality, Some("BRRip".into()));
}

#[test]
fn test_quality_hdrip() {
    let data = parse("Ghost In The Shell 2017 720p HC HDRip X264 AC3-EVO");
    assert_eq!(data.quality, Some("HDRip".into()));
}

#[test]
fn test_quality_web() {
    let data = parse("Movie.Title.WEB.x264");
    assert_eq!(data.quality, Some("WEB".into()));
}

#[test]
fn test_quality_pdtv() {
    let data = parse("Movie.Title.PDTV.x264");
    assert_eq!(data.quality, Some("PDTV".into()));
}

#[test]
fn test_quality_satrip() {
    let data = parse("Movie.Title.SATRip.x264");
    assert_eq!(data.quality, Some("SATRip".into()));
}

#[test]
fn test_quality_dvd() {
    let data = parse("Movie.Title.DVD.x264");
    assert_eq!(data.quality, Some("DVD".into()));
}

#[test]
fn test_quality_vhsrip() {
    let data = parse("Movie.Title.VHSRip.x264");
    assert_eq!(data.quality, Some("VHSRip".into()));
}

#[test]
fn test_quality_r5() {
    let data = parse("Movie.Title.R5.x264");
    assert_eq!(data.quality, Some("R5".into()));
}

#[test]
fn test_quality_webmux() {
    let data = parse("Movie.Title.WEBMUX.x264");
    assert_eq!(data.quality, Some("WEBMux".into()));
}

#[test]
fn test_quality_hdtvrip() {
    let data = parse("Rebecca.1940.720p.HDTVRip.HDCLUB");
    assert_eq!(data.quality, Some("HDTVRip".to_string()));
}

#[test]
fn test_quality_webdl() {
    let data = parse("The Vet Life S02E01 Dunk-A-Doctor 1080p ANPL WEB-DL AAC2 0 H 264-RTN");
    assert_eq!(data.quality, Some("WEB-DL".to_string()));
}

#[test]
fn test_quality_scr_dvdscr() {
    let data = parse("The.Shape.of.Water.2017.DVDScr.XVID.AC3.HQ.Hive-CM8");
    assert_eq!(data.quality, Some("SCR".to_string()));
}

#[test]
fn test_quality_ppvrip() {
    let data = parse("Cloudy With A Chance Of Meatballs 2 2013 720p PPVRip x264 AAC-FooKaS");
    assert_eq!(data.quality, Some("PPVRip".to_string()));
}

#[test]
fn test_quality_bdrip_bracket() {
    let data = parse(
        "[UsifRenegade] Cardcaptor Sakura [BD][Remastered][1080p][HEVC_10Bit][Dual] + Movies",
    );
    assert_eq!(data.quality, Some("BDRip".to_string()));
}

#[test]
fn test_quality_bdrip_bd_rm() {
    let data =
        parse("[UsifRenegade] Cardcaptor Sakura - 54 [BD-RM][1080p][x265_10Bit][Dual_AAC].mkv");
    assert_eq!(data.quality, Some("BDRip".to_string()));
}

#[test]
fn test_quality_hdrip_microhd() {
    let data = parse("Elvis & Nixon (MicroHD-1080p)");
    assert_eq!(data.quality, Some("HDRip".to_string()));
}

#[test]
fn test_quality_uhdrip() {
    let data = parse("Bohemian Rhapsody 2018.2160p.UHDrip.x265.HDR.DD+.5.1-DTOne");
    assert_eq!(data.quality, Some("UHDRip".to_string()));
}

#[test]
fn test_quality_bluray_uhd() {
    let data = parse("Blade.Runner.2049.2017.4K.UltraHD.BluRay.2160p.x264.TrueHD.Atmos");
    assert_eq!(data.quality, Some("BluRay".to_string()));
}

#[test]
fn test_quality_bluray_remux_explicit() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
}

#[test]
fn test_quality_bluray_remux_blu_ray_remux() {
    let data = parse("Warcraft 2016 1080p Blu-ray Remux AVC TrueHD Atmos-KRaLiMaRKo");
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
}

#[test]
fn test_quality_bluray_remux_uhd() {
    let data = parse("Joker.2019.UHD.BluRay.2160p.TrueHD.Atmos.7.1.HEVC.REMUX-JAT");
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
}

#[test]
fn test_quality_bluray_remux_bdremux() {
    let data = parse("Son of God 2014 HDR BDRemux 1080p.mkv");
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
}

#[test]
fn test_quality_bluray_remux_uhdremux() {
    let data = parse(
        "Peter Rabbit 2 [4K UHDremux][2160p][HDR10][DTS-HD 5.1 Castellano-TrueHD 7.1-Ingles+Subs][ES-EN]",
    );
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
}

#[test]
fn test_quality_scr_good_deeds() {
    let data = parse("Good Deeds 2012 SCR XViD-KiNGDOM");
    assert_eq!(data.quality, Some("SCR".to_string()));
}

#[test]
fn test_quality_webdlrip() {
    let data = parse("Звонок из прошлого / Kol / The Call (2020) WEB-DLRip | ViruseProject");
    assert_eq!(data.quality, Some("WEB-DLRip".to_string()));
}

#[test]
fn test_quality_vhs() {
    let data = parse("Мужчины без женщин (Альгимантас Видугирис) [1981, Драма, VHS]");
    assert_eq!(data.quality, Some("VHS".to_string()));
}

#[test]
fn test_quality_scr_hdscr() {
    let data = parse("Companion.2025.1080p.HDSCR.x264-Nuxl.mkv");
    assert_eq!(data.quality, Some("SCR".to_string()));
}

#[test]
fn test_quality_web_standalone() {
    let data = parse("Movie.Title.2023.1080p WEB x264-GROUP");
    assert_eq!(data.quality, Some("WEB".to_string()));
}

#[test]
fn test_quality_telesync_hdts() {
    let data = parse("Godzilla 2014 HDTS HC XVID AC3 ACAB");
    assert_eq!(data.quality, Some("TeleSync".to_string()));
}

#[test]
fn test_ptt_quality_corpus_sample_direct() {
    let cases = [
        (
            "Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2",
            "BluRay",
        ),
        (
            "doctor_who_2005.8x12.death_in_heaven.720p_hdtv_x264-fov",
            "HDTV",
        ),
        ("Rebecca.1940.720p.HDTVRip.HDCLUB", "HDTVRip"),
        ("Gossip Girl - 1ª Temporada. (SAT-Rip)", "SATRip"),
        ("A Stable Life S01E01 DVDRip x264-Ltu", "DVDRip"),
        (
            "The Vet Life S02E01 Dunk-A-Doctor 1080p ANPL WEB-DL AAC2 0 H 264-RTN",
            "WEB-DL",
        ),
        ("Brown Nation S01E05 1080p WEBRip x264-JAWN", "WEBRip"),
        (
            "Star Wars The Last Jedi 2017 TeleSync AAC x264-MiniMe",
            "TeleSync",
        ),
        ("The.Shape.of.Water.2017.DVDScr.XVID.AC3.HQ.Hive-CM8", "SCR"),
        (
            "Cloudy With A Chance Of Meatballs 2 2013 720p PPVRip x264 AAC-FooKaS",
            "PPVRip",
        ),
        (
            "The.OA.1x08.L.Io.Invisibile.ITA.WEBMux.x264-UBi.mkv",
            "WEBMux",
        ),
        (
            "[UsifRenegade] Cardcaptor Sakura [BD][Remastered][1080p][HEVC_10Bit][Dual] + Movies",
            "BDRip",
        ),
        (
            "[UsifRenegade] Cardcaptor Sakura - 54 [BD-RM][1080p][x265_10Bit][Dual_AAC].mkv",
            "BDRip",
        ),
        ("Elvis & Nixon (MicroHD-1080p)", "HDRip"),
        (
            "Bohemian Rhapsody 2018.2160p.UHDrip.x265.HDR.DD+.5.1-DTOne",
            "UHDRip",
        ),
        (
            "Blade.Runner.2049.2017.4K.UltraHD.BluRay.2160p.x264.TrueHD.Atmos",
            "BluRay",
        ),
        (
            "Terminator.Dark.Fate.2019.2160p.UHD.BluRay.X265.10bit.HDR.TrueHD",
            "BluRay",
        ),
        ("When We Were Boys 2013 BD Rip x264 titohmr", "BDRip"),
        (
            "Key.and.Peele.s03e09.720p.web.dl.mrlss.sujaidr (pimprg)",
            "WEB-DL",
        ),
        ("Godzilla 2014 HDTS HC XVID AC3 ACAB", "TeleSync"),
        (
            "Solo: A Star Wars Story (2018) English 720p TC x264 900MBTEAM TR",
            "TeleCine",
        ),
        ("You're Next (2013) cam XVID", "CAM"),
        ("Good Deeds 2012 SCR XViD-KiNGDOM", "SCR"),
        ("Vampire in Vegas (2009) NL Subs DVDR DivXNL-Team", "DVD"),
        (
            "Звонок из прошлого / Kol / The Call (2020) WEB-DLRip | ViruseProject",
            "WEB-DLRip",
        ),
        (
            "La nube (2020) [BluRay Rip][AC3 5.1 Castellano][www.maxitorrent.com]",
            "BRRip",
        ),
        (
            "Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT",
            "BluRay REMUX",
        ),
        (
            "Warcraft 2016 1080p Blu-ray Remux AVC TrueHD Atmos-KRaLiMaRKo",
            "BluRay REMUX",
        ),
        (
            "Троя / Troy [2004 HDDVDRip-AVC] Dub + Original + Sub]",
            "DVDRip",
        ),
        (
            "Структура момента (Расим Исмайлов) [1980, Драма, VHSRip]",
            "VHSRip",
        ),
        (
            "Преферанс по пятницам (Игорь Шешуков) [1984, Детектив, DVB]",
            "HDTV",
        ),
        ("Dragon Blade (2015) HDTSRip Exclusive", "TeleSync"),
        ("Criminal (2016) Hindi Dubbed HDTCRip", "TeleCine"),
        ("Avatar La Voie de l'eau.FRENCH.CAMHD.H264.AAC", "CAM"),
        ("Companion.2025.1080p.HDSCR.x264-Nuxl.mkv", "SCR"),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.quality.as_deref(), Some(expected), "{raw}");
    }
}

#[test]
fn test_ptt_quality_corpus_sample_direct_more() {
    let cases = [
        (
            "Star Wars The Last Jedi 2017 TeleSync AAC x264-MiniMe",
            "TeleSync",
        ),
        (
            "Solo: A Star Wars Story (2018) English 720p TC x264 900MBTEAM TR",
            "TeleCine",
        ),
        (
            "Thor : Love and Thunder (2022) Hindi HQCAM x264 AAC - QRips.mkv",
            "CAM",
        ),
        ("Vampire in Vegas (2009) NL Subs DVDR DivXNL-Team", "DVD"),
        (
            "Звонок из прошлого / Kol / The Call (2020) WEB-DLRip | ViruseProject",
            "WEB-DLRip",
        ),
        (
            "La nube (2020) [BluRay Rip][AC3 5.1 Castellano][www.maxitorrent.com]",
            "BRRip",
        ),
        ("Dragon Blade (2015) HDTSRip Exclusive", "TeleSync"),
        ("Avatar La Voie de l'eau.FRENCH.CAMHD.H264.AAC", "CAM"),
        (
            "www.1TamilBlasters.link - Indian 2 (2024) [Tamil - 1080p Proper HQ PRE-HDRip - x264 - AAC - 6.3GB - HQ Real Audio].mkv",
            "SCR",
        ),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.quality.as_deref(), Some(expected), "{raw}");
    }
}
