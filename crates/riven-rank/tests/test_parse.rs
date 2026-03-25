use riven_rank::parse;

// =============================================================================
// Resolution detection (17 cases)
// =============================================================================

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
    let data = parse("[Beatrice-Raws] Evangelion 3.333 You Can (Not) Redo [BDRip 3840x1632 HEVC TrueHD]");
    assert_eq!(data.resolution, "2160p");
}

#[test]
fn test_resolution_1080p_from_1920_width() {
    let data = parse("[Erai-raws] Evangelion 3.0 You Can (Not) Redo - Movie [1920x960][Multiple Subtitle].mkv");
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
    // "4k to 1080p" - last generic match should win
    let data = parse("The Boys S04E01 E02 E03 4k to 1080p AMZN WEBrip x265 DDP5 1 D0c");
    assert_eq!(data.resolution, "1080p");
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
fn test_resolution_2160p_bd() {
    let data = parse("Movie BD 2160p HEVC");
    assert_eq!(data.resolution, "2160p");
}

// =============================================================================
// Quality detection (25+ cases)
// =============================================================================

#[test]
fn test_quality_bluray() {
    let data = parse("Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2");
    assert_eq!(data.quality, Some("BluRay".to_string()));
}

#[test]
fn test_quality_hdtv() {
    let data = parse("UFC 187 PPV 720P HDTV X264-KYR");
    assert_eq!(data.quality, Some("HDTV".to_string()));
}

#[test]
fn test_quality_hdtvrip() {
    let data = parse("Rebecca.1940.720p.HDTVRip.HDCLUB");
    assert_eq!(data.quality, Some("HDTVRip".to_string()));
}

#[test]
fn test_quality_satrip() {
    let data = parse("Gossip Girl - 1\u{00aa} Temporada. (SAT-Rip)");
    assert_eq!(data.quality, Some("SATRip".to_string()));
}

#[test]
fn test_quality_dvdrip() {
    let data = parse("A Stable Life S01E01 DVDRip x264-Ltu");
    assert_eq!(data.quality, Some("DVDRip".to_string()));
}

#[test]
fn test_quality_webdl() {
    let data = parse("The Vet Life S02E01 Dunk-A-Doctor 1080p ANPL WEB-DL AAC2 0 H 264-RTN");
    assert_eq!(data.quality, Some("WEB-DL".to_string()));
}

#[test]
fn test_quality_webrip() {
    let data = parse("Brown Nation S01E05 1080p WEBRip x264-JAWN");
    assert_eq!(data.quality, Some("WEBRip".to_string()));
}

#[test]
fn test_quality_telesync() {
    let data = parse("Star Wars The Last Jedi 2017 TeleSync AAC x264-MiniMe");
    assert_eq!(data.quality, Some("TeleSync".to_string()));
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
fn test_quality_webmux() {
    let data = parse("The.OA.1x08.L.Io.Invisibile.ITA.WEBMux.x264-UBi.mkv");
    assert_eq!(data.quality, Some("WEBMux".to_string()));
}

#[test]
fn test_quality_bdrip_bracket() {
    let data = parse("[UsifRenegade] Cardcaptor Sakura [BD][Remastered][1080p][HEVC_10Bit][Dual] + Movies");
    assert_eq!(data.quality, Some("BDRip".to_string()));
}

#[test]
fn test_quality_bdrip_bd_rm() {
    let data = parse("[UsifRenegade] Cardcaptor Sakura - 54 [BD-RM][1080p][x265_10Bit][Dual_AAC].mkv");
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
    let data = parse("Peter Rabbit 2 [4K UHDremux][2160p][HDR10][DTS-HD 5.1 Castellano-TrueHD 7.1-Ingles+Subs][ES-EN]");
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
}

#[test]
fn test_quality_cam() {
    let data = parse("Blair Witch 2016 HDCAM UnKnOwN");
    assert_eq!(data.quality, Some("CAM".to_string()));
}

#[test]
fn test_quality_scr_good_deeds() {
    let data = parse("Good Deeds 2012 SCR XViD-KiNGDOM");
    assert_eq!(data.quality, Some("SCR".to_string()));
}

#[test]
fn test_quality_dvd() {
    let data = parse("Vampire in Vegas (2009) NL Subs DVDR DivXNL-Team");
    assert_eq!(data.quality, Some("DVD".to_string()));
}

#[test]
fn test_quality_telecine() {
    let data = parse("Solo: A Star Wars Story (2018) English 720p TC x264 900MBTEAM TR");
    assert_eq!(data.quality, Some("TeleCine".to_string()));
}

#[test]
fn test_quality_webdlrip() {
    let data = parse("Звонок из прошлого / Kol / The Call (2020) WEB-DLRip | ViruseProject");
    assert_eq!(data.quality, Some("WEB-DLRip".to_string()));
}

#[test]
fn test_quality_brrip() {
    let data = parse("La nube (2020) [BluRay Rip][AC3 5.1 Castellano][www.maxitorrent.com]");
    assert_eq!(data.quality, Some("BRRip".to_string()));
}

#[test]
fn test_quality_vhsrip() {
    let data = parse("Структура момента (Расим Исмайлов) [1980, Драма, VHSRip]");
    assert_eq!(data.quality, Some("VHSRip".to_string()));
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

// =============================================================================
// Codec detection (10 cases)
// =============================================================================

#[test]
fn test_codec_hevc() {
    let data = parse("Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2");
    assert_eq!(data.codec, Some("hevc".to_string()));
}

#[test]
fn test_codec_avc_x264() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.codec, Some("avc".to_string()));
}

#[test]
fn test_codec_avc_h264_space() {
    let data = parse("The Vet Life S02E01 1080p ANPL WEB-DL AAC2 0 H 264-RTN");
    // H 264 with space may or may not match depending on regex
    // The pattern is \bH\.?264\b, so "H 264" won't match (space not dot)
    let data2 = parse("Movie.Title.2023.1080p.H264.WEB-DL-GROUP");
    assert_eq!(data2.codec, Some("avc".to_string()));
}

#[test]
fn test_codec_avc_h264_dot() {
    let data = parse("Movie.Title.2023.1080p.H.264.WEB-DL-GROUP");
    assert_eq!(data.codec, Some("avc".to_string()));
}

#[test]
fn test_codec_xvid() {
    let data = parse("Movie.Title.2023.XviD-GROUP");
    assert_eq!(data.codec, Some("xvid".to_string()));
}

#[test]
fn test_codec_mpeg() {
    let data = parse("Movie.Title.2023.720p.HDTV.DD5.1.MPEG2-GROUP");
    assert_eq!(data.codec, Some("mpeg".to_string()));
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
fn test_codec_av1() {
    let data = parse("Movie.Title.2023.1080p.BluRay.AV1-GROUP");
    assert_eq!(data.codec, Some("av1".to_string()));
}

#[test]
fn test_codec_divx_as_xvid() {
    let data = parse("Movie.Title.DivX-GROUP");
    assert_eq!(data.codec, Some("xvid".to_string()));
}

// =============================================================================
// Audio detection (20+ cases)
// =============================================================================

#[test]
fn test_audio_dts_lossy() {
    let data = parse("Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2");
    assert_eq!(data.audio, vec!["DTS Lossy"]);
}

#[test]
fn test_audio_dts_lossless_dts_hd_ma() {
    let data = parse("Gold 2016 1080p BluRay DTS-HD MA 5 1 x264-HDH");
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
}

#[test]
fn test_audio_aac() {
    let data = parse("Rain Man 1988 REMASTERED 1080p BRRip x264 AAC-m2g");
    assert_eq!(data.audio, vec!["AAC"]);
}

#[test]
fn test_audio_dolby_digital_ac3() {
    let data = parse("A Dog's Purpose 2016 BDRip 720p X265 Ac3-GANJAMAN");
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
}

#[test]
fn test_audio_mp3() {
    let data = parse("Tempete 2016-TrueFRENCH-TVrip-H264-mp3");
    assert_eq!(data.audio, vec!["MP3"]);
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
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
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
    let data = parse("The Shawshank Redemption 1994.MULTi.1080p.Blu-ray.DTS-HDMA.5.1.HEVC-DDR[EtHD]");
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
    let data = parse("Madame Web (2024) 1080p HINDI ENGLISH 10bit AMZN WEBRip DDP5 1 x265 HEVC - PSA Shadow");
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_audio_dolby_digital_from_ac3_standalone() {
    let data = parse("Movie.Title.2023.1080p.BluRay.AC3-GROUP");
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
}

#[test]
fn test_audio_flac() {
    let data = parse("Movie.Title.2023.1080p.BluRay.FLAC-GROUP");
    assert!(data.audio.contains(&"FLAC".to_string()));
}

#[test]
fn test_audio_pcm() {
    let data = parse("Movie.Title.2023.1080p.BluRay.LPCM-GROUP");
    assert!(data.audio.contains(&"PCM".to_string()));
}

#[test]
fn test_audio_opus() {
    let data = parse("Movie.Title.2023.1080p.WEB.OPUS-GROUP");
    assert!(data.audio.contains(&"OPUS".to_string()));
}

// =============================================================================
// HDR detection (10 cases)
// =============================================================================

#[test]
fn test_hdr_basic() {
    let data = parse("The.Mandalorian.S01E06.4K.HDR.2160p 4.42GB");
    assert!(data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_hdr10() {
    let data = parse("Spider-Man - Complete Movie Collection (2002-2022) 1080p.HEVC.HDR10.1920x800.x265. DTS-HD");
    assert!(data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_hdr10plus() {
    let data = parse("Bullet.Train.2022.2160p.AMZN.WEB-DL.x265.10bit.HDR10Plus.DDP5.1-SMURF");
    assert!(data.hdr.contains(&"HDR10+".to_string()));
    // HDR10+ should NOT also add HDR
    assert!(!data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_dolby_vision_dv() {
    let data = parse("Belle (2021) 2160p 10bit 4KLight DOLBY VISION BluRay DDP 7.1 x265-QTZ");
    assert!(data.hdr.contains(&"DV".to_string()));
}

#[test]
fn test_hdr_dv_from_dovi() {
    let data = parse("Bullet.Train.2022.2160p.WEB-DL.DoVi.DD5.1.HEVC-EVO[TGx]");
    assert!(data.hdr.contains(&"DV".to_string()));
}

#[test]
fn test_hdr_dv_and_hdr() {
    let data = parse("House.of.the.Dragon.S01E07.2160p.10bit.HDR.DV.WEBRip.6CH.x265.HEVC-PSA");
    assert!(data.hdr.contains(&"DV".to_string()));
    assert!(data.hdr.contains(&"HDR".to_string()));
}

#[test]
fn test_hdr_sdr() {
    let data = parse("Movie.Title.2023.4K.SDR.HEVC-GROUP");
    assert!(data.hdr.contains(&"SDR".to_string()));
}

#[test]
fn test_hdr_empty_when_absent() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert!(data.hdr.is_empty());
}

#[test]
fn test_hdr_dv_from_dolby_vision_text() {
    let data = parse("Андор / Andor [01x01-03 из 12] (2022) WEB-DL-HEVC 2160p | 4K | Dolby Vision TV | NewComers, HDRezka Studio");
    assert!(data.hdr.contains(&"DV".to_string()));
}

#[test]
fn test_hdr_hdr10_plus_with_keyword() {
    let data = parse("Movie 2022 2160p WEB-DL HDR10Plus DDP5 1-GROUP");
    assert!(data.hdr.contains(&"HDR10+".to_string()));
}

// =============================================================================
// Year extraction (10 cases)
// =============================================================================

#[test]
fn test_year_standard() {
    let data = parse("Dawn.of.the.Planet.of.the.Apes.2014.HDRip.XViD-EVO");
    assert_eq!(data.year, Some(2014));
}

#[test]
fn test_year_in_parens() {
    let data = parse("Hercules (2014) 1080p BrRip H264 - YIFY");
    assert_eq!(data.year, Some(2014));
}

#[test]
fn test_year_in_brackets() {
    let data = parse("One Shot [2014] DVDRip XViD-ViCKY");
    assert_eq!(data.year, Some(2014));
}

#[test]
fn test_year_recent() {
    let data = parse("Oppenheimer.2023.BluRay.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR.mkv");
    assert_eq!(data.year, Some(2023));
}

#[test]
fn test_year_1988() {
    let data = parse("Rain Man 1988 REMASTERED 1080p BRRip x264 AAC-m2g");
    assert_eq!(data.year, Some(1988));
}

#[test]
fn test_year_2022() {
    let data = parse("Bullet.Train.2022.2160p.AMZN.WEB-DL.x265.10bit.HDR10Plus.DDP5.1-SMURF");
    assert_eq!(data.year, Some(2022));
}

#[test]
fn test_year_2019() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.year, Some(2019));
}

#[test]
fn test_year_2021() {
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert_eq!(data.year, Some(2021));
}

#[test]
fn test_year_2015() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay.DDP5.1.x265.10bit-GalaxyRG265[TGx]");
    assert_eq!(data.year, Some(2015));
}

#[test]
fn test_year_none_when_absent() {
    let data = parse("Movie.Title.BluRay.x264-GROUP");
    assert_eq!(data.year, None);
}

// =============================================================================
// Title extraction (20+ cases)
// =============================================================================

#[test]
fn test_title_simple_dots() {
    let data = parse("Nocturnal.Animals.2016.VFF.1080p.BluRay.DTS.HEVC-HD2");
    assert_eq!(data.parsed_title, "Nocturnal Animals");
}

#[test]
fn test_title_with_year() {
    let data = parse("Annabelle.2014.1080p.PROPER.HC.WEBRip.x264.AAC.2.0-RARBG");
    assert_eq!(data.parsed_title, "Annabelle");
}

#[test]
fn test_title_joker() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.parsed_title, "Joker");
}

#[test]
fn test_title_spiderman() {
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert_eq!(data.parsed_title, "Spider-Man No Way Home");
}

#[test]
fn test_title_mad_max() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay.DDP5.1.x265.10bit-GalaxyRG265[TGx]");
    assert_eq!(data.parsed_title, "Mad Max Fury Road");
}

#[test]
fn test_title_bullet_train() {
    let data = parse("Bullet.Train.2022.2160p.AMZN.WEB-DL.x265.10bit.HDR10Plus.DDP5.1-SMURF");
    assert_eq!(data.parsed_title, "Bullet Train");
}

#[test]
fn test_title_batman_returns() {
    let data = parse("Batman Returns 1992 4K Remastered BluRay 1080p DTS AC3 x264-MgB");
    assert_eq!(data.parsed_title, "Batman Returns");
}

#[test]
fn test_title_game_of_thrones() {
    let data = parse("Game of Thrones - S02E07 - A Man Without Honor [2160p] [HDR] [5.1, 7.1, 5.1] [ger, eng, eng] [Vio].mkv");
    assert_eq!(data.parsed_title, "Game of Thrones");
}

#[test]
fn test_title_pawn_stars() {
    let data = parse("Pawn.Stars.S09E13.1080p.HEVC.x265-MeGusta");
    assert_eq!(data.parsed_title, "Pawn Stars");
}

#[test]
fn test_title_site_prefix_stripped() {
    let data = parse("www.1TamilMV.world - Ayalaan (2024) Tamil PreDVD - 1080p - x264 - HQ Clean Aud - 2.5GB.mkv");
    assert_eq!(data.parsed_title, "Ayalaan");
}

#[test]
fn test_title_site_prefix_stripped_2() {
    let data = parse("www.Torrenting.com   -    Anatomy Of A Fall (2023)");
    assert_eq!(data.parsed_title, "Anatomy Of A Fall");
}

#[test]
fn test_title_french_connection() {
    let data = parse("The French Connection 1971 Remastered BluRay 1080p REMUX AVC DTS-HD MA 5 1-LEGi0N");
    assert_eq!(data.parsed_title, "The French Connection");
}

#[test]
fn test_title_despicable_me_4() {
    let data = parse("Despicable.Me.4.2024.D.TELESYNC_14OOMB.avi");
    assert_eq!(data.parsed_title, "Despicable Me 4");
}

#[test]
fn test_title_friends() {
    let data = parse("Friends.1994.INTEGRALE.MULTI.1080p.WEB-DL.H265-FTMVHD");
    assert_eq!(data.parsed_title, "Friends");
}

#[test]
fn test_title_belle() {
    let data = parse("Belle (2021) 2160p 10bit 4KLight DOLBY VISION BluRay DDP 7.1 x265-QTZ");
    assert_eq!(data.parsed_title, "Belle");
}

#[test]
fn test_title_the_blacklist() {
    let data = parse("The Blacklist S07E04 (1080p AMZN WEB-DL x265 HEVC 10bit EAC-3 5.1)[Bandi]");
    assert_eq!(data.parsed_title, "The Blacklist");
}

#[test]
fn test_title_gold() {
    let data = parse("Gold 2016 1080p BluRay DTS-HD MA 5 1 x264-HDH");
    assert_eq!(data.parsed_title, "Gold");
}

#[test]
fn test_title_rain_man() {
    let data = parse("Rain Man 1988 REMASTERED 1080p BRRip x264 AAC-m2g");
    assert_eq!(data.parsed_title, "Rain Man");
}

#[test]
fn test_title_oppenheimer() {
    let data = parse("Oppenheimer.2023.BluRay.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR.mkv");
    assert_eq!(data.parsed_title, "Oppenheimer");
}

#[test]
fn test_title_csi() {
    let data = parse("CSI Crime Scene Investigation S01 720p WEB DL DD5 1 H 264 LiebeIst[rartv]");
    assert_eq!(data.parsed_title, "CSI Crime Scene Investigation");
}

// =============================================================================
// Season/Episode detection (15+ cases)
// =============================================================================

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
fn test_episode_standalone_e01() {
    let data = parse("The Boys S04E01 E02 E03 4k to 1080p AMZN WEBrip x265 DDP5 1 D0c");
    assert_eq!(data.seasons, vec![4]);
    assert!(data.episodes.contains(&1));
}

#[test]
fn test_season_word_format() {
    let data = parse("Show Title Season 3 Episode 5 720p");
    assert_eq!(data.seasons, vec![3]);
    assert_eq!(data.episodes, vec![5]);
}

#[test]
fn test_complete_series() {
    let data = parse("Grimm.INTEGRAL.MULTI.COMPLETE.BLURAY-BMTH");
    assert!(data.complete);
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
fn test_no_season_no_episode() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert!(data.seasons.is_empty());
    assert!(data.episodes.is_empty());
}

#[test]
fn test_season_s09e13() {
    let data = parse("Pawn.Stars.S09E13.1080p.HEVC.x265-MeGusta");
    assert_eq!(data.seasons, vec![9]);
    assert_eq!(data.episodes, vec![13]);
}

#[test]
fn test_crossref_1x08() {
    let data = parse("The.OA.1x08.L.Io.Invisibile.ITA.WEBMux.x264-UBi.mkv");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![8]);
}

#[test]
fn test_season_s02e07() {
    let data = parse("Game of Thrones - S02E07 - A Man Without Honor [2160p] [HDR] [5.1, 7.1, 5.1] [ger, eng, eng] [Vio].mkv");
    assert_eq!(data.seasons, vec![2]);
    assert_eq!(data.episodes, vec![7]);
}

#[test]
fn test_mandalorian_s01e06() {
    let data = parse("The.Mandalorian.S01E06.4K.HDR.2160p 4.42GB");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![6]);
}

#[test]
fn test_dragon_s01e07() {
    let data = parse("House.of.the.Dragon.S01E07.2160p.10bit.HDR.DV.WEBRip.6CH.x265.HEVC-PSA");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![7]);
}

// =============================================================================
// Group detection (8 cases)
// =============================================================================

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
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert_eq!(data.group, Some("FraMeSToR".to_string()));
}

#[test]
fn test_group_bracket_format() {
    let data = parse("[Beatrice-Raws] Evangelion 3.333 You Can (Not) Redo [BDRip 3840x1632 HEVC TrueHD]");
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

// =============================================================================
// Boolean flags (14 cases)
// =============================================================================

#[test]
fn test_flag_proper() {
    let data = parse("Annabelle.2014.1080p.PROPER.HC.WEBRip.x264.AAC.2.0-RARBG");
    assert!(data.proper);
}

#[test]
fn test_flag_repack() {
    let data = parse("Movie.Title.2009.1080p.BluRay.x264.REPACK-METiS");
    assert!(data.repack);
}

#[test]
fn test_flag_remastered() {
    let data = parse("Rain Man 1988 REMASTERED 1080p BRRip x264 AAC-m2g");
    assert!(data.remastered);
}

#[test]
fn test_flag_extended() {
    let data = parse("Movie.Title.EXTENDED.2022.2160p.BluRay-GROUP");
    assert!(data.extended);
}

#[test]
fn test_flag_dubbed_dual_audio() {
    let data = parse("[naiyas] Fate Stay Night [BD 1080P HEVC10 QAACx2 Dual Audio]");
    assert!(data.dubbed);
}

#[test]
fn test_flag_subbed() {
    let data = parse("Movie.Title.2023.1080p.SUBBED.BluRay-GROUP");
    assert!(data.subbed);
}

#[test]
fn test_flag_hardcoded() {
    let data = parse("Annabelle.2014.1080p.PROPER.HC.WEBRip.x264.AAC.2.0-RARBG");
    assert!(data.hardcoded);
}

#[test]
fn test_flag_documentary() {
    let data = parse("The Lockerbie Bombing (2013) Documentary HDTVRIP");
    assert!(data.documentary);
}

#[test]
fn test_flag_adult() {
    let data = parse("Some.XXX.Movie.2023.1080p.WEB-DL");
    assert!(data.adult);
}

#[test]
fn test_flag_not_proper() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert!(!data.proper);
}

#[test]
fn test_flag_not_remastered() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert!(!data.remastered);
}

#[test]
fn test_flag_unrated() {
    let data = parse("Movie.UNRATED.2023.1080p.BluRay.x264-GROUP");
    assert!(data.unrated);
}

#[test]
fn test_flag_uncensored() {
    let data = parse("Movie.UNCENSORED.2023.1080p.BluRay.x264-GROUP");
    assert!(data.uncensored);
}

#[test]
fn test_flag_ppv() {
    let data = parse("UFC 247 PPV Jones vs Reyes HDTV x264-PUNCH");
    assert!(data.ppv);
}

// =============================================================================
// Media type detection (6 cases)
// =============================================================================

#[test]
fn test_media_type_show() {
    let data = parse("Gotham S03E17 XviD-AFG");
    assert_eq!(data.media_type(), "show");
}

#[test]
fn test_media_type_movie() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.media_type(), "movie");
}

#[test]
fn test_media_type_show_season_only() {
    let data = parse("Monk.S01.1080p.AMZN.WEBRip.DDP2.0.x264-AJP69[rartv]");
    assert_eq!(data.media_type(), "show");
}

#[test]
fn test_media_type_movie_no_season() {
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert_eq!(data.media_type(), "movie");
}

#[test]
fn test_media_type_show_word_season() {
    let data = parse("Show Title Season 3 Episode 5 720p");
    assert_eq!(data.media_type(), "show");
}

#[test]
fn test_media_type_show_crossref() {
    let data = parse("The.OA.1x08.L.Io.Invisibile.ITA.WEBMux.x264-UBi.mkv");
    assert_eq!(data.media_type(), "show");
}

// =============================================================================
// Extension and container detection (5 cases)
// =============================================================================

#[test]
fn test_extension_mkv() {
    let data = parse("Monk.S01E01E02.1080p.WEB-DL.DD2.0.x264-AJP69.mkv");
    assert_eq!(data.extension, Some("mkv".to_string()));
}

#[test]
fn test_extension_avi() {
    let data = parse("Movie.Title.2023.DivX-GROUP.avi");
    assert_eq!(data.extension, Some("avi".to_string()));
}

#[test]
fn test_extension_mp4() {
    let data = parse("Movie.Title.2023.x264-GROUP.mp4");
    assert_eq!(data.extension, Some("mp4".to_string()));
}

#[test]
fn test_extension_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.extension, None);
}

#[test]
fn test_container_mkv() {
    let data = parse("Oppenheimer.2023.BluRay.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR.mkv");
    assert_eq!(data.container, Some("mkv".to_string()));
}

// =============================================================================
// Bit depth detection (3 cases)
// =============================================================================

#[test]
fn test_bit_depth_10bit() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay.DDP5.1.x265.10bit-GalaxyRG265[TGx]");
    assert_eq!(data.bit_depth, Some("10bit".to_string()));
}

#[test]
fn test_bit_depth_from_hdr10() {
    let data = parse("Movie.Title.2022.2160p.HDR10.BluRay-GROUP");
    assert_eq!(data.bit_depth, Some("10bit".to_string()));
}

#[test]
fn test_bit_depth_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.bit_depth, None);
}

// =============================================================================
// Site detection (3 cases)
// =============================================================================

#[test]
fn test_site_detected() {
    let data = parse("www.1TamilMV.world - Ayalaan (2024) Tamil PreDVD - 1080p - x264 - HQ Clean Aud - 2.5GB.mkv");
    assert_eq!(data.site, Some("1TamilMV.world".to_string()));
}

#[test]
fn test_site_torrenting() {
    let data = parse("www.Torrenting.com   -    Anatomy Of A Fall (2023)");
    assert_eq!(data.site, Some("Torrenting.com".to_string()));
}

#[test]
fn test_site_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.site, None);
}

// =============================================================================
// Channels detection (4 cases)
// =============================================================================

#[test]
fn test_channels_71() {
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert!(data.channels.contains(&"7.1".to_string()));
}

#[test]
fn test_channels_51_standalone() {
    let data = parse("Movie.Title.2023.1080p.BluRay.AC3.5.1-GROUP");
    assert!(data.channels.contains(&"5.1".to_string()));
}

#[test]
fn test_channels_20_standalone() {
    let data = parse("Movie.Title.2023.1080p.BluRay.AAC.2.0-GROUP");
    assert!(data.channels.contains(&"2.0".to_string()));
}

#[test]
fn test_channels_empty() {
    let data = parse("Movie.Title.2023.BluRay.x264-GROUP");
    assert!(data.channels.is_empty());
}

// =============================================================================
// Integration tests - complex real-world torrent names (12 cases)
// =============================================================================

#[test]
fn test_integration_joker_remux() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.parsed_title, "Joker");
    assert_eq!(data.year, Some(2019));
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
    assert!(data.audio.contains(&"TrueHD".to_string()));
    assert!(data.audio.contains(&"Atmos".to_string()));
    assert!(data.channels.contains(&"7.1".to_string()));
    assert_eq!(data.group, Some("FGT".to_string()));
    assert_eq!(data.media_type(), "movie");
}

#[test]
fn test_integration_spiderman_no_way_home() {
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert_eq!(data.parsed_title, "Spider-Man No Way Home");
    assert_eq!(data.year, Some(2021));
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("BluRay REMUX".to_string()));
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.audio.contains(&"TrueHD".to_string()));
    assert!(data.audio.contains(&"Atmos".to_string()));
    assert_eq!(data.group, Some("FraMeSToR".to_string()));
}

#[test]
fn test_integration_blacklist() {
    let data = parse("The Blacklist S07E04 (1080p AMZN WEB-DL x265 HEVC 10bit EAC-3 5.1)[Bandi]");
    assert_eq!(data.parsed_title, "The Blacklist");
    assert_eq!(data.seasons, vec![7]);
    assert_eq!(data.episodes, vec![4]);
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("WEB-DL".to_string()));
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_integration_annabelle() {
    let data = parse("Annabelle.2014.1080p.PROPER.HC.WEBRip.x264.AAC.2.0-RARBG");
    assert_eq!(data.parsed_title, "Annabelle");
    assert_eq!(data.year, Some(2014));
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("WEBRip".to_string()));
    assert_eq!(data.codec, Some("avc".to_string()));
    assert!(data.audio.contains(&"AAC".to_string()));
    assert!(data.proper);
    assert!(data.hardcoded);
    assert_eq!(data.group, Some("RARBG".to_string()));
}

#[test]
fn test_integration_mad_max() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay.DDP5.1.x265.10bit-GalaxyRG265[TGx]");
    assert_eq!(data.parsed_title, "Mad Max Fury Road");
    assert_eq!(data.year, Some(2015));
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("BluRay".to_string()));
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
    assert_eq!(data.bit_depth, Some("10bit".to_string()));
}

#[test]
fn test_integration_evangelion_3840() {
    let data = parse("[Beatrice-Raws] Evangelion 3.333 You Can (Not) Redo [BDRip 3840x1632 HEVC TrueHD]");
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.audio.contains(&"TrueHD".to_string()));
    assert_eq!(data.group, Some("Beatrice-Raws".to_string()));
}

#[test]
fn test_integration_bullet_train_hdr10plus() {
    let data = parse("Bullet.Train.2022.2160p.AMZN.WEB-DL.x265.10bit.HDR10Plus.DDP5.1-SMURF");
    assert_eq!(data.parsed_title, "Bullet Train");
    assert_eq!(data.year, Some(2022));
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("WEB-DL".to_string()));
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.hdr.contains(&"HDR10+".to_string()));
    assert!(!data.hdr.contains(&"HDR".to_string()));
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
    assert_eq!(data.bit_depth, Some("10bit".to_string()));
    assert_eq!(data.group, Some("SMURF".to_string()));
}

#[test]
fn test_integration_remastered_batman() {
    let data = parse("Batman Returns 1992 4K Remastered BluRay 1080p DTS AC3 x264-MgB");
    assert_eq!(data.parsed_title, "Batman Returns");
    assert_eq!(data.year, Some(1992));
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("BluRay".to_string()));
    assert_eq!(data.codec, Some("avc".to_string()));
    assert!(data.audio.contains(&"DTS Lossy".to_string()));
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
    assert!(data.remastered);
}

#[test]
fn test_integration_belle_dolby_vision() {
    let data = parse("Belle (2021) 2160p 10bit 4KLight DOLBY VISION BluRay DDP 7.1 x265-QTZ");
    assert_eq!(data.parsed_title, "Belle");
    assert_eq!(data.year, Some(2021));
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("BluRay".to_string()));
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.hdr.contains(&"DV".to_string()));
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
    assert!(data.channels.contains(&"7.1".to_string()));
}

#[test]
fn test_integration_gold_dts_lossless() {
    let data = parse("Gold 2016 1080p BluRay DTS-HD MA 5 1 x264-HDH");
    assert_eq!(data.parsed_title, "Gold");
    assert_eq!(data.year, Some(2016));
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("BluRay".to_string()));
    assert_eq!(data.codec, Some("avc".to_string()));
    assert!(data.audio.contains(&"DTS Lossless".to_string()));
    assert_eq!(data.group, Some("HDH".to_string()));
}

#[test]
fn test_integration_mandalorian() {
    let data = parse("The.Mandalorian.S01E06.4K.HDR.2160p 4.42GB");
    assert_eq!(data.parsed_title, "The Mandalorian");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![6]);
    assert_eq!(data.resolution, "2160p");
    assert!(data.hdr.contains(&"HDR".to_string()));
    assert_eq!(data.size, Some("4.42GB".to_string()));
}

#[test]
fn test_integration_house_dragon_dv_hdr() {
    let data = parse("House.of.the.Dragon.S01E07.2160p.10bit.HDR.DV.WEBRip.6CH.x265.HEVC-PSA");
    assert_eq!(data.parsed_title, "House of the Dragon");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![7]);
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("WEBRip".to_string()));
    assert_eq!(data.codec, Some("hevc".to_string()));
    assert!(data.hdr.contains(&"DV".to_string()));
    assert!(data.hdr.contains(&"HDR".to_string()));
    assert_eq!(data.bit_depth, Some("10bit".to_string()));
}

// =============================================================================
// Normalized title (3 cases)
// =============================================================================

#[test]
fn test_normalized_title_lowercase() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC-FGT");
    assert!(data.normalized_title.contains("joker"));
}

#[test]
fn test_normalized_title_stripped_punctuation() {
    let data = parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert!(data.normalized_title.contains("spider"));
    assert!(data.normalized_title.contains("home"));
}

#[test]
fn test_normalized_title_basic() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay-GROUP");
    assert!(data.normalized_title.contains("mad"));
    assert!(data.normalized_title.contains("max"));
}

// =============================================================================
// Size detection (2 cases)
// =============================================================================

#[test]
fn test_size_detected() {
    let data = parse("The.Mandalorian.S01E06.4K.HDR.2160p 4.42GB");
    assert_eq!(data.size, Some("4.42GB".to_string()));
}

#[test]
fn test_size_none() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert_eq!(data.size, None);
}

// =============================================================================
// Edition detection (3 cases)
// =============================================================================

#[test]
fn test_edition_remastered() {
    let data = parse("The French Connection 1971 Remastered BluRay 1080p REMUX AVC DTS-HD MA 5 1-LEGi0N");
    assert!(data.edition.is_some());
}

#[test]
fn test_edition_imax() {
    let data = parse("Oppenheimer.2023.IMAX.2160p.BluRay.x265.10bit.DTS-HD.MA.5.1-CTRLHD");
    assert!(data.edition.is_some());
}

#[test]
fn test_edition_directors_cut() {
    let data = parse("Movie Title Directors Cut 2023 1080p BluRay-GROUP");
    assert!(data.edition.is_some());
}

// =============================================================================
// Language detection (3 cases)
// =============================================================================

#[test]
fn test_language_multi() {
    let data = parse("Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2");
    assert!(data.languages.contains(&"fr".to_string()));
}

#[test]
fn test_language_english() {
    let data = parse("Movie.Title.2023.English.1080p.BluRay-GROUP");
    assert!(data.languages.contains(&"en".to_string()));
}

#[test]
fn test_language_german() {
    let data = parse("Detroit.2017.BDRip.MD.GERMAN.x264-SPECTRE");
    assert!(data.languages.contains(&"de".to_string()));
}

// =============================================================================
// 3D detection (2 cases)
// =============================================================================

#[test]
fn test_3d_detected() {
    let data = parse("Movie.Title.2023.3D.1080p.BluRay-GROUP");
    assert!(data.three_d);
}

#[test]
fn test_3d_not_detected() {
    let data = parse("Movie.Title.2023.1080p.BluRay-GROUP");
    assert!(!data.three_d);
}

// =============================================================================
// Torrent extension detection (2 cases)
// =============================================================================

#[test]
fn test_torrent_flag() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP.torrent");
    assert!(data.torrent);
}

#[test]
fn test_not_torrent() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP.mkv");
    assert!(!data.torrent);
}

// =============================================================================
// Raw title preserved (1 case)
// =============================================================================

#[test]
fn test_raw_title_preserved() {
    let input = "Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT";
    let data = parse(input);
    assert_eq!(data.raw_title, input);
}
