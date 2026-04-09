use riven_rank::parse;

#[test]
fn test_ptt_main_sons_of_anarchy() {
    let data = parse("sons.of.anarchy.s05e10.480p.BluRay.x264-GAnGSteR");
    assert_eq!(data.parsed_title, "sons of anarchy");
    assert_eq!(data.resolution, "480p");
    assert_eq!(data.seasons, vec![5]);
    assert_eq!(data.episodes, vec![10]);
    assert_eq!(data.quality, Some("BluRay".into()));
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.group, Some("GAnGSteR".into()));
}

#[test]
fn test_ptt_main_da_vinci_code() {
    let data = parse("Da Vinci Code DVDRip");
    assert_eq!(data.parsed_title, "Da Vinci Code");
    assert_eq!(data.quality, Some("DVDRip".into()));
}

#[test]
fn test_ptt_main_some_girls_1998() {
    let data = parse("Some.girls.1998.DVDRip");
    assert_eq!(data.parsed_title, "Some girls");
    assert_eq!(data.quality, Some("DVDRip".into()));
    assert_eq!(data.year, Some(1998));
}

#[test]
fn test_ptt_main_2019_after_fall_of_ny() {
    let data = parse("2019 After The Fall Of New York 1983 REMASTERED BDRip x264-GHOULS");
    assert_eq!(data.parsed_title, "2019 After The Fall Of New York");
    assert_eq!(data.quality, Some("BDRip".into()));
    assert_eq!(data.year, Some(1983));
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.group, Some("GHOULS".into()));
    assert!(data.remastered);
}

#[test]
fn test_ptt_main_ghost_in_shell() {
    let data = parse("Ghost In The Shell 2017 720p HC HDRip X264 AC3-EVO");
    assert_eq!(data.parsed_title, "Ghost In The Shell");
    assert_eq!(data.quality, Some("HDRip".into()));
    assert!(data.hardcoded);
    assert_eq!(data.year, Some(2017));
    assert_eq!(data.resolution, "720p");
    assert_eq!(data.codec, Some("avc".into()));
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
    assert_eq!(data.group, Some("EVO".into()));
}

#[test]
fn test_ptt_main_rogue_one() {
    let data = parse("Rogue One 2016 1080p BluRay x264.DTS-JYK");
    assert_eq!(data.parsed_title, "Rogue One");
    assert_eq!(data.year, Some(2016));
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("BluRay".into()));
    assert_eq!(data.codec, Some("avc".into()));
    assert!(data.audio.contains(&"DTS Lossy".to_string()));
    assert_eq!(data.group, Some("JYK".into()));
}

#[test]
fn test_ptt_main_joker() {
    let data = parse("Joker.2019.2160p.4K.BluRay.x265.10bit.HDR.AAC5.1");
    assert_eq!(data.parsed_title, "Joker");
    assert_eq!(data.year, Some(2019));
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("BluRay".into()));
    assert_eq!(data.codec, Some("hevc".into()));
    assert_eq!(data.bit_depth, Some("10bit".into()));
    assert!(data.hdr.contains(&"HDR".to_string()));
    assert!(data.audio.contains(&"AAC".to_string()));
    assert!(data.channels.contains(&"5.1".to_string()));
}

#[test]
fn test_ptt_main_ecrit_dans_le_ciel() {
    let data = parse("Ecrit.Dans.Le.Ciel.1954.MULTI.DVDRIP.x264.AC3-gismo65");
    assert_eq!(data.parsed_title, "Ecrit Dans Le Ciel");
    assert_eq!(data.quality, Some("DVDRip".into()));
    assert_eq!(data.year, Some(1954));
    assert!(data.dubbed);
    assert_eq!(data.codec, Some("avc".into()));
    assert!(data.audio.contains(&"Dolby Digital".to_string()));
    assert_eq!(data.group, Some("gismo65".into()));
}

#[test]
fn test_ptt_main_color_of_night() {
    let data = parse("Color.Of.Night.Unrated.DC.VostFR.BRrip.x264");
    assert_eq!(data.parsed_title, "Color Of Night");
    assert!(data.unrated);
    assert!(data.languages.contains(&"fr".to_string()));
    assert_eq!(data.quality, Some("BRRip".into()));
    assert_eq!(data.codec, Some("avc".into()));
}

#[test]
fn test_ptt_main_avengers_endgame() {
    let data = parse("Avengers.Endgame.2019.2160p.UHD.BluRay.REMUX.HDR.HEVC.Atmos-EPSiLON");
    assert_eq!(data.parsed_title, "Avengers Endgame");
    assert_eq!(data.year, Some(2019));
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("BluRay REMUX".into()));
    assert!(data.hdr.contains(&"HDR".to_string()));
    assert_eq!(data.codec, Some("hevc".into()));
    assert!(data.audio.contains(&"Atmos".to_string()));
    assert_eq!(data.group, Some("EPSiLON".into()));
}

#[test]
fn test_ptt_main_beatrice_raws_evangelion() {
    let data =
        parse("[Beatrice-Raws] Evangelion 3.333 You Can (Not) Redo [BDRip 3840x1632 HEVC TrueHD]");
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.codec, Some("hevc".into()));
    assert!(data.audio.contains(&"TrueHD".to_string()));
}

#[test]
fn test_ptt_main_web_dl_ddp() {
    let data = parse("Show.Title.S01E01.1080p.WEB-DL.DDP5.1.H.264-GROUP");
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("WEB-DL".into()));
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
    assert!(data.channels.contains(&"5.1".to_string()));
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![1]);
}

#[test]
fn test_ptt_main_desperation() {
    let data = parse("Desperation 2006 Multi Pal DvdR9-TBW1973");
    assert_eq!(data.parsed_title, "Desperation");
    assert_eq!(data.quality, Some("DVD".into()));
    assert_eq!(data.year, Some(2006));
    assert!(data.dubbed);
    assert_eq!(data.region, Some("R9".into()));
    assert_eq!(data.group, Some("TBW1973".into()));
}

#[test]
fn test_ptt_main_maman_jai_rate_lavion() {
    let data = parse("Maman, j'ai raté l'avion 1990 VFI 1080p BluRay DTS x265-HTG");
    assert_eq!(data.parsed_title, "Maman, j'ai raté l'avion");
    assert_eq!(data.quality, Some("BluRay".into()));
    assert_eq!(data.year, Some(1990));
    assert!(data.audio.contains(&"DTS Lossy".to_string()));
    assert_eq!(data.resolution, "1080p");
    assert!(data.languages.contains(&"fr".to_string()));
    assert_eq!(data.codec, Some("hevc".into()));
    assert_eq!(data.group, Some("HTG".into()));
}

#[test]
fn test_ptt_main_house_md_complete_mkv() {
    let data = parse("House MD Season 7 Complete MKV");
    assert_eq!(data.parsed_title, "House MD");
    assert_eq!(data.seasons, vec![7]);
    assert_eq!(data.container, Some("mkv".into()));
    assert!(data.complete);
}

#[test]
fn test_ptt_main_soul_land_episode_range() {
    let data = parse(
        "[OFFICIAL ENG SUB] Soul Land Episode 121-125 [1080p][Soft Sub][Web-DL][Douluo Dalu][斗罗大陆]",
    );
    assert_eq!(data.parsed_title, "Soul Land");
    assert_eq!(data.episodes, vec![121, 122, 123, 124, 125]);
    assert!(data.languages.contains(&"en".to_string()));
    assert!(data.languages.contains(&"zh".to_string()));
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("WEB-DL".into()));
    assert!(data.subbed);
}

#[test]
fn test_ptt_main_sprint_complete() {
    let data = parse("Sprint.2024.S01.COMPLETE.1080p.WEB.h264-EDITH[TGx]");
    assert_eq!(data.parsed_title, "Sprint");
    assert_eq!(data.year, Some(2024));
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.quality, Some("WEB".into()));
    assert_eq!(data.resolution, "1080p");
    assert!(data.scene);
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.group, Some("EDITH".into()));
    assert!(data.complete);
}

#[test]
fn test_ptt_main_madame_web_remux() {
    let data = parse("Madame Web 2024 UHD BluRay 2160p TrueHD Atmos 7 1 DV HEVC REMUX-FraMeSToR");
    assert_eq!(data.parsed_title, "Madame Web");
    assert_eq!(data.year, Some(2024));
    assert_eq!(data.quality, Some("BluRay REMUX".into()));
    assert_eq!(data.resolution, "2160p");
    assert!(data.channels.contains(&"7.1".to_string()));
    assert!(data.audio.contains(&"Atmos".to_string()));
    assert!(data.audio.contains(&"TrueHD".to_string()));
    assert_eq!(data.codec, Some("hevc".into()));
    assert!(data.hdr.contains(&"DV".to_string()));
    assert_eq!(data.group, Some("FraMeSToR".into()));
}

#[test]
fn test_ptt_main_the_great_indian_suicide() {
    let data = parse(
        "[www.1TamilMV.pics]_The.Great.Indian.Suicide.2023.Tamil.TRUE.WEB-DL.4K.SDR.HEVC.(DD+5.1.384Kbps.&.AAC).3.2GB.ESub.mkv",
    );
    assert_eq!(data.parsed_title, "The Great Indian Suicide");
    assert_eq!(data.year, Some(2023));
    assert!(data.languages.contains(&"en".to_string()));
    assert!(data.languages.contains(&"ta".to_string()));
    assert_eq!(data.quality, Some("WEB-DL".into()));
    assert_eq!(data.resolution, "2160p");
    assert!(data.hdr.contains(&"SDR".to_string()));
    assert_eq!(data.codec, Some("hevc".into()));
    assert_eq!(data.site, Some("www.1TamilMV.pics".into()));
    assert_eq!(data.size, Some("3.2GB".into()));
    assert_eq!(data.container, Some("mkv".into()));
    assert_eq!(data.extension, Some("mkv".into()));
    assert_eq!(data.bitrate, Some("384kbps".into()));
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
    assert!(data.audio.contains(&"AAC".to_string()));
    assert!(data.channels.contains(&"5.1".to_string()));
}

#[test]
fn test_ptt_main_bikram_yogi_guru_predator() {
    let data = parse(
        "www.MovCr.to - Bikram Yogi, Guru, Predator (2019) 720p WEB_DL x264 ESubs [Dual Audio]-[Hindi + Eng] - 950MB - MovCr.mkv",
    );
    assert_eq!(data.parsed_title, "Bikram Yogi, Guru, Predator");
    assert_eq!(data.year, Some(2019));
    assert!(data.languages.contains(&"en".to_string()));
    assert!(data.languages.contains(&"hi".to_string()));
    assert_eq!(data.quality, Some("WEB-DL".into()));
    assert_eq!(data.resolution, "720p");
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.container, Some("mkv".into()));
    assert_eq!(data.extension, Some("mkv".into()));
    assert_eq!(data.site, Some("www.MovCr.to".into()));
    assert!(data.dubbed);
    assert_eq!(data.group, Some("MovCr".into()));
    assert_eq!(data.size, Some("950MB".into()));
}

#[test]
fn test_ptt_main_28_days() {
    let data = parse("28.days.2000.1080p.bluray.x264-mimic.mkv");
    assert_eq!(data.parsed_title, "28 days");
    assert_eq!(data.year, Some(2000));
    assert_eq!(data.resolution, "1080p");
    assert_eq!(data.quality, Some("BluRay".into()));
    assert_eq!(data.codec, Some("avc".into()));
    assert_eq!(data.container, Some("mkv".into()));
    assert_eq!(data.extension, Some("mkv".into()));
    assert_eq!(data.group, Some("mimic".into()));
}

#[test]
fn test_ptt_main_dune_part_two() {
    let data = parse("Dune.Part.Two.2024.2160p.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX[TGx]");
    assert_eq!(data.parsed_title, "Dune Part Two");
    assert_eq!(data.year, Some(2024));
    assert_eq!(data.resolution, "2160p");
    assert_eq!(data.quality, Some("WEB-DL".into()));
    assert_eq!(data.codec, Some("hevc".into()));
    assert!(data.audio.contains(&"Atmos".to_string()));
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
    assert!(data.channels.contains(&"5.1".to_string()));
    assert_eq!(data.group, Some("FLUX".into()));
    assert!(data.hdr.contains(&"DV".to_string()));
    assert!(data.hdr.contains(&"HDR".to_string()));
}
