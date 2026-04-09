use riven_rank::parse;

#[test]
fn test_bit_depth_10bit() {
    let data = parse("Movie.Title.10bit.BluRay");
    assert_eq!(data.bit_depth, Some("10bit".into()));
}

#[test]
fn test_bit_depth_10bit_from_hdr10() {
    let data = parse("Movie.Title.HDR10.BluRay");
    assert_eq!(data.bit_depth, Some("10bit".into()));
}

#[test]
fn test_bit_depth_8bit() {
    let data = parse("Movie.Title.8bit.BluRay");
    assert_eq!(data.bit_depth, Some("8bit".into()));
}

#[test]
fn test_bit_depth_12bit() {
    let data = parse("Movie.Title.12bit.BluRay");
    assert_eq!(data.bit_depth, Some("12bit".into()));
}

#[test]
fn test_channels_51() {
    let data = parse("Movie.Title.5.1.BluRay");
    assert!(data.channels.contains(&"5.1".to_string()));
}

#[test]
fn test_channels_71() {
    let data = parse("Movie.Title.7.1.BluRay");
    assert!(data.channels.contains(&"7.1".to_string()));
}

#[test]
fn test_channels_20() {
    let data = parse("Movie.Title.2.0.BluRay");
    assert!(data.channels.contains(&"2.0".to_string()));
}

#[test]
fn test_channels_stereo() {
    let data = parse("Movie.Title.Stereo.BluRay");
    assert!(data.channels.contains(&"stereo".to_string()));
}

#[test]
fn test_channels_mono() {
    let data = parse("Movie.Title.Mono.DVDRip");
    assert!(data.channels.contains(&"mono".to_string()));
}

#[test]
fn test_uncensored() {
    let data = parse("Movie.Title.UNCENSORED.1080p.BluRay");
    assert!(data.uncensored);
}

#[test]
fn test_subbed() {
    let data = parse("Movie.Title.SUBBED.720p.BluRay");
    assert!(data.subbed);
}

#[test]
fn test_extended() {
    let data = parse("Movie.Title.EXTENDED.1080p.BluRay");
    assert!(data.extended);
}

#[test]
fn test_remastered_flag() {
    let data = parse("Movie.Title.Remastered.1080p.BluRay");
    assert!(data.remastered);
}

#[test]
fn test_documentary() {
    let data = parse("Movie.Title.DOCUMENTARY.1080p.BluRay");
    assert!(data.documentary);
}

#[test]
fn test_commentary() {
    let data = parse("Movie.Title.COMMENTARY.1080p.BluRay");
    assert!(data.commentary);
}

#[test]
fn test_upscaled() {
    let data = parse("Movie.Title.Upscaled.1080p.BluRay");
    assert!(data.upscaled);
}

#[test]
fn test_upscaled_ai() {
    let data = parse("Movie.Title.AI.Enhanced.1080p.BluRay");
    assert!(data.upscaled);
}

#[test]
fn test_scene_by_group() {
    let data = parse("Movie.Title.2023.1080p.WEB.x264-CAKES");
    assert!(data.scene);
}

#[test]
fn test_not_scene() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264-GROUP");
    assert!(!data.scene);
}

#[test]
fn test_3d() {
    let data = parse("Movie.Title.2023.3D.1080p.BluRay");
    assert!(data.three_d);
}

#[test]
fn test_3d_sbs() {
    let data = parse("Movie.Title.2023.SBS.1080p.BluRay");
    assert!(data.three_d);
}

#[test]
fn test_torrent_extension() {
    let data = parse("Movie.Title.2023.1080p.BluRay.torrent");
    assert!(data.torrent);
}

#[test]
fn test_part() {
    let data = parse("Movie.Title.Part.2.1080p.BluRay");
    assert_eq!(data.part, Some(2));
}

#[test]
fn test_part_pt() {
    let data = parse("Movie.Title.Pt.1.1080p.BluRay");
    assert_eq!(data.part, Some(1));
}

#[test]
fn test_no_season_no_episode() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert!(data.seasons.is_empty());
    assert!(data.episodes.is_empty());
}

#[test]
fn test_crossref_1x08() {
    let data = parse("The.OA.1x08.L.Io.Invisibile.ITA.WEBMux.x264-UBi.mkv");
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, vec![8]);
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
    let data =
        parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
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
    let data =
        parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
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
    let data =
        parse("[Beatrice-Raws] Evangelion 3.333 You Can (Not) Redo [BDRip 3840x1632 HEVC TrueHD]");
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

#[test]
fn test_ptt_parity_formula1_four_digit_season() {
    let data = parse("Formula1.S2025E86.1080p.F1TV.WEB-DL.AAC2.0.H.264-BTN");
    assert_eq!(data.seasons, vec![2025]);
    assert_eq!(data.episodes, vec![86]);
}

#[test]
fn test_ptt_parity_turkish_season() {
    let data = parse("2.Sezon");
    assert_eq!(data.seasons, vec![2]);
    assert_eq!(data.parsed_title, "");
}

#[test]
fn test_ptt_parity_turkish_episode() {
    let data = parse("The Voice 7.Bölüm 1080p WEB-DL");
    assert_eq!(data.episodes, vec![7]);
    assert_eq!(data.parsed_title, "The Voice");
}

#[test]
fn test_ptt_parity_anime_bare_episode_range() {
    let data = parse("[Erai-raws] One Piece - 01 ~ 12 [1080p][Multiple Subtitle][0A0B0C0D]");
    assert_eq!(data.parsed_title, "One Piece");
    assert_eq!(data.episodes, (1..=12).collect::<Vec<_>>());
    assert!(data.anime);
}

#[test]
fn test_ptt_parity_anime_bare_episode_range_zero_start() {
    let data = parse("[SubsPlease] One Piece - 00~25 (1080p) [ABCDEF12]");
    assert_eq!(data.parsed_title, "One Piece");
    assert_eq!(data.episodes, (0..=25).collect::<Vec<_>>());
    assert!(data.anime);
}

#[test]
fn test_ptt_parity_ddp_does_not_truncate_title() {
    let data = parse("Fairly.OddParents.Fairly.Odder.S01.1080p.PMTP.WEB-DL.DDP5.1.H.264-NTb");
    assert_eq!(data.parsed_title, "Fairly OddParents Fairly Odder");
    assert_eq!(data.seasons, vec![1]);
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_ptt_parity_eac3_sxx_not_season() {
    let data = parse("Movie.Title.E-AC3-S78.1080p.WEB-DL");
    assert!(data.seasons.is_empty());
    assert!(data.audio.contains(&"Dolby Digital Plus".to_string()));
}

#[test]
fn test_ptt_parity_webdl_not_group() {
    let data = parse("Movie.Title.E-AC3-S78.1080p.WEB-DL");
    assert_eq!(data.group, None);
}

#[test]
fn test_ptt_parity_preserve_1080i() {
    let data = parse("Movie.Title.1080i.HDTV");
    assert_eq!(data.resolution, "1080i");
}

#[test]
fn test_ptt_parity_language_cluster_codes() {
    let data = parse("Movie.Title.DK.EN.ES.HR.NO.PL.SV.1080p.BluRay");
    for lang in ["da", "en", "es", "hr", "no", "pl", "sv"] {
        assert!(data.languages.contains(&lang.to_string()), "missing {lang}");
    }
}

#[test]
fn test_ptt_parity_arab_subtitle_language() {
    let data = parse("Movie.Title.Arab.Subtitle.1080p.WEB-DL");
    assert!(data.languages.contains(&"ar".to_string()));
}

#[test]
fn test_ptt_parity_season_range_double_dash() {
    let data = parse("Doctor Who S01--S07--Complete with holiday episodes");
    assert_eq!(data.seasons, vec![1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn test_ptt_parity_crossref_episode_range() {
    let data = parse("BoJack Horseman [06x01-08 of 16] (2019-2020) WEB-DLRip 720p");
    assert_eq!(data.seasons, vec![6]);
    assert_eq!(data.episodes, (1..=8).collect::<Vec<_>>());
}

#[test]
fn test_ptt_parity_episode_word_range() {
    let data = parse("Orange Is The New Black Season 5 Episodes 1-10 INCOMPLETE (LEAKED)");
    assert_eq!(data.seasons, vec![5]);
    assert_eq!(data.episodes, (1..=10).collect::<Vec<_>>());
}

#[test]
fn test_ptt_parity_parenthesized_episode_range() {
    let data = parse(
        "[BenjiD] Quan Zhi Gao Shou (The King’s Avatar) / Full-Time Master S01 (01 - 12) [1080p x265] [Soft sub] V2",
    );
    assert_eq!(
        data.parsed_title,
        "Quan Zhi Gao Shou (The King’s Avatar) / Full-Time Master"
    );
    assert_eq!(data.seasons, vec![1]);
    assert_eq!(data.episodes, (1..=12).collect::<Vec<_>>());
}

#[test]
fn test_ptt_parity_anime_bare_range_second_season() {
    let data = parse("[Erai-raws] 3D Kanojo - Real Girl 2nd Season - 01 ~ 12 [720p]");
    assert_eq!(data.parsed_title, "3D Kanojo - Real Girl");
    assert_eq!(data.seasons, vec![2]);
    assert_eq!(data.episodes, (1..=12).collect::<Vec<_>>());
}

#[test]
fn test_ptt_parity_compact_season_episode_pair() {
    let data = parse("[HR] Boku no Hero Academia 87 (S4-24) [1080p HEVC Multi-Subs] HR-GZ");
    assert_eq!(data.parsed_title, "Boku no Hero Academia 87");
    assert_eq!(data.seasons, vec![4]);
    assert_eq!(data.episodes, vec![24]);
}

#[test]
fn test_ptt_parity_3xeng_language() {
    let data = parse("Deadpool 2016 1080p BluRay DTS Rus Ukr 3xEng HDCL");
    for lang in ["ru", "uk"] {
        assert!(data.languages.contains(&lang.to_string()), "missing {lang}");
    }
    assert!(!data.languages.contains(&"en".to_string()));
}

#[test]
fn test_ptt_parity_jp_underscore_language() {
    let data = parse("[POPAS] Neon Genesis Evangelion: The End of Evangelion [jp_PT-pt");
    assert!(data.languages.contains(&"ja".to_string()));
    assert!(data.languages.contains(&"pt".to_string()));
}

#[test]
fn test_ptt_parser_fairy_tail_season_range() {
    let data = parse("[F-D] Fairy Tail Season 1 - 6 + Extras [480P][Dual-Audio]");
    assert_eq!(data.seasons, vec![1, 2, 3, 4, 5, 6]);
}

#[test]
fn test_ptt_parser_lost_crossref_season() {
    let data = parse("Lost.[Perdidos].6x05.HDTV.XviD.[www.DivxTotaL.com]");
    assert_eq!(data.seasons, vec![6]);
}

#[test]
fn test_ptt_parser_bojack_crossref_range() {
    let data = parse("BoJack Horseman [06x01-08 of 16] (2019-2020) WEB-DLRip 720p");
    assert_eq!(data.seasons, vec![6]);
    assert_eq!(data.episodes, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}
