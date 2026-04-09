use riven_rank::parse;

#[test]
fn test_title_basic() {
    let data = parse("sons.of.anarchy.s05e10.480p.BluRay.x264-GAnGSteR");
    assert_eq!(data.parsed_title, "sons of anarchy");
}

#[test]
fn test_title_with_year() {
    let data = parse("Some.girls.1998.DVDRip");
    assert_eq!(data.parsed_title, "Some girls");
}

#[test]
fn test_title_strip_site() {
    let data = parse("www.Torrenting.com - Movie.Title.2023.1080p.BluRay");
    assert_eq!(data.site, Some("www.Torrenting.com".into()));
}

#[test]
fn test_title_strip_bracket_group() {
    let data = parse("[SubsPlease] Anime Title - 01 [1080p].mkv");
    assert!(data.parsed_title.contains("Anime Title"));
}

#[test]
fn test_title_year_at_start() {
    let data = parse("2019 After The Fall Of New York 1983 REMASTERED BDRip x264-GHOULS");
    assert_eq!(data.parsed_title, "2019 After The Fall Of New York");
}

#[test]
fn test_title_simple_dots() {
    let data = parse("Nocturnal.Animals.2016.VFF.1080p.BluRay.DTS.HEVC-HD2");
    assert_eq!(data.parsed_title, "Nocturnal Animals");
}

#[test]
fn test_title_joker() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT");
    assert_eq!(data.parsed_title, "Joker");
}

#[test]
fn test_title_spiderman() {
    let data =
        parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
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
    let data = parse(
        "Game of Thrones - S02E07 - A Man Without Honor [2160p] [HDR] [5.1, 7.1, 5.1] [ger, eng, eng] [Vio].mkv",
    );
    assert_eq!(data.parsed_title, "Game of Thrones");
}

#[test]
fn test_title_pawn_stars() {
    let data = parse("Pawn.Stars.S09E13.1080p.HEVC.x265-MeGusta");
    assert_eq!(data.parsed_title, "Pawn Stars");
}

#[test]
fn test_title_site_prefix_stripped() {
    let data = parse(
        "www.1TamilMV.world - Ayalaan (2024) Tamil PreDVD - 1080p - x264 - HQ Clean Aud - 2.5GB.mkv",
    );
    assert_eq!(data.parsed_title, "Ayalaan");
}

#[test]
fn test_title_site_prefix_stripped_2() {
    let data = parse("www.Torrenting.com   -    Anatomy Of A Fall (2023)");
    assert_eq!(data.parsed_title, "Anatomy Of A Fall");
}

#[test]
fn test_title_french_connection() {
    let data =
        parse("The French Connection 1971 Remastered BluRay 1080p REMUX AVC DTS-HD MA 5 1-LEGi0N");
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

#[test]
fn test_normalized_title_lowercase() {
    let data = parse("Joker.2019.2160p.BluRay.REMUX.HEVC-FGT");
    assert!(data.normalized_title.contains("joker"));
}

#[test]
fn test_normalized_title_stripped_punctuation() {
    let data =
        parse("Spider-Man.No.Way.Home.2021.2160p.BluRay.REMUX.HEVC.TrueHD.7.1.Atmos-FraMeSToR");
    assert!(data.normalized_title.contains("spider"));
    assert!(data.normalized_title.contains("home"));
}

#[test]
fn test_normalized_title_basic() {
    let data = parse("Mad.Max.Fury.Road.2015.1080p.BluRay-GROUP");
    assert!(data.normalized_title.contains("mad"));
    assert!(data.normalized_title.contains("max"));
}

#[test]
fn test_raw_title_preserved() {
    let input = "Joker.2019.2160p.BluRay.REMUX.HEVC.DTS-HD.MA.TrueHD.7.1.Atmos-FGT";
    let data = parse(input);
    assert_eq!(data.raw_title, input);
}

#[test]
fn test_ptt_title_corpus_sample() {
    let cases = [
        ("La.famille.bélier", "La famille bélier"),
        (
            "[Seed-Raws] 劇場版 ペンギン・ハイウェイ Penguin Highway The Movie (BD 1280x720 AVC AACx4 [5.1+2.0+2.0+2.0]).mp4",
            "Penguin Highway The Movie",
        ),
        (
            "www.Torrenting.com   -    14.Peaks.Nothing.Is.Impossible.2021.1080p.WEB.h264-RUMOUR",
            "14 Peaks Nothing Is Impossible",
        ),
        ("Too Many Cooks _ Adult Swim.mp4", "Too Many Cooks"),
        ("S.W.A.T.2017.S08E01.720p.HDTV.x264-SYNCOPY[TGx]", "S W A T"),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.parsed_title, expected, "{input}");
    }
}

#[test]
fn test_ptt_title_corpus_sample_direct() {
    let cases = [
        ("La famille bélier", "La famille bélier"),
        ("Mr. Nobody", "Mr. Nobody"),
        (
            "doctor_who_2005.8x12.death_in_heaven.720p_hdtv_x264-fov",
            "doctor who",
        ),
        (
            "[GM-Team][国漫][太乙仙魔录 灵飞纪 第3季][Magical Legend of Rise to immortality Ⅲ][01-26][AVC][GB][1080P]",
            "Magical Legend of Rise to immortality Ⅲ",
        ),
        (
            "【喵萌奶茶屋】★01月新番★[Rebirth][01][720p][简体][招募翻译]",
            "Rebirth",
        ),
        (
            "【喵萌奶茶屋】★01月新番★[別對映像研出手！/映像研には手を出すな！/Eizouken ni wa Te wo Dasu na!][01][1080p][繁體]",
            "Eizouken ni wa Te wo Dasu na!",
        ),
        (
            "[SweetSub][Mutafukaz / MFKZ][Movie][BDRip][1080P][AVC 8bit][简体内嵌]",
            "Mutafukaz / MFKZ",
        ),
        ("[Erai-raws] Kingdom 3rd Season - 02 [1080p].mkv", "Kingdom"),
        ("Голубая волна / Blue Crush (2002) DVDRip", "Blue Crush"),
        ("Жихарка (2007) DVDRip", "Жихарка"),
        (
            "Американские животные / American Animals (Барт Лэйтон / Bart Layton) [2018, Великобритания, США, драма, криминал, BDRip] MVO (СВ Студия)",
            "American Animals",
        ),
        (
            "Бастер / Buster (Дэвид Грин / David Green) [1988, Великобритания, Комедия, мелодрама, драма, приключения, криминал, биография, DVDRip]",
            "Buster",
        ),
        (
            "(2000) Le follie dell'imperatore - The Emperor's New Groove (DvdRip Ita Eng AC3 5.1).avi",
            "Le follie dell'imperatore - The Emperor's New Groove",
        ),
        (
            "[NC-Raws] 间谍过家家 / SPY×FAMILY - 04 (B-Global 1920x1080 HEVC AAC MKV)",
            "SPY×FAMILY",
        ),
        (
            "GTO (Great Teacher Onizuka) (Ep. 1-43) Sub 480p lakshay",
            "GTO (Great Teacher Onizuka)",
        ),
        (
            "www.1TamilMV.world - Ayalaan (2024) Tamil PreDVD - 1080p - x264 - HQ Clean Aud - 2.5GB.mkv",
            "Ayalaan",
        ),
        (
            "www.Torrenting.com   -    Anatomy Of A Fall (2023)",
            "Anatomy Of A Fall",
        ),
        (
            "[www.arabp2p.net]_-_تركي مترجم ومدبلج Last.Call.for.Istanbul.2023.1080p.NF.WEB-DL.DDP5.1.H.264.MKV.torrent",
            "Last Call for Istanbul",
        ),
        (
            "ww.Tamilblasters.sbs - 8 Bit Christmas (2021) HQ HDRip - x264 - Telugu (Fan Dub) - 400MB].mkv",
            "8 Bit Christmas",
        ),
        (
            "www.1TamilMV.pics - 777 Charlie (2022) Tamil HDRip - 720p - x264 - HQ Clean Aud - 1.4GB.mkv",
            "777 Charlie",
        ),
        (
            "UFC.247.PPV.Jones.vs.Reyes.HDTV.x264-PUNCH[TGx]",
            "UFC 247 Jones vs Reyes",
        ),
        (
            "www.Torrenting.com   -    14.Peaks.Nothing.Is.Impossible.2021.1080p.WEB.h264-RUMOUR",
            "14 Peaks Nothing Is Impossible",
        ),
        (
            "О мышах и людях (Of Mice and Men) 1992 BDRip 1080p.mkv",
            "Of Mice and Men",
        ),
        (
            "Wonder Woman 1984 (2020) [UHDRemux 2160p DoVi P8 Es-DTSHD AC3 En-AC3].mkv",
            "Wonder Woman 1984",
        ),
        ("S.W.A.T.2017.S08E01.720p.HDTV.x264-SYNCOPY[TGx]", "S W A T"),
        ("Grimm.INTEGRAL.MULTI.COMPLETE.BLURAY-BMTH", "Grimm"),
        (
            "Friends.1994.INTEGRALE.MULTI.1080p.WEB-DL.H265-FTMVHD",
            "Friends",
        ),
        (
            "STEVE.martin.a.documentary.in.2.pieces.S01.COMPLETE.1080p.WEB.H264-SuccessfulCrab[TGx]",
            "STEVE martin a documentary in 2 pieces",
        ),
        (
            "The Lockerbie Bombing (2013) Documentary HDTVRIP",
            "The Lockerbie Bombing",
        ),
        (
            "CSI Crime Scene Investigation S01 720p WEB DL DD5 1 H 264 LiebeIst[rartv]",
            "CSI Crime Scene Investigation",
        ),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.parsed_title, expected, "{raw}");
    }
}

#[test]
fn test_ptt_title_corpus_sample_direct_more() {
    let cases = [
        (
            "[Seed-Raws] 劇場版 ペンギン・ハイウェイ Penguin Highway The Movie (BD 1280x720 AVC AACx4 [5.1+2.0+2.0+2.0]).mp4",
            "Penguin Highway The Movie",
        ),
        (
            "【喵萌奶茶屋】★01月新番★[別對映像研出手！/Eizouken ni wa Te wo Dasu na!/映像研には手を出すな！][01][1080p][繁體]",
            "Eizouken ni wa Te wo Dasu na!",
        ),
        (
            "1. Детские игры. 1988. 1080p. HEVC. 10bit..mkv",
            "1. Детские игры",
        ),
        ("Yurusarezaru_mono2.srt", "Yurusarezaru mono2"),
        (
            "www,1TamilMV.phd - The Great Indian Suicide (2023) Tamil TRUE WEB-DL - 4K SDR - HEVC - (DD+5.1 - 384Kbps & AAC) - 3.2GB - ESub.mkv",
            "The Great Indian Suicide",
        ),
        (
            "Jurassic.World.Dominion.CUSTOM.EXTENDED.2022.2160p.MULTi.VF2.UHD.Blu-ray.REMUX.HDR.DoVi.HEVC.DTS-X.DTS-HDHRA.7.1-MOONLY.mkv",
            "Jurassic World Dominion",
        ),
        ("Too Many Cooks _ Adult Swim.mp4", "Too Many Cooks"),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(data.parsed_title, expected, "{raw}");
    }
}
