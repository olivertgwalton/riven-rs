use riven_rank::parse;

#[test]
fn test_language_french_vostfr() {
    let data = parse("Color.Of.Night.Unrated.DC.VostFR.BRrip.x264");
    assert!(data.languages.contains(&"fr".to_string()));
}

#[test]
fn test_language_multi_dubbed() {
    let data = parse("Movie.Title.MULTI.1080p.BluRay");
    assert!(data.dubbed);
}

#[test]
fn test_language_english() {
    let data = parse("Movie.Title.English.1080p.BluRay");
    assert!(data.languages.contains(&"en".to_string()));
}

#[test]
fn test_language_german() {
    let data = parse("Movie.Title.German.1080p.BluRay");
    assert!(data.languages.contains(&"de".to_string()));
}

#[test]
fn test_language_spanish() {
    let data = parse("Movie.Title.Spanish.1080p.BluRay");
    assert!(data.languages.contains(&"es".to_string()));
}

#[test]
fn test_language_italian() {
    let data = parse("Movie.Title.ITA.1080p.BluRay");
    assert!(data.languages.contains(&"it".to_string()));
}

#[test]
fn test_language_russian() {
    let data = parse("Movie.Title.RUS.1080p.BluRay");
    assert!(data.languages.contains(&"ru".to_string()));
}

#[test]
fn test_language_portuguese() {
    let data = parse("Movie.Title.Portuguese.1080p.BluRay");
    assert!(data.languages.contains(&"pt".to_string()));
}

#[test]
fn test_language_japanese() {
    let data = parse("Movie.Title.JPN.1080p.BluRay");
    assert!(data.languages.contains(&"ja".to_string()));
}

#[test]
fn test_language_korean() {
    let data = parse("Movie.Title.Korean.1080p.BluRay");
    assert!(data.languages.contains(&"ko".to_string()));
}

#[test]
fn test_language_chinese() {
    let data = parse("Movie.Title.Chinese.1080p.BluRay");
    assert!(data.languages.contains(&"zh".to_string()));
}

#[test]
fn test_language_hindi() {
    let data = parse("Movie.Title.Hindi.1080p.WEB-DL");
    assert!(data.languages.contains(&"hi".to_string()));
}

#[test]
fn test_language_polish() {
    let data = parse("Movie.Title.PL.1080p.BluRay");
    assert!(data.languages.contains(&"pl".to_string()));
}

#[test]
fn test_language_dutch() {
    let data = parse("Movie.Title.NL.1080p.BluRay");
    assert!(data.languages.contains(&"nl".to_string()));
}

#[test]
fn test_language_danish_nordic() {
    let data = parse("Movie.Title.Nordic.1080p.BluRay");
    assert!(
        data.languages.contains(&"da".to_string())
            || data.languages.contains(&"fi".to_string())
            || data.languages.contains(&"sv".to_string())
            || data.languages.contains(&"no".to_string())
    );
}

#[test]
fn test_language_multi() {
    let data = parse("Nocturnal Animals 2016 VFF 1080p BluRay DTS HEVC-HD2");
    assert!(data.languages.contains(&"fr".to_string()));
}

#[test]
fn test_ptt_language_corpus_sample() {
    let cases = [
        (
            "Ponyo[2008]DvDrip-H264 Quad Audio[Eng Jap Fre Spa]AC3 5.1[DXO]",
            vec!["en", "ja", "fr", "es"],
        ),
        ("Subs(ara,fre,ger).srt", vec!["fr", "de", "ar"]),
        (
            "The.Gorge.2025.PLSUB.1080p.ATVP.WEB-DL.DDP5.1.Atmos.H.264-APEX.mkv",
            vec!["pl"],
        ),
        (
            "Deadpool 2016 1080p BluRay DTS Rus Ukr 3xEng HDCL",
            vec!["ru", "uk"],
        ),
        (
            "[POPAS] Neon Genesis Evangelion: The End of Evangelion [jp_PT-pt",
            vec!["ja", "pt"],
        ),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        for lang in expected {
            assert!(
                data.languages.contains(&lang.to_string()),
                "{input} missing {lang}"
            );
        }
    }
}

#[test]
fn test_ptt_language_corpus_sample_direct() {
    let cases = [
        (
            "Deadpool 2016 1080p BluRay DTS Rus Ukr 3xEng HDCL",
            vec!["ru", "uk"],
        ),
        ("VAIANA: MOANA (2017) NL-Retail [2D] EAGLE", vec!["nl"]),
        (
            "The Intern 2015 TRUEFRENCH 720p BluRay x264-PiNKPANTERS",
            vec!["fr"],
        ),
        (
            "South.Park.S21E10.iTALiAN.FiNAL.AHDTV.x264-NTROPiC",
            vec!["it"],
        ),
        ("Borat med Norsk Undertekst", vec!["no"]),
        (
            "Curious.George.2.Follow.That.Monkey.2009.DK.SWE.UK.PAL.DVDR-CATC",
            vec!["da", "sv"],
        ),
        (
            "Spider-Man (2002) Blu-Ray [720p] Dual Ingles-Español",
            vec!["en", "es"],
        ),
        (
            "Inception 2010 1080p BRRIP[dual-audio][eng-hindi]",
            vec!["en", "hi"],
        ),
        ("Carros 2 Dublado - Portugues BR (2011)", vec!["pt"]),
        (
            "[POPAS] Neon Genesis Evangelion: The End of Evangelion [jp_PT-pt",
            vec!["ja", "pt"],
        ),
        (
            "The Guard 2011.DK.EN.ES.HR.NL.PT.RO.Subtitles",
            vec!["en", "es", "pt", "ro", "hr", "nl", "da"],
        ),
        ("Subs(ara,fre,ger).srt", vec!["fr", "de", "ar"]),
        (
            "Subs(chi,eng,ind,kor,may,tha,vie).srt",
            vec!["en", "ko", "zh", "vi", "id", "th", "ms"],
        ),
        (
            "Miami.Bici.2020.1080p.NETFLIX.WEB-DL.DDP5.1.H.264.EN-ROSub-ExtremlymTorrents",
            vec!["en", "ro"],
        ),
        (
            "Fauda.S01.HEBREW.1080p.NF.WEBRip.DD5.1.x264-TrollHD[rartv]",
            vec!["he"],
        ),
        (
            "The.Protector.2018.S03.TURKISH.WEBRip.x264-ION10",
            vec!["tr"],
        ),
        (
            "Much Loved (2015) - DVDRip x265 HEVC - ARAB-ITA-FRE AUDIO (ENG S",
            vec!["en", "fr", "it", "ar"],
        ),
        (
            "Godzilla.x.Kong.The.New.Empire.2024.2160p.BluRay.REMUX.DV.P7.HDR.ENG.LATINO.GER.ITA.FRE.HINDI.CHINESE.TrueHD.Atmos.7.1.H265-BEN.THE.MEN",
            vec!["en", "zh", "fr", "la", "it", "de", "hi"],
        ),
        (
            "Sampurna.2023.Bengali.S02.1080p.AMZN.WEB-DL.DD+2.0.H.265-TheBiscuitMan",
            vec!["bn"],
        ),
        (
            "The.Gorge.2025.PLSUB.1080p.ATVP.WEB-DL.DDP5.1.Atmos.H.264-APEX.mkv",
            vec!["pl"],
        ),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(
            data.languages,
            expected.into_iter().map(str::to_string).collect::<Vec<_>>(),
            "{raw}"
        );
    }
}

#[test]
fn test_ptt_language_corpus_sample_direct_more() {
    let cases = [
        (
            "House S 1 CD 1-6 svensk, danska, norsk, finsk sub",
            vec!["da", "fi", "sv", "no"],
        ),
        (
            "The.Prisoner.1967-1968.Complete.Series.Subs.English+Nordic",
            vec!["en", "da", "fi", "sv", "no"],
        ),
        (
            "EMPIRE STATE 2013 DVDRip TRNC English and Española Latin",
            vec!["en", "la", "es"],
        ),
        (
            "The Curse Of The Weeping Woman 2019 BluRay 1080p Tel+Tam+hin+eng",
            vec!["en", "hi", "te", "ta"],
        ),
        (
            "Ghost.Rider.DivX_Gamonet(Ingles-Port.BR)-AC3.avi",
            vec!["en", "pt"],
        ),
        (
            "The Hit List (2011) DVD NTSC WS (eng-fre-pt-spa) [Sk]",
            vec!["en", "fr", "es", "pt"],
        ),
        (
            "Ip.Man.4.The.Finale.2019.CHINESE.1080p.BluRay.x264.TrueHD.7.1.Atmos-HDC",
            vec!["zh"],
        ),
        ("Burning.2018.KOREAN.720p.BluRay.H264.AAC-VXT", vec!["ko"]),
        (
            "Into.the.Night.S01E03.Mathieu.1080p.NF.WEB-DL.DDP5.1.x264-NTG_track33_[vie].srt",
            vec!["vi"],
        ),
        ("Subs/Dear.S01E05.WEBRip.x265-ION265/25_may.srt", vec!["ms"]),
    ];

    for (raw, expected) in cases {
        let data = parse(raw);
        assert_eq!(
            data.languages,
            expected.into_iter().map(str::to_string).collect::<Vec<_>>(),
            "{raw}"
        );
    }
}
