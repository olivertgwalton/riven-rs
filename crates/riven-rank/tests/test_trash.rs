use riven_rank::parse;

#[test]
fn test_trash_cam() {
    let data = parse("Movie.Title.2023.CAM.x264");
    assert!(data.trash);
}

#[test]
fn test_trash_telesync() {
    let data = parse("Movie.Title.2023.TS.x264");
    assert!(data.trash);
}

#[test]
fn test_trash_telecine() {
    let data = parse("Movie.Title.2023.TC.x264");
    assert!(data.trash);
}

#[test]
fn test_trash_screener() {
    let data = parse("Movie.Title.2023.SCR.x264");
    assert!(data.trash);
}

#[test]
fn test_trash_r5() {
    let data = parse("Movie.Title.2023.R5.x264");
    assert!(data.trash);
}

#[test]
fn test_trash_leaked() {
    let data = parse("Movie.Title.2023.LEAKED.1080p");
    assert!(data.trash);
}

#[test]
fn test_not_trash_bluray() {
    let data = parse("Movie.Title.2023.1080p.BluRay.x264");
    assert!(!data.trash);
}

#[test]
fn test_ptt_trash_corpus_sample() {
    let cases = [
        ("Body.Cam.S08E07.1080p.WEB.h264-EDITH[EZTVx.to].mkv", false),
        (
            "Avengers Infinity War 2018 NEW PROPER 720p HD-CAM X264 HQ-CPG",
            true,
        ),
        ("Brave.2012.R5.DVDRip.XViD.LiNE-UNiQUE", true),
        (
            "Guardians of the Galaxy (2014) 1080p BluRay 5.1 DTS-HD MA 7.1 [YTS] [YIFY]",
            false,
        ),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.trash, expected, "{input}");
    }
}
