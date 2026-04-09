use riven_rank::parse;

#[test]
fn test_volume_single() {
    let data = parse("Manga Title Vol 5 720p");
    assert_eq!(data.volumes, vec![5]);
}

#[test]
fn test_volume_range() {
    let data = parse("Manga Title Vol 1-3 720p");
    assert_eq!(data.volumes, vec![1, 2, 3]);
}

#[test]
fn test_ptt_volume_corpus_sample() {
    let volume_cases = [
        (
            "[MTBB] Sword Art Onlineː Alicization - Volume 2 (BD 1080p)",
            vec![2],
        ),
        ("[MTBB] Cross Game - Volume 1-3 (WEB 720p)", vec![1, 2, 3]),
        (
            "Altair - A Record of Battles Vol. 01-08 (Digital) (danke-Empire)",
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        ),
    ];
    for (input, expected) in volume_cases {
        let data = parse(input);
        assert_eq!(data.volumes, expected, "{input}");
    }

    let title_cases = [
        (
            "Guardians of the Galaxy Vol. 2 (2017) 720p HDTC x264 MKVTV",
            "Guardians of the Galaxy Vol 2",
        ),
        (
            "Kill Bill: Vol. 1 (2003) BluRay 1080p 5.1CH x264 Ganool",
            "Kill Bill: Vol 1",
        ),
    ];
    for (input, expected_title) in title_cases {
        let data = parse(input);
        assert_eq!(data.parsed_title, expected_title, "{input}");
        assert!(data.volumes.is_empty(), "{input}");
    }
}
