use riven_rank::parse;

#[test]
fn test_network_netflix() {
    let data = parse("Show.Title.S01E01.NF.WEB-DL.1080p");
    assert_eq!(data.network, Some("Netflix".into()));
}

#[test]
fn test_network_amazon() {
    let data = parse("Show.Title.S01E01.AMZN.WEB-DL.1080p");
    assert_eq!(data.network, Some("Amazon".into()));
}

#[test]
fn test_network_apple_tv() {
    let data = parse("Show.Title.S01E01.ATVP.WEB-DL.1080p");
    assert_eq!(data.network, Some("Apple TV".into()));
}

#[test]
fn test_network_disney() {
    let data = parse("Show.Title.S01E01.DSNP.WEB-DL.1080p");
    assert_eq!(data.network, Some("Disney".into()));
}

#[test]
fn test_network_hbo() {
    let data = parse("Show.Title.S01E01.HMAX.WEB-DL.1080p");
    assert_eq!(data.network, Some("HBO".into()));
}

#[test]
fn test_network_crunchyroll() {
    let data = parse("Anime.Title.S01E01.Crunchyroll.WEB-DL.1080p");
    assert_eq!(data.network, Some("Crunchyroll".into()));
}

#[test]
fn test_network_adult_swim() {
    let data = parse("Show.Title.S01E01.Adult.Swim.WEB-DL.720p");
    assert_eq!(data.network, Some("Adult Swim".into()));
}

#[test]
fn test_ptt_network_corpus_sample() {
    let cases = [
        (
            "The Vet Life S02E01 Dunk-A-Doctor 1080p ANPL WEB-DL AAC2 0 H 264-RTN",
            Some("Animal Planet"),
            "The Vet Life",
        ),
        (
            "Extraction.2020.720p.NF.WEB-DL.Dual.Atmos.5.1.x264-BonsaiHD",
            Some("Netflix"),
            "Extraction",
        ),
        (
            "The.Bear.S03.COMPLETE.1080p.HULU.WEB.H264-SuccessfulCrab[TGx]",
            Some("Hulu"),
            "The Bear",
        ),
        (
            "Amazon.Queen.2021.720p.AMZN.WEBRip.800MB.x264-GalaxyRG",
            Some("Amazon"),
            "Amazon Queen",
        ),
    ];

    for (input, expected_network, expected_title) in cases {
        let data = parse(input);
        assert_eq!(data.network.as_deref(), expected_network, "{input}");
        assert_eq!(data.parsed_title, expected_title, "{input}");
    }
}
