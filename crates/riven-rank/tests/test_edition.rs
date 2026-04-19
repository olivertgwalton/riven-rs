use riven_rank::parse;

#[test]
fn test_edition_directors_cut() {
    let data = parse("Movie.Title.2023.Directors.Cut.1080p.BluRay");
    assert_eq!(data.edition, Some("Directors Cut".into()));
}

#[test]
fn test_edition_extended() {
    let data = parse("Movie.Title.2023.Extended.1080p.BluRay");
    assert_eq!(data.edition, Some("Extended Edition".into()));
}

#[test]
fn test_edition_theatrical() {
    let data = parse("Movie.Title.2023.Theatrical.1080p.BluRay");
    assert_eq!(data.edition, Some("Theatrical".into()));
}

#[test]
fn test_edition_imax() {
    let data = parse("Movie.Title.2023.IMAX.1080p.BluRay");
    assert_eq!(data.edition, Some("IMAX".into()));
}

#[test]
fn test_edition_uncut() {
    let data = parse("Movie.Title.2023.Uncut.1080p.BluRay");
    assert_eq!(data.edition, Some("Uncut".into()));
}

#[test]
fn test_edition_remastered() {
    let data = parse("Movie.Title.2023.Remastered.1080p.BluRay");
    assert_eq!(data.edition, Some("Remastered".into()));
}

#[test]
fn test_ptt_edition_corpus_sample_direct() {
    let cases = [
        (
            "Mary.Poppins.1964.50th.ANNIVERSARY.EDITION.REMUX.1080p.Bluray.AVC.DTS-HD.MA.5.1-LEGi0N",
            Some("Anniversary Edition"),
        ),
        (
            "The.Lord.of.the.Rings.The.Motion.Picture.Trilogy.Extended.Editions.2001-2003.1080p.BluRay.x264.DTS-WiKi",
            Some("Extended Edition"),
        ),
        ("Better.Call.Saul.S03E04.CONVERT.720p.WEB.h264-TBS", None),
        ("Uncut.Gems.2019.1080p.NF.WEB-DL.DDP5.1.x264-NTG", None),
    ];

    for (input, expected) in cases {
        let data = parse(input);
        assert_eq!(data.edition.as_deref(), expected, "{input}");
    }
}
