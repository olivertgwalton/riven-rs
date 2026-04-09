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
