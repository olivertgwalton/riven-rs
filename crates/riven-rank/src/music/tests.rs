use super::*;

fn judge(raw: &str, artist: &str, album: &str) -> Result<MusicRelease, MusicRejection> {
    judge_album(raw, artist, &[], album)
}

#[test]
fn a_tagged_release_reads_its_format_and_depth() {
    let release = judge(
        "Radiohead - OK Computer (1997) [FLAC 24bit-96kHz]",
        "Radiohead",
        "OK Computer",
    )
    .unwrap();
    assert_eq!(release.format, Some(AudioFormat::Flac));
    assert_eq!(release.bit_depth, Some(24));
    assert!(release.names_album_exactly);
}

#[test]
fn a_scene_release_reads_its_source() {
    let release = judge(
        "Radiohead-OK_Computer-(CDNODATA22)-WEB-FLAC-2016-GROUP",
        "Radiohead",
        "OK Computer",
    )
    .unwrap();
    assert_eq!(release.format, Some(AudioFormat::Flac));
    assert_eq!(release.source, Some(AudioSource::Web));
}

#[test]
fn another_artists_album_is_refused() {
    assert_eq!(
        judge(
            "Muse - OK Computer Tribute [MP3 320]",
            "Radiohead",
            "OK Computer"
        ),
        Err(MusicRejection::WrongArtist)
    );
}

#[test]
fn another_album_by_the_artist_is_refused() {
    assert_eq!(
        judge(
            "Radiohead - The Bends (1995) FLAC",
            "Radiohead",
            "OK Computer"
        ),
        Err(MusicRejection::WrongAlbum)
    );
}

#[test]
fn a_discography_is_not_one_album() {
    assert_eq!(
        judge(
            "Radiohead - Discography 1993-2016 (OK Computer, Kid A) FLAC",
            "Radiohead",
            "OK Computer"
        ),
        Err(MusicRejection::Discography)
    );
    assert_eq!(
        judge(
            "Radiohead - OK Computer + Kid A 1997-2000 FLAC",
            "Radiohead",
            "OK Computer"
        ),
        Err(MusicRejection::Discography)
    );
}

/// `normalize_title` drops years for films; an album can be one.
#[test]
fn a_year_is_an_album_title_too() {
    let release = judge("Taylor Swift - 1989 (2014) [FLAC]", "Taylor Swift", "1989").unwrap();
    assert!(release.names_album_exactly);
}

#[test]
fn an_artist_may_drop_its_article() {
    assert!(
        judge(
            "Beatles - Abbey Road (2019 Remaster) FLAC",
            "The Beatles",
            "Abbey Road"
        )
        .is_ok()
    );
}

#[test]
fn an_alias_names_the_artist() {
    let aliases = vec!["Sigur Ros".to_owned()];
    assert!(
        judge_album(
            "Sigur Ros - Agaetis byrjun FLAC",
            "Sigur Rós",
            &aliases,
            "Ágætis byrjun"
        )
        .is_ok()
    );
}

#[test]
fn a_self_titled_album_is_named_once() {
    assert!(judge("Weezer - Weezer (1994) [FLAC]", "Weezer", "Weezer").is_ok());
    assert!(judge("Metallica (1991) FLAC", "Metallica", "Metallica").is_ok());
}

#[test]
fn a_longer_title_names_the_album_only_partly() {
    let release = judge("Radiohead - Kid A Mnesia (2021) FLAC", "Radiohead", "Kid A").unwrap();
    assert!(!release.names_album_exactly);
}

#[test]
fn a_format_word_in_the_title_is_not_the_format() {
    let release = judge("Schiller - Opus (2013) MP3 320", "Schiller", "Opus").unwrap();
    assert_eq!(release.format, Some(AudioFormat::Mp3));
    assert_eq!(release.lossy_quality, Some(LossyQuality::Cbr(320)));
}

#[test]
fn cd_quality_lossless_ranks_above_hi_res_and_lossy() {
    let rank_of = |raw: &str| rank(&judge(raw, "Radiohead", "OK Computer").unwrap());
    let cd = rank_of("Radiohead - OK Computer (1997) [FLAC]");
    let hi_res = rank_of("Radiohead - OK Computer (1997) [FLAC 24-96]");
    let mp3 = rank_of("Radiohead - OK Computer (1997) [MP3 320]");
    let v0 = rank_of("Radiohead - OK Computer (1997) [MP3 V0]");
    let low = rank_of("Radiohead - OK Computer (1997) [MP3 128kbps]");
    assert!(cd > hi_res && hi_res > mp3 && mp3 > v0 && v0 > low);
}

#[test]
fn a_vinyl_rip_ranks_below_the_same_format_from_cd() {
    let rank_of = |raw: &str| rank(&judge(raw, "Radiohead", "OK Computer").unwrap());
    assert!(
        rank_of("Radiohead - OK Computer (1997) CD FLAC")
            > rank_of("Radiohead - OK Computer (1997) Vinyl FLAC")
    );
}
