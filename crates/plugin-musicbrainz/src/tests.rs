use serde_json::json;

use super::*;

fn release(id: &str, date: Option<&str>, front: bool, media: serde_json::Value) -> MbRelease {
    serde_json::from_value(json!({
        "id": id,
        "date": date,
        "cover-art-archive": { "front": front },
        "media": media,
    }))
    .unwrap()
}

fn cd(position: i32, titles: &[&str]) -> serde_json::Value {
    let tracks: Vec<_> = titles
        .iter()
        .enumerate()
        .map(|(i, title)| json!({ "position": i + 1, "title": title, "recording": { "id": format!("rec-{position}-{i}") } }))
        .collect();
    json!({ "position": position, "format": "CD", "tracks": tracks })
}

const STANDARD: &[&str] = &["Airbag", "Paranoid Android", "Subterranean Homesick Alien"];
const DELUXE: &[&str] = &[
    "Airbag",
    "Paranoid Android",
    "Subterranean Homesick Alien",
    "Lull",
];

/// A deluxe edition is one release among many; the album most files are
/// named for is the tracklist the other editions share.
#[test]
fn the_commonest_tracklist_wins_over_a_deluxe_edition() {
    let releases = vec![
        release("deluxe", Some("1996-01-01"), true, json!([cd(1, DELUXE)])),
        release("us", Some("1997-07-01"), true, json!([cd(1, STANDARD)])),
        release("jp", Some("1997-05-21"), true, json!([cd(1, STANDARD)])),
    ];
    assert_eq!(choose_release(&releases).unwrap().id, "jp");
}

#[test]
fn a_release_with_cover_art_beats_an_earlier_one_without() {
    let releases = vec![
        release("bare", Some("1997"), false, json!([cd(1, STANDARD)])),
        release("art", Some("1998"), true, json!([cd(1, STANDARD)])),
    ];
    assert_eq!(choose_release(&releases).unwrap().id, "art");
}

#[test]
fn a_film_on_the_same_release_adds_no_tracks() {
    let film = json!({ "position": 2, "format": "DVD-Video", "tracks": [{ "position": 1, "title": "Meeting People Is Easy" }] });
    let release = release("cd+dvd", Some("1997"), true, json!([cd(1, STANDARD), film]));
    let tracks = tracks_of(&release);
    assert_eq!(tracks.len(), STANDARD.len());
    assert!(tracks.iter().all(|track| track.disc_number == 1));
}

#[test]
fn tracks_keep_their_disc_and_position() {
    let release = release(
        "2cd",
        None,
        false,
        json!([cd(1, STANDARD), cd(2, &["Lull"])]),
    );
    let tracks = tracks_of(&release);
    let last = tracks.last().unwrap();
    assert_eq!(
        (last.disc_number, last.number, last.title.as_str()),
        (2, 1, "Lull")
    );
    assert_eq!(last.musicbrainz_id.as_deref(), Some("rec-2-0"));
}

#[test]
fn a_group_with_no_tracklist_is_no_album() {
    let group: MbReleaseGroup =
        serde_json::from_value(json!({ "id": "rg", "title": "Untitled" })).unwrap();
    assert!(album_from_releases(&group, &[]).is_none());
}

#[test]
fn an_album_takes_its_group_date_and_cover() {
    let group: MbReleaseGroup = serde_json::from_value(json!({
        "id": "rg",
        "title": "OK Computer",
        "first-release-date": "1997-05-21",
    }))
    .unwrap();
    let releases = vec![release(
        "jp",
        Some("1997-05-21"),
        true,
        json!([cd(1, STANDARD)]),
    )];
    let album = album_from_releases(&group, &releases).unwrap();
    assert_eq!(album.aired_at, NaiveDate::from_ymd_opt(1997, 5, 21));
    assert_eq!(album.release_id.as_deref(), Some("jp"));
    assert_eq!(
        album.poster_path.as_deref(),
        Some("https://coverartarchive.org/release/jp/front-500")
    );
}

#[test]
fn partial_dates_start_at_their_first_day() {
    assert_eq!(parse_date("1997"), NaiveDate::from_ymd_opt(1997, 1, 1));
    assert_eq!(parse_date("1997-07"), NaiveDate::from_ymd_opt(1997, 7, 1));
    assert_eq!(parse_date(""), None);
}

#[test]
fn the_newest_cover_stands_for_the_artist() {
    let artist: MbArtist =
        serde_json::from_value(json!({ "id": "a", "name": "Radiohead" })).unwrap();
    let album = |date: &str, poster: &str| IndexedAlbum {
        aired_at: parse_date(date),
        poster_path: Some(poster.to_owned()),
        ..IndexedAlbum::default()
    };
    let item = artist_item(
        artist,
        vec![album("2007", "in-rainbows"), album("1997", "ok")],
    );
    assert_eq!(item.poster_path.as_deref(), Some("in-rainbows"));
    assert_eq!(item.albums.unwrap()[0].poster_path.as_deref(), Some("ok"));
}

#[test]
fn an_unset_discography_is_albums_only() {
    let settings = PluginSettings::load("MUSICBRAINZ_TEST_UNSET");
    assert_eq!(
        ReleaseType::from_setting(&settings),
        vec![ReleaseType::Album]
    );
}

#[test]
fn a_discography_can_take_eps_too() {
    let mut settings = PluginSettings::load("MUSICBRAINZ_TEST_EPS");
    settings.merge_db_override(&json!({ "release_types": "[\"album\",\"ep\"]" }));
    assert_eq!(
        ReleaseType::from_setting(&settings),
        vec![ReleaseType::Album, ReleaseType::Ep]
    );
}

#[test]
fn a_search_term_is_percent_encoded() {
    assert_eq!(url_encode("Sigur Rós & co"), "Sigur%20R%C3%B3s%20%26%20co");
}
