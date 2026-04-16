use super::*;

#[test]
fn parse_torrent_status_maps_known_store_states() {
    assert_eq!(parse_torrent_status("cached"), TorrentStatus::Cached);
    assert_eq!(parse_torrent_status("queued"), TorrentStatus::Queued);
    assert_eq!(
        parse_torrent_status("downloading"),
        TorrentStatus::Downloading
    );
    assert_eq!(
        parse_torrent_status("processing"),
        TorrentStatus::Processing
    );
    assert_eq!(
        parse_torrent_status("downloaded"),
        TorrentStatus::Downloaded
    );
    assert_eq!(parse_torrent_status("uploading"), TorrentStatus::Uploading);
    assert_eq!(parse_torrent_status("failed"), TorrentStatus::Failed);
    assert_eq!(parse_torrent_status("invalid"), TorrentStatus::Invalid);
    assert_eq!(parse_torrent_status("surprise"), TorrentStatus::Unknown);
}
