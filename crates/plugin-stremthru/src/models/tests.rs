use super::*;

#[test]
fn cache_item_status_maps_known_store_states() {
    for (raw, expected) in [
        ("cached", TorrentStatus::Cached),
        ("queued", TorrentStatus::Queued),
        ("downloading", TorrentStatus::Downloading),
        ("processing", TorrentStatus::Processing),
        ("downloaded", TorrentStatus::Downloaded),
        ("uploading", TorrentStatus::Uploading),
        ("failed", TorrentStatus::Failed),
        ("invalid", TorrentStatus::Invalid),
        ("surprise", TorrentStatus::Unknown),
    ] {
        let item: StremthruCacheItem =
            serde_json::from_value(serde_json::json!({ "hash": "abc", "status": raw }))
                .expect("cache item deserializes");
        assert_eq!(item.status, expected, "{raw}");
    }
}
