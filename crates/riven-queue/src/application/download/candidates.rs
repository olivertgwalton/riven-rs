use std::collections::HashMap;

use riven_core::types::CachedStoreEntry;
use riven_db::entities::{MediaItem, Stream};
use riven_rank::{ParsedData, RankSettings};

use crate::context::DownloadHierarchyContext;

pub struct CachedCandidate<'a> {
    pub stream: &'a Stream,
    /// Pre-checked store availability from the bulk cache check.
    /// Empty when operating in direct mode.
    pub stores: Vec<CachedStoreEntry>,
}

pub fn build_cached_candidates<'a>(
    id: i64,
    _item: &MediaItem,
    _hierarchy: Option<&DownloadHierarchyContext>,
    streams: &'a [Stream],
    cached_info: &HashMap<String, Vec<CachedStoreEntry>>,
    max_size_bytes: Option<u64>,
    min_size_bytes: Option<u64>,
) -> Vec<CachedCandidate<'a>> {
    streams
        .iter()
        .filter_map(|stream| {
            let entries = cached_info.get(&stream.info_hash.to_lowercase())?;

            // Use the largest total size reported across stores for the bitrate check.
            let total_size = entries
                .iter()
                .map(|e| e.files.iter().filter_map(|f| f.size).sum::<u64>())
                .max()
                .unwrap_or(0);

            tracing::debug!(id, info_hash = %stream.info_hash, total_size, "stream is cached");

            if max_size_bytes.is_some_and(|max| total_size > max) {
                tracing::debug!(
                    id,
                    info_hash = %stream.info_hash,
                    total_size,
                    "stream filtered: exceeds max bitrate"
                );
                return None;
            }
            if min_size_bytes.is_some_and(|min| total_size < min) {
                tracing::debug!(
                    id,
                    info_hash = %stream.info_hash,
                    total_size,
                    "stream filtered: below min bitrate"
                );
                return None;
            }

            Some(CachedCandidate { stream, stores: entries.clone() })
        })
        .collect()
}

pub fn find_preferred_candidate<'a>(
    candidates: &'a [CachedCandidate<'a>],
    preferred_info_hash: &str,
) -> Option<&'a CachedCandidate<'a>> {
    candidates.iter().find(|candidate| {
        candidate
            .stream
            .info_hash
            .eq_ignore_ascii_case(preferred_info_hash)
    })
}

/// Returns all candidates that pass the profile's fetch checks, sorted best-first.
/// Matches riven-ts: iterate every ranked stream until one succeeds, rather than
/// giving up after the top pick fails.
pub fn rank_candidates_for_profile<'a>(
    candidates: &'a [CachedCandidate<'a>],
    item: &MediaItem,
    profile: &RankSettings,
) -> Vec<&'a CachedCandidate<'a>> {
    let download_profile = build_download_candidate_profile(profile);
    let model = riven_rank::RankingModel::default();

    let mut scored: Vec<(&'a CachedCandidate<'a>, i64, i64)> = candidates
        .iter()
        .filter_map(|candidate| {
            let Some(parsed_data) = candidate.stream.parsed_data.as_ref() else {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %candidate.stream.info_hash,
                    "cached stream rejected: missing parsed_data"
                );
                return None;
            };

            let Ok(parsed) = serde_json::from_value::<ParsedData>(parsed_data.clone()) else {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %candidate.stream.info_hash,
                    "cached stream rejected: invalid parsed_data"
                );
                return None;
            };

            let (fetch, failed_checks) = riven_rank::rank::check_fetch(&parsed, &download_profile);
            if !fetch {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %candidate.stream.info_hash,
                    raw_title = %parsed.raw_title,
                    resolution = parsed.resolution,
                    quality = ?parsed.quality,
                    codec = ?parsed.codec,
                    audio = ?parsed.audio,
                    hdr = ?parsed.hdr,
                    checks = ?failed_checks,
                    "cached stream does not match download preference checks"
                );
                return None;
            }

            let score =
                riven_rank::rank::scores::get_rank_total(&parsed, &download_profile, &model);
            Some((candidate, score, pack_preference(item, &parsed)))
        })
        .collect();

    scored.sort_by(|(a, sa, pa), (b, sb, pb)| {
        sb.cmp(sa).then_with(|| pb.cmp(pa)).then_with(|| {
            let ra = download_profile
                .resolution_ranks
                .rank_for(stream_resolution(a.stream));
            let rb = download_profile
                .resolution_ranks
                .rank_for(stream_resolution(b.stream));
            rb.cmp(&ra)
        })
    });

    scored.into_iter().map(|(c, _, _)| c).collect()
}

fn build_download_candidate_profile(profile: &RankSettings) -> RankSettings {
    let mut download_profile = profile.clone();

    download_profile.custom_ranks.quality.av1.fetch = true;
    download_profile.custom_ranks.quality.remux.fetch = true;
    download_profile.custom_ranks.rips.bdrip.fetch = true;
    download_profile.custom_ranks.rips.dvdrip.fetch = true;
    download_profile.custom_ranks.rips.tvrip.fetch = true;
    download_profile.custom_ranks.rips.uhdrip.fetch = true;
    download_profile.custom_ranks.rips.webdlrip.fetch = true;
    download_profile.custom_ranks.hdr.dolby_vision.fetch = true;
    download_profile.custom_ranks.extras.documentary.fetch = true;
    download_profile.custom_ranks.extras.site.fetch = true;

    // Avoid hard-failing sparse TV releases that only parse weakly.
    download_profile.custom_ranks.quality.hdtv.fetch = true;
    download_profile.custom_ranks.quality.dvd.fetch = true;
    download_profile.custom_ranks.audio.mono.fetch = true;
    download_profile.custom_ranks.audio.mp3.fetch = true;
    download_profile.custom_ranks.audio.stereo.fetch = true;
    download_profile.custom_ranks.hdr.sdr.fetch = true;
    download_profile.custom_ranks.hdr.bit10.fetch = true;

    if download_profile.custom_ranks.audio.stereo.rank.is_none() {
        download_profile.custom_ranks.audio.stereo.rank = Some(0);
    }
    if download_profile.custom_ranks.audio.mono.rank.is_none() {
        download_profile.custom_ranks.audio.mono.rank = Some(-250);
    }
    if download_profile.custom_ranks.audio.mp3.rank.is_none() {
        download_profile.custom_ranks.audio.mp3.rank = Some(-250);
    }
    if download_profile.custom_ranks.hdr.sdr.rank.is_none() {
        download_profile.custom_ranks.hdr.sdr.rank = Some(0);
    }
    if download_profile.custom_ranks.quality.hdtv.rank.is_none() {
        download_profile.custom_ranks.quality.hdtv.rank = Some(-5000);
    }
    if download_profile.custom_ranks.quality.dvd.rank.is_none() {
        download_profile.custom_ranks.quality.dvd.rank = Some(-10000);
    }

    download_profile
}

#[cfg(test)]
mod tests {
    use super::{rank_candidates_for_profile, CachedCandidate};
    use riven_core::types::MediaItemType;
    use riven_db::entities::{MediaItem, Stream};
    use riven_rank::RankSettings;

    fn media_item() -> MediaItem {
        MediaItem {
            id: 1,
            title: "Shrek 2".to_string(),
            full_title: None,
            imdb_id: None,
            tvdb_id: None,
            tmdb_id: None,
            poster_path: None,
            created_at: chrono::Utc::now(),
            updated_at: Some(chrono::Utc::now()),
            indexed_at: None,
            scraped_at: None,
            scraped_times: 0,
            aliases: None,
            network: None,
            country: None,
            language: None,
            is_anime: false,
            aired_at: None,
            year: Some(2004),
            genres: None,
            rating: None,
            content_rating: None,
            state: riven_core::types::MediaItemState::Scraped,
            failed_attempts: 0,
            item_type: MediaItemType::Movie,
            is_requested: false,
            show_status: None,
            season_number: None,
            is_special: None,
            parent_id: None,
            episode_number: None,
            absolute_number: None,
            runtime: None,
            item_request_id: None,
            active_stream_id: None,
        }
    }

    fn stream(id: i64, info_hash: &str, title: &str) -> Stream {
        Stream {
            id,
            info_hash: info_hash.to_string(),
            magnet: format!("magnet:?xt=urn:btih:{info_hash}"),
            created_at: chrono::Utc::now(),
            updated_at: Some(chrono::Utc::now()),
            parsed_data: Some(serde_json::to_value(riven_rank::parse(title)).expect("parse")),
            rank: Some(10),
            file_size_bytes: None,
        }
    }

    #[test]
    fn pick_best_for_profile_respects_profile_resolution_filters() {
        let mut profile = RankSettings::default();
        profile.resolutions.high_definition.r2160p = false;
        profile.resolutions.high_definition.r1080p = true;
        profile.resolution_ranks.r2160p = 0;
        profile.resolution_ranks.r1080p = 5;

        let stream_2160p = stream(1, "hash2160", "Shrek.2.2160p.BluRay");
        let stream_1080p = stream(2, "hash1080", "Shrek.2.1080p.BluRay");
        let candidates = vec![
            CachedCandidate { stream: &stream_2160p, stores: vec![] },
            CachedCandidate { stream: &stream_1080p, stores: vec![] },
        ];

        let best = rank_candidates_for_profile(&candidates, &media_item(), &profile)
            .into_iter()
            .next()
            .expect("1080p stream should remain eligible");

        assert_eq!(best.stream.info_hash, "hash1080");
    }
}

fn pack_preference(item: &MediaItem, parsed: &ParsedData) -> i64 {
    if item.item_type != riven_core::types::MediaItemType::Season {
        return 0;
    }

    let has_one_season = parsed.seasons.len() == 1;
    let has_no_episodes = parsed.episodes.is_empty();
    let has_many_episodes = parsed.episodes.len() > 2;

    match (
        parsed.complete,
        has_one_season,
        has_no_episodes,
        has_many_episodes,
    ) {
        (true, true, true, _) => 3,
        (_, true, true, _) => 2,
        (_, true, _, true) => 1,
        _ => 0,
    }
}

fn stream_resolution(stream: &Stream) -> &str {
    stream
        .parsed_data
        .as_ref()
        .and_then(|parsed| parsed.get("resolution"))
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
}
