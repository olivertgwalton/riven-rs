use std::collections::HashMap;

use riven_core::types::CacheCheckFile;
use riven_db::entities::{MediaItem, Stream};
use riven_rank::{ParsedData, RankSettings};

use crate::context::DownloadHierarchyContext;
use crate::flows::download_item::helpers::has_matching_file;

pub struct CachedCandidate<'a> {
    pub stream: &'a Stream,
}

pub fn build_cached_candidates<'a>(
    id: i64,
    item: &MediaItem,
    hierarchy: Option<&DownloadHierarchyContext>,
    streams: &'a [Stream],
    cached_info: &HashMap<String, Vec<CacheCheckFile>>,
    max_size_bytes: Option<u64>,
    min_size_bytes: Option<u64>,
) -> Vec<CachedCandidate<'a>> {
    streams
        .iter()
        .filter_map(|stream| {
            let files = cached_info.get(&stream.info_hash.to_lowercase())?;
            let total_size: u64 = files.iter().map(|file| file.size).sum();
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
            if !has_matching_file(files, item, hierarchy) {
                return None;
            }

            Some(CachedCandidate { stream })
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

pub fn pick_best_for_profile<'a>(
    candidates: &'a [CachedCandidate<'a>],
    item: &MediaItem,
    profile: &RankSettings,
) -> Option<&'a Stream> {
    let model = riven_rank::RankingModel::default();

    let mut scored: Vec<(&Stream, i64, i64)> = candidates
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

            let (fetch, failed_checks) = riven_rank::rank::check_fetch(&parsed, profile);
            if !fetch {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %candidate.stream.info_hash,
                    resolution = parsed.resolution,
                    quality = ?parsed.quality,
                    codec = ?parsed.codec,
                    audio = ?parsed.audio,
                    hdr = ?parsed.hdr,
                    checks = ?failed_checks,
                    "cached stream rejected by active profile fetch checks"
                );
                return None;
            }

            let (score, _) = riven_rank::rank::scores::get_rank(&parsed, profile, &model);
            Some((candidate.stream, score, pack_preference(item, &parsed)))
        })
        .collect();

    scored.sort_by(|(a, sa, pa), (b, sb, pb)| {
        sb.cmp(sa).then_with(|| pb.cmp(pa)).then_with(|| {
            let ra = profile.resolution_ranks.rank_for(stream_resolution(a));
            let rb = profile.resolution_ranks.rank_for(stream_resolution(b));
            rb.cmp(&ra)
        })
    });

    scored.into_iter().next().map(|(stream, _, _)| stream)
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
