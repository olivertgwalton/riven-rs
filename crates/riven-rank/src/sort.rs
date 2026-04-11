use crate::rank::RankedTorrent;
use crate::settings::RankSettings;

struct SortKeyedTorrent {
    bucket: usize,
    resolution_rank: i32,
    torrent: RankedTorrent,
}

/// Map a resolution string to one of six canonical bucket indices.
///   1440p → 1080p bucket, 576p → 480p, 240p → 360p.
#[inline]
fn bucket(res: &str) -> usize {
    match res {
        "2160p" => 0,
        "1080p" | "1440p" => 1,
        "720p" => 2,
        "480p" | "576p" => 3,
        "360p" | "240p" => 4,
        _ => 5, // unknown
    }
}

/// Whether a resolution bucket is enabled in settings.
#[inline]
const fn bucket_enabled(idx: usize, settings: &RankSettings) -> bool {
    match idx {
        0 => settings.resolutions.high_definition.r2160p,
        1 => settings.resolutions.high_definition.r1080p,
        2 => settings.resolutions.high_definition.r720p,
        3 => settings.resolutions.standard_definition.r480p,
        4 => settings.resolutions.standard_definition.r360p,
        _ => settings.resolutions.unknown,
    }
}

/// Sort torrents by rank descending and apply a per-resolution bucket limit.
///
/// 1. Filter to enabled resolutions (with aliases).
/// 2. Sort by rank descending. Equal ranks are broken by the configured
///    resolution tiebreaker.
/// 3. Walk the sorted list in order; admit each torrent into its resolution
///    bucket until that bucket reaches `bucket_limit`.
///
/// A `bucket_limit` of `usize::MAX` disables the limit.
#[must_use]
pub fn sort_torrents(
    torrents: Vec<RankedTorrent>,
    settings: &RankSettings,
    bucket_limit: usize,
) -> Vec<RankedTorrent> {
    // 1. Filter disabled resolutions and cache sort keys once.
    let mut torrents: Vec<SortKeyedTorrent> = torrents
        .into_iter()
        .filter_map(|torrent| {
            let bucket = bucket(&torrent.data.resolution);
            if !bucket_enabled(bucket, settings) {
                return None;
            }

            Some(SortKeyedTorrent {
                bucket,
                resolution_rank: settings.resolution_ranks.rank_for(&torrent.data.resolution),
                torrent,
            })
        })
        .collect();

    // 2. Sort: rank desc, then resolution rank desc as tiebreaker.
    torrents.sort_unstable_by(|a, b| {
        b.torrent
            .rank
            .cmp(&a.torrent.rank)
            .then_with(|| b.resolution_rank.cmp(&a.resolution_rank))
    });

    if bucket_limit == usize::MAX {
        return torrents.into_iter().map(|entry| entry.torrent).collect();
    }

    // 3. Per-resolution bucket limit — fixed-size array, zero allocation.
    //    Walk in rank order; admit each torrent into its bucket until full.
    let mut counts = [0usize; 6];
    torrents.retain(|entry| {
        if counts[entry.bucket] < bucket_limit {
            counts[entry.bucket] += 1;
            true
        } else {
            false
        }
    });

    torrents.into_iter().map(|entry| entry.torrent).collect()
}
