use crate::rank::RankedTorrent;
use crate::settings::RankSettings;

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
fn bucket_enabled(idx: usize, settings: &RankSettings) -> bool {
    match idx {
        0 => settings.resolutions.r2160p,
        1 => settings.resolutions.r1080p,
        2 => settings.resolutions.r720p,
        3 => settings.resolutions.r480p,
        4 => settings.resolutions.r360p,
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
pub fn sort_torrents(
    mut torrents: Vec<RankedTorrent>,
    settings: &RankSettings,
    bucket_limit: usize,
) -> Vec<RankedTorrent> {
    // 1. Filter disabled resolution buckets.
    torrents.retain(|t| {
        let idx = bucket(&t.data.resolution);
        bucket_enabled(idx, settings)
    });

    // 2. Sort: rank desc, then resolution rank desc as tiebreaker.
    torrents.sort_by(|a, b| {
        b.rank.cmp(&a.rank).then_with(|| {
            settings
                .resolution_ranks
                .rank_for(&b.data.resolution)
                .cmp(&settings.resolution_ranks.rank_for(&a.data.resolution))
        })
    });

    if bucket_limit == usize::MAX {
        return torrents;
    }

    // 3. Per-resolution bucket limit — fixed-size array, zero allocation.
    //    Walk in rank order; admit each torrent into its bucket until full.
    let mut counts = [0usize; 6];
    torrents.retain(|t| {
        let idx = bucket(&t.data.resolution);
        if counts[idx] < bucket_limit {
            counts[idx] += 1;
            true
        } else {
            false
        }
    });

    torrents
}
