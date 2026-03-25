use crate::rank::RankedTorrent;
use crate::settings::RankSettings;

/// Sort torrents by rank descending, using resolution as a tiebreaker when ranks
/// are equal — mirroring the TypeScript `sortByRankAndResolution` function.
///
/// Disabled resolutions are filtered out, and the result is limited to
/// `bucket_limit` entries (applied after sorting, across all resolutions).
pub fn sort_torrents(
    mut torrents: Vec<RankedTorrent>,
    settings: &RankSettings,
    bucket_limit: usize,
) -> Vec<RankedTorrent> {
    // Filter to enabled resolutions
    torrents.retain(|t| match t.data.resolution.as_str() {
        "2160p" => settings.resolutions.r2160p,
        "1080p" => settings.resolutions.r1080p,
        "720p" => settings.resolutions.r720p,
        "480p" => settings.resolutions.r480p,
        "360p" => settings.resolutions.r360p,
        _ => settings.resolutions.unknown,
    });

    // Primary: rank descending. Tiebreaker: configured resolution rank descending.
    torrents.sort_by(|a, b| {
        b.rank.cmp(&a.rank).then_with(|| {
            settings.resolution_ranks.rank_for(&b.data.resolution)
                .cmp(&settings.resolution_ranks.rank_for(&a.data.resolution))
        })
    });

    torrents.truncate(bucket_limit);
    torrents
}
