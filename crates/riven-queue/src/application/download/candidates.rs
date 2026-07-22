use std::collections::{HashMap, HashSet};

use riven_db::entities::{MediaItem, Stream};
use riven_rank::{ParsedData, RankSettings};

use super::helpers::stream_resolution;

fn year_candidates(year: i32) -> [i32; 3] {
    [year - 1, year, year + 1]
}

/// The item-side facts needed to re-validate a persisted stream's title
/// against the item it is linked to.
pub struct TitleMatchContext {
    pub correct_title: String,
    pub item_country: Option<String>,
    pub aliases: HashMap<String, Vec<String>>,
    /// The item's own year (episode air year / movie year).
    pub item_year: Option<i32>,
    /// The parent show's year, used so that a wrong-year remake (e.g. the 2021
    /// reboot of a 2007 show) is rejected even when the title matches.
    pub parent_year: Option<i32>,
}

impl TitleMatchContext {
    /// Build from the item and its (optional) show hierarchy context: episodes
    /// and seasons match against the show's title/aliases/country, movies and
    /// shows against their own.
    pub fn new(
        item: &MediaItem,
        hierarchy: Option<&crate::context::DownloadHierarchyContext>,
    ) -> Self {
        let aliases = hierarchy
            .and_then(|h| h.show_aliases.clone())
            .or_else(|| item.aliases.clone())
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        Self {
            correct_title: hierarchy
                .and_then(|h| h.show_title.clone())
                .unwrap_or_else(|| item.title.clone()),
            item_country: hierarchy
                .and_then(|h| h.show_country.clone())
                .or_else(|| item.country.clone()),
            aliases,
            item_year: item.year,
            parent_year: hierarchy.and_then(|h| h.show_year),
        }
    }

    /// Returns true when the parsed release's year is incompatible with the
    /// item's year (and its parent show's year), mirroring the scrape-time
    /// check in `discovery::validate`. A release with no parsed year passes.
    fn year_mismatch(&self, parsed: &ParsedData) -> bool {
        let Some(py) = parsed.year else {
            return false;
        };
        let mut candidates: HashSet<i32> = HashSet::new();
        if let Some(y) = self.item_year {
            candidates.extend(year_candidates(y));
        }
        if let Some(y) = self.parent_year {
            candidates.extend(year_candidates(y));
        }
        !candidates.is_empty() && !candidates.contains(&py)
    }
}

/// Returns the streams that pass the title re-check and the profile's fetch
/// checks, sorted best-first. Cache state is no longer consulted here — the
/// download loop asks each `(plugin, provider)` for cache hits per-stream.
pub fn rank_streams_for_profile<'a>(
    streams: &'a [Stream],
    item: &MediaItem,
    profile: &RankSettings,
    title_ctx: &TitleMatchContext,
) -> Vec<&'a Stream> {
    let download_profile = build_download_candidate_profile(profile);
    let model = riven_rank::RankingModel::default();

    let mut scored: Vec<(&'a Stream, i64, i64)> = streams
        .iter()
        .filter_map(|stream| {
            let Some(parsed_data) = stream.parsed_data.as_ref() else {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %stream.info_hash,
                    "download: stream rejected, it was stored without parsed release data (re-scrape the item to fix)"
                );
                return None;
            };

            let Ok(parsed) = serde_json::from_value::<ParsedData>(parsed_data.clone()) else {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %stream.info_hash,
                    "download: stream rejected, its stored release data could not be read (re-scrape the item to fix)"
                );
                return None;
            };

            if riven_rank::is_extras_only_release(&parsed.raw_title) {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %stream.info_hash,
                    raw_title = %parsed.raw_title,
                    "download: stream rejected, the release only contains extras/bonus material"
                );
                return None;
            }

            if !riven_rank::title_matches(
                &parsed,
                &title_ctx.correct_title,
                title_ctx.item_country.as_deref(),
                &title_ctx.aliases,
                &download_profile,
            ) {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %stream.info_hash,
                    raw_title = %parsed.raw_title,
                    correct_title = %title_ctx.correct_title,
                    "download: stream rejected, the release title is for a different title than this item"
                );
                return None;
            }

            if title_ctx.year_mismatch(&parsed) {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %stream.info_hash,
                    raw_title = %parsed.raw_title,
                    parsed_year = ?parsed.year,
                    item_year = ?title_ctx.item_year,
                    parent_year = ?title_ctx.parent_year,
                    "stream rejected: incorrect year"
                );
                return None;
            }

            let (fetch, failed_checks) = riven_rank::rank::check_fetch(&parsed, &download_profile);
            if !fetch {
                tracing::debug!(
                    item_id = item.id,
                    title = %item.title,
                    info_hash = %stream.info_hash,
                    raw_title = %parsed.raw_title,
                    resolution = parsed.resolution,
                    quality = ?parsed.quality,
                    codec = ?parsed.codec,
                    audio = ?parsed.audio,
                    hdr = ?parsed.hdr,
                    checks = ?failed_checks,
                    "download: stream rejected by the quality profile"
                );
                return None;
            }

            let score =
                riven_rank::rank::scores::get_rank_total(&parsed, &download_profile, &model);
            Some((stream, score, pack_preference(item, &parsed)))
        })
        .collect();

    scored.sort_by(|(a, sa, pa), (b, sb, pb)| {
        sb.cmp(sa).then_with(|| pb.cmp(pa)).then_with(|| {
            let ra = download_profile
                .resolution_ranks
                .rank_for(stream_resolution(a));
            let rb = download_profile
                .resolution_ranks
                .rank_for(stream_resolution(b));
            rb.cmp(&ra)
        })
    });

    scored.into_iter().map(|(s, _, _)| s).collect()
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

#[cfg(test)]
mod tests {
    use super::{TitleMatchContext, rank_streams_for_profile};
    use riven_core::types::MediaItemType;
    use riven_db::entities::{MediaItem, Stream};
    use riven_rank::RankSettings;
    use std::collections::HashMap;

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
            aired_at_utc: None,
            network_timezone: None,
            year: Some(2004),
            genres: None,
            rating: None,
            content_rating: None,
            state: riven_core::types::MediaItemState::Scraped,
            failed_attempts: 0,
            last_scrape_attempt_at: None,
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

    fn title_ctx(correct_title: &str, country: Option<&str>) -> TitleMatchContext {
        TitleMatchContext {
            correct_title: correct_title.to_string(),
            item_country: country.map(str::to_string),
            aliases: HashMap::new(),
            item_year: None,
            parent_year: None,
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
        let streams = [stream_2160p, stream_1080p];

        let best = rank_streams_for_profile(
            &streams,
            &media_item(),
            &profile,
            &title_ctx("Shrek 2", None),
        )
        .into_iter()
        .next()
        .expect("1080p stream should remain eligible");

        assert_eq!(best.info_hash, "hash1080");
    }

    #[test]
    fn wrong_title_streams_are_not_download_candidates() {
        let profile = RankSettings::default();
        let ctx = title_ctx("Top Gear", Some("gbr"));

        // "Top.Gear.UK." releases tag the item's own country and must stay
        // eligible even when every scrape-time ranking profile rejected them
        // (rank = NULL); the wrong-version "France" release must still be dropped.
        let wrong_show = stream(
            1,
            "hashwrongversion",
            "Top.Gear.France.S09E01.1080p.WEB.H264",
        );
        let mut correct_show = stream(2, "hashcorrect", "Top.Gear.UK.S09E01.1080p.WEB.H264");
        correct_show.rank = None;
        let streams = [wrong_show, correct_show];

        let candidates = rank_streams_for_profile(&streams, &media_item(), &profile, &ctx);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].info_hash, "hashcorrect");
    }

    #[test]
    fn wrong_year_reboot_streams_are_not_download_candidates() {
        // Requesting the 2007 "iCarly" must not match the 2021 reboot when the
        // season pack fails and we fall back to per-episode downloads.
        let profile = RankSettings::default();
        let mut ctx = title_ctx("iCarly", None);
        ctx.item_year = Some(2010); // a S03 episode's air year
        ctx.parent_year = Some(2007); // the show's year

        let reboot = stream(1, "hashreboot", "iCarly.2021.S01E01.1080p.WEB.h264-EDITH");
        let original = stream(2, "hashoriginal", "iCarly.S03E18.1080p.WEB.h264-DiRT");
        let streams = [reboot, original];

        let candidates = rank_streams_for_profile(&streams, &media_item(), &profile, &ctx);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].info_hash, "hashoriginal");
    }

    #[test]
    fn extras_only_releases_are_not_download_candidates() {
        let profile = RankSettings::default();
        let ctx = title_ctx("Top Gear", Some("gbr"));

        let extras_disc = stream(1, "hashextras", "Top.Gear.S17.EXTRAS.1080p.BluRay.x264-aAF");
        let season_pack = stream(2, "hashseason", "Top.Gear.S17.1080p.BluRay.x264-aAF");
        let streams = [extras_disc, season_pack];

        let candidates = rank_streams_for_profile(&streams, &media_item(), &profile, &ctx);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].info_hash, "hashseason");
    }
}
