use std::collections::{HashMap, HashSet};

use riven_core::types::*;
use riven_db::repo;
use riven_rank::rank::RankError;
use riven_rank::{QualityProfile, RankSettings};

use crate::flows::merge_builtin_profile_settings;

#[derive(Clone)]
pub struct ParseContext {
    pub item_type: MediaItemType,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_number: Option<i32>,
    pub item_year: Option<i32>,
    pub parent_year: Option<i32>,
    pub item_country: Option<String>,
    pub season_episodes: Vec<(i32, Option<i32>)>,
    pub show_season_numbers: Vec<i32>,
    pub show_status: Option<ShowStatus>,
    pub correct_title: String,
    pub aliases: HashMap<String, Vec<String>>,
    pub profiles: Vec<(String, RankSettings)>,
    pub dubbed_anime_only: bool,
}

#[derive(Clone)]
pub struct RankedStreamCandidate {
    pub info_hash: String,
    pub title: String,
    pub parsed_data: Option<serde_json::Value>,
    pub rank: Option<i64>,
    pub file_size_bytes: Option<u64>,
}

fn log_rank_rejection(info_hash: &str, title: &str, profile_name: Option<&str>, error: &RankError) {
    match error {
        RankError::FetchChecksFailed { checks } => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                checks = ?checks,
                "stream rejected by ranking fetch checks"
            );
        }
        RankError::TitleSimilarity { ratio, threshold } => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                ratio,
                threshold,
                "stream rejected by title similarity"
            );
        }
        RankError::RankUnderThreshold { rank, threshold } => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                rank,
                threshold,
                "stream rejected by rank threshold"
            );
        }
        _ => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                error = %error,
                "stream rejected by ranking"
            );
        }
    }
}

fn year_candidates(year: i32) -> [i32; 3] {
    [year - 1, year, year + 1]
}

fn validate(ctx: &ParseContext, parsed: &riven_rank::ParsedData) -> Option<String> {
    let has_episodes = !parsed.episodes.is_empty();
    let has_seasons = !parsed.seasons.is_empty();

    if ctx.dubbed_anime_only && parsed.anime && !parsed.dubbed {
        return Some("non-dubbed anime torrent (dubbed_anime_only=true)".into());
    }

    if !parsed.anime
        && let (Some(pc), Some(ic)) = (parsed.country.as_deref(), ctx.item_country.as_deref())
        && !pc.eq_ignore_ascii_case(ic)
    {
        return Some(format!("incorrect country: {pc} vs {ic}"));
    }

    if let Some(py) = parsed.year {
        let mut candidates: HashSet<i32> = HashSet::new();
        if let Some(y) = ctx.item_year {
            candidates.extend(year_candidates(y));
        }
        if let Some(y) = ctx.parent_year {
            candidates.extend(year_candidates(y));
        }
        if !candidates.is_empty() && !candidates.contains(&py) {
            return Some(format!("incorrect year: {py}"));
        }
    }

    match ctx.item_type {
        MediaItemType::Movie => {
            if has_seasons || has_episodes {
                return Some("show torrent for movie".into());
            }
        }
        MediaItemType::Episode => {
            if !has_episodes && !has_seasons {
                return Some("no seasons or episodes for episode item".into());
            }
            if has_episodes {
                let ep_num = ctx.episode_number.unwrap_or(0);
                let matches_relative = parsed.episodes.contains(&ep_num);
                let matches_absolute = ctx
                    .absolute_number
                    .is_some_and(|a| parsed.episodes.contains(&a));
                if !matches_relative && !matches_absolute {
                    return Some(format!(
                        "incorrect episode number for episode item: {:?} does not include ep {} (abs {:?})",
                        parsed.episodes, ep_num, ctx.absolute_number
                    ));
                }
            }
            if has_seasons {
                let season_num = ctx.season_number.unwrap_or(0);
                if !parsed.seasons.contains(&season_num) {
                    return Some(format!(
                        "incorrect season number for episode item: {:?} does not include season {}",
                        parsed.seasons, season_num
                    ));
                }
            }
        }
        MediaItemType::Season => {
            if !has_seasons {
                if has_episodes {
                    let abs_eps: HashSet<i32> = ctx
                        .season_episodes
                        .iter()
                        .filter_map(|(_, abs)| *abs)
                        .collect();
                    if !abs_eps.is_empty() {
                        let torrent_eps: HashSet<i32> = parsed.episodes.iter().copied().collect();
                        let intersection = abs_eps.intersection(&torrent_eps).count();
                        if intersection == 0 {
                            return Some("incorrect absolute episode range for season item".into());
                        }
                    }
                }
            } else {
                let season_num = ctx.season_number.unwrap_or(1);
                if !parsed.seasons.contains(&season_num) {
                    return Some(format!(
                        "incorrect season number for season item: {:?} does not include season {}",
                        parsed.seasons, season_num
                    ));
                }
                if has_episodes {
                    let rel_eps: HashSet<i32> =
                        ctx.season_episodes.iter().map(|(ep, _)| *ep).collect();
                    if !rel_eps.is_empty() {
                        let torrent_eps: HashSet<i32> = parsed.episodes.iter().copied().collect();
                        let intersection = rel_eps.intersection(&torrent_eps).count();
                        if intersection != rel_eps.len() {
                            return Some("incorrect episodes for season item".into());
                        }
                    }
                }
            }
        }
        MediaItemType::Show => {
            if !has_episodes && !has_seasons {
                return Some("no seasons or episodes for show item".into());
            }
            if has_seasons && !ctx.show_season_numbers.is_empty() {
                let show_seasons: HashSet<i32> = ctx.show_season_numbers.iter().copied().collect();
                let torrent_seasons: HashSet<i32> = parsed.seasons.iter().copied().collect();
                let intersection = show_seasons.intersection(&torrent_seasons).count();
                let expected = if ctx.show_status == Some(ShowStatus::Continuing) {
                    show_seasons.len().saturating_sub(1)
                } else {
                    show_seasons.len()
                };
                if intersection < expected {
                    return Some(format!(
                        "incorrect number of seasons for show: {intersection} < {expected}"
                    ));
                }
            }
        }
    }

    None
}

pub fn rank_streams(
    ctx: ParseContext,
    streams: HashMap<String, riven_core::types::ScrapeEntry>,
) -> Vec<RankedStreamCandidate> {
    let mut ordered_streams: Vec<(&String, &riven_core::types::ScrapeEntry)> =
        streams.iter().collect();
    ordered_streams.sort_by(|(a, _), (b, _)| a.cmp(b));

    ordered_streams
        .into_iter()
        .filter_map(|(info_hash, entry)| {
            let title = &entry.title;
            let parsed = riven_rank::parse(title);

            if let Some(reason) = validate(&ctx, &parsed) {
                tracing::debug!(info_hash, title, reason, "torrent skipped");
                return None;
            }

            let best = ctx.profiles
                .iter()
                .filter_map(|(profile_name, settings)| {
                    match riven_rank::rank_torrent_fast(
                        title,
                        info_hash,
                        &ctx.correct_title,
                        &ctx.aliases,
                        settings,
                    ) {
                        Ok(ranked) => Some(ranked),
                        Err(error) => {
                            log_rank_rejection(
                                info_hash,
                                title,
                                Some(profile_name.as_str()),
                                &error,
                            );
                            None
                        }
                    }
                })
                .max_by_key(|r| r.rank);

            let (parsed_value, rank) = match best {
                Some(ranked) => {
                    if let Some(bitrate) = ranked.data.bitrate.as_deref() {
                        tracing::info!(
                            info_hash,
                            rank = ranked.rank,
                            bitrate,
                            title,
                            "stream ranked"
                        );
                    } else {
                        tracing::info!(info_hash, rank = ranked.rank, title, "stream ranked");
                    }
                    (serde_json::to_value(&ranked.data).ok(), Some(ranked.rank))
                }
                None => {
                    tracing::debug!(info_hash, title, "stream rejected by all ranking profiles");
                    (serde_json::to_value(&parsed).ok(), None)
                }
            };

            Some(RankedStreamCandidate {
                info_hash: info_hash.clone(),
                title: title.clone(),
                parsed_data: parsed_value,
                rank,
                file_size_bytes: entry.file_size_bytes,
            })
        })
        .collect()
}

pub async fn load_active_profiles(db_pool: &sqlx::PgPool) -> Vec<(String, RankSettings)> {
    let profiles = match repo::get_enabled_profiles(db_pool).await {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "failed to load enabled ranking profiles");
            return vec![("ultra_hd".to_string(), QualityProfile::UltraHd.base_settings().prepare())];
        }
    };

    let mut result: Vec<(String, RankSettings)> = profiles
        .into_iter()
        .filter_map(|p| {
            let settings = if p.is_builtin {
                QualityProfile::ALL
                    .iter()
                    .find(|q| q.id() == p.name.as_str())
                    .map(|q| {
                        let db_empty = matches!(&p.settings, serde_json::Value::Object(m) if m.is_empty())
                            || matches!(&p.settings, serde_json::Value::Null);
                        if db_empty {
                            return q.base_settings().prepare();
                        }
                        match merge_builtin_profile_settings(*q, &p.settings) {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(profile = p.name, error = %e, "failed to parse DB settings, falling back to Rust defaults");
                                q.base_settings().prepare()
                            }
                        }
                    })
            } else {
                serde_json::from_value::<RankSettings>(p.settings)
                    .ok()
                    .map(RankSettings::prepare)
            };
            settings.map(|s| (p.name, s))
        })
        .collect();

    if result.is_empty() {
        result.push(("ultra_hd".to_string(), QualityProfile::UltraHd.base_settings().prepare()));
    }
    result
}

pub async fn load_dubbed_anime_only(db_pool: &sqlx::PgPool) -> bool {
    match repo::get_setting(db_pool, "general").await {
        Ok(Some(v)) => v
            .get("dubbed_anime_only")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        _ => false,
    }
}
