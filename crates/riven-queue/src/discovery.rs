use std::collections::{HashMap, HashSet};

use riven_core::types::*;
use riven_db::repo;
use riven_rank::rank::RankError;
use riven_rank::{QualityProfile, RankSettings};
use serde_json::Value;

#[derive(Clone)]
pub struct ParseContext {
    pub item_type: MediaItemType,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_number: Option<i32>,
    pub item_year: Option<i32>,
    pub parent_year: Option<i32>,
    pub item_country: Option<String>,
    pub item_language: Option<String>,
    pub season_episodes: Vec<(i32, Option<i32>)>,
    pub show_season_numbers: Vec<i32>,
    pub show_status: Option<ShowStatus>,
    /// The item's own (episode) title and air date, used to event-match
    /// sports-style releases that carry no SxxExx numbering.
    pub item_title: String,
    pub item_aired_at: Option<chrono::NaiveDate>,
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

/// ISO 639-1 form of a language code, accepting the alpha-2 codes the release
/// parser emits and the alpha-3 codes metadata providers store (e.g. TVDB's
/// "eng"). Returns `None` for unrecognized values.
fn normalize_lang_code(code: &str) -> Option<String> {
    let lower = code.to_ascii_lowercase();
    let two = match lower.as_str() {
        "eng" => "en",
        "deu" | "ger" => "de",
        "fra" | "fre" => "fr",
        "spa" => "es",
        "ita" => "it",
        "nld" | "dut" => "nl",
        "jpn" => "ja",
        "kor" => "ko",
        "zho" | "chi" => "zh",
        "rus" => "ru",
        "por" => "pt",
        "pol" => "pl",
        "swe" => "sv",
        "nor" => "no",
        "dan" => "da",
        "fin" => "fi",
        "ces" | "cze" => "cs",
        "hun" => "hu",
        "tur" => "tr",
        "ell" | "gre" => "el",
        "heb" => "he",
        "ara" => "ar",
        "hin" => "hi",
        "tha" => "th",
        "vie" => "vi",
        "ukr" => "uk",
        other if other.len() == 2 => other,
        _ => return None,
    };
    Some(two.to_string())
}

fn validate(ctx: &ParseContext, parsed: &riven_rank::ParsedData) -> Option<String> {
    let has_episodes = !parsed.episodes.is_empty();
    let has_seasons = !parsed.seasons.is_empty();

    if ctx.dubbed_anime_only && parsed.anime && !parsed.dubbed {
        return Some("non-dubbed anime torrent (dubbed_anime_only=true)".into());
    }

    if riven_rank::is_extras_only_release(&parsed.raw_title) {
        return Some("extras-only release".into());
    }

    if !parsed.anime
        && !parsed.subbed
        && let (Some(pc), Some(ic)) = (parsed.country.as_deref(), ctx.item_country.as_deref())
        && !riven_rank::countries_match(pc, ic)
    {
        return Some(format!("incorrect country: {pc} vs {ic}"));
    }

    if !parsed.anime
        && !parsed.subbed
        && !parsed.languages.is_empty()
        && let Some(item_lang) = ctx.item_language.as_deref().and_then(normalize_lang_code)
        && !parsed
            .languages
            .iter()
            .any(|l| *l == item_lang || l == "en")
    {
        return Some(format!(
            "incorrect language: {:?} vs {item_lang}",
            parsed.languages
        ));
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
                if crate::application::download::event_match::release_matches_episode(
                    &parsed.raw_title,
                    &ctx.item_title,
                    ctx.item_aired_at,
                ) {
                    return None;
                }
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
    ordered_streams.sort_by_key(|(a, _)| *a);

    ordered_streams
        .into_iter()
        .filter_map(|(info_hash, entry)| {
            let title = &entry.title;
            let parsed = riven_rank::parse(title);

            if let Some(reason) = validate(&ctx, &parsed) {
                tracing::debug!(info_hash, title, reason, "torrent skipped");
                return None;
            }

            let best = ctx
                .profiles
                .iter()
                .filter_map(|(profile_name, settings)| {
                    match riven_rank::rank_torrent_fast(
                        title,
                        info_hash,
                        &ctx.correct_title,
                        ctx.item_country.as_deref(),
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
            return vec![(
                "ultra_hd".to_string(),
                QualityProfile::UltraHd.base_settings().prepare(),
            )];
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
        result.push((
            "ultra_hd".to_string(),
            QualityProfile::UltraHd.base_settings().prepare(),
        ));
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

pub fn merge_builtin_profile_settings(
    profile: QualityProfile,
    override_settings: &Value,
) -> serde_json::Result<RankSettings> {
    let mut merged = serde_json::to_value(profile.base_settings())?;
    merge_json_value(&mut merged, override_settings);
    serde_json::from_value::<RankSettings>(merged).map(RankSettings::prepare)
}

fn merge_json_value(base: &mut Value, override_value: &Value) {
    match (base, override_value) {
        (Value::Object(base_obj), Value::Object(override_obj)) => {
            for (key, value) in override_obj {
                match base_obj.get_mut(key) {
                    Some(existing) => merge_json_value(existing, value),
                    None => {
                        base_obj.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (base_slot, replacement) => {
            *base_slot = replacement.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ParseContext, merge_builtin_profile_settings, validate};
    use riven_core::types::MediaItemType;
    use riven_rank::QualityProfile;
    use serde_json::json;
    use std::collections::HashMap;

    fn episode_ctx() -> ParseContext {
        ParseContext {
            item_type: MediaItemType::Episode,
            season_number: Some(9),
            episode_number: Some(1),
            absolute_number: None,
            item_year: None,
            parent_year: None,
            item_country: Some("gbr".to_string()),
            item_language: Some("eng".to_string()),
            season_episodes: vec![],
            show_season_numbers: vec![],
            show_status: None,
            item_title: "Episode 1".to_string(),
            item_aired_at: None,
            correct_title: "Top Gear".to_string(),
            aliases: HashMap::new(),
            profiles: vec![],
            dubbed_anime_only: false,
        }
    }

    #[test]
    fn validate_rejects_foreign_language_release() {
        let ctx = episode_ctx();
        let parsed = riven_rank::parse("Top.Gear.S09E01.GERMAN.DL.1080p.WEB.x264-TSCC");
        let reason = validate(&ctx, &parsed).expect("German dub should be rejected");
        assert!(reason.starts_with("incorrect language"), "{reason}");
    }

    #[test]
    fn validate_allows_subbed_release_with_foreign_sub_tag() {
        let ctx = episode_ctx();
        let parsed = riven_rank::parse("Top.Gear.S09E01.1080p.WEB.x264.NL.Subs");
        assert_eq!(validate(&ctx, &parsed), None);
    }

    #[test]
    fn validate_rejects_other_country_release() {
        let ctx = episode_ctx();
        let parsed = riven_rank::parse("Top.Gear.US.S09E01.1080p.WEB.x264-GROUP");
        let reason = validate(&ctx, &parsed).expect("US release should be rejected for GB item");
        assert!(reason.starts_with("incorrect country"), "{reason}");
    }

    #[test]
    fn validate_allows_release_tagged_with_item_country() {
        let ctx = episode_ctx();
        let parsed = riven_rank::parse("Top.Gear.UK.S09E01.1080p.WEB.x264-GROUP");
        assert_eq!(validate(&ctx, &parsed), None);
    }

    #[test]
    fn built_in_profile_overrides_are_merged_on_top_of_preset() {
        let settings = merge_builtin_profile_settings(
            QualityProfile::UltraHd,
            &json!({
                "resolutions": {
                    "r1080p": true
                }
            }),
        )
        .expect("settings should parse");

        assert!(settings.resolutions.high_definition.r2160p);
        assert!(settings.resolutions.high_definition.r1080p);
        assert!(!settings.resolutions.high_definition.r720p);
        assert!(!settings.custom_ranks.quality.hdtv.fetch);
        assert!(!settings.custom_ranks.rips.webrip.fetch);
        assert!(!settings.custom_ranks.audio.mono.fetch);
    }
}
