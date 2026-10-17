use std::collections::{HashMap, HashSet};

use riven_core::types::*;
use riven_db::repo;
use riven_rank::rank::RankError;
use riven_rank::{QualityProfile, RankSettings};
use serde_json::Value;

/// One episode of the item's show, so a release that names a different
/// episode than its `SxxEyy` claims can be routed to that episode.
#[derive(Clone)]
pub struct ShowEpisode {
    /// `None` for index-only discovery: a contradicting release can only be
    /// dropped there, not reassigned.
    pub id: Option<i64>,
    pub season: i32,
    pub episode: i32,
    pub title: String,
    /// Scene (season, episode) from TheXEM, when known.
    pub scene: Option<(i32, i32)>,
}

/// The (season, episode) numberings a release may use for an episode: its
/// scene numbering (TheXEM) when known, and TVDB's unless another episode of
/// the show has that as its scene numbering. Kitchen Nightmares' TVDB S08E05
/// is scene S09E05, and a release numbered S08E05 is TVDB's S07E05; Naruto
/// Shippuden's TVDB S02E01 ships as both S02E01 and scene S01E33.
pub(crate) fn release_numberings(
    tvdb: (i32, i32),
    scene: Option<(i32, i32)>,
    show_episodes: &[ShowEpisode],
) -> Vec<(i32, i32)> {
    let taken = show_episodes
        .iter()
        .any(|e| e.scene == Some(tvdb) && (e.season, e.episode) != tvdb);
    scene.into_iter().chain((!taken).then_some(tvdb)).collect()
}

/// The scene season a whole TVDB season ships as: TheXEM maps every one of
/// its episodes to one scene season that no other TVDB season uses (Kitchen
/// Nightmares' TVDB S07 is scene S08). `None` when it maps to itself, is
/// split (KN's TVDB S01 is scene S01 + S02) or shared (every Naruto Shippuden
/// season is scene S01): no single pack number names it then.
pub fn season_scene_number(season: i32, show_episodes: &[ShowEpisode]) -> Option<i32> {
    let mut own = show_episodes
        .iter()
        .filter(|e| e.season == season)
        .map(|e| e.scene.map(|(s, _)| s));
    let first = own.next()??;
    let shared = show_episodes
        .iter()
        .any(|e| e.season != season && e.scene.is_some_and(|(s, _)| s == first));
    (first != season && own.all(|s| s == Some(first)) && !shared).then_some(first)
}

/// Season numbers a pack may use for TVDB `season`, each with the episode
/// numbers a pack under it should hold: its scene season (above), and TVDB's
/// own unless other seasons' episodes ship under that number while this
/// season's don't (a KN "S07" pack is TVDB S06).
fn season_pack_numberings(
    season: i32,
    tvdb_episodes: HashSet<i32>,
    show_episodes: &[ShowEpisode],
) -> Vec<(i32, HashSet<i32>)> {
    let scene = season_scene_number(season, show_episodes).map(|scene_season| {
        let eps = show_episodes
            .iter()
            .filter(|e| e.season == season)
            .filter_map(|e| e.scene.map(|(_, ep)| ep))
            .collect();
        (scene_season, eps)
    });
    let used_by_others = show_episodes
        .iter()
        .any(|e| e.season != season && e.scene.is_some_and(|(s, _)| s == season));
    let all_own = show_episodes
        .iter()
        .filter(|e| e.season == season)
        .all(|e| e.scene.is_some_and(|(s, _)| s == season));
    let taken = used_by_others && !all_own;
    scene
        .into_iter()
        .chain((!taken).then_some((season, tvdb_episodes)))
        .collect()
}

#[derive(Clone)]
pub struct ParseContext {
    pub item_type: MediaItemType,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_number: Option<i32>,
    /// The episode's scene (season, episode) from TheXEM. When set, releases
    /// are numbered this way instead of by `season_number`/`episode_number`.
    pub scene: Option<(i32, i32)>,
    pub item_year: Option<i32>,
    pub parent_year: Option<i32>,
    pub item_country: Option<String>,
    pub season_episodes: Vec<(i32, Option<i32>)>,
    pub show_season_numbers: Vec<i32>,
    pub show_status: Option<ShowStatus>,
    /// The item's own (episode) title and air date, used to event-match
    /// sports-style releases that carry no SxxExx numbering.
    pub item_title: String,
    pub item_aired_at: Option<chrono::NaiveDate>,
    /// Every episode of the show, for episode items; empty disables the
    /// episode-title check.
    pub show_episodes: Vec<ShowEpisode>,
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
    /// See [`riven_core::types::ScrapeEntry::source_url`]: the fetchable
    /// handle for sources whose info_hash is synthetic, carried through to
    /// the stream row.
    pub source_url: Option<String>,
    /// Set when the release names another episode of the show by title: the
    /// stream belongs to that episode, not the one being scraped.
    pub reassign_to: Option<i64>,
}

fn log_rank_rejection(info_hash: &str, title: &str, profile_name: Option<&str>, error: &RankError) {
    match error {
        RankError::FetchChecksFailed { checks } => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                checks = ?checks,
                "parse: release rejected, it fails this profile's requirements"
            );
        }
        RankError::TitleSimilarity { ratio, threshold } => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                ratio,
                threshold,
                "parse: release rejected, its title is too different from the item's title"
            );
        }
        RankError::RankUnderThreshold { rank, threshold } => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                rank,
                threshold,
                "parse: release rejected, its quality score is below the profile's minimum"
            );
        }
        _ => {
            tracing::debug!(
                info_hash,
                title,
                profile = profile_name,
                error = %error,
                "parse: release rejected while ranking it"
            );
        }
    }
}

/// True when a release year `parsed_year` is more than one year away from
/// every known year (the item's own and its parent show's). With neither year
/// known, nothing mismatches.
pub(crate) fn year_mismatch(
    parsed_year: i32,
    item_year: Option<i32>,
    parent_year: Option<i32>,
) -> bool {
    let years = [item_year, parent_year];
    years.iter().any(Option::is_some)
        && !years
            .iter()
            .flatten()
            .any(|y| (y - 1..=y + 1).contains(&parsed_year))
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

    if let Some(py) = parsed.year
        && year_mismatch(py, ctx.item_year, ctx.parent_year)
    {
        return Some(format!("incorrect year: {py}"));
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
            let numberings = release_numberings(
                (
                    ctx.season_number.unwrap_or(0),
                    ctx.episode_number.unwrap_or(0),
                ),
                ctx.scene,
                &ctx.show_episodes,
            );
            let matches_absolute = ctx
                .absolute_number
                .is_some_and(|a| parsed.episodes.contains(&a));
            let matches = |&(season, episode): &(i32, i32)| {
                (!has_episodes || matches_absolute || parsed.episodes.contains(&episode))
                    && (!has_seasons || parsed.seasons.contains(&season))
            };
            // A title naming this very episode outranks any number: Amazon
            // ships KN's TVDB S06E10 "Revisited #9" as "S06E10.Revisited.No.9",
            // which is TVDB S05E10's scene number.
            let names_this = crate::application::download::event_match::named_episode(
                &parsed.raw_title,
                &ctx.show_episodes,
                |e| e.title.as_str(),
            )
            .is_some_and(|e| {
                (Some(e.season), Some(e.episode)) == (ctx.season_number, ctx.episode_number)
            });
            if !names_this && !numberings.iter().any(matches) && (has_seasons || !matches_absolute)
            {
                return Some(format!(
                    "incorrect season/episode for episode item: seasons {:?} episodes {:?} match none of {numberings:?} (abs {:?})",
                    parsed.seasons, parsed.episodes, ctx.absolute_number
                ));
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
                let numberings = season_pack_numberings(
                    season_num,
                    ctx.season_episodes.iter().map(|(ep, _)| *ep).collect(),
                    &ctx.show_episodes,
                );
                let torrent_eps: HashSet<i32> = parsed.episodes.iter().copied().collect();
                let Some((_, want_eps)) = numberings
                    .iter()
                    .find(|(season, _)| parsed.seasons.contains(season))
                else {
                    return Some(format!(
                        "incorrect season number for season item: {:?} does not include season {:?}",
                        parsed.seasons,
                        numberings.iter().map(|(s, _)| s).collect::<Vec<_>>()
                    ));
                };
                if has_episodes && !want_eps.is_empty() && !want_eps.is_subset(&torrent_eps) {
                    return Some("incorrect episodes for season item".into());
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

            let named = (ctx.item_type == MediaItemType::Episode)
                .then(|| {
                    crate::application::download::event_match::named_episode(
                        &parsed.raw_title,
                        &ctx.show_episodes,
                        |e| e.title.as_str(),
                    )
                })
                .flatten()
                .filter(|e| {
                    (Some(e.season), Some(e.episode)) != (ctx.season_number, ctx.episode_number)
                });
            let reassign_to = match named {
                Some(other) => {
                    tracing::debug!(
                        info_hash,
                        title,
                        season = other.season,
                        episode = other.episode,
                        episode_title = %other.title,
                        "parse: release names a different episode by title"
                    );
                    Some(other.id?)
                }
                None => {
                    if let Some(reason) = validate(&ctx, &parsed) {
                        tracing::debug!(
                            info_hash,
                            title,
                            reason,
                            "parse: release does not belong to this item"
                        );
                        return None;
                    }
                    None
                }
            };

            let best = ctx
                .profiles
                .iter()
                .filter_map(|(profile_name, settings)| {
                    match riven_rank::rank_torrent(
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
                        tracing::debug!(
                            info_hash,
                            rank = ranked.rank,
                            bitrate,
                            title,
                            "parse: release accepted as a download candidate"
                        );
                    } else {
                        tracing::debug!(
                            info_hash,
                            rank = ranked.rank,
                            title,
                            "parse: release accepted as a download candidate"
                        );
                    }
                    (serde_json::to_value(&ranked.data).ok(), Some(ranked.rank))
                }
                None => {
                    tracing::debug!(
                        info_hash,
                        title,
                        "parse: release rejected by every enabled quality profile"
                    );
                    (serde_json::to_value(&parsed).ok(), None)
                }
            };

            Some(RankedStreamCandidate {
                info_hash: info_hash.clone(),
                title: title.clone(),
                parsed_data: parsed_value,
                rank,
                file_size_bytes: entry.file_size_bytes,
                source_url: entry.source_url.clone(),
                reassign_to,
            })
        })
        .collect()
}

pub async fn load_active_profiles() -> Vec<(String, RankSettings)> {
    let profiles = match repo::get_enabled_profiles().await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "parse: could not load the configured quality profiles; falling back to the built-in defaults");
            return default_profiles();
        }
    };

    let result: Vec<(String, RankSettings)> = profiles
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
                                tracing::warn!(profile = p.name, error = %e, "parse: this quality profile's saved settings could not be read; using the built-in defaults for it");
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
        return default_profiles();
    }
    result
}

fn default_profiles() -> Vec<(String, RankSettings)> {
    vec![(
        "ultra_hd".to_string(),
        QualityProfile::UltraHd.base_settings().prepare(),
    )]
}

pub async fn load_dubbed_anime_only() -> bool {
    match repo::get_setting("general").await {
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
    use super::{
        ParseContext, ShowEpisode, merge_builtin_profile_settings, rank_streams,
        season_scene_number, validate,
    };
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
            scene: None,
            item_year: None,
            parent_year: None,
            item_country: Some("gbr".to_string()),
            season_episodes: vec![],
            show_season_numbers: vec![],
            show_status: None,
            item_title: "Episode 1".to_string(),
            item_aired_at: None,
            show_episodes: vec![],
            correct_title: "Top Gear".to_string(),
            aliases: HashMap::new(),
            profiles: vec![],
            dubbed_anime_only: false,
        }
    }

    #[test]
    fn validate_does_not_hard_reject_foreign_language_release() {
        // Language is RTN's job, not a pre-RTN hard filter: `riven_rank`'s
        // `language_handler` already ports RTN's permissive-by-default model
        // (empty `required`/`exclude`, ranking-only `preferred`). `validate`
        // itself has no language check, matching riven-ts's `validateTorrent`.
        let ctx = episode_ctx();
        let parsed = riven_rank::parse("Top.Gear.S09E01.GERMAN.DL.1080p.WEB.x264-TSCC");
        assert_eq!(validate(&ctx, &parsed), None);
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
    fn release_naming_another_episode_is_reassigned_to_it() {
        // TVDB has "Simmer Down" as S08E11; releases ship it as S09E11, which
        // TVDB gives to a different episode.
        let ep = |id, season, episode, title: &str| ShowEpisode {
            id: Some(id),
            season,
            episode,
            title: title.to_string(),
            scene: None,
        };
        let ctx = ParseContext {
            season_number: Some(9),
            episode_number: Some(11),
            item_country: None,
            item_title: "Wick 'n Ore Kitchen & Tap".to_string(),
            correct_title: "Kitchen Nightmares".to_string(),
            show_episodes: vec![
                ep(1, 8, 11, "Simmer Down"),
                ep(2, 9, 11, "Wick 'n Ore Kitchen & Tap"),
            ],
            ..episode_ctx()
        };
        let streams = HashMap::from([
            (
                "a".to_string(),
                riven_core::types::ScrapeEntry::new(
                    "Kitchen.Nightmares.S09E11.Simmer.Down.1080p.AMZN.WEB-DL-FLUX",
                ),
            ),
            (
                "b".to_string(),
                riven_core::types::ScrapeEntry::new("Kitchen.Nightmares.S09E11.1080p.WEB-EDITH"),
            ),
        ]);
        let ranked = rank_streams(ctx.clone(), streams.clone());
        let by_hash: HashMap<_, _> = ranked
            .iter()
            .map(|c| (c.info_hash.as_str(), c.reassign_to))
            .collect();
        assert_eq!(by_hash.get("a"), Some(&Some(1)));
        assert_eq!(by_hash.get("b"), Some(&None));

        // Without DB ids (index-only discovery) it is dropped instead.
        let ctx = ParseContext {
            show_episodes: ctx
                .show_episodes
                .into_iter()
                .map(|e| ShowEpisode { id: None, ..e })
                .collect(),
            ..ctx
        };
        let ranked = rank_streams(ctx, streams);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].info_hash, "b");
    }

    #[test]
    fn validate_uses_scene_numbering_when_known() {
        // Kitchen Nightmares (US), tvdb 80552: TVDB S07E05 "Juicy Box" is
        // scene S08E05, and TVDB S08E05 "3 Southern Girls" is scene S09E05.
        let ep = |season, scene, title: &str| ShowEpisode {
            id: None,
            season,
            episode: 5,
            title: title.to_string(),
            scene: Some(scene),
        };
        let show_episodes = vec![
            ep(7, (8, 5), "Juicy Box"),
            ep(8, (9, 5), "3 Southern Girls"),
        ];
        let ctx = |season, scene| ParseContext {
            season_number: Some(season),
            episode_number: Some(5),
            scene: Some(scene),
            item_country: None,
            show_episodes: show_episodes.clone(),
            correct_title: "Kitchen Nightmares".to_string(),
            ..episode_ctx()
        };
        let parsed = riven_rank::parse("Kitchen.Nightmares.S08E05.1080p.HULU-ViETNAM");
        assert_eq!(validate(&ctx(7, (8, 5)), &parsed), None);
        assert!(validate(&ctx(8, (9, 5)), &parsed).is_some());
        // TVDB S07E05 is nobody's scene number, so TVDB-numbered releases
        // of it still count.
        let parsed = riven_rank::parse("Kitchen.Nightmares.S07E05.720p.WEB-GROUP");
        assert_eq!(validate(&ctx(7, (8, 5)), &parsed), None);
    }

    #[test]
    fn validate_accepts_a_title_naming_this_episode_whatever_its_number() {
        // Kitchen Nightmares: TVDB S06E10 "Revisited #9" is scene S07E10, and
        // S06E10 is TVDB S05E10's scene number.
        let ep = |season, episode, scene, title: &str| ShowEpisode {
            id: None,
            season,
            episode,
            title: title.to_string(),
            scene: Some(scene),
        };
        let ctx = ParseContext {
            season_number: Some(6),
            episode_number: Some(10),
            scene: Some((7, 10)),
            item_country: None,
            show_episodes: vec![
                ep(5, 10, (6, 10), "Spanish Pavillion"),
                ep(6, 10, (7, 10), "Revisited #9"),
            ],
            correct_title: "Kitchen Nightmares".to_string(),
            ..episode_ctx()
        };
        let titled =
            riven_rank::parse("Kitchen.Nightmares.S06E10.Revisited.No.9.480p.AMZN.WEB-DL-SiGMA");
        assert_eq!(validate(&ctx, &titled), None);
        let untitled = riven_rank::parse("Kitchen.Nightmares.S06E10.1080p.WEB-GROUP");
        assert!(validate(&ctx, &untitled).is_some());
    }

    #[test]
    fn validate_accepts_tvdb_and_scene_numbering_when_unambiguous() {
        // Naruto Shippuden: TVDB S02E01 (abs 33) is scene S01E33.
        let ctx = ParseContext {
            season_number: Some(2),
            episode_number: Some(1),
            absolute_number: Some(33),
            scene: Some((1, 33)),
            item_country: None,
            correct_title: "Naruto Shippuden".to_string(),
            ..episode_ctx()
        };
        for release in [
            "Naruto.Shippuden.S02E01.1080p.WEB-GROUP",
            "Naruto.Shippuden.S01E33.1080p.WEB-GROUP",
            "[AnimeRG] Naruto Shippuden - 033 [1080p]",
        ] {
            assert_eq!(
                validate(&ctx, &riven_rank::parse(release)),
                None,
                "{release}"
            );
        }
        let parsed = riven_rank::parse("Naruto.Shippuden.S01E01.1080p.WEB-GROUP");
        assert!(validate(&ctx, &parsed).is_some());
    }

    #[test]
    fn season_packs_use_the_scene_season_when_unshared() {
        // Kitchen Nightmares (US): TVDB S01 is scene S01+S02, then every
        // TVDB season n is scene n+1.
        let ep = |season, episode, scene| ShowEpisode {
            id: None,
            season,
            episode,
            title: String::new(),
            scene: Some(scene),
        };
        let mut show_episodes = vec![ep(1, 1, (1, 1)), ep(1, 2, (2, 1))];
        for season in 2..=8 {
            show_episodes.extend((1..=3).map(|e| ep(season, e, (season + 1, e))));
        }
        assert_eq!(season_scene_number(7, &show_episodes), Some(8));
        assert_eq!(season_scene_number(1, &show_episodes), None, "split");

        let ctx = |season| ParseContext {
            item_type: MediaItemType::Season,
            season_number: Some(season),
            episode_number: None,
            season_episodes: (1..=3).map(|e| (e, None)).collect(),
            item_country: None,
            show_episodes: show_episodes.clone(),
            correct_title: "Kitchen Nightmares".to_string(),
            ..episode_ctx()
        };
        let pack = |title| riven_rank::parse(title);
        assert_eq!(
            validate(
                &ctx(7),
                &pack("Kitchen.Nightmares.US.S08.1080p.WEB.h264-BAE")
            ),
            None
        );
        assert!(
            validate(
                &ctx(7),
                &pack("Kitchen.Nightmares.US.S07.1080p.WEB.h264-BAE")
            )
            .is_some(),
            "scene S07 is TVDB S06"
        );
        assert!(
            validate(&ctx(2), &pack("Kitchen.Nightmares.US.S02.720p.HDTV-GROUP")).is_some(),
            "scene S02 is half of TVDB S01"
        );

        // Naruto Shippuden: every TVDB season is scene S01, so no season
        // has a scene pack number and TVDB-numbered packs still count.
        let shared: Vec<ShowEpisode> = (1..=3)
            .flat_map(|season| (1..=3).map(move |e| ep(season, e, (1, season * 3 + e))))
            .collect();
        assert_eq!(season_scene_number(2, &shared), None);
        let ctx = ParseContext {
            show_episodes: shared,
            correct_title: "Naruto Shippuden".to_string(),
            ..ctx(2)
        };
        assert_eq!(
            validate(&ctx, &pack("Naruto.Shippuden.S02.1080p.WEB-GROUP")),
            None
        );
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
