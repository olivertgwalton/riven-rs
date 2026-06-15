mod fetch;
pub mod scores;

use std::collections::HashMap;
use std::sync::LazyLock;

use thiserror::Error;

use crate::parse::{ParsedData, parse};
use crate::settings::RankSettings;

pub use fetch::check_fetch;
pub use scores::{get_rank, get_rank_total};

static DEFAULT_MODEL: LazyLock<crate::defaults::RankingModel> =
    LazyLock::new(crate::defaults::RankingModel::default);

#[derive(Debug, Error)]
pub enum RankError {
    #[error("title similarity too low: {ratio:.2} < {threshold:.2}")]
    TitleSimilarity { ratio: f64, threshold: f64 },
    #[error("fetch checks failed: {checks:?}")]
    FetchChecksFailed { checks: Vec<String> },
    #[error("rank {rank} under threshold {threshold}")]
    RankUnderThreshold { rank: i64, threshold: i64 },
    #[error("invalid hash format")]
    InvalidHash,
    #[error("adult content filtered")]
    AdultContent,
}

#[derive(Debug, Clone)]
pub struct RankedTorrent {
    pub data: ParsedData,
    pub hash: String,
    pub rank: i64,
    pub fetch: bool,
    pub failed_checks: Vec<String>,
    pub score_parts: HashMap<String, i64>,
    pub lev_ratio: f64,
}

/// Accepts the hash shapes the pipeline produces:
///   - 32 or 40 hex chars (sha1/sha256 torrent info_hash)
///   - `nzb-` + 40 hex chars (synthetic NZB info_hash from `plugin-newznab`)
fn is_valid_info_hash(hash: &str) -> bool {
    let body = hash.strip_prefix("nzb-").unwrap_or(hash);
    if body.len() == 40 || body.len() == 32 {
        body.bytes().all(|b| b.is_ascii_hexdigit())
    } else {
        false
    }
}

/// Compute a similarity ratio in [0, 1] between two pre-normalised strings.
///
/// Callers are responsible for normalising (lowercasing, removing punctuation)
/// before passing strings here. Both `data.normalized_title` and the output of
/// `normalize_title()` satisfy this contract.
fn lev_ratio(a: &str, b: &str) -> f64 {
    let distance = strsim::levenshtein(a, b);
    let total_len = a.len() + b.len();
    if total_len == 0 {
        return 1.0;
    }
    let total_len = f64::from(u32::try_from(total_len).unwrap_or(u32::MAX));
    let distance = f64::from(u32::try_from(distance).unwrap_or(u32::MAX));
    (total_len - distance) / total_len
}

/// If the last word of a normalized title is the (lowercased) country code
/// `country`, return the title without it.
fn strip_trailing_country(normalized_title: &str, country: &str) -> Option<String> {
    let stripped = normalized_title
        .strip_suffix(&country.to_ascii_lowercase())?
        .trim_end();
    (!stripped.is_empty()).then(|| stripped.to_string())
}

/// Best similarity ratio between a parsed release title and the correct
/// title or any alias.
///
/// `item_country` is the item's metadata country. Same-named international
/// versions are disambiguated by a country tag in the release name ("Top Gear
/// UK"); when the tagged country IS the item's own country, similarity is also
/// computed with the tag stripped so correctly-tagged releases pass, while
/// releases tagged with another country still fail.
fn best_title_ratio(
    data: &ParsedData,
    correct_title: &str,
    item_country: Option<&str>,
    aliases: &HashMap<String, Vec<String>>,
) -> f64 {
    let normalized_query = crate::parse::normalize_title(correct_title);
    let mut best_ratio = lev_ratio(&data.normalized_title, &normalized_query);

    if let (Some(parsed_country), Some(item_country)) = (data.country.as_deref(), item_country)
        && crate::country::countries_match(parsed_country, item_country)
        && let Some(stripped) = strip_trailing_country(&data.normalized_title, parsed_country)
    {
        best_ratio = best_ratio.max(lev_ratio(&stripped, &normalized_query));
    }

    for alias in aliases.values().flatten() {
        let normalized_alias = crate::parse::normalize_title(alias);
        best_ratio = best_ratio.max(lev_ratio(&data.normalized_title, &normalized_alias));
    }

    best_ratio
}

/// Whether an already-parsed release matches `correct_title` (or an alias)
/// within the profile's similarity threshold.
///
/// This is the same check `rank_torrent` applies at scrape time; the download
/// path re-runs it on persisted streams before selecting candidates so a
/// stream linked to the wrong title can never be auto-downloaded.
#[must_use]
pub fn title_matches(
    data: &ParsedData,
    correct_title: &str,
    item_country: Option<&str>,
    aliases: &HashMap<String, Vec<String>>,
    settings: &RankSettings,
) -> bool {
    if correct_title.is_empty() {
        return true;
    }
    best_title_ratio(data, correct_title, item_country, aliases)
        >= settings.options.title_similarity
}

/// Shared front half of the ranking pipeline: validates the hash, parses the
/// title, filters adult content, and computes the best title-similarity ratio
/// (rejecting below the configured threshold).
///
/// Returns the parsed data and similarity ratio on success.
fn prepare_torrent(
    raw_title: &str,
    hash: &str,
    correct_title: &str,
    item_country: Option<&str>,
    aliases: &HashMap<String, Vec<String>>,
    settings: &RankSettings,
) -> Result<(ParsedData, f64), RankError> {
    if !is_valid_info_hash(hash) {
        return Err(RankError::InvalidHash);
    }

    let data = parse(raw_title);

    if settings.options.content.remove_adult_content && data.adult {
        return Err(RankError::AdultContent);
    }

    if correct_title.is_empty() {
        return Ok((data, 0.0));
    }

    let best_ratio = best_title_ratio(&data, correct_title, item_country, aliases);

    if best_ratio < settings.options.title_similarity {
        return Err(RankError::TitleSimilarity {
            ratio: best_ratio,
            threshold: settings.options.title_similarity,
        });
    }

    Ok((data, best_ratio))
}

/// Shared back half of the pipeline: runs fetch checks and the rank threshold.
///
/// Returns the fetch outcome and the failed-check list on success.
fn finalize_torrent(
    data: &ParsedData,
    total_score: i64,
    settings: &RankSettings,
) -> Result<(bool, Vec<String>), RankError> {
    let (fetch, failed_checks) = check_fetch(data, settings);

    if !fetch {
        return Err(RankError::FetchChecksFailed {
            checks: failed_checks,
        });
    }

    if total_score < settings.options.remove_ranks_under {
        return Err(RankError::RankUnderThreshold {
            rank: total_score,
            threshold: settings.options.remove_ranks_under,
        });
    }

    Ok((fetch, failed_checks))
}

/// Full ranking pipeline for a single torrent, including the per-category score
/// breakdown ([`RankedTorrent::score_parts`]).
///
/// # Errors
///
/// Returns [`RankError`] when the torrent hash is invalid, the parsed content
/// is filtered, title similarity falls below the configured threshold, fetch
/// checks fail, or the final rank is below the configured minimum.
pub fn rank_torrent(
    raw_title: &str,
    hash: &str,
    correct_title: &str,
    item_country: Option<&str>,
    aliases: &HashMap<String, Vec<String>>,
    settings: &RankSettings,
) -> Result<RankedTorrent, RankError> {
    let (data, lev_ratio) = prepare_torrent(
        raw_title,
        hash,
        correct_title,
        item_country,
        aliases,
        settings,
    )?;
    let (total_score, score_parts) = get_rank(&data, settings, &DEFAULT_MODEL);
    let (fetch, failed_checks) = finalize_torrent(&data, total_score, settings)?;

    Ok(RankedTorrent {
        data,
        hash: hash.to_lowercase(),
        rank: total_score,
        fetch,
        failed_checks,
        score_parts,
        lev_ratio,
    })
}

/// Ranking pipeline variant that skips score-part materialization.
///
/// This is intended for internal hot paths that only need the final rank and
/// fetch outcome, not the per-category score breakdown.
///
/// # Errors
///
/// Same conditions as [`rank_torrent`].
pub fn rank_torrent_fast(
    raw_title: &str,
    hash: &str,
    correct_title: &str,
    item_country: Option<&str>,
    aliases: &HashMap<String, Vec<String>>,
    settings: &RankSettings,
) -> Result<RankedTorrent, RankError> {
    let (data, lev_ratio) = prepare_torrent(
        raw_title,
        hash,
        correct_title,
        item_country,
        aliases,
        settings,
    )?;
    let total_score = get_rank_total(&data, settings, &DEFAULT_MODEL);
    finalize_torrent(&data, total_score, settings)?;

    Ok(RankedTorrent {
        data,
        hash: hash.to_lowercase(),
        rank: total_score,
        fetch: true,
        failed_checks: Vec::new(),
        score_parts: HashMap::new(),
        lev_ratio,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lev_ratio_toy_story() {
        let ratio = lev_ratio("toy story", "toy story 1995");
        assert!(ratio > 0.78 && ratio < 0.79);

        assert_eq!(lev_ratio("toy story", "toy story"), 1.0);

        assert!(lev_ratio("abc", "xyz") < 0.6);
    }
}
