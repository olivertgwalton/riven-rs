mod fetch;
pub mod scores;

use std::collections::HashMap;
use std::hash::BuildHasher;
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

/// Full ranking pipeline for a single torrent.
///
/// # Errors
///
/// Returns [`RankError`] when the torrent hash is invalid, the parsed content
/// is filtered, title similarity falls below the configured threshold, fetch
/// checks fail, or the final rank is below the configured minimum.
pub fn rank_torrent<S: BuildHasher>(
    raw_title: &str,
    hash: &str,
    correct_title: &str,
    aliases: &HashMap<String, Vec<String>, S>,
    settings: &RankSettings,
) -> Result<RankedTorrent, RankError> {
    // 1. Validate hash (32 or 40 hex chars)
    let hash_len = hash.len();
    if (hash_len != 32 && hash_len != 40) || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(RankError::InvalidHash);
    }

    // 2. Parse title
    let data = parse(raw_title);

    // 3. Check adult content
    if settings.options.content.remove_adult_content && data.adult {
        return Err(RankError::AdultContent);
    }

    // 4. Check title similarity
    let best_ratio = if correct_title.is_empty() {
        0.0
    } else {
        let normalized_query = crate::parse::normalize_title(correct_title);
        let mut best_ratio = lev_ratio(&data.normalized_title, &normalized_query);
        for alias_list in aliases.values() {
            for alias in alias_list {
                let normalized_alias = crate::parse::normalize_title(alias);
                let r = lev_ratio(&data.normalized_title, &normalized_alias);
                if r > best_ratio {
                    best_ratio = r;
                }
            }
        }

        if best_ratio < settings.options.title_similarity {
            tracing::info!(
                parsed = %data.normalized_title,
                query = %normalized_query,
                ratio = best_ratio,
                threshold = settings.options.title_similarity,
                "stream rejected: similarity too low"
            );
            return Err(RankError::TitleSimilarity {
                ratio: best_ratio,
                threshold: settings.options.title_similarity,
            });
        }

        best_ratio
    };

    // 5. Compute score
    let (total_score, score_parts) = get_rank(&data, settings, &DEFAULT_MODEL);

    // 6. Check fetch
    let (fetch, failed_checks) = check_fetch(&data, settings);

    if !fetch {
        return Err(RankError::FetchChecksFailed {
            checks: failed_checks,
        });
    }

    // 7. Check score threshold
    if total_score < settings.options.remove_ranks_under {
        return Err(RankError::RankUnderThreshold {
            rank: total_score,
            threshold: settings.options.remove_ranks_under,
        });
    }

    Ok(RankedTorrent {
        data,
        hash: hash.to_lowercase(),
        rank: total_score,
        fetch,
        failed_checks,
        score_parts,
        lev_ratio: best_ratio,
    })
}

/// Ranking pipeline variant that skips score-part materialization.
///
/// This is intended for internal hot paths that only need the final rank and
/// fetch outcome, not the per-category score breakdown.
pub fn rank_torrent_fast<S: BuildHasher>(
    raw_title: &str,
    hash: &str,
    correct_title: &str,
    aliases: &HashMap<String, Vec<String>, S>,
    settings: &RankSettings,
) -> Result<RankedTorrent, RankError> {
    let hash_len = hash.len();
    if (hash_len != 32 && hash_len != 40) || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(RankError::InvalidHash);
    }

    let data = parse(raw_title);

    if settings.options.content.remove_adult_content && data.adult {
        return Err(RankError::AdultContent);
    }

    let best_ratio = if correct_title.is_empty() {
        0.0
    } else {
        let normalized_query = crate::parse::normalize_title(correct_title);
        let mut best_ratio = lev_ratio(&data.normalized_title, &normalized_query);
        for alias_list in aliases.values() {
            for alias in alias_list {
                let normalized_alias = crate::parse::normalize_title(alias);
                let r = lev_ratio(&data.normalized_title, &normalized_alias);
                if r > best_ratio {
                    best_ratio = r;
                }
            }
        }

        if best_ratio < settings.options.title_similarity {
            return Err(RankError::TitleSimilarity {
                ratio: best_ratio,
                threshold: settings.options.title_similarity,
            });
        }

        best_ratio
    };

    let total_score = get_rank_total(&data, settings, &DEFAULT_MODEL);
    let (fetch, failed_checks) = check_fetch(&data, settings);

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

    Ok(RankedTorrent {
        data,
        hash: hash.to_lowercase(),
        rank: total_score,
        fetch,
        failed_checks: Vec::new(),
        score_parts: HashMap::new(),
        lev_ratio: best_ratio,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lev_ratio_toy_story() {
        let ratio = lev_ratio("toy story", "toy story 1995");
        assert!(ratio > 0.78 && ratio < 0.79);

        // Exact match (both inputs pre-normalised to lowercase)
        assert_eq!(lev_ratio("toy story", "toy story"), 1.0);

        // Completely different (all 3 chars differ: dist=3, total=6, ratio=0.5)
        assert!(lev_ratio("abc", "xyz") < 0.6);
    }
}
