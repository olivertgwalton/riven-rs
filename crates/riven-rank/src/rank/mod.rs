mod fetch;
pub mod scores;

use std::collections::HashMap;

use thiserror::Error;

use crate::parse::{ParsedData, parse};
use crate::settings::RankSettings;

pub use fetch::check_fetch;
pub use scores::get_rank;

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
    (total_len as f64 - distance as f64) / total_len as f64
}

/// Full ranking pipeline for a single torrent.
pub fn rank_torrent(
    raw_title: &str,
    hash: &str,
    correct_title: &str,
    aliases: &HashMap<String, Vec<String>>,
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
    if settings.options.remove_adult_content && data.adult {
        return Err(RankError::AdultContent);
    }

    // 4. Check title similarity
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

    // 5. Compute score
    let model = crate::defaults::RankingModel::default();
    let (total_score, score_parts) = get_rank(&data, settings, &model);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lev_ratio_toy_story() {
        // Matches the TS formula: (len_a + len_b - dist) / (len_a + len_b)
        // a="toy story" (9), b="toy story 1995" (14), dist=5
        // (9 + 14 - 5) / (9 + 14) = 18 / 23 ≈ 0.7826
        let ratio = lev_ratio("toy story", "toy story 1995");
        assert!(ratio > 0.78 && ratio < 0.79);

        // Exact match (both inputs pre-normalised to lowercase)
        assert_eq!(lev_ratio("toy story", "toy story"), 1.0);

        // Completely different (all 3 chars differ: dist=3, total=6, ratio=0.5)
        assert!(lev_ratio("abc", "xyz") < 0.6);
    }
}
