//! Ranking behaviour, as tables rather than one hand-built settings struct per
//! scenario:
//!
//! * `fetch` — a release, a settings patch, and whether it is accepted.
//! * `order` — releases a profile must rank best-first. Order is what the
//!   pipeline actually consumes, and it survives score re-tuning that exact
//!   numbers would not.
//! * `similarity` — the title/country gate in front of ranking.
//!
//! On top of the hand-written tables, every built-in profile is run against the
//! whole release corpus and the outcome is pinned in
//! `tests/fixtures/profile_selection.json`:
//!
//! ```text
//! cargo test -p riven-rank --test ranking -- --ignored bless_profile_selection
//! ```

mod common;

use std::collections::{BTreeMap, HashMap};

use common::corpus;
use riven_rank::rank::{RankError, check_fetch, scores::get_rank_total};
use riven_rank::{QualityProfile, RankSettings, RankingModel, parse, rank_torrent};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const CASES: &str = include_str!("fixtures/ranking_cases.json");
const SELECTION: &str = include_str!("fixtures/profile_selection.json");
const SELECTION_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/profile_selection.json"
);
const PROFILES: &[QualityProfile] = &[
    QualityProfile::UltraHd,
    QualityProfile::Hd,
    QualityProfile::Standard,
];
/// Enough of the ranked head to notice a scoring change, short enough to read.
const TOP_RANKED: usize = 20;

#[derive(Deserialize)]
struct Cases {
    fetch: Vec<FetchCase>,
    order: Vec<OrderCase>,
    similarity: Vec<SimilarityCase>,
}

/// Every case carries an optional `why` that is reported on failure.
#[derive(Deserialize)]
struct FetchCase {
    #[serde(default)]
    why: String,
    #[serde(default)]
    profile: Option<QualityProfile>,
    #[serde(default)]
    settings: Value,
    title: String,
    fetch: bool,
    #[serde(default)]
    failed: Vec<String>,
}

#[derive(Deserialize)]
struct OrderCase {
    #[serde(default)]
    why: String,
    profile: QualityProfile,
    best_to_worst: Vec<String>,
}

#[derive(Deserialize)]
struct SimilarityCase {
    #[serde(default)]
    why: String,
    title: String,
    correct_title: String,
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    lev_ratio: Option<f64>,
    #[serde(default)]
    error: Option<String>,
}

fn cases() -> Cases {
    serde_json::from_str(CASES).expect("ranking cases are valid JSON")
}

/// Profile defaults (or plain defaults) with a JSON patch applied on top, so a
/// case only spells out the settings it is about.
fn settings_for(profile: Option<QualityProfile>, patch: &Value) -> RankSettings {
    let base = profile.map_or_else(RankSettings::default, QualityProfile::base_settings);
    let mut merged = serde_json::to_value(base).expect("settings serialize");
    merge(&mut merged, patch);
    serde_json::from_value::<RankSettings>(merged)
        .expect("patched settings deserialize")
        .prepare()
}

fn merge(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                merge(target.entry(key).or_insert(Value::Null), value);
            }
        }
        (target, patch) => {
            if !patch.is_null() {
                *target = patch.clone();
            }
        }
    }
}

fn report(kind: &str, failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{} {kind} case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn fetch_decisions() {
    let failures: Vec<String> = cases()
        .fetch
        .iter()
        .filter_map(|case| {
            let settings = settings_for(case.profile, &case.settings);
            let (fetch, failed) = check_fetch(&parse(&case.title), &settings);
            (fetch != case.fetch || failed != case.failed).then(|| {
                format!(
                    "  {}\n      {}\n      got  fetch={fetch} failed={failed:?}\n      want fetch={} failed={:?}",
                    case.title, case.why, case.fetch, case.failed
                )
            })
        })
        .collect();

    report("fetch", &failures);
}

#[test]
fn profiles_rank_releases_best_first() {
    let model = RankingModel::default();

    let failures: Vec<String> = cases()
        .order
        .iter()
        .filter_map(|case| {
            let settings = case.profile.base_settings().prepare();
            let ranked: Vec<(&str, i64, bool)> = case
                .best_to_worst
                .iter()
                .map(|title| {
                    let data = parse(title);
                    let (fetch, _) = check_fetch(&data, &settings);
                    (
                        title.as_str(),
                        get_rank_total(&data, &settings, &model),
                        fetch,
                    )
                })
                .collect();

            let rejected = ranked.iter().any(|(_, _, fetch)| !fetch);
            let out_of_order = ranked.windows(2).any(|pair| pair[0].1 <= pair[1].1);

            (rejected || out_of_order).then(|| {
                format!(
                    "  {} ({})\n{}",
                    case.profile.id(),
                    case.why,
                    ranked
                        .iter()
                        .map(|(title, rank, fetch)| format!(
                            "      {rank:>7}{} {title}",
                            if *fetch { "" } else { " REJECTED" }
                        ))
                        .collect::<Vec<_>>()
                        .join("\n")
                )
            })
        })
        .collect();

    report("ordering", &failures);
}

#[test]
fn title_similarity_gate() {
    let settings = RankSettings::default().prepare();
    let aliases = HashMap::new();

    let failures: Vec<String> = cases()
        .similarity
        .iter()
        .filter_map(|case| {
            let result = rank_torrent(
                &case.title,
                "c08a9ee8ce3a5c2c08865e2b05406273cabc97e7",
                &case.correct_title,
                case.country.as_deref(),
                &aliases,
                &settings,
            );

            let got = match &result {
                Ok(ranked) => format!("lev_ratio={}", ranked.lev_ratio),
                Err(error) => format!("error={}", slug(error)),
            };
            let want = match (case.lev_ratio, &case.error) {
                (Some(ratio), None) => format!("lev_ratio={ratio}"),
                (None, Some(error)) => format!("error={error}"),
                _ => panic!(
                    "case must expect exactly one of lev_ratio/error: {}",
                    case.title
                ),
            };

            (got != want).then(|| {
                format!(
                    "  {}\n      {}\n      got {got}, want {want}",
                    case.title, case.why
                )
            })
        })
        .collect();

    report("similarity", &failures);
}

const fn slug(error: &RankError) -> &'static str {
    match error {
        RankError::TitleSimilarity { .. } => "title_similarity",
        RankError::FetchChecksFailed { .. } => "fetch_checks_failed",
        RankError::RankUnderThreshold { .. } => "rank_under_threshold",
        RankError::InvalidHash => "invalid_hash",
        RankError::AdultContent => "adult_content",
    }
}

/// What a profile does to the whole corpus: how much it accepts, what it turns
/// down and why, and which releases end up at the top.
#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct Selection {
    profile: String,
    accepted: usize,
    top_ranked: Vec<String>,
    rejected: BTreeMap<String, Vec<String>>,
}

fn selection(profile: QualityProfile) -> Selection {
    let settings = profile.base_settings().prepare();
    let model = RankingModel::default();

    let mut accepted: Vec<(i64, &str)> = Vec::new();
    let mut rejected = BTreeMap::new();
    for data in corpus().iter().map(|(_, actual)| actual) {
        let (fetch, failed) = check_fetch(data, &settings);
        if fetch {
            accepted.push((get_rank_total(data, &settings, &model), &data.raw_title));
        } else {
            rejected.insert(data.raw_title.clone(), failed);
        }
    }

    // Rank descending, then by title so equal ranks keep a stable order.
    accepted.sort_by(|(a_rank, a_title), (b_rank, b_title)| {
        b_rank.cmp(a_rank).then(a_title.cmp(b_title))
    });

    Selection {
        profile: profile.id().to_owned(),
        accepted: accepted.len(),
        top_ranked: accepted
            .iter()
            .take(TOP_RANKED)
            .map(|(_, title)| (*title).to_owned())
            .collect(),
        rejected,
    }
}

#[test]
fn profile_selection_matches_golden() {
    let golden: Vec<Selection> = serde_json::from_str(SELECTION).expect("golden is valid JSON");
    assert_eq!(golden.len(), PROFILES.len(), "golden covers every profile");

    let mut failures = Vec::new();
    for (profile, golden) in PROFILES.iter().zip(&golden) {
        let current = selection(*profile);
        assert_eq!(current.profile, golden.profile, "golden profile order");
        if current == *golden {
            continue;
        }

        let mut detail = Vec::new();
        if current.accepted != golden.accepted {
            detail.push(format!(
                "      accepts {} titles, golden says {}",
                current.accepted, golden.accepted
            ));
        }
        for (title, reasons) in &current.rejected {
            match golden.rejected.get(title) {
                None => detail.push(format!("      newly rejected ({reasons:?}): {title}")),
                Some(was) if was != reasons => {
                    detail.push(format!(
                        "      reason changed {was:?} -> {reasons:?}: {title}"
                    ));
                }
                Some(_) => {}
            }
        }
        for title in golden.rejected.keys() {
            if !current.rejected.contains_key(title) {
                detail.push(format!("      newly accepted: {title}"));
            }
        }
        if current.top_ranked != golden.top_ranked {
            detail.push(format!(
                "      top {TOP_RANKED} changed:\n        got  {:#?}\n        want {:#?}",
                current.top_ranked, golden.top_ranked
            ));
        }
        failures.push(format!("  {}\n{}", golden.profile, detail.join("\n")));
    }

    report("profile selection", &failures);
}

#[test]
#[ignore = "rewrites tests/fixtures/profile_selection.json from current ranking behaviour"]
fn bless_profile_selection() {
    let rendered: Vec<String> = PROFILES.iter().map(|p| render(&selection(*p))).collect();
    std::fs::write(SELECTION_PATH, format!("[\n{}\n]\n", rendered.join(",\n")))
        .expect("golden is writable");
}

/// One title per line, so a diff reads as a list of releases that changed side.
fn render(selection: &Selection) -> String {
    let quote = |value: &str| serde_json::to_string(value).expect("string serializes");

    let top_ranked: Vec<String> = selection
        .top_ranked
        .iter()
        .map(|title| format!("      {}", quote(title)))
        .collect();
    let rejected: Vec<String> = selection
        .rejected
        .iter()
        .map(|(title, reasons)| {
            format!(
                "      {}: {}",
                quote(title),
                serde_json::to_string(reasons).expect("reasons serialize")
            )
        })
        .collect();

    format!(
        "  {{\n    \"profile\": {},\n    \"accepted\": {},\n    \"top_ranked\": [\n{}\n    ],\n    \"rejected\": {{\n{}\n    }}\n  }}",
        quote(&selection.profile),
        selection.accepted,
        top_ranked.join(",\n"),
        rejected.join(",\n"),
    )
}
