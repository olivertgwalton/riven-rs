//! Shared release-title corpus: `tests/fixtures/release_titles.json`.
//!
//! Every title is parsed exactly once per test binary, so a suite can run as
//! many checks over the whole corpus as it likes.

#![allow(dead_code, reason = "each test binary uses a different part of this")]

use std::sync::LazyLock;

use riven_rank::{ParsedData, parse};
use serde::Deserialize;

pub const CORPUS_JSON: &str = include_str!("../fixtures/release_titles.json");
pub const CORPUS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/release_titles.json"
);

#[derive(Deserialize)]
pub struct Case {
    #[serde(flatten)]
    pub expected: ParsedData,
}

impl Case {
    pub fn raw(&self) -> &str {
        &self.expected.raw_title
    }

    pub fn parse(&self) -> ParsedData {
        parse(self.raw())
    }
}

static CORPUS: LazyLock<Vec<(Case, ParsedData)>> = LazyLock::new(|| {
    let mut cases: Vec<Case> = serde_json::from_str(CORPUS_JSON).expect("corpus is valid JSON");
    for case in &mut cases {
        assert!(!case.raw().is_empty(), "corpus entry without a raw_title");
        // The parser reports "unknown" rather than "" when it finds no resolution.
        if case.expected.resolution.is_empty() {
            case.expected.resolution = "unknown".to_owned();
        }
    }
    cases
        .into_iter()
        .map(|case| {
            let actual = case.parse();
            (case, actual)
        })
        .collect()
});

/// Every corpus entry paired with what the parser makes of it today.
pub fn corpus() -> &'static [(Case, ParsedData)] {
    &CORPUS
}

/// Just the parser output, for checks that need no expectations.
pub fn parsed() -> impl Iterator<Item = &'static ParsedData> {
    corpus().iter().map(|(_, actual)| actual)
}
