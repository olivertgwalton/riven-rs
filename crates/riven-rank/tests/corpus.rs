//! Every parser field is checked against every release title in the shared
//! corpus at `tests/fixtures/release_titles.json`.
//!
//! A corpus entry is a `ParsedData` record keyed by `raw_title`; any field left
//! out is asserted to come back as its default (`false`, `null`, `[]`,
//! `"unknown"` for resolution). To add coverage, append `{"raw_title": "..."}`
//! and regenerate the expectations:
//!
//! ```text
//! cargo test -p riven-rank --test corpus -- --ignored bless_corpus
//! ```
//!
//! Blessing records what the parser does today, not what it ought to do, so
//! read the diff. `tests/invariants.rs` is what checks the output is *sane*.

mod common;

use common::{CORPUS_PATH, corpus};
use riven_rank::ParsedData;
use serde_json::{Map, Value};

/// One test per parsed field, each run against the whole corpus.
macro_rules! corpus_tests {
    ($($field:ident),+ $(,)?) => {
        $(
            #[test]
            fn $field() {
                for (case, actual) in corpus() {
                    assert_eq!(actual.$field, case.expected.$field, "{}", case.raw());
                }
            }
        )+
    };
}

corpus_tests!(
    parsed_title,
    normalized_title,
    trash,
    adult,
    anime,
    year,
    resolution,
    seasons,
    episodes,
    complete,
    volumes,
    languages,
    quality,
    hdr,
    codec,
    audio,
    channels,
    dubbed,
    subbed,
    date,
    group,
    edition,
    bit_depth,
    bitrate,
    network,
    extended,
    converted,
    hardcoded,
    region,
    ppv,
    three_d,
    site,
    size,
    proper,
    repack,
    retail,
    upscaled,
    remastered,
    unrated,
    uncensored,
    documentary,
    commentary,
    episode_code,
    part,
    country,
    container,
    extension,
    extras,
    torrent,
    scene,
);

#[test]
#[ignore = "rewrites tests/fixtures/release_titles.json from current parser output"]
fn bless_corpus() {
    let defaults = to_map(&ParsedData::default());

    let mut entries: Vec<(&str, String)> = corpus()
        .iter()
        .map(|(case, actual)| {
            let mut head = Map::new();
            let mut fields = to_map(actual);
            head.insert(
                "raw_title".to_owned(),
                fields.remove("raw_title").unwrap_or_default(),
            );
            if case.translate_languages {
                head.insert("translate_languages".to_owned(), Value::Bool(true));
            }
            fields.retain(|key, value| {
                defaults.get(key) != Some(&*value)
                    && (key.as_str() != "resolution" || *value != "unknown")
            });
            (case.raw(), join_objects(&head, &fields))
        })
        .collect();

    entries.sort_by(|(a, _), (b, _)| a.to_lowercase().cmp(&b.to_lowercase()).then(a.cmp(b)));
    entries.dedup_by(|(a, _), (b, _)| a == b);

    // One line per title keeps fixture diffs readable.
    let lines: Vec<String> = entries
        .into_iter()
        .map(|(_, json)| format!("  {json}"))
        .collect();
    std::fs::write(CORPUS_PATH, format!("[\n{}\n]\n", lines.join(",\n"))).expect("corpus writable");
}

/// Serialise two objects as one, keeping `head`'s keys in front of `tail`'s.
fn join_objects(head: &Map<String, Value>, tail: &Map<String, Value>) -> String {
    let head = serde_json::to_string(head).expect("head serializes");
    if tail.is_empty() {
        return head;
    }
    let tail = serde_json::to_string(tail).expect("tail serializes");
    format!(
        "{},{}",
        head.strip_suffix('}').expect("object ends with }"),
        tail.strip_prefix('{').expect("object starts with {"),
    )
}

fn to_map(data: &ParsedData) -> Map<String, Value> {
    match serde_json::to_value(data).expect("ParsedData serializes") {
        Value::Object(map) => map,
        other => panic!("expected an object, got {other}"),
    }
}
