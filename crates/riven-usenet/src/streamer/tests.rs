use super::ingest::pick_primary_media_index;
use crate::nzb::{NzbFile, NzbSegment};

#[test]
fn primary_media_index_picks_largest_media() {
    let mk = |subject: &str, total: u64| NzbFile {
        subject: subject.into(),
        poster: String::new(),
        groups: vec![],
        segments: vec![NzbSegment {
            bytes: total,
            number: 1,
            message_id: "x".into(),
        }],
    };
    let files = vec![
        mk(r#""sample.mkv" yEnc"#, 10),
        mk(r#""main.mkv" yEnc"#, 100),
        mk(r#""extra.nfo" yEnc"#, 1),
    ];
    let idx = pick_primary_media_index(&files).unwrap();
    assert_eq!(files[idx].subject, r#""main.mkv" yEnc"#);
}

#[test]
fn offset_table_heuristic_flags_estimates_only() {
    use super::direct_offsets_look_approximate;
    // Exact uniform-part table: identical interior steps, partial last segment.
    let exact = [0u64, 716800, 1433600, 2150400, 2867200, 3000000];
    assert!(!direct_offsets_look_approximate(&exact));
    // Pre-fix encoded-byte estimate: interior steps drift from segment 2 on
    // (mirrors the real S5E3 table).
    let estimate = [0u64, 716800, 1433457, 2150130, 2866758, 3000000];
    assert!(direct_offsets_look_approximate(&estimate));
    // Too short to judge (single full part) → left alone.
    assert!(!direct_offsets_look_approximate(&[0, 716800, 900000]));
    assert!(!direct_offsets_look_approximate(&[0, 500000]));
}
