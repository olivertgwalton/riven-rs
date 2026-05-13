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
