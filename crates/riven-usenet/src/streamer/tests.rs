use super::ingest::{first_slice_gap, par2_sample_block_indices, pick_primary_media_index};
use super::NzbRarSlice;
use crate::nzb::{NzbFile, NzbSegment};

fn mk_slice(part_index: usize) -> NzbRarSlice {
    NzbRarSlice {
        part_index,
        start_in_part: 0,
        length: 1,
        encryption: None,
        ciphertext_length: 1,
    }
}

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
    let exact = [0u64, 716800, 1433600, 2150400, 2867200, 3000000];
    assert!(!direct_offsets_look_approximate(&exact));
    let estimate = [0u64, 716800, 1433457, 2150130, 2866758, 3000000];
    assert!(direct_offsets_look_approximate(&estimate));
    assert!(!direct_offsets_look_approximate(&[0, 716800, 900000]));
    assert!(!direct_offsets_look_approximate(&[0, 500000]));
}

#[test]
fn first_slice_gap_detects_skipped_volume() {
    // Reproduces the Black Mirror S02 season-pack incident: the byte-sum
    // check alone let a stale header from a neighbouring file (whose real
    // data landed beyond the front-of-volume probe) pass as if this file's
    // reconstruction were complete, when volume 21 was silently never
    // claimed by either file.
    let slices = vec![mk_slice(19), mk_slice(20), mk_slice(22), mk_slice(23)];
    assert_eq!(first_slice_gap(&slices), Some((20, 22)));
}

#[test]
fn first_slice_gap_allows_contiguous_slices() {
    let slices = vec![mk_slice(11), mk_slice(12), mk_slice(13)];
    assert_eq!(first_slice_gap(&slices), None);
}

#[test]
fn par2_sample_indices_cover_whole_volume_substitution() {
    // Reproduces the Black Mirror S02E02 incident: a volume whose every
    // block mismatched PAR2. First/middle/last must be enough to catch that
    // regardless of which single block a test happens to look at.
    assert_eq!(par2_sample_block_indices(94), vec![0, 47, 93]);
}

#[test]
fn par2_sample_indices_handle_small_counts() {
    assert_eq!(par2_sample_block_indices(0), Vec::<usize>::new());
    assert_eq!(par2_sample_block_indices(1), vec![0]);
    assert_eq!(par2_sample_block_indices(2), vec![0, 1]);
    // n/2 collapsing onto first/last shouldn't produce duplicates.
    assert_eq!(par2_sample_block_indices(3), vec![0, 1, 2]);
}
