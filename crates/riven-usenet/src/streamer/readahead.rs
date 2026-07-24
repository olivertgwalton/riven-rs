//! Adaptive per-stream read-ahead.
//!
//! One task per armed playback stream keeps windows of decoded segments in
//! the shared [`SegmentCache`](crate::cache::SegmentCache) ahead of the
//! player. There are no estimators to tune and no rates to guess — the
//! design is pure backpressure:
//!
//! - **Cursors.** Players read a file at more than one offset at a time
//!   (Infuse issues interleaved range requests). Each reported position
//!   attaches to the nearest *cursor* — an independent read sequence with
//!   its own frontier — or founds a new one. Cursors never fight over a
//!   shared frontier, so interleaved readers can't thrash the window, and
//!   stale cursors simply expire.
//! - **Backpressure, not rate math.** Each cursor fills until it is
//!   `CURSOR_TARGET_BYTES` ahead of its consumer, then stops. Consumption
//!   drains the window and fill resumes — pacing falls out of the buffer
//!   level with no bitrate estimate anywhere.
//! - **AIMD width.** Fetch parallelism starts small and doubles while any
//!   cursor is running low (fill losing the race), decays by one when all
//!   cursors are comfortably full. It converges on whatever the current
//!   provider latency and pipe actually need — high-bitrate content on a
//!   far provider widens; an easy stream idles at the floor.
//!
//! Fetches ride the pool's `Stream` lane: they yield only to reads a player
//! is actively blocked on and are never throttled behind bulk work. The VFS
//! reports positions through [`ReadAheads::report`]; dropping the stream
//! (unregister) tears the task down. In-flight fetches are never cancelled
//! by cursor churn — a fetched segment always lands in the shared cache.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::watch;

use super::{NzbMetaFile, NzbMetaSource, UsenetStreamer};

/// How far ahead of its consumer each cursor fills. Fixed and byte-based:
/// backpressure paces low-bitrate content automatically (the window just
/// covers more seconds), and the cap bounds per-stream memory.
const CURSOR_TARGET_BYTES: u64 = 48 * 1024 * 1024;
/// A cursor is "hungry" below this much buffer — fill is losing the race to
/// the consumer and the fetch width should grow.
const CURSOR_LOW_WATER: u64 = CURSOR_TARGET_BYTES / 3;
/// Max simultaneous read sequences tracked per stream. Real players use two
/// (video + a probing/second range); a third absorbs transient jumps.
const MAX_CURSORS: usize = 3;
/// A report within this distance behind a cursor's frontier belongs to that
/// cursor (its consumer advancing / re-reading); anything else outside the
/// attach range founds a new cursor.
const CURSOR_ATTACH_BEHIND: u64 = CURSOR_TARGET_BYTES * 2;
/// A report at most this far past a cursor's frontier is still that cursor
/// (kernel read-ahead overshoot), not a seek.
const CURSOR_ATTACH_AHEAD: u64 = 16 * 1024 * 1024;
/// Cursors with no reports for this long are dropped.
const CURSOR_TTL: Duration = Duration::from_secs(30);
/// AIMD width bounds: additive floor, doubling growth while hungry, -1 decay
/// when every cursor is full.
const MIN_WIDTH: usize = 4;
/// Exit if the VFS stops reporting positions for this long (safety net for
/// a session that never unregisters).
const IDLE_EXIT: Duration = Duration::from_secs(300);

/// Registry of live read-ahead tasks, keyed by `"{info_hash}:{file_index}"`.
#[derive(Default)]
pub struct ReadAheads {
    inner: Mutex<HashMap<String, watch::Sender<u64>>>,
}

impl ReadAheads {
    /// Report the player's byte position, spawning the stream's read-ahead
    /// task on first report. Cheap: one map lookup + a watch store.
    pub(crate) fn report(
        &self,
        streamer: &UsenetStreamer,
        info_hash: &str,
        file_index: usize,
        position: u64,
    ) {
        let key = format!("{info_hash}:{file_index}");
        let mut map = self.inner.lock();
        if let Some(tx) = map.get(&key) {
            if tx.send(position).is_ok() {
                return;
            }
            // Task exited (idle timeout / meta failure); respawn below.
            map.remove(&key);
        }
        let (tx, rx) = watch::channel(position);
        map.insert(key, tx);
        let streamer = streamer.clone();
        let info_hash = info_hash.to_string();
        tokio::spawn(async move {
            run(streamer, info_hash, file_index, rx).await;
        });
    }

    /// Tear down the stream's read-ahead. Dropping the watch sender ends the
    /// task at its next loop iteration, cancelling unfinished fetches.
    pub(crate) fn remove(&self, info_hash: &str, file_index: usize) {
        self.remove_key(&format!("{info_hash}:{file_index}"));
    }

    /// Same, keyed by the VFS handle key (`"{info_hash}:{file_index}"`).
    pub(crate) fn remove_key(&self, key: &str) {
        self.inner.lock().remove(key);
    }
}

/// Collect up to `max` segment message-ids starting at byte `start`, plus the
/// exact (Direct) or estimated (RAR) byte offset the window then ends at.
fn window_from(file: &NzbMetaFile, start: u64, max: usize) -> (Vec<String>, u64) {
    match &file.source {
        NzbMetaSource::Direct { offsets, segments } => {
            let last_idx = segments.len().saturating_sub(1);
            let first = offsets
                .partition_point(|&o| o <= start)
                .saturating_sub(1)
                .min(last_idx);
            let mids: Vec<String> = segments
                .iter()
                .skip(first)
                .take(max)
                .map(|s| s.message_id.clone())
                .collect();
            let end_idx = first + mids.len();
            let end = offsets.get(end_idx).copied().unwrap_or(file.total_size);
            (mids, end)
        }
        NzbMetaSource::Rar { parts, slices } => {
            let mut mids = Vec::with_capacity(max);
            let mut virtual_pos = 0u64;
            for slice in slices {
                let slice_end = virtual_pos.saturating_add(slice.length);
                if slice_end <= start {
                    virtual_pos = slice_end;
                    continue;
                }
                let offset_in_slice = start.saturating_sub(virtual_pos);
                let offset_in_part = slice.start_in_part.saturating_add(offset_in_slice);
                if let Some(part) = parts.get(slice.part_index) {
                    let first = part
                        .decoded_seg_size
                        .filter(|size| *size > 0)
                        .map_or_else(
                            || {
                                part.offsets
                                    .partition_point(|&offset| offset <= offset_in_part)
                                    .saturating_sub(1)
                            },
                            |size| (offset_in_part / size) as usize,
                        )
                        .min(part.segments.len().saturating_sub(1));
                    for segment in part.segments.iter().skip(first) {
                        mids.push(segment.message_id.clone());
                        if mids.len() >= max {
                            break;
                        }
                    }
                }
                if mids.len() >= max {
                    break;
                }
                virtual_pos = slice_end;
            }
            let end = start.saturating_add(mids.len() as u64 * avg_segment_bytes(file));
            (mids, end)
        }
    }
}

fn avg_segment_bytes(file: &NzbMetaFile) -> u64 {
    let n = match &file.source {
        NzbMetaSource::Direct { segments, .. } => segments.len(),
        NzbMetaSource::Rar { parts, .. } => parts.iter().map(|p| p.segments.len()).sum(),
    };
    (file.total_size / n.max(1) as u64).max(64 * 1024)
}

/// One independent read sequence within a stream: the consumer's last
/// reported position and the byte offset scheduling has reached.
struct Cursor {
    position: u64,
    frontier: u64,
    last_report: Instant,
}

impl Cursor {
    fn new(position: u64) -> Self {
        Self {
            position,
            frontier: position,
            last_report: Instant::now(),
        }
    }

    /// Bytes scheduled (fetched or cached) ahead of the consumer.
    fn buffered(&self) -> u64 {
        self.frontier.saturating_sub(self.position)
    }

    /// Whether a position report belongs to this sequence: at/behind the
    /// frontier within the attach range (consumer advancing or re-reading),
    /// or slightly past it (kernel read-ahead overshoot).
    fn owns(&self, pos: u64) -> bool {
        pos.saturating_add(CURSOR_ATTACH_BEHIND) >= self.frontier
            && pos <= self.frontier.saturating_add(CURSOR_ATTACH_AHEAD)
    }
}

/// Route a position report to its cursor, founding (or recycling) one for a
/// genuinely new sequence. Never resets an existing cursor's frontier — an
/// interleaved reader advancing a *different* sequence must not throw away
/// this one's scheduled window.
fn attach_report(cursors: &mut Vec<Cursor>, pos: u64) {
    if let Some(cursor) = cursors.iter_mut().filter(|c| c.owns(pos)).min_by_key(|c| {
        // Prefer the nearest frontier when attach ranges overlap.
        c.frontier.abs_diff(pos)
    }) {
        // Only forward progress moves the consumer mark: an overlapping
        // re-read behind the window must not re-open already-filled budget.
        if pos > cursor.position {
            cursor.position = pos;
            // Overshoot past the frontier (seek within attach range or
            // kernel read-ahead outrunning fill): jump the frontier forward
            // so scheduling resumes from what the consumer actually needs.
            if pos > cursor.frontier {
                cursor.frontier = pos;
            }
        }
        cursor.last_report = Instant::now();
        return;
    }
    if cursors.len() >= MAX_CURSORS {
        // Recycle the stalest sequence.
        if let Some(oldest) = cursors
            .iter_mut()
            .min_by_key(|c| std::cmp::Reverse(c.last_report.elapsed()))
        {
            *oldest = Cursor::new(pos);
        }
        return;
    }
    cursors.push(Cursor::new(pos));
}

async fn run(
    streamer: UsenetStreamer,
    info_hash: String,
    file_index: usize,
    mut positions: watch::Receiver<u64>,
) {
    let Ok(meta) = streamer.load_meta(&info_hash).await else {
        return;
    };
    let Some(file) = meta.files.get(file_index) else {
        return;
    };
    let client = streamer.pool.stream_client();
    let width_cap = client.capacity().saturating_sub(2).max(MIN_WIDTH);
    let file_label: Arc<str> = Arc::from(file.filename.as_str());

    let mut cursors: Vec<Cursor> = vec![Cursor::new(*positions.borrow())];
    let mut width = MIN_WIDTH;
    let mut last_log = Instant::now();
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    loop {
        cursors.retain(|c| c.last_report.elapsed() < CURSOR_TTL);
        if cursors.is_empty() {
            cursors.push(Cursor::new(*positions.borrow()));
        }

        if last_log.elapsed() >= Duration::from_secs(15) {
            last_log = Instant::now();
            let buffered: Vec<u64> = cursors.iter().map(|c| c.buffered() >> 20).collect();
            tracing::debug!(
                file = %file_label,
                cursors = cursors.len(),
                buffered_mb = ?buffered,
                width,
                in_flight = in_flight.len(),
                "usenet read-ahead adapting"
            );
        }

        // Fill the hungriest cursor first until every cursor is at target or
        // the pipeline is at width.
        while in_flight.len() < width {
            let Some(cursor) = cursors
                .iter_mut()
                .filter(|c| c.frontier < file.total_size && c.buffered() < CURSOR_TARGET_BYTES)
                .min_by_key(|c| c.buffered())
            else {
                break;
            };
            let want = (width - in_flight.len()).min(8);
            let (mids, end) = window_from(file, cursor.frontier, want);
            if mids.is_empty() || end <= cursor.frontier {
                cursor.frontier = file.total_size;
                continue;
            }
            for mid in mids {
                if streamer.state.cache.contains(&mid) || streamer.state.fails.is_dead(&mid) {
                    continue;
                }
                let s = streamer.clone();
                let c = client.clone();
                let label = file_label.clone();
                in_flight.push(async move {
                    s.fetch_decoded_cached(&c, &mid, &label).await.is_ok()
                });
            }
            cursor.frontier = end;
        }

        tokio::select! {
            changed = positions.changed() => {
                if changed.is_err() {
                    // Stream unregistered — exit. Dropping `in_flight`
                    // cancels pending fetches safely: the single-flight
                    // owner guard releases waiters and the pool skips jobs
                    // whose requester is gone.
                    return;
                }
                let pos = *positions.borrow_and_update();
                attach_report(&mut cursors, pos);
            }
            Some(_ok) = in_flight.next(), if !in_flight.is_empty() => {
                // AIMD: double while any active cursor is running low (fill
                // is losing the race to its consumer), decay by one once
                // every cursor is comfortably ahead.
                let hungry = cursors.iter().any(|c| {
                    c.frontier < file.total_size && c.buffered() < CURSOR_LOW_WATER
                });
                let all_full = cursors.iter().all(|c| {
                    c.frontier >= file.total_size || c.buffered() >= CURSOR_TARGET_BYTES
                });
                if hungry {
                    width = (width * 2).min(width_cap);
                } else if all_full {
                    width = width.saturating_sub(1).max(MIN_WIDTH);
                }
            }
            _ = tokio::time::sleep(IDLE_EXIT), if in_flight.is_empty() => {
                streamer.state.readaheads.remove(&info_hash, file_index);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nzb::NzbSegment;
    use crate::streamer::NzbMetaFile;

    fn direct_file(n: usize, seg: u64) -> NzbMetaFile {
        let segments: Vec<NzbSegment> = (0..n)
            .map(|i| NzbSegment {
                bytes: seg,
                number: i as u32 + 1,
                message_id: format!("seg-{i}@test"),
            })
            .collect();
        let offsets: Vec<u64> = (0..=n as u64).map(|i| i * seg).collect();
        NzbMetaFile {
            filename: "file.mkv".into(),
            total_size: n as u64 * seg,
            source: NzbMetaSource::Direct { offsets, segments },
        }
    }

    #[test]
    fn window_walks_forward_with_exact_offsets() {
        let file = direct_file(10, 1000);
        let (mids, end) = window_from(&file, 0, 3);
        assert_eq!(mids, vec!["seg-0@test", "seg-1@test", "seg-2@test"]);
        assert_eq!(end, 3000);

        let (mids, end) = window_from(&file, end, 3);
        assert_eq!(mids[0], "seg-3@test");
        assert_eq!(end, 6000);
    }

    #[test]
    fn window_mid_segment_start_anchors_containing_segment() {
        let file = direct_file(10, 1000);
        let (mids, end) = window_from(&file, 1500, 2);
        assert_eq!(mids, vec!["seg-1@test", "seg-2@test"]);
        assert_eq!(end, 3000);
    }

    #[test]
    fn interleaved_readers_get_independent_cursors() {
        let mut cursors = vec![Cursor::new(0)];
        cursors[0].frontier = 10 * 1024 * 1024;

        // A second sequence far away founds its own cursor instead of
        // resetting the first one's frontier.
        let far = 2_000 * 1024 * 1024;
        attach_report(&mut cursors, far);
        assert_eq!(cursors.len(), 2);
        assert_eq!(cursors[0].frontier, 10 * 1024 * 1024, "cursor 0 untouched");
        assert_eq!(cursors[1].position, far);

        // Reports keep routing to their own sequences.
        attach_report(&mut cursors, 5 * 1024 * 1024);
        attach_report(&mut cursors, far + 1024 * 1024);
        assert_eq!(cursors.len(), 2);
        assert_eq!(cursors[0].position, 5 * 1024 * 1024);
        assert_eq!(cursors[1].position, far + 1024 * 1024);
    }

    #[test]
    fn overshoot_past_frontier_advances_it() {
        let mut cursors = vec![Cursor::new(0)];
        cursors[0].frontier = 4 * 1024 * 1024;
        let pos = 8 * 1024 * 1024; // within CURSOR_ATTACH_AHEAD of frontier
        attach_report(&mut cursors, pos);
        assert_eq!(cursors.len(), 1);
        assert_eq!(cursors[0].frontier, pos, "frontier jumps to consumer");
        assert_eq!(cursors[0].buffered(), 0);
    }

    #[test]
    fn cursor_pool_recycles_stalest_when_full() {
        let mut cursors: Vec<Cursor> = (0..MAX_CURSORS as u64)
            .map(|i| Cursor::new(i * 10_000_000_000))
            .collect();
        cursors[1].last_report = Instant::now() - Duration::from_secs(20);
        let newcomer = 500_000_000_000;
        attach_report(&mut cursors, newcomer);
        assert_eq!(cursors.len(), MAX_CURSORS);
        assert!(
            cursors.iter().any(|c| c.position == newcomer),
            "newcomer must replace the stalest cursor"
        );
        assert!(
            !cursors
                .iter()
                .any(|c| c.position == 10_000_000_000 && c.last_report.elapsed().as_secs() >= 19),
            "the stale cursor is the one recycled"
        );
    }

    #[test]
    fn window_clamps_at_eof() {
        let file = direct_file(4, 1000);
        let (mids, end) = window_from(&file, 3500, 10);
        assert_eq!(mids, vec!["seg-3@test"]);
        assert_eq!(end, 4000);
        let (mids, _) = window_from(&file, 4000, 4);
        assert!(mids.len() <= 1, "at/past EOF must not schedule the file");
    }
}
