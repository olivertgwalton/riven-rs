//! Per-stream read-ahead.
//!
//! One task per armed playback stream keeps decoded segments in the shared
//! [`SegmentCache`](crate::cache::SegmentCache) ahead of the player. The
//! whole design is backpressure — no rate estimates, no control loops:
//!
//! - **Cursors.** A player reads a file at more than one offset at a time
//!   (Infuse opens the video handle plus a probe/second range, and their
//!   positions interleave into this one task). Each reported position
//!   attaches to the nearest *cursor* — an independent read sequence with
//!   its own frontier — or founds a new one, so interleaved readers never
//!   thrash a shared frontier. Stale cursors expire.
//! - **Fixed window.** Each cursor fills until it is `TARGET_BYTES` ahead of
//!   its consumer, then stops. Consumption drains the window and fill
//!   resumes; pacing falls out of the buffer level. The window is small
//!   relative to the cache, so LRU never evicts an unread segment during
//!   single-stream playback.
//! - **Capacity-derived fan-out.** Concurrent fetches scale with the
//!   provider's configured connection count (see [`readahead_fanout`]),
//!   holding back headroom for blocked reads. The window cap is the real
//!   throttle — once full, scheduling simply stops.
//!
//! Fetches ride the pool's `Stream` lane: they yield only to reads a player
//! is actively blocked on and are never throttled behind bulk work. The VFS
//! reports positions through [`ReadAheads::report`]; unregister tears the
//! task down.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::watch;

use super::{NzbMetaFile, NzbMetaSource, UsenetStreamer};

/// How far ahead of its consumer each cursor fills. Byte-based, so
/// low-bitrate content automatically buffers more seconds; small enough
/// (relative to the ≥256 MB cache) that an unread segment is never evicted
/// during normal playback.
const TARGET_BYTES: u64 = 48 * 1024 * 1024;
/// Hard cap on how many segments a cursor may run ahead of its consumer,
/// whichever comes first with `TARGET_BYTES`.
///
/// Depth and parallelism are separate concerns: the connection count adapts
/// to whatever the account allows (see [`readahead_fanout`]), while *depth*
/// stays a fixed segment count so behaviour does not swing with segment size
/// or with how many connections a provider happens to grant. Bounding it in
/// segments rather than only in bytes also keeps in-flight fetches bounded
/// on files whose segments are unusually small. 60 matches the prefetch
/// depth altmount settles on.
const MAX_PREFETCH_SEGMENTS: usize = 60;
/// Max concurrent fetches for the whole stream, across all cursors, derived
/// from the provider's own connection allowance.
///
/// Read-ahead throughput is `concurrency × article_size ÷ latency`, and
/// latency is the provider's to set — so when a provider answers a ~700 KB
/// article slowly, parallelism is the only lever that keeps supply above a
/// high-bitrate title's demand. A fixed ceiling silently throttled large
/// accounts: 24 held a 100-connection account to ~5 MB/s, under a 2160p
/// stream, leaving the window permanently short of `TARGET_BYTES`.
///
/// Only a small constant is held back, for the `Hot` lane: a read the player
/// is *blocked* on should find an idle slot rather than wait out an in-flight
/// read-ahead fetch. Hot already preempts at pop time, so this covers just
/// the gap until a slot frees. Bulk work needs no reserve here: the pool
/// already shrinks bulk admission while a stream is active.
///
/// There is deliberately no fixed ceiling on the *connection* count on top of
/// that — the account's own limit is the input, and the read-ahead window is
/// what bounds useful concurrency. Depth is capped separately, in segments,
/// by [`MAX_PREFETCH_SEGMENTS`].
fn readahead_fanout(capacity: usize) -> usize {
    // Proportional only on tiny accounts, so an 8-connection provider still
    // reserves 2 rather than half its pool.
    capacity.saturating_sub(HOT_RESERVE.min(capacity / 4)).max(1)
}

/// Connections kept free for reads a player is blocked on. See
/// [`readahead_fanout`].
const HOT_RESERVE: usize = 4;
/// Max simultaneous read sequences tracked per stream. Real players use two
/// (video + a probing/second range); a third absorbs transient jumps.
const MAX_CURSORS: usize = 3;
/// A report within this distance behind a cursor's frontier belongs to that
/// cursor (its consumer advancing / re-reading); anything else outside the
/// attach range founds a new cursor.
const CURSOR_ATTACH_BEHIND: u64 = TARGET_BYTES * 2;
/// A report at most this far past a cursor's frontier is still that cursor
/// (kernel read-ahead overshoot), not a seek.
const CURSOR_ATTACH_AHEAD: u64 = 16 * 1024 * 1024;
/// Cursors with no reports for this long are dropped.
const CURSOR_TTL: Duration = Duration::from_secs(30);
/// A backward position report at least this far behind the cursor's consumer
/// mark is a real backward seek (reset), not kernel re-read jitter (ignore).
const BACKWARD_RESEEK_TOLERANCE: u64 = 4 * 1024 * 1024;
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
        if pos > cursor.position {
            cursor.position = pos;
            // Overshoot past the frontier (seek within attach range or
            // kernel read-ahead outrunning fill): jump the frontier forward
            // so scheduling resumes from what the consumer actually needs.
            if pos > cursor.frontier {
                cursor.frontier = pos;
            }
        } else if pos.saturating_add(BACKWARD_RESEEK_TOLERANCE) < cursor.position {
            // A real backward seek within this sequence's attach range. The
            // consumer mark MUST regress with it — leaving it stale makes
            // `buffered()` report a full window measured from the wrong
            // place, which stops refilling and pins the wrong span. Reset
            // the frontier too: still-cached segments re-schedule as free
            // cache hits, evicted ones genuinely need refetching.
            cursor.position = pos;
            cursor.frontier = pos;
        }
        // Small regressions (kernel read-ahead re-reading just behind) are
        // ignored: they must not re-open already-filled budget.
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
    let max_in_flight = readahead_fanout(client.capacity());
    // Byte span of `MAX_PREFETCH_SEGMENTS` segments for *this* file, measured
    // with the same layout logic scheduling uses, so the segment bound holds
    // for RAR (estimated spans) as well as Direct. Computed once: segment
    // size is fixed per file.
    let (_, seg_limit_end) = window_from(file, 0, MAX_PREFETCH_SEGMENTS);
    let window_cap = TARGET_BYTES.min(seg_limit_end.max(1));
    let file_label: Arc<str> = Arc::from(file.filename.as_str());

    let mut cursors: Vec<Cursor> = vec![Cursor::new(*positions.borrow())];
    let mut last_log = Instant::now();
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    loop {
        cursors.retain(|c| c.last_report.elapsed() < CURSOR_TTL);
        if cursors.is_empty() {
            cursors.push(Cursor::new(*positions.borrow()));
        }

        if last_log.elapsed() >= Duration::from_secs(30) {
            last_log = Instant::now();
            let buffered: Vec<u64> = cursors.iter().map(|c| c.buffered() >> 20).collect();
            tracing::debug!(
                file = %file_label,
                cursors = cursors.len(),
                buffered_mb = ?buffered,
                in_flight = in_flight.len(),
                "usenet read-ahead"
            );
        }

        // Fill the hungriest cursor first until every cursor is at target or
        // the fetch pipeline is full.
        while in_flight.len() < max_in_flight {
            let Some(cursor) = cursors
                .iter_mut()
                .filter(|c| c.frontier < file.total_size && c.buffered() < window_cap)
                .min_by_key(|c| c.buffered())
            else {
                break;
            };
            let want = (max_in_flight - in_flight.len()).min(8);
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
                    let _ = s.fetch_decoded_cached(&c, &mid, &label).await;
                });
            }
            cursor.frontier = end;
        }

        tokio::select! {
            changed = positions.changed() => {
                if changed.is_err() {
                    // Stream unregistered — exit.
                    return;
                }
                let pos = *positions.borrow_and_update();
                attach_report(&mut cursors, pos);
            }
            Some(()) = in_flight.next(), if !in_flight.is_empty() => {}
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

    #[test]
    fn fanout_scales_with_configured_capacity() {
        // The whole point: a large account is not clipped to a constant, and
        // what it gives up is a small fixed reserve — not a share that grows
        // with the account and leaves connections parked doing nothing.
        // Large accounts spend what they are entitled to, minus the reserve;
        // the read-ahead window, not a constant, bounds what is useful.
        assert_eq!(readahead_fanout(100), 96);
        assert_eq!(readahead_fanout(50), 46);
        assert_eq!(readahead_fanout(20), 16);
        // Small accounts keep the previous `capacity - 2` behaviour.
        assert_eq!(readahead_fanout(8), 6);
        assert_eq!(readahead_fanout(4), 3);
    }

    #[test]
    fn fanout_always_leaves_a_usable_slot() {
        // Degenerate capacities must still yield a runnable fetch rather
        // than 0, which would park read-ahead forever. Below the point where
        // the reserve applies, a tiny account spends everything it has —
        // holding a connection back from a 2-connection provider would halve
        // it to protect a lane that already preempts.
        assert_eq!(readahead_fanout(0), 1);
        assert_eq!(readahead_fanout(1), 1);
        assert_eq!(readahead_fanout(2), 2);
        assert_eq!(readahead_fanout(3), 3);
        for capacity in 0..=512 {
            assert!(readahead_fanout(capacity) >= 1, "capacity={capacity}");
        }
    }

    #[test]
    fn fanout_never_exceeds_capacity() {
        for capacity in 0..=512 {
            assert!(readahead_fanout(capacity) <= capacity.max(1));
        }
    }
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
    fn segment_limit_caps_window_below_target_bytes() {
        // Segments small enough that 60 of them are far short of
        // TARGET_BYTES: the segment cap must be what binds, so a cursor
        // never runs more than MAX_PREFETCH_SEGMENTS ahead.
        let seg = 64 * 1024;
        let file = direct_file(500, seg);
        let (mids, end) = window_from(&file, 0, MAX_PREFETCH_SEGMENTS);
        assert_eq!(mids.len(), MAX_PREFETCH_SEGMENTS);
        let window_cap = TARGET_BYTES.min(end.max(1));
        assert_eq!(window_cap, seg * MAX_PREFETCH_SEGMENTS as u64);
        assert!(window_cap < TARGET_BYTES, "segment cap should bind here");
    }

    #[test]
    fn target_bytes_binds_when_segments_are_large() {
        // Above ~800 KB per segment, 60 segments exceed TARGET_BYTES, so the
        // byte window is the governor and memory stays bounded regardless of
        // how large a provider's segments are.
        let file = direct_file(500, 1024 * 1024);
        let (_, end) = window_from(&file, 0, MAX_PREFETCH_SEGMENTS);
        assert_eq!(TARGET_BYTES.min(end.max(1)), TARGET_BYTES);
    }

    #[test]
    fn segment_cap_binds_at_typical_segment_size() {
        // Documents the real-world crossover: at the ~704 KB segments these
        // providers use, 60 segments is ~41 MB — under TARGET_BYTES, so the
        // segment cap is what actually governs a normal stream.
        let file = direct_file(500, 704 * 1024);
        let (_, end) = window_from(&file, 0, MAX_PREFETCH_SEGMENTS);
        let window_cap = TARGET_BYTES.min(end.max(1));
        assert_eq!(window_cap, 704 * 1024 * MAX_PREFETCH_SEGMENTS as u64);
        assert!(window_cap < TARGET_BYTES);
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
