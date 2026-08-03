use bytes::Bytes;

use crate::nntp::NntpError;
use crate::segments::SegmentList;

use super::salvage::ReadSalvage;
use super::{NzbMetaSource, StreamerError, UsenetStreamer, concat_slices};

/// Articles past the anchor that one read may start up front.
///
/// A bound on *tasks spawned*, not on concurrency — the per-provider semaphore
/// already caps what reaches the wire, and anything queued behind it is work
/// this read was going to do anyway. 16 covers the largest tail probe
/// (10 MiB) at a typical segment size while keeping a pathological range from
/// spawning a task per article of a whole file.
const WARM_SPAN_MAX: usize = 16;

impl UsenetStreamer {
    /// Read `[start, end_inclusive]` from `file_index` as a single contiguous
    /// buffer. Buffered HTTP responses and the RAR decrypt path need one
    /// buffer; the VFS should prefer [`read_range_slices`] and avoid the
    /// concatenation.
    pub async fn read_range(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Bytes, StreamerError> {
        let slices = self
            .read_range_slices(info_hash, file_index, start, end_inclusive)
            .await?;
        let mut buf = concat_slices(slices, start, end_inclusive);
        let want = (end_inclusive - start + 1) as usize;
        if buf.len() > want {
            buf.truncate(want);
        }
        Ok(buf)
    }

    /// Same as [`read_range`] but returns the per-segment decoded slices
    /// instead of concatenating them, so a single-segment read is served by
    /// slicing the cached `Bytes` with no copy.
    ///
    /// Both demand and speculative calls come from the one unified VFS window;
    /// there is no nested Usenet scheduler.
    pub async fn read_range_slices(
        &self,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let loaded = self.load_file(info_hash, file_index).await?;
        self.read_range_slices_of(&loaded, info_hash, file_index, start, end_inclusive)
            .await
    }

    /// The read itself, against an already-resolved file map.
    ///
    /// Split out so the FUSE handle can resolve the map once at open and hand
    /// it in on every read — see [`LocalByteSource`](riven_core::local_source::LocalByteSource).
    pub(crate) async fn read_range_slices_of(
        &self,
        loaded: &super::FileMeta,
        info_hash: &str,
        file_index: usize,
        start: u64,
        end_inclusive: u64,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let file = &loaded.file;
        if start > end_inclusive || end_inclusive >= file.total_size {
            return Err(StreamerError::BadRange);
        }

        // One budget for the whole read, so a range spanning several articles
        // cannot quietly fake more of itself than a single article's worth.
        let mut salvage = ReadSalvage::new();
        let result = match &file.source {
            NzbMetaSource::Direct { offsets, segments } => {
                self.read_direct(offsets, segments, start, end_inclusive, &mut salvage)
                    .await
            }
            NzbMetaSource::Rar { parts, slices } => self
                .read_rar(
                    parts,
                    slices,
                    loaded.password.as_deref(),
                    start,
                    end_inclusive,
                    &mut salvage,
                )
                .await
                .map(|buf| {
                    if buf.is_empty() {
                        Vec::new()
                    } else {
                        vec![buf]
                    }
                }),
        };

        // Deliberately *not* reported as a dead segment. `report_dead_segment`
        // drives read-time repair, which blacklists the release and re-grabs it
        // there and then — on the title being watched, with no active-stream
        // guard. Salvaging and then reporting would swap the file out from
        // under the viewer, which is the exact outcome continuing to stream
        // exists to avoid. The read succeeded; nothing failed.
        //
        // The release is not forgotten: the scheduled availability scanner
        // finds it on its own tick and repairs it once nothing is streaming it.
        if salvage.used() > 0 {
            tracing::warn!(
                info_hash,
                file_index,
                filename = %file.filename,
                holes = salvage.used(),
                start,
                "serving a hole for a dead article; playback continues degraded"
            );
        }
        if let Err(StreamerError::Nntp(NntpError::ArticleNotFound(status))) = &result {
            crate::state::report_dead_segment(info_hash, file_index, &file.filename, status);
        }
        result
    }

    /// Read a byte range from a `Direct` source: one contiguous file composed
    /// of yEnc-encoded articles.
    ///
    /// Assembly is anchored at the segment whose offset-table slot contains
    /// `start`, then walks forward accumulating each segment's **actual
    /// decoded length** until the requested byte count is satisfied. The
    /// offset table only picks the anchor and the in-segment skip; it never
    /// sizes a slice. That is deliberate: the table is a cumulative-decoded
    /// map that may be slightly approximate, and sizing from it drops bytes
    /// whenever a segment decodes to a different length than its slot. A short
    /// return mid-file is catastrophic — the Linux kernel treats a short FUSE
    /// read as EOF and truncates the file's cached size — so the only
    /// legitimate short return is at the true end of the file.
    async fn read_direct(
        &self,
        offsets: &[u64],
        segments: &SegmentList,
        start: u64,
        end_inclusive: u64,
        salvage: &mut ReadSalvage,
    ) -> Result<Vec<Bytes>, StreamerError> {
        let want = (end_inclusive - start + 1) as usize;
        if want == 0 || segments.is_empty() {
            return Ok(Vec::new());
        }

        let anchor = direct_anchor_segment(offsets, segments.len(), start);
        let mut skip = start.saturating_sub(offsets[anchor]) as usize;

        // Start every article the range spans before walking any of them, the
        // way the RAR path already does. A read covering one article is
        // unaffected — the steady-state playback case, since a read-ahead unit
        // *is* an article here. What this is for is the reads that span many:
        // the tail probe a player issues at open runs to 10 MiB (~15 articles
        // at a typical segment size) and the HTTP bridge asks for whole ranges,
        // and walking those serially paid every round trip end to end.
        //
        // The offsets table is approximate, so it picks the horizon and never
        // sizes a slice; over- or under-shooting by an article costs one
        // speculative fetch or one unwarmed hop, not correctness.
        let horizon = direct_anchor_segment(offsets, segments.len(), end_inclusive)
            .min(anchor.saturating_add(WARM_SPAN_MAX));
        self.warm_articles(
            segments
                .range(anchor, horizon)
                .map(|segment| segment.message_id),
        );

        let mut slices: Vec<Bytes> = Vec::new();
        let mut produced: usize = 0;

        // Walk forward from the anchor until the requested byte count is
        // satisfied or the file ends — the offset table says where to start,
        // never where to stop. Bytes are still assembled in order; the walk
        // collects the fetches started above rather than opening one per
        // segment. Beyond the warmed horizon it falls back to fetching as it
        // goes, so a range longer than the cap still completes.
        for (index, segment) in segments.iter().enumerate().skip(anchor) {
            // The table's own span for this segment. Sizing a *present*
            // segment from it would drift, which is why the walk below uses
            // actual decoded lengths — but for a segment that will never
            // arrive it is the best estimate there is, and this path already
            // tolerates the table being approximate.
            let hole_len = offsets
                .get(index + 1)
                .zip(offsets.get(index))
                .map_or(0, |(end, begin)| end.saturating_sub(*begin));
            let decoded = self
                .fetch_article_or_hole(segment.message_id, hole_len, salvage)
                .await?;
            if skip >= decoded.len() {
                skip -= decoded.len();
                continue;
            }
            let take = (want - produced).min(decoded.len() - skip);
            slices.push(decoded.slice(skip..skip + take));
            produced += take;
            skip = 0;
            if produced >= want {
                break;
            }
        }
        Ok(slices)
    }

    /// Fetch one article through the shared segment path.
    pub(super) async fn fetch_article(&self, message_id: &str) -> Result<Bytes, StreamerError> {
        Ok(self.pool.fetch_segment(message_id).await?)
    }

    /// [`fetch_article`](Self::fetch_article), but on a **permanently dead**
    /// article return `hole_len` zero bytes instead of an error, so playback
    /// continues past it. See [`super::salvage`] for the rules.
    ///
    /// The permanence check is the pool's missing set rather than the error
    /// itself. `fetch_sequential` returns whatever the last provider said, so
    /// an `ArticleNotFound` can come back when an earlier provider merely
    /// errored — the missing set is written only when every provider agreed,
    /// which is the condition that makes a hole the right answer.
    pub(super) async fn fetch_article_or_hole(
        &self,
        message_id: &str,
        hole_len: u64,
        salvage: &mut ReadSalvage,
    ) -> Result<Bytes, StreamerError> {
        match self.pool.fetch_segment(message_id).await {
            Ok(bytes) => Ok(bytes),
            Err(NntpError::ArticleNotFound(status)) => {
                if !self.pool.missing().contains(message_id) || !salvage.claim(hole_len) {
                    return Err(StreamerError::Nntp(NntpError::ArticleNotFound(status)));
                }
                tracing::debug!(
                    message_id,
                    hole_len,
                    "article dead on every provider; substituting a hole"
                );
                Ok(Bytes::from(vec![0u8; hole_len as usize]))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Start every article a range spans, without waiting for any of them.
    ///
    /// This is streamnzb's `ReadAheadSegment`: warm the shared segment cache so
    /// the walk that follows either hits it or joins a fetch already on the
    /// wire, instead of paying each article's round trip end to end. The pool's
    /// single-flight keys on message-id, so overlapping ranges coalesce and a
    /// warm start is never a duplicate fetch.
    pub(super) fn warm_articles<'a>(&self, message_ids: impl Iterator<Item = &'a str>) {
        for message_id in message_ids {
            let pool = self.pool.clone();
            let message_id = message_id.to_string();
            tokio::spawn(async move { drop(pool.fetch_segment(&message_id).await) });
        }
    }
}

/// Index of the segment whose cumulative byte range contains `start`.
/// `offsets` is sorted with length `n_segments + 1`; `offsets[i]..offsets[i+1]`
/// is segment `i`'s span.
pub(super) fn direct_anchor_segment(offsets: &[u64], n_segments: usize, start: u64) -> usize {
    offsets
        .partition_point(|&o| o <= start)
        .saturating_sub(1)
        .min(n_segments.saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nntp::{NntpConfig, NntpProvider, NntpServerConfig};
    use std::time::Duration;

    /// Decoded payload length of every article the fake server serves.
    const PAYLOAD: &[u8] = crate::nntp::tests::FAKE_SEGMENT_PAYLOAD;

    fn streamer(addrs: &[std::net::SocketAddr]) -> UsenetStreamer {
        let providers = addrs
            .iter()
            .enumerate()
            .map(|(index, addr)| NntpProvider {
                config: NntpServerConfig {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                    user: None,
                    pass: None,
                    use_tls: false,
                    max_connections: 4,
                    article_timeout: Duration::from_millis(200),
                },
                priority: index as i32,
                is_backup: false,
            })
            .collect();
        // `read_direct` plans from the meta it is handed and fetches through
        // the pool; it never touches the database.
        UsenetStreamer::new(
            NntpConfig { providers },
            sea_orm::DatabaseConnection::default(),
        )
    }

    fn segments(ids: &[&str]) -> SegmentList {
        use crate::segments::NzbSegment;

        ids.iter()
            .map(|id| NzbSegment {
                bytes: PAYLOAD.len() as u64,
                message_id: (*id).to_string(),
            })
            .collect()
    }

    fn offsets(count: usize) -> Vec<u64> {
        (0..=count as u64)
            .map(|i| i * PAYLOAD.len() as u64)
            .collect()
    }

    /// The regression this exists for: one article missing from every provider
    /// used to fail the read, which stops the player. Now the read completes
    /// with a hole where those bytes were.
    #[tokio::test]
    async fn a_dead_article_becomes_a_hole_instead_of_failing_the_read() {
        let (addr, _server) = spawn_selective_server().await;
        let streamer = streamer(&[addr]);
        let segments = segments(&["a@test", "dead@test", "c@test"]);
        let offsets = offsets(3);
        let total = PAYLOAD.len() as u64 * 3;

        let slices = streamer
            .read_direct(
                &offsets,
                &segments,
                0,
                total - 1,
                &mut super::ReadSalvage::allowing(),
            )
            .await
            .expect("a single dead article must not fail the read");

        let joined: Vec<u8> = slices.iter().flat_map(|s| s.to_vec()).collect();
        assert_eq!(
            joined.len(),
            total as usize,
            "a short read would make FUSE truncate the file"
        );
        assert_eq!(&joined[..PAYLOAD.len()], PAYLOAD);
        assert!(
            joined[PAYLOAD.len()..PAYLOAD.len() * 2]
                .iter()
                .all(|&b| b == 0),
            "the dead article's span must be zero-filled"
        );
        assert_eq!(&joined[PAYLOAD.len() * 2..], PAYLOAD);
    }

    /// A couple of gaps is a title worth watching; a read that needs more than
    /// that is a title worth re-grabbing, and must not be papered over.
    #[tokio::test]
    async fn a_read_past_its_hole_budget_still_fails() {
        let (addr, _server) = spawn_selective_server().await;
        let streamer = streamer(&[addr]);
        let ids: Vec<String> = (0..super::super::salvage::MAX_HOLES_PER_READ + 1)
            .map(|i| format!("dead{i}@test"))
            .collect();
        let segments = segments(&ids.iter().map(String::as_str).collect::<Vec<_>>());
        let offsets = offsets(segments.len());
        let total = PAYLOAD.len() as u64 * segments.len() as u64;

        let result = streamer
            .read_direct(
                &offsets,
                &segments,
                0,
                total - 1,
                &mut super::ReadSalvage::allowing(),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(StreamerError::Nntp(NntpError::ArticleNotFound(_)))
            ),
            "too many dead articles must surface, not be faked"
        );
    }

    #[tokio::test]
    async fn a_dead_article_still_fails_the_read_when_skipping_is_off() {
        let (addr, _server) = spawn_selective_server().await;
        let streamer = streamer(&[addr]);
        let segments = segments(&["a@test", "dead@test"]);
        let offsets = offsets(2);

        let result = streamer
            .read_direct(
                &offsets,
                &segments,
                0,
                PAYLOAD.len() as u64 * 2 - 1,
                &mut super::ReadSalvage::refusing(),
            )
            .await;
        assert!(matches!(
            result,
            Err(StreamerError::Nntp(NntpError::ArticleNotFound(_)))
        ));
    }

    /// A `430` from one provider while another never answered is not proof the
    /// article is gone — the one that failed might have had it. Only the
    /// pool's every-provider-agreed set makes a hole the right answer, and an
    /// unreachable peer keeps an id out of that set.
    #[tokio::test]
    async fn a_miss_no_one_could_confirm_is_never_salvaged() {
        let (serving, _server) = spawn_selective_server().await;
        // Port 1 on loopback refuses connections, standing in for a provider
        // that is down rather than one that answered `430`.
        let unreachable: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let streamer = streamer(&[serving, unreachable]);

        let result = streamer
            .fetch_article_or_hole(
                "dead@test",
                PAYLOAD.len() as u64,
                &mut super::ReadSalvage::allowing(),
            )
            .await;

        assert!(
            matches!(result, Err(StreamerError::Nntp(_))),
            "an unconfirmed miss must surface as an error, not a hole"
        );
        assert!(
            !streamer.pool.missing().contains("dead@test"),
            "one provider failing means the article is not confirmed gone"
        );
    }

    /// Conversely, once every provider has agreed, the hole is legitimate.
    #[tokio::test]
    async fn a_confirmed_miss_becomes_a_hole() {
        let (serving, _server) = spawn_selective_server().await;
        let streamer = streamer(&[serving]);

        let bytes = streamer
            .fetch_article_or_hole(
                "dead@test",
                PAYLOAD.len() as u64,
                &mut super::ReadSalvage::allowing(),
            )
            .await
            .expect("the only provider said 430, so the article is gone");
        assert_eq!(bytes.len(), PAYLOAD.len());
        assert!(bytes.iter().all(|&b| b == 0));
        assert!(streamer.pool.missing().contains("dead@test"));
    }

    /// Loopback NNTP that serves any article except ones whose id contains
    /// `dead`, which it reports `430` for.
    async fn spawn_selective_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let article = crate::yenc::tests::encode_single(PAYLOAD, "fake.bin");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let article = article.clone();
                tokio::spawn(async move {
                    let (read_half, mut write_half) = socket.into_split();
                    if write_half.write_all(b"200 fake\r\n").await.is_err() {
                        return;
                    }
                    let mut lines = BufReader::new(read_half).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let dead = line.contains("dead");
                        let reply: Vec<u8> = if line.starts_with("QUIT") {
                            return;
                        } else if line.starts_with("BODY") {
                            if dead {
                                b"430 no such article\r\n".to_vec()
                            } else {
                                let mut out = b"222 0 <exists>\r\n".to_vec();
                                out.extend_from_slice(&article);
                                out.extend_from_slice(b"\r\n.\r\n");
                                out
                            }
                        } else if line.starts_with("STAT") {
                            if dead {
                                b"430 no such article\r\n".to_vec()
                            } else {
                                b"223 0 ok\r\n".to_vec()
                            }
                        } else {
                            b"111 20260101000000\r\n".to_vec()
                        };
                        if write_half.write_all(&reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, handle)
    }

    #[test]
    fn direct_anchor_segment_locates_the_start() {
        let offsets = [0u64, 100, 250, 400];
        assert_eq!(direct_anchor_segment(&offsets, 3, 0), 0);
        assert_eq!(direct_anchor_segment(&offsets, 3, 50), 0);
        assert_eq!(direct_anchor_segment(&offsets, 3, 100), 1);
        assert_eq!(direct_anchor_segment(&offsets, 3, 120), 1);
        assert_eq!(direct_anchor_segment(&offsets, 3, 399), 2);
        assert_eq!(direct_anchor_segment(&offsets, 3, 10_000), 2);
    }
}
