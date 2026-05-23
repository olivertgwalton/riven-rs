//! yEnc decoder for NNTP article bodies.
//!
//! yEnc is the de-facto binary-over-text encoding for Usenet binaries.
//! Each byte `b` is encoded as `(b + 42) mod 256`, with the special bytes
//! `\0`, `\n`, `\r`, `=` escaped: emit `=` followed by `(b + 42 + 64) mod 256`.
//! Lines are CRLF-terminated. Headers are `=ybegin`, `=ypart` (multipart),
//! `=yend`. We only need to decode the payload; CRC validation is
//! best-effort (logged on mismatch but not fatal).
//!
//! The hot loop uses `memchr3` to find the next `=`, `\r`, or `\n` and
//! bulk-decodes the run of plain bytes between them by subtracting 42 in a
//! tight loop the compiler vectorises (LLVM emits NEON on aarch64 / SSE on
//! x86-64). CRC32 is computed incrementally during decode rather than as a
//! second pass.
//!
//! The decoded buffer is borrowed from a process-wide free list and the
//! resulting [`Bytes`] owns it via [`Bytes::from_owner`]: when the segment
//! cache evicts the entry, the buffer's `Drop` returns the underlying
//! allocation to the pool instead of releasing pages back to the kernel.
//! musl serves ~700 KB allocations via `mmap` + `madvise(MADV_DONTNEED)`,
//! so fresh-page first-touch faults were ~6 % of CPU during 4K HDR
//! streaming (see `scripts/profile/captures/baseline-s6e1.svg`); reusing
//! the same pages eliminates that path.
//!
//! The pool is process-wide rather than thread-local because the two
//! sides of the lifecycle run on different runtimes: yEnc decode happens
//! on Tokio's blocking pool (via `spawn_blocking`), while cache eviction
//! — and therefore buffer `Drop` — happens on whichever Tokio worker is
//! holding the `SegmentCache` mutex when a `put` runs over budget.

use bytes::Bytes;
use memchr::memchr3;

use crate::bufpool::{BufPool, PooledBuf};

/// Decoded-segment buffer pool. ~3× a typical 720 KB decoded segment cap,
/// 64 retained — comfortably covers the in-flight + recently-evicted set.
static DECODE_BUF_POOL: BufPool = BufPool::new(64, 2 * 1024 * 1024);

#[derive(Debug, Clone, Default)]
pub struct YencInfo {
    /// Total decoded size of the *whole file* (from `=ybegin size=`). For
    /// single-part articles this matches the article's payload size; for
    /// multi-part it's the size after all segments are stitched.
    pub total_size: Option<u64>,
    pub name: Option<String>,
    /// Multipart range within the original file, 1-based inclusive. `None`
    /// for single-part articles.
    pub part_begin: Option<u64>,
    pub part_end: Option<u64>,
    /// Per-part decoded size declared in `=yend`.
    pub part_size: Option<u64>,
    /// CRC32 declared in `=yend pcrc32=`. Compared against the computed CRC.
    pub declared_pcrc32: Option<u32>,
    pub computed_pcrc32: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
pub enum YencError {
    #[error("no =ybegin marker found in article body")]
    MissingBegin,
    #[error("article body ended before =yend marker")]
    Truncated,
}

/// Decode a yEnc-encoded NNTP article body. Input is the raw body bytes
/// (CRLF-separated lines). Returns the decoded payload as `Bytes` plus
/// parsed headers.
pub fn decode(body: &[u8]) -> Result<(Bytes, YencInfo), YencError> {
    let mut info = YencInfo::default();

    // Locate `=ybegin` at the start of a line. The NNTP server may prepend
    // a `222 <id>` status line and/or blank lines before the article body;
    // we just scan forward for the marker rather than parsing those.
    let begin_idx = find_line_starting_with(body, b"=ybegin").ok_or(YencError::MissingBegin)?;
    let after_begin = line_end(body, begin_idx);
    parse_kv(&body[begin_idx..after_begin], &mut |k, v| match k {
        "size" => info.total_size = v.parse().ok(),
        "name" => info.name = Some(v.to_string()),
        _ => {}
    });

    // Optional `=ypart` line immediately after `=ybegin`.
    let mut payload_start = after_begin;
    if body[payload_start..].starts_with(b"=ypart") {
        let part_end = line_end(body, payload_start);
        parse_kv(&body[payload_start..part_end], &mut |k, v| match k {
            "begin" => info.part_begin = v.parse().ok(),
            "end" => info.part_end = v.parse().ok(),
            _ => {}
        });
        payload_start = part_end;
    }

    // Locate `=yend` at the start of a line within the remaining body.
    // Payload bytes never contain unescaped `\r` or `\n`, so any `\n=yend`
    // is the terminator. (Handle the corner case of `=yend` being the very
    // first thing after the headers via `starts_with`.)
    let yend_idx = if body[payload_start..].starts_with(b"=yend") {
        payload_start
    } else {
        let rest = &body[payload_start..];
        memchr::memmem::find(rest, b"\n=yend")
            .map(|n| payload_start + n + 1)
            .ok_or(YencError::Truncated)?
    };
    let yend_line_end = line_end(body, yend_idx);
    parse_kv(&body[yend_idx..yend_line_end], &mut |k, v| match k {
        "size" => info.part_size = v.parse().ok(),
        "pcrc32" => info.declared_pcrc32 = u32::from_str_radix(v, 16).ok(),
        _ => {}
    });

    let payload = &body[payload_start..yend_idx];
    let mut out = PooledBuf::take(&DECODE_BUF_POOL, payload.len());
    decode_payload(payload, &mut info, out.as_mut_vec());

    if let (Some(declared), Some(computed)) = (info.declared_pcrc32, info.computed_pcrc32)
        && declared != computed
    {
        tracing::warn!(declared, computed, "yEnc pcrc32 mismatch");
    }

    // `out` (a `PooledBuf`) owns the decoded allocation; `Bytes::from_owner`
    // keeps it alive while the cache holds the segment and returns it to the
    // pool when the last `Bytes` referencing it drops.
    Ok((Bytes::from_owner(out), info))
}

/// Decode the yEnc payload bytes (everything between `=ybegin`/`=ypart` and
/// `=yend`, CRLF terminators included). CRC32 is computed in a single
/// pass over the decoded output AFTER the decode loop completes, rather
/// than via `Hasher::update` per memchr3-found chunk during the loop —
/// profiling showed ~2 % of CPU was being spent in `Hasher::update`'s
/// dispatch around the ~50 small per-chunk updates per segment. A single
/// `crc32fast::hash` call lets the ARMv8 CRC32 instruction run in its
/// most-unrolled inner loop with no per-call setup; the extra pass over
/// ~700 KB of just-written (cache-hot) bytes costs ~30 µs at memory
/// speed. Writes into the caller-provided `out` so the buffer can be
/// reused from the process-wide pool — `out` must be empty on entry.
fn decode_payload(payload: &[u8], info: &mut YencInfo, out: &mut Vec<u8>) {
    debug_assert!(out.is_empty());
    let mut i = 0;

    while i < payload.len() {
        // Find the next byte that needs special handling: `=` starts an
        // escape; `\r` and `\n` are line terminators. Everything else is a
        // plain encoded byte we can bulk-decode.
        let rel = memchr3(b'=', b'\r', b'\n', &payload[i..]).unwrap_or(payload.len() - i);

        if rel > 0 {
            let src = &payload[i..i + rel];
            let dst_start = out.len();
            // The tight loop over `spare_capacity_mut` is what LLVM
            // vectorises here — measurably faster than
            // `extend(iter.map(...))`, which goes through Iterator and
            // doesn't auto-SIMD.
            out.reserve(rel);
            let spare = &mut out.spare_capacity_mut()[..rel];
            for (slot, &b) in spare.iter_mut().zip(src) {
                slot.write(b.wrapping_sub(42));
            }
            // SAFETY: the loop above initialised exactly `rel` bytes of
            // spare capacity that we reserved on the previous line.
            unsafe {
                out.set_len(dst_start + rel);
            }
            i += rel;
        }

        if i >= payload.len() {
            break;
        }
        match payload[i] {
            b'=' => {
                // Escape sequence: next byte carries the actual value
                // shifted by an additional 64. A trailing `=` with no
                // following byte means the article body was truncated
                // mid-escape; tolerate by stopping rather than indexing
                // out of bounds.
                if i + 1 >= payload.len() {
                    break;
                }
                let v = payload[i + 1].wrapping_sub(64).wrapping_sub(42);
                out.push(v);
                i += 2;
            }
            _ => {
                // `\r` or `\n` — line terminator, drop and continue.
                i += 1;
            }
        }
    }

    info.computed_pcrc32 = Some(crc32fast::hash(out));
}

/// Index of the first occurrence of `marker` that begins a line — either at
/// position 0 or immediately after a `\n`.
fn find_line_starting_with(body: &[u8], marker: &[u8]) -> Option<usize> {
    if body.starts_with(marker) {
        return Some(0);
    }
    let mut needle = Vec::with_capacity(marker.len() + 1);
    needle.push(b'\n');
    needle.extend_from_slice(marker);
    memchr::memmem::find(body, &needle).map(|n| n + 1)
}

/// Byte index one past the next `\n` after `start`, or `body.len()` if the
/// body ends without a final newline.
fn line_end(body: &[u8], start: usize) -> usize {
    match memchr::memchr(b'\n', &body[start..]) {
        Some(n) => start + n + 1,
        None => body.len(),
    }
}

/// Parse `key=value key2="value 2"` style keyword args from a yEnc header line.
/// Quoted values aren't actually used by yEnc but are tolerated.
fn parse_kv(line: &[u8], cb: &mut dyn FnMut(&str, &str)) {
    let s = match std::str::from_utf8(line) {
        Ok(s) => s.trim_end_matches(['\r', '\n']),
        Err(_) => return,
    };
    // Skip the leading directive token (e.g. `=ybegin`).
    let mut iter = s.splitn(2, char::is_whitespace);
    let _ = iter.next();
    let rest = iter.next().unwrap_or("");

    // `name=...` is special: it consumes the rest of the line. Handle it
    // ourselves before tokenizing the remaining `key=value` pairs.
    let prefix: &str = if let Some(idx) = rest.find("name=") {
        let (before, name_part) = rest.split_at(idx);
        cb("name", name_part[5..].trim());
        before
    } else {
        rest
    };

    // Tokenize the prefix on whitespace; skip the consumed `name=...` portion.
    for tok in prefix.split_ascii_whitespace() {
        if let Some((k, v)) = tok.split_once('=') {
            cb(k, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode `data` as yEnc (single part) for round-trip testing.
    fn encode_single(data: &[u8], name: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(
            format!("=ybegin line=128 size={} name={}\r\n", data.len(), name).as_bytes(),
        );
        for &b in data {
            let e = b.wrapping_add(42);
            match e {
                b'\0' | b'\n' | b'\r' | b'=' => {
                    out.push(b'=');
                    out.push(e.wrapping_add(64));
                }
                _ => out.push(e),
            }
        }
        out.extend_from_slice(b"\r\n");
        let crc = crc32fast::hash(data);
        out.extend_from_slice(
            format!("=yend size={} pcrc32={:08x}\r\n", data.len(), crc).as_bytes(),
        );
        out
    }

    #[test]
    fn round_trip_simple() {
        let payload: Vec<u8> = (0..=255u8).collect();
        let encoded = encode_single(&payload, "test.bin");
        let (decoded, info) = decode(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), payload.as_slice());
        assert_eq!(info.total_size, Some(256));
        assert_eq!(info.name.as_deref(), Some("test.bin"));
        assert_eq!(info.declared_pcrc32, info.computed_pcrc32);
    }

    #[test]
    fn round_trip_with_escapes() {
        // Bytes that require escaping: \0 \n \r =. After +42 these become
        // 42, 52, 55, 107 — but the encoder additionally escapes
        // post-+42 values that hit the same set, so a payload of bytes that
        // produce =/\0/\n/\r when shifted will exercise the escape path.
        let payload: Vec<u8> = vec![214, 222, 223, 19]; // +42 = 256(=0), 264(=8), 265, 61(=)
        let encoded = encode_single(&payload, "x");
        let (decoded, _) = decode(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), payload.as_slice());
    }

    #[test]
    fn round_trip_large_payload() {
        // Exercise the bulk-decode path: a long run of plain bytes
        // interrupted by occasional escapes.
        let mut payload = Vec::with_capacity(64 * 1024);
        for i in 0..64u32 * 1024 {
            payload.push((i & 0xff) as u8);
        }
        let encoded = encode_single(&payload, "big.bin");
        let (decoded, info) = decode(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), payload.as_slice());
        assert_eq!(info.declared_pcrc32, info.computed_pcrc32);
        assert_eq!(info.part_size, Some(64 * 1024));
    }

    #[test]
    fn multipart_parses_ypart() {
        // Build a minimal multi-part article: =ybegin (no size on the
        // single-part stand-in) + =ypart begin=/end= + payload + =yend.
        let data = b"hello world";
        let mut encoded = Vec::new();
        encoded.extend_from_slice(b"=ybegin part=1 line=128 size=11 name=hello.bin\r\n");
        encoded.extend_from_slice(b"=ypart begin=1 end=11\r\n");
        for &b in data {
            encoded.push(b.wrapping_add(42));
        }
        encoded.extend_from_slice(b"\r\n");
        let crc = crc32fast::hash(data);
        encoded.extend_from_slice(
            format!("=yend size=11 part=1 pcrc32={:08x}\r\n", crc).as_bytes(),
        );

        let (decoded, info) = decode(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), data.as_slice());
        assert_eq!(info.part_begin, Some(1));
        assert_eq!(info.part_end, Some(11));
        assert_eq!(info.declared_pcrc32, info.computed_pcrc32);
    }

    #[test]
    fn skips_nntp_status_preamble() {
        // Real NNTP responses begin with `222 <id>\r\n` before =ybegin.
        let payload = b"abc";
        let body = encode_single(payload, "x");
        let mut with_preamble = b"222 0 <foo@bar>\r\n".to_vec();
        with_preamble.extend_from_slice(&body);
        let (decoded, _) = decode(&with_preamble).unwrap();
        assert_eq!(decoded.as_ref(), payload.as_slice());
    }
}
