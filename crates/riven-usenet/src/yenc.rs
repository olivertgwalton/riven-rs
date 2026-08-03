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
//! The decoded buffer is allocated at exactly the encoded payload's length,
//! which is the tightest upper bound on the decoded size (yEnc only ever
//! shrinks: escapes are two bytes in, one out, and the CRLFs are dropped). So
//! the buffer never grows mid-decode, and the `Bytes` handed to the segment
//! cache pins a capacity within ~3 % of the `len` the cache charges itself.
//!
//! This used to draw from a process-wide free list, because musl serves
//! ~700 KB allocations via `mmap` + `madvise(MADV_DONTNEED)` and the
//! fresh-page first-touch faults were ~6 % of CPU during 4K HDR streaming.
//! The binary now sets mimalloc as its global allocator, which keeps those
//! pages on a thread-local free list for every allocation in the process
//! rather than the two that module covered.

use bytes::Bytes;
use memchr::memchr3;

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

    let begin_idx = find_line_starting_with(body, b"=ybegin").ok_or(YencError::MissingBegin)?;
    let after_begin = line_end(body, begin_idx);
    parse_kv(&body[begin_idx..after_begin], &mut |k, v| match k {
        "size" => info.total_size = v.parse().ok(),
        "name" => info.name = Some(v.to_string()),
        _ => {}
    });

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
    // Decoding only ever shrinks, so the encoded length is an upper bound the
    // decode loop can never outgrow — one allocation, no realloc mid-decode.
    let mut out = Vec::with_capacity(payload.len());
    decode_payload(payload, &mut info, &mut out);

    if let (Some(declared), Some(computed)) = (info.declared_pcrc32, info.computed_pcrc32)
        && declared != computed
    {
        tracing::warn!(declared, computed, "yEnc pcrc32 mismatch");
    }

    Ok((Bytes::from(out), info))
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
        let rel = memchr3(b'=', b'\r', b'\n', &payload[i..]).unwrap_or(payload.len() - i);

        if rel > 0 {
            // `Map` over a slice iterator is `TrustedLen`, so `extend`
            // reserves once and writes straight into spare capacity.
            out.extend(payload[i..i + rel].iter().map(|&b| b.wrapping_sub(42)));
            i += rel;
        }

        if i >= payload.len() {
            break;
        }
        match payload[i] {
            b'=' => {
                if i + 1 >= payload.len() {
                    break;
                }
                let v = payload[i + 1].wrapping_sub(64).wrapping_sub(42);
                out.push(v);
                i += 2;
            }
            _ => {
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
    let mut iter = s.splitn(2, char::is_whitespace);
    let _ = iter.next();
    let rest = iter.next().unwrap_or("");

    let prefix: &str = if let Some(idx) = rest.find("name=") {
        let (before, name_part) = rest.split_at(idx);
        cb("name", name_part[5..].trim());
        before
    } else {
        rest
    };

    for tok in prefix.split_ascii_whitespace() {
        if let Some((k, v)) = tok.split_once('=') {
            cb(k, v);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Encode `data` as yEnc (single part) for round-trip testing.
    pub(crate) fn encode_single(data: &[u8], name: &str) -> Vec<u8> {
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
        let payload: Vec<u8> = vec![214, 222, 223, 19];
        let encoded = encode_single(&payload, "x");
        let (decoded, _) = decode(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), payload.as_slice());
    }

    #[test]
    fn round_trip_large_payload() {
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
        let data = b"hello world";
        let mut encoded = Vec::new();
        encoded.extend_from_slice(b"=ybegin part=1 line=128 size=11 name=hello.bin\r\n");
        encoded.extend_from_slice(b"=ypart begin=1 end=11\r\n");
        for &b in data {
            encoded.push(b.wrapping_add(42));
        }
        encoded.extend_from_slice(b"\r\n");
        let crc = crc32fast::hash(data);
        encoded
            .extend_from_slice(format!("=yend size=11 part=1 pcrc32={:08x}\r\n", crc).as_bytes());

        let (decoded, info) = decode(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), data.as_slice());
        assert_eq!(info.part_begin, Some(1));
        assert_eq!(info.part_end, Some(11));
        assert_eq!(info.declared_pcrc32, info.computed_pcrc32);
    }

    #[test]
    fn skips_nntp_status_preamble() {
        let payload = b"abc";
        let body = encode_single(payload, "x");
        let mut with_preamble = b"222 0 <foo@bar>\r\n".to_vec();
        with_preamble.extend_from_slice(&body);
        let (decoded, _) = decode(&with_preamble).unwrap();
        assert_eq!(decoded.as_ref(), payload.as_slice());
    }
}
