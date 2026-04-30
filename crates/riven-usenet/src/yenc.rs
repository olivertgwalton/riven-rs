//! yEnc decoder for NNTP article bodies.
//!
//! yEnc is the de-facto binary-over-text encoding for Usenet binaries.
//! Each byte `b` is encoded as `(b + 42) mod 256`, with the special bytes
//! `\0`, `\n`, `\r`, `=` escaped: emit `=` followed by `(b + 42 + 64) mod 256`.
//! Lines are CRLF-terminated. Headers are `=ybegin`, `=ypart` (multipart),
//! `=yend`. We only need to decode the payload; CRC validation is
//! best-effort (logged on mismatch but not fatal).

use std::collections::HashMap;

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
/// (CRLF-separated lines). Returns the decoded payload plus parsed headers.
pub fn decode(body: &[u8]) -> Result<(Vec<u8>, YencInfo), YencError> {
    let mut info = YencInfo::default();
    let mut out = Vec::with_capacity(body.len());
    let mut began = false;
    let mut ended = false;
    let mut escape = false;

    for line in split_lines(body) {
        if line.is_empty() {
            continue;
        }
        if line.starts_with(b"=ybegin") {
            parse_kv(line, &mut |k, v| match k {
                "size" => info.total_size = v.parse().ok(),
                "name" => info.name = Some(v.to_string()),
                _ => {}
            });
            began = true;
            continue;
        }
        if line.starts_with(b"=ypart") {
            parse_kv(line, &mut |k, v| match k {
                "begin" => info.part_begin = v.parse().ok(),
                "end" => info.part_end = v.parse().ok(),
                _ => {}
            });
            continue;
        }
        if line.starts_with(b"=yend") {
            parse_kv(line, &mut |k, v| match k {
                "size" => info.part_size = v.parse().ok(),
                "pcrc32" => info.declared_pcrc32 = u32::from_str_radix(v, 16).ok(),
                _ => {}
            });
            ended = true;
            break;
        }
        if !began {
            // Pre-yEnc preamble (NNTP 222 response, blank lines, etc.). Skip.
            continue;
        }

        // Decode this line's bytes into `out`.
        for &b in line {
            if escape {
                let v = b.wrapping_sub(64).wrapping_sub(42);
                out.push(v);
                escape = false;
            } else if b == b'=' {
                escape = true;
            } else {
                out.push(b.wrapping_sub(42));
            }
        }
    }

    if !began {
        return Err(YencError::MissingBegin);
    }
    if !ended {
        return Err(YencError::Truncated);
    }

    info.computed_pcrc32 = Some(crc32fast::hash(&out));
    if let (Some(declared), Some(computed)) = (info.declared_pcrc32, info.computed_pcrc32)
        && declared != computed
    {
        tracing::warn!(declared, computed, "yEnc pcrc32 mismatch");
    }

    Ok((out, info))
}

fn split_lines(body: &[u8]) -> impl Iterator<Item = &[u8]> {
    body.split(|&b| b == b'\n').map(|line| {
        // Trim trailing CR.
        if line.last() == Some(&b'\r') {
            &line[..line.len() - 1]
        } else {
            line
        }
    })
}

/// Parse `key=value key2="value 2"` style keyword args from a yEnc header line.
/// Quoted values aren't actually used by yEnc but are tolerated.
fn parse_kv(line: &[u8], cb: &mut dyn FnMut(&str, &str)) {
    let s = match std::str::from_utf8(line) {
        Ok(s) => s,
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

/// Convenience: ignore declared headers, just decode the payload.
pub fn decode_payload(body: &[u8]) -> Result<Vec<u8>, YencError> {
    decode(body).map(|(p, _)| p)
}

/// Extract the field map without decoding the body. Useful for sniffing
/// `=ybegin size=` and `=ypart begin/end` from a small prefix.
pub fn parse_headers_only(body: &[u8]) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for line in split_lines(body) {
        if line.starts_with(b"=ybegin") || line.starts_with(b"=ypart") || line.starts_with(b"=yend") {
            parse_kv(line, &mut |k, v| {
                map.insert(k.to_string(), v.to_string());
            });
            if line.starts_with(b"=yend") {
                break;
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode `data` as yEnc (single part) for round-trip testing.
    fn encode_single(data: &[u8], name: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("=ybegin line=128 size={} name={}\r\n", data.len(), name).as_bytes());
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
        out.extend_from_slice(format!("=yend size={} pcrc32={:08x}\r\n", data.len(), crc).as_bytes());
        out
    }

    #[test]
    fn round_trip_simple() {
        let payload: Vec<u8> = (0..=255u8).collect();
        let encoded = encode_single(&payload, "test.bin");
        let (decoded, info) = decode(&encoded).unwrap();
        assert_eq!(decoded, payload);
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
        assert_eq!(decoded, payload);
    }
}
