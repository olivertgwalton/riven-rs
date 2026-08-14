//! Shared "is this a safe external HTTP(S) target" check, for the handful of
//! places an authenticated-but-not-fully-trusted caller supplies a URL this
//! process then fetches itself (manual NZB URLs, both the synchronous
//! preview/enqueue path in `riven-api` and `plugin-usenet`'s deferred fetch
//! of an already-enqueued one). The two halves only work together: resolving
//! and validating a hostname is pointless if the connection that actually
//! uses it re-resolves the same hostname a moment later (DNS can answer
//! differently — deliberately, for a DNS-rebinding attack) or follows a
//! redirect the check never saw. [`build_pinned_client`] must always be used
//! with the exact address [`resolve_public_target`] returned, never with the
//! caller's original hostname handed to an ordinary client.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Every caller-supplied NZB body this process reads into memory shares this
/// cap — whether it arrived as an upload (`MAX_UPLOAD_BYTES`, `riven-api`'s
/// `server` module) or was fetched from a URL ([`read_capped_text`]).
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Deliberately hand-rolled against the well-known private/reserved ranges
/// rather than the standard library's `is_global()` (still unstable) or an
/// extra dependency — this only needs to be right for IPv4 RFC 1918 /
/// loopback / link-local / CGNAT / reserved and the IPv6 loopback /
/// unique-local / link-local equivalents, including an IPv4 address embedded
/// in any of the three IPv6 forms that carry one (`::ffff:a.b.c.d` mapped,
/// the deprecated `::a.b.c.d` compatible form, and `64:ff9b::a.b.c.d`
/// NAT64), which covers every realistic internal address a container on a
/// host like this could be reached at.
pub fn is_global_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_global_ipv4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_global_ipv4(mapped);
            }
            let segments = v6.segments();
            // NAT64 well-known prefix (64:ff9b::/96, RFC 6052): a NAT64
            // gateway routes this straight to the embedded IPv4 address, so
            // it deserves exactly the same trust level as if the caller had
            // supplied that address directly.
            if segments[0..6] == [0x0064, 0xff9b, 0, 0, 0, 0] {
                return is_global_ipv4(embedded_ipv4(segments));
            }
            // The RFC 8215 *local-use* IPv4/IPv6 translation prefix
            // (64:ff9b:1::/48) is a distinct block from the well-known one
            // above: reserved for a site's own internal NAT64, not globally
            // routable. Unlike 64:ff9b::/96, there's no embedded-IPv4
            // fallback to defer to here — the whole point of this prefix is
            // that it's local-use only, so reject it outright.
            if segments[0] == 0x0064 && segments[1] == 0xff9b && segments[2] == 0x0001 {
                return false;
            }
            // The deprecated IPv4-compatible form (`::a.b.c.d`, RFC 4291
            // §2.5.5.1): high 96 bits zero. This overlaps a handful of
            // genuinely-reserved low-value addresses (`::1`, `::2`, ...),
            // but every one of those decodes to a `0.0.0.0/8` IPv4 address,
            // which `is_global_ipv4` already rejects outright — so treating
            // them as "compatible" here changes nothing about whether they
            // pass.
            if segments[0..6] == [0, 0, 0, 0, 0, 0] {
                return is_global_ipv4(embedded_ipv4(segments));
            }
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (segments[0] & 0xfe00) == 0xfc00 // unique local, fc00::/7
                || (segments[0] & 0xffc0) == 0xfe80) // link-local, fe80::/10
        }
    }
}

fn embedded_ipv4(segments: [u16; 8]) -> Ipv4Addr {
    Ipv4Addr::new(
        (segments[6] >> 8) as u8,
        (segments[6] & 0xff) as u8,
        (segments[7] >> 8) as u8,
        (segments[7] & 0xff) as u8,
    )
}

fn is_global_ipv4(v4: Ipv4Addr) -> bool {
    let octets = v4.octets();
    !(v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_documentation()
        || v4.is_multicast()
        || octets[0] == 0 // 0.0.0.0/8 ("this network"), broader than is_unspecified's exact 0.0.0.0
        || (octets[0] == 100 && (64..=127).contains(&octets[1])) // 100.64.0.0/10, CGNAT
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0) // 192.0.0.0/24, IETF protocol assignments
        || octets[0] >= 240) // 240.0.0.0/4 reserved, includes 255.255.255.255
}

/// Resolves `host:port` and picks one address to pin the real connection to.
/// `Ok(None)` covers both "nothing resolved" and "resolved, but at least one
/// answer wasn't a public address" — a caller-supplied target must fail
/// closed either way, so there's no reason for callers to tell those two
/// apart. Rejecting the whole answer set when *any* entry is non-global
/// (rather than just picking a global one and ignoring the rest) matters
/// because the one this function ends up pinning to is a completely
/// arbitrary member of the set — nothing here controls which address a
/// caller's later, independent DNS lookup would have preferred.
pub async fn resolve_public_target(host: &str, port: u16) -> std::io::Result<Option<SocketAddr>> {
    let addrs = tokio::net::lookup_host((host, port))
        .await?
        .collect::<Vec<_>>();
    if addrs.is_empty() || !addrs.iter().all(|addr| is_global_ip(addr.ip())) {
        return Ok(None);
    }
    Ok(Some(addrs[0]))
}

/// A one-shot client scoped to exactly the address [`resolve_public_target`]
/// validated: redirects are disabled outright (nothing that fetches a
/// caller-supplied URL here has a legitimate need to follow one — a
/// redirecting upstream just means the caller should have pasted the final
/// URL), and `.resolve` pins `host` to `addr` so the connection can't re-run
/// DNS and land somewhere the earlier check never saw.
pub fn build_pinned_client(host: &str, addr: SocketAddr) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve(host, addr)
        .timeout(Duration::from_secs(20))
        .build()
}

/// Why a [`read_capped_text`] call failed. Kept distinct from a bare
/// `reqwest::Error` so callers can tell "the upstream said no"/"too big"/"not
/// text" apart from a genuine transport failure, without groveling through
/// an error message to do it.
#[derive(Debug)]
pub enum CappedReadError {
    NotSuccess(reqwest::StatusCode),
    Transport(reqwest::Error),
    TooLarge,
    InvalidUtf8,
}

impl std::fmt::Display for CappedReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSuccess(status) => write!(f, "response status {status}"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
            Self::TooLarge => write!(f, "response exceeded {MAX_RESPONSE_BYTES} bytes"),
            Self::InvalidUtf8 => write!(f, "response was not valid UTF-8"),
        }
    }
}

impl std::error::Error for CappedReadError {}

/// Reads `response` as text, rejecting it once it exceeds
/// [`MAX_RESPONSE_BYTES`] rather than after — buffering a whole body
/// unconditionally before a caller ever gets to check its length would let a
/// malicious or merely oversized upstream exhaust memory before any size
/// check could run. Reading the raw response chunk-by-chunk with a running
/// total closes that.
pub async fn read_capped_text(mut response: reqwest::Response) -> Result<String, CappedReadError> {
    if !response.status().is_success() {
        return Err(CappedReadError::NotSuccess(response.status()));
    }

    let mut buf = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(CappedReadError::Transport)? {
        if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(CappedReadError::TooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    String::from_utf8(buf).map_err(|_utf8_error| CappedReadError::InvalidUtf8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_global_addresses() {
        let non_global: &[&str] = &[
            "10.0.0.1",           // RFC 1918 private
            "127.0.0.1",          // loopback
            "169.254.169.254",    // link-local / cloud metadata
            "100.64.0.1",         // CGNAT
            "0.0.0.0",            // unspecified / 0.0.0.0/8
            "192.0.0.1",          // IETF protocol assignments
            "240.0.0.1",          // reserved
            "255.255.255.255",    // broadcast
            "::1",                // IPv6 loopback
            "fd00::1",            // unique local
            "fe80::1",            // link-local
            "::ffff:127.0.0.1",   // IPv4-mapped loopback
            "::ffff:10.0.0.1",    // IPv4-mapped private
            "::7f00:1",           // deprecated IPv4-compatible loopback (::127.0.0.1)
            "64:ff9b::a9fe:a9fe", // NAT64-embedded link-local (169.254.169.254)
            "64:ff9b:1::1",       // RFC 8215 local-use translation prefix
            "64:ff9b:1::808:808", // same prefix, would decode to a public IPv4 if allowed
        ];
        for addr in non_global {
            let ip: IpAddr = addr.parse().unwrap();
            assert!(!is_global_ip(ip), "{addr} should not be global");
        }
    }

    #[test]
    fn accepts_public_addresses() {
        let global: &[&str] = &[
            "8.8.8.8",
            "1.1.1.1",
            "2606:4700:4700::1111",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808", // NAT64-embedded 8.8.8.8
        ];
        for addr in global {
            let ip: IpAddr = addr.parse().unwrap();
            assert!(is_global_ip(ip), "{addr} should be global");
        }
    }
}
