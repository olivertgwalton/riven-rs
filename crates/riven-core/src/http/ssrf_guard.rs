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

/// Deliberately hand-rolled against the well-known private/reserved ranges
/// rather than the standard library's `is_global()` (still unstable) or an
/// extra dependency — this only needs to be right for IPv4 RFC 1918 /
/// loopback / link-local / CGNAT / reserved and the IPv6 loopback /
/// unique-local / link-local equivalents (including an IPv4 address embedded
/// in an IPv4-mapped IPv6 address, e.g. `::ffff:127.0.0.1`, which the bare
/// `Ipv6Addr` checks below don't catch), which covers every realistic
/// internal address a container on a host like this could be reached at.
pub fn is_global_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_global_ipv4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_global_ipv4(mapped);
            }
            let segments = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (segments[0] & 0xfe00) == 0xfc00 // unique local, fc00::/7
                || (segments[0] & 0xffc0) == 0xfe80) // link-local, fe80::/10
        }
    }
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
