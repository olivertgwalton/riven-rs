//! Shared async DNS resolution with a serve-stale cache.
//!
//! Replaces musl's blocking `getaddrinfo` — which returns `EAI_AGAIN` under the
//! concurrency our NNTP dialer and HTTP client generate in Docker/OrbStack —
//! with a process-wide hickory resolver. A thin cache reuses the last good
//! address when the resolver transiently fails, so a DNS blip can't wedge dials
//! or HTTP requests.
//!
//! Both paths resolve through the exact same code: the NNTP dialer calls
//! [`resolve_cached`] directly, and the shared `reqwest` client uses
//! [`CachedDnsResolver`] (a [`reqwest::dns::Resolve`] impl over the same cache).

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use hickory_resolver::TokioResolver;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::name_server::TokioConnectionProvider;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// How long a resolved address is reused before re-resolving.
const DNS_CACHE_TTL: Duration = Duration::from_secs(300);

/// Process-wide async DNS resolver (hickory). Built once from the system
/// resolver config (`/etc/resolv.conf`); falls back to a default config if
/// that's unreadable. hickory's default `Ipv4thenIpv6` strategy prefers A
/// records, which keeps egress on IPv4 when a container advertises AAAA records
/// it can't actually route.
fn resolver() -> &'static TokioResolver {
    static RESOLVER: OnceLock<TokioResolver> = OnceLock::new();
    RESOLVER.get_or_init(|| match TokioResolver::builder_tokio() {
        Ok(builder) => builder.build(),
        Err(error) => {
            tracing::warn!(%error, "hickory: system resolver config unavailable; using defaults");
            TokioResolver::builder_with_config(
                ResolverConfig::default(),
                TokioConnectionProvider::default(),
            )
            .build()
        }
    })
}

struct CachedAddrs {
    addrs: Vec<IpAddr>,
    resolved_at: Instant,
}

fn dns_cache() -> &'static Mutex<HashMap<String, CachedAddrs>> {
    static CACHE: OnceLock<Mutex<HashMap<String, CachedAddrs>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve `host` to its IP addresses with a cache that **serves stale entries
/// when the resolver fails**. Re-resolving on every connect turned a transient
/// DNS blip into a storm that failed every dial and tripped the NNTP circuit
/// breaker; resolving once per host per TTL (and falling back to the last good
/// address on failure) removes that failure mode while still picking up real
/// address changes.
pub async fn resolve_cached(host: &str) -> std::io::Result<Vec<IpAddr>> {
    if let Some(entry) = dns_cache().lock().expect("dns cache poisoned").get(host)
        && entry.resolved_at.elapsed() < DNS_CACHE_TTL
    {
        return Ok(entry.addrs.clone());
    }

    match resolver().lookup_ip(host).await {
        Ok(lookup) => {
            let addrs: Vec<IpAddr> = lookup.iter().collect();
            if !addrs.is_empty() {
                dns_cache().lock().expect("dns cache poisoned").insert(
                    host.to_string(),
                    CachedAddrs {
                        addrs: addrs.clone(),
                        resolved_at: Instant::now(),
                    },
                );
                return Ok(addrs);
            }
        }
        Err(error) => {
            // Resolver failed — reuse the last good address rather than failing.
            // This is what keeps a flaky resolver from wedging us (EAI_AGAIN storm).
            if let Some(entry) = dns_cache().lock().expect("dns cache poisoned").get(host) {
                tracing::debug!(host, %error, "DNS resolve failed; using cached address");
                return Ok(entry.addrs.clone());
            }
            return Err(std::io::Error::other(error.to_string()));
        }
    }

    if let Some(entry) = dns_cache().lock().expect("dns cache poisoned").get(host) {
        return Ok(entry.addrs.clone());
    }
    Err(std::io::Error::other(format!(
        "no addresses resolved for {host}"
    )))
}

/// Warm the cache for a host with a single low-concurrency resolve, before a
/// burst of concurrent dials. Without this, a cold cache lets every dial call
/// the resolver at once — the exact storm the cache exists to avoid. Errors are
/// swallowed: the dials retry resolution themselves.
pub async fn warm(host: &str) {
    drop(resolve_cached(host).await);
}

/// Drop a host's cached address so the next lookup re-resolves — used when a
/// cached address fails to connect (e.g. the host rotated IPs).
pub fn invalidate(host: &str) {
    dns_cache().lock().expect("dns cache poisoned").remove(host);
}

/// A [`reqwest::dns::Resolve`] backed by the shared serve-stale cache, so the
/// HTTP client resolves through the exact same path as the NNTP dialer.
#[derive(Debug, Default, Clone)]
pub struct CachedDnsResolver;

impl Resolve for CachedDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let ips = resolve_cached(name.as_str()).await?;
            let addrs: Addrs = Box::new(ips.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addrs)
        })
    }
}
