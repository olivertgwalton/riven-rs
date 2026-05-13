use std::sync::Arc;

use tokio::sync::Semaphore;

use super::{NntpConnection, NntpError, NntpProvider, NntpServerConfig};

struct ProviderSlot {
    provider: NntpProvider,
    permits: Arc<Semaphore>,
}

/// Connection pool spanning one or more NNTP providers with failover.
///
/// `fetch_body` and `stat` try each provider in priority order:
///   - Connection / transient errors → fall through to the next provider
///   - `ArticleNotFound` on a primary → try the next primary
///   - All primaries returned `ArticleNotFound` → try backups
///   - All providers returned `ArticleNotFound` → return `ArticleNotFound`
///   - Any provider returned a non-`ArticleNotFound` error and no provider
///     succeeded → return that error
///
/// Each provider has its own semaphore bound to its `max_connections` so
/// a saturated primary doesn't block fetches from a healthy backup.
///
/// Connections are not currently kept alive between checkouts — for
/// streaming workloads where each range request fetches multiple segments
/// in parallel the dominant cost is BODY round-trips, not connect +
/// AUTHINFO. Future work: per-provider keep-alive pool.
pub struct NntpPool {
    /// Sorted: all primaries (by priority asc), then all backups (by priority asc).
    slots: Vec<ProviderSlot>,
}

impl NntpPool {
    /// Build from a single legacy `NntpServerConfig` (back-compat).
    pub fn new(cfg: NntpServerConfig) -> Self {
        Self::new_multi(vec![NntpProvider {
            config: cfg,
            priority: 0,
            is_backup: false,
        }])
    }

    pub fn new_multi(mut providers: Vec<NntpProvider>) -> Self {
        providers.sort_by(|a, b| {
            a.is_backup
                .cmp(&b.is_backup)
                .then(a.priority.cmp(&b.priority))
        });
        let slots = providers
            .into_iter()
            .map(|p| {
                let permits = Arc::new(Semaphore::new(p.config.max_connections.max(1) as usize));
                ProviderSlot {
                    provider: p,
                    permits,
                }
            })
            .collect();
        Self { slots }
    }

    /// Legacy accessor — returns the first configured provider's config.
    /// Used by callers that just need to know one server's hostname for
    /// startup logs; new callers should iterate `providers()`.
    pub fn config(&self) -> &NntpServerConfig {
        &self.slots[0].provider.config
    }

    pub fn providers(&self) -> impl Iterator<Item = &NntpProvider> {
        self.slots.iter().map(|s| &s.provider)
    }

    /// Try each provider in priority order. Returns first success.
    /// `op` is invoked with a fresh connection to each provider; on
    /// `ArticleNotFound` we move to the next; on other errors we record
    /// and move on, returning the last seen error if no provider produced
    /// `ArticleNotFound`.
    async fn try_each<F, Fut, T>(&self, op: F) -> Result<T, NntpError>
    where
        F: Fn(NntpConnection) -> Fut,
        Fut: std::future::Future<Output = (NntpConnection, Result<T, NntpError>)>,
    {
        let mut not_found = false;
        let mut last_err: Option<NntpError> = None;

        for slot in &self.slots {
            let permit = match slot.permits.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return Err(NntpError::Protocol("pool closed")),
            };
            let conn = match NntpConnection::connect(&slot.provider.config).await {
                Ok(c) => c,
                Err(e) => {
                    drop(permit);
                    tracing::debug!(
                        host = %slot.provider.config.host,
                        backup = slot.provider.is_backup,
                        error = %e,
                        "NNTP connect failed; trying next provider"
                    );
                    last_err = Some(e);
                    continue;
                }
            };
            let (mut conn, result) = op(conn).await;
            conn.quit().await;
            drop(permit);
            match result {
                Ok(v) => return Ok(v),
                Err(NntpError::ArticleNotFound(_)) => {
                    not_found = true;
                    continue;
                }
                Err(e) => {
                    tracing::debug!(
                        host = %slot.provider.config.host,
                        backup = slot.provider.is_backup,
                        error = %e,
                        "NNTP op failed; trying next provider"
                    );
                    last_err = Some(e);
                    continue;
                }
            }
        }

        if not_found {
            return Err(NntpError::ArticleNotFound(
                "article not found on any provider".to_string(),
            ));
        }
        Err(last_err.unwrap_or(NntpError::Protocol("no providers configured")))
    }

    pub async fn fetch_body(&self, message_id: &str) -> Result<Vec<u8>, NntpError> {
        let mid = message_id.to_string();
        self.try_each(|mut conn| {
            let mid = mid.clone();
            async move {
                let r = conn.fetch_body(&mid).await;
                (conn, r)
            }
        })
        .await
    }

    pub async fn stat(&self, message_id: &str) -> Result<bool, NntpError> {
        let mid = message_id.to_string();
        self.try_each(|mut conn| {
            let mid = mid.clone();
            async move {
                let r = conn.stat(&mid).await;
                (conn, r)
            }
        })
        .await
    }
}
