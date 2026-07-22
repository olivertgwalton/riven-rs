use super::*;

impl JobQueue {
    /// Begin a fan-in flow expecting `expected` children. Records the expected
    /// total and clears any leftover state for this scope so a re-run (e.g. a
    /// re-scrape of the same item) starts clean. The pending key now holds the
    /// immutable expected count; per-child progress lives in the done set.
    pub async fn init_flow(&self, prefix: &str, id: i64, expected: usize) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .del(flow_results_key(prefix, id))
            .del(flow_done_key(prefix, id))
            .del(flow_rate_limited_key(prefix, id))
            .cmd("SET")
            .arg(flow_pending_key(prefix, id))
            .arg(expected)
            .arg("EX")
            .arg(3600i64)
            .query_async(&mut conn)
            .await;
    }

    pub async fn flow_store_result<T: Serialize>(
        &self,
        prefix: &str,
        id: i64,
        field: &str,
        value: &T,
    ) {
        let Ok(payload) = serde_json::to_string(value) else {
            tracing::error!(prefix, id, field, "failed to serialize flow result");
            return;
        };
        let key = flow_results_key(prefix, id);
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .hset(&key, field, &payload)
            .expire(&key, 3600i64)
            .query_async(&mut conn)
            .await;
    }

    /// Mark `child` (the plugin name) done for this flow and report whether it
    /// was the final outstanding child — the signal to run `finalize`.
    ///
    /// apalis redelivers jobs at least once, so completion is tracked in a
    /// Redis set rather than a counter: a retried child re-adds the same member
    /// (a no-op), so it can neither finalize twice nor push the total past the
    /// expected count and skip the finalize entirely (the old `DECR` counter
    /// could go negative on a retry and strand the item). The add-and-compare
    /// runs in one Lua script so exactly one caller observes completion even
    /// when siblings finish concurrently.
    pub async fn flow_complete_child(&self, prefix: &str, id: i64, child: &str) -> bool {
        let script = redis::Script::new(
            r"
            local added = redis.call('SADD', KEYS[1], ARGV[1])
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            redis.call('EXPIRE', KEYS[2], ARGV[2])
            if added == 0 then return 0 end
            local expected = tonumber(redis.call('GET', KEYS[2]))
            if not expected or expected <= 0 then return 0 end
            if redis.call('SCARD', KEYS[1]) == expected then return 1 else return 0 end
            ",
        );
        let mut conn = self.redis.clone();
        let last: i64 = script
            .key(flow_done_key(prefix, id))
            .key(flow_pending_key(prefix, id))
            .arg(child)
            .arg(3600i64)
            .invoke_async(&mut conn)
            .await
            .unwrap_or(0);
        last == 1
    }

    /// Atomically read and clear the flow results hash. Use this when the
    /// caller is the sole consumer of the results and should not leave the
    /// key behind on bail-out paths.
    pub async fn drain_flow_results<T: DeserializeOwned>(&self, prefix: &str, id: i64) -> Vec<T> {
        let key = flow_results_key(prefix, id);
        let mut conn = self.redis.clone();
        let (raw, _): (Vec<String>, i64) = redis::pipe()
            .cmd("HVALS")
            .arg(&key)
            .cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap_or_default();
        deserialize_flow_results(prefix, id, raw)
    }

    pub async fn clear_flow(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_pending_key(prefix, id))
            .arg(flow_done_key(prefix, id))
            .query_async(&mut conn)
            .await;
    }

    /// Drop every Redis key associated with a flow in a single round-trip.
    /// The DEL is a no-op for keys that don't exist, so this is safe to call
    /// from any bail-out path regardless of which keys have been written.
    pub async fn clear_flow_all(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_pending_key(prefix, id))
            .arg(flow_done_key(prefix, id))
            .arg(flow_results_key(prefix, id))
            .arg(flow_rate_limited_key(prefix, id))
            .query_async(&mut conn)
            .await;
    }

    /// Increment the count of rate-limited plugin completions for this flow.
    /// Called instead of (and before) `flow_complete_child` when a 429 is received
    /// so `finalize` can distinguish "every scraper was rate-limited" from
    /// "scrapers ran but found nothing".
    pub async fn flow_increment_rate_limited(&self, prefix: &str, id: i64) {
        let key = flow_rate_limited_key(prefix, id);
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .cmd("INCR")
            .arg(&key)
            .cmd("EXPIRE")
            .arg(&key)
            .arg(3600i64)
            .query_async(&mut conn)
            .await;
    }

    /// Return the number of rate-limited plugin completions recorded for this flow.
    pub async fn flow_rate_limited_count(&self, prefix: &str, id: i64) -> i64 {
        let mut conn = self.redis.clone();
        redis::cmd("GET")
            .arg(flow_rate_limited_key(prefix, id))
            .query_async::<Option<i64>>(&mut conn)
            .await
            .unwrap_or(None)
            .unwrap_or(0)
    }

    /// Delete the rate-limited counter for this flow (called in `finalize`).
    pub async fn clear_flow_rate_limited(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_rate_limited_key(prefix, id))
            .query_async(&mut conn)
            .await;
    }

    pub async fn flow_result_count(&self, prefix: &str, id: i64) -> i64 {
        let mut conn = self.redis.clone();
        redis::cmd("HLEN")
            .arg(flow_results_key(prefix, id))
            .query_async(&mut conn)
            .await
            .unwrap_or(0)
    }

    /// Persist orchestrator parent state (e.g. the original `ScrapeJob`) so
    /// `finalize` — invoked on the last child completion in a different
    /// worker — can recover the rate-limit retry counter and any other
    /// fields not encoded in the per-plugin event payload.
    pub async fn flow_set_context<T: Serialize>(&self, prefix: &str, scope: i64, ctx: &T) {
        let Ok(payload) = serde_json::to_string(ctx) else {
            tracing::error!(prefix, scope, "failed to serialize flow context");
            return;
        };
        let key = flow_context_key(prefix, scope);
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::pipe()
            .cmd("SET")
            .arg(&key)
            .arg(payload)
            .arg("EX")
            .arg(3600i64)
            .query_async(&mut conn)
            .await;
    }

    pub async fn flow_get_context<T: DeserializeOwned>(
        &self,
        prefix: &str,
        scope: i64,
    ) -> Option<T> {
        let key = flow_context_key(prefix, scope);
        let mut conn = self.redis.clone();
        let raw: Option<String> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .ok()
            .flatten();
        raw.and_then(|s| serde_json::from_str(&s).ok())
    }

    pub async fn flow_clear_context(&self, prefix: &str, scope: i64) {
        let mut conn = self.redis.clone();
        let _result: Result<(), _> = redis::cmd("DEL")
            .arg(flow_context_key(prefix, scope))
            .query_async(&mut conn)
            .await;
    }
}
