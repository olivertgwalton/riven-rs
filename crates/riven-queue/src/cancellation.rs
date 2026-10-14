//! Hand-rolled cancellation, kept deliberately: apalis-redis can only release
//! tasks a worker itself holds. Cancelling every task belonging to a deleted
//! media item requires scanning payloads, which no native API offers.
//!
//! Only pending tasks are purged. A running one is left to finish (download
//! handlers poll `is_cancelled`): deleting a claimed task's data would strand
//! an id that apalis's fetch cannot decode once the claim is requeued.
use super::*;

impl JobQueue {
    /// Returns true if `cancel_items` was called for this id recently. In-flight
    /// download handlers poll this between candidates so deleting an item
    /// stops provider churn immediately, not only after the whole candidate list
    /// has been walked.
    pub async fn is_cancelled(&self, id: i64) -> bool {
        let mut conn = self.redis.clone();
        redis::cmd("SISMEMBER")
            .arg(CANCELLED_ITEMS_SET)
            .arg(id)
            .query_async::<bool>(&mut conn)
            .await
            .unwrap_or(false)
    }

    /// Purge any queued or scheduled apalis jobs whose payload references one
    /// of the given media item ids. Also clears dedup keys and flow state so
    /// the deleted item leaves no debris.
    ///
    /// Called from the `remove_items` mutation so deleting a request from the
    /// UI immediately stops its jobs from churning the download providers.
    pub async fn cancel_items(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        let id_set: std::collections::HashSet<i64> = ids.iter().copied().collect();

        let mut conn = self.redis.clone();
        let mut pipe = redis::pipe();
        for id in ids {
            pipe.cmd("SADD").arg(CANCELLED_ITEMS_SET).arg(*id).ignore();
        }
        pipe.cmd("EXPIRE")
            .arg(CANCELLED_ITEMS_SET)
            .arg(600i64)
            .ignore();
        let _result: Result<(), _> = pipe.query_async(&mut conn).await;

        for config in self.orchestrator_queue_configs() {
            let purged = self
                .purge_queue_with_id_extractor(config, &id_set, |value| {
                    value.get("id").and_then(serde_json::Value::as_i64)
                })
                .await;
            if let Err(error) = purged {
                tracing::warn!(error = %error, queue = %config.job_data_hash(), "failed to purge queue");
            }
        }

        for ((_plugin, event_type), storage) in &self.plugin_hook_storages {
            if !matches!(
                event_type,
                EventType::MediaItemScrapeRequested | EventType::MediaItemIndexRequested
            ) {
                continue;
            }
            let config = storage.get_config().clone();
            // Hook payloads are `PluginHookJob`: the media item id is `event.id`.
            let purged = self
                .purge_queue_with_id_extractor(&config, &id_set, |value| {
                    value
                        .get("event")
                        .and_then(|e| e.get("id"))
                        .and_then(serde_json::Value::as_i64)
                })
                .await;
            if let Err(error) = purged {
                tracing::warn!(error = %error, queue = %config.job_data_hash(), "failed to purge plugin-hook queue");
            }
        }

        let mut conn = self.redis.clone();
        for id in ids {
            for prefix in ["index", "scrape", "parse", "download", "rank-streams"] {
                let _result: Result<(), _> = redis::cmd("DEL")
                    .arg(dedup_key(prefix, *id))
                    .query_async(&mut conn)
                    .await;
            }
        }
    }

    /// Purge every pending task in `config`'s queue whose payload id (read by
    /// `extract_id`) is in `ids`.
    async fn purge_queue_with_id_extractor<F>(
        &self,
        config: &Config,
        ids: &std::collections::HashSet<i64>,
        extract_id: F,
    ) -> redis::RedisResult<()>
    where
        F: Fn(&serde_json::Value) -> Option<i64>,
    {
        let mut conn = self.redis.clone();
        let data_hash = config.job_data_hash();

        let mut cursor: u64 = 0;
        let mut matching_task_ids: Vec<String> = Vec::new();

        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("HSCAN")
                .arg(&data_hash)
                .arg(cursor)
                .arg("COUNT")
                .arg(200u32)
                .query_async(&mut conn)
                .await?;

            let mut iter = batch.into_iter();
            while let (Some(task_id), Some(payload)) = (iter.next(), iter.next()) {
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
                    continue;
                };
                let Some(id) = extract_id(&value) else {
                    continue;
                };
                if ids.contains(&id) {
                    matching_task_ids.push(task_id);
                }
            }

            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        if matching_task_ids.is_empty() {
            return Ok(());
        }

        tracing::info!(
            queue = %data_hash,
            count = matching_task_ids.len(),
            "purging queued jobs for cancelled items"
        );

        // Atomic, so a task a worker claims mid-purge is either purged first
        // or left whole.
        const PURGE_PENDING: &str = r#"
            for _, id in ipairs(ARGV) do
                if redis.call("LREM", KEYS[1], 0, id) + redis.call("ZREM", KEYS[2], id) > 0 then
                    redis.call("HDEL", KEYS[3], id)
                    redis.call("DEL", KEYS[4] .. ":" .. id)
                end
            end
        "#;
        let () = redis::Script::new(PURGE_PENDING)
            .key(config.active_jobs_list())
            .key(config.scheduled_jobs_set())
            .key(&data_hash)
            .key(config.job_meta_hash())
            .arg(&matching_task_ids)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }
}
