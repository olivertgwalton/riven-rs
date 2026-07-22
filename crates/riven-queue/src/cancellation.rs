use super::*;

impl JobQueue {
    /// Returns true if `cancel_items` was called for this id recently. In-flight
    /// download handlers poll this between candidates so deleting an item
    /// stops debrid churn immediately, not only after the whole candidate list
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
    /// UI immediately stops its jobs from churning the debrid service.
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
            if let Err(error) = self.purge_queue_for_ids(config, &id_set).await {
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
            if let Err(error) = self.purge_plugin_hook_queue_for_ids(&config, &id_set).await {
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
            for prefix in ["scrape", "parse", "index"] {
                let _result: Result<(), _> = redis::pipe()
                    .cmd("DEL")
                    .arg(flow_pending_key(prefix, *id))
                    .cmd("DEL")
                    .arg(flow_done_key(prefix, *id))
                    .cmd("DEL")
                    .arg(flow_results_key(prefix, *id))
                    .cmd("DEL")
                    .arg(flow_rate_limited_key(prefix, *id))
                    .query_async(&mut conn)
                    .await;
            }
        }
    }

    /// Same as `purge_queue_for_ids` but reads the media item id from
    /// `event.id` instead of the job's top-level `id`. Used for the
    /// per-(plugin, event) hook queues whose payload is `PluginHookJob`.
    async fn purge_plugin_hook_queue_for_ids(
        &self,
        config: &apalis_redis::RedisConfig,
        ids: &std::collections::HashSet<i64>,
    ) -> redis::RedisResult<()> {
        self.purge_queue_with_id_extractor(config, ids, |value| {
            value
                .get("event")
                .and_then(|e| e.get("id"))
                .and_then(serde_json::Value::as_i64)
        })
        .await
    }

    async fn purge_queue_for_ids(
        &self,
        config: &apalis_redis::RedisConfig,
        ids: &std::collections::HashSet<i64>,
    ) -> redis::RedisResult<()> {
        self.purge_queue_with_id_extractor(config, ids, |value| {
            value.get("id").and_then(serde_json::Value::as_i64)
        })
        .await
    }

    async fn purge_queue_with_id_extractor<F>(
        &self,
        config: &apalis_redis::RedisConfig,
        ids: &std::collections::HashSet<i64>,
        extract_id: F,
    ) -> redis::RedisResult<()>
    where
        F: Fn(&serde_json::Value) -> Option<i64>,
    {
        let mut conn = self.redis.clone();
        let data_hash = config.job_data_hash();
        let active_list = config.active_jobs_list();
        let scheduled_set = config.scheduled_jobs_set();
        let inflight_set = config.inflight_jobs_set();
        let done_set = config.done_jobs_set();
        let dead_set = config.dead_jobs_set();
        let failed_set = config.failed_jobs_set();
        let meta_hash_prefix = config.job_meta_hash();

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

        let mut pipe = redis::pipe();
        pipe.atomic();
        for task_id in &matching_task_ids {
            pipe.cmd("HDEL").arg(&data_hash).arg(task_id).ignore();
            pipe.cmd("LREM")
                .arg(&active_list)
                .arg(0)
                .arg(task_id)
                .ignore();
            pipe.cmd("ZREM").arg(&scheduled_set).arg(task_id).ignore();
            pipe.cmd("SREM").arg(&inflight_set).arg(task_id).ignore();
            pipe.cmd("ZREM").arg(&done_set).arg(task_id).ignore();
            pipe.cmd("ZREM").arg(&dead_set).arg(task_id).ignore();
            pipe.cmd("ZREM").arg(&failed_set).arg(task_id).ignore();
            pipe.cmd("DEL")
                .arg(format!("{meta_hash_prefix}:{task_id}"))
                .ignore();
        }
        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }
}
