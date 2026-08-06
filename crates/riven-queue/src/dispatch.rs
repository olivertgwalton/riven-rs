use super::*;

impl JobQueue {
    pub async fn push_index(&self, job: IndexJob) {
        self.push_deduped("index", job.id, "IndexJob", || async {
            self.index_storage.clone().push(job).await
        })
        .await;
    }
    pub async fn push_scrape(&self, job: ScrapeJob) {
        self.push_deduped("scrape", job.id, "ScrapeJob", || async {
            self.scrape_storage.clone().push(job).await
        })
        .await;
    }

    /// Push a `ScrapeJob` to run after `delay` via apalis's native `run_at`
    /// scheduling. Bypasses `push_deduped` since the dedup key only covers the
    /// in-flight orchestrator phase.
    pub async fn push_scrape_after(&self, job: ScrapeJob, delay: std::time::Duration) {
        let task = TaskBuilder::new(job).run_after(delay).build();
        if let Err(e) = self.scrape_storage.clone().push_task(task).await {
            tracing::error!(error = %e, "failed to push delayed ScrapeJob");
        }
    }
    /// Bypasses `push_deduped`: this job carries the only copy of a scrape
    /// run's collected responses, so a dedup miss (a second scrape finishing
    /// while the first item's parse is still in flight, or a stale key left by
    /// a hard kill) would silently discard them. Duplicate parses are harmless
    /// — `upsert_stream`/`link_stream_to_item` are idempotent.
    pub async fn push_parse_scrape_results(&self, job: ParseScrapeResultsJob) {
        if let Err(e) = self.parse_storage.clone().push(job).await {
            tracing::error!(error = %e, "failed to push ParseScrapeResultsJob");
        }
    }
    pub async fn push_download(&self, job: DownloadJob) {
        self.push_deduped("download", job.id, "DownloadJob", || async {
            self.download_storage.clone().push(job).await
        })
        .await;
    }

    /// Entry point for the download flow. Pushes a `RankStreamsJob` which loads
    /// streams, runs the cache check, builds ranked candidates, hands off to
    /// `DownloadJob` (find-valid-torrent + persist).
    pub async fn push_rank_streams(&self, job: RankStreamsJob) {
        self.push_deduped("rank-streams", job.id, "RankStreamsJob", || async {
            self.rank_streams_storage.clone().push(job).await
        })
        .await;
    }

    /// Fan out `event` to every subscribed plugin's hook queue and await each
    /// child's [`HookOutcome`] through apalis's task-result storage. Returns
    /// one `(plugin, outcome)` per subscriber — empty means no plugin
    /// subscribed. A child that times out, is purged, or crashes reports
    /// `HookOutcome::Failed`, so the wait is always bounded.
    pub async fn fan_out_and_collect(&self, event: &RivenEvent) -> Vec<(String, HookOutcome)> {
        let event_type = event.event_type();
        if event_type.dispatch_strategy() != DispatchStrategy::FanIn {
            tracing::error!(?event_type, "fan_out_and_collect called for non-FanIn event");
            return Vec::new();
        }
        let subscribers = self.registry.subscriber_names(event_type).await;
        future::join_all(subscribers.into_iter().filter_map(|plugin| {
            let storage = self
                .plugin_hook_storages
                .get(&(plugin.clone(), event_type))?
                .clone();
            let job = PluginHookJob {
                plugin_name: plugin.clone(),
                event: event.clone(),
            };
            Some(async move {
                // Explicit task id: apalis's sink invents one otherwise and
                // never reports it back, and the id is the wait handle.
                let task_id = Ulid::new();
                let task = TaskBuilder::new(job)
                    .with_task_id(TaskId::new(task_id))
                    .build();
                if let Err(e) = storage.clone().push_task(task).await {
                    tracing::error!(plugin = %plugin, ?event_type, error = %e, "failed to push plugin-hook job");
                    return (plugin, HookOutcome::Failed);
                }
                let outcome = wait_for_hook_outcome(&storage, task_id, &plugin).await;
                (plugin, outcome)
            })
        }))
        .await
    }

    /// Push a per-plugin hook job onto the queue dedicated to
    /// `(plugin_name, event.event_type())` without awaiting its result.
    /// Used for broadcast (notification) events.
    pub async fn push_plugin_hook(&self, plugin_name: &str, event: RivenEvent) {
        let event_type = event.event_type();
        let key = (plugin_name.to_string(), event_type);
        let Some(storage) = self.plugin_hook_storages.get(&key) else {
            tracing::warn!(
                plugin = plugin_name,
                ?event_type,
                "no plugin-hook storage registered for (plugin, event); skipping push"
            );
            return;
        };
        let job = PluginHookJob {
            plugin_name: plugin_name.to_string(),
            event,
        };
        if let Err(e) = storage.clone().push(job).await {
            tracing::error!(
                plugin = plugin_name,
                ?event_type,
                error = %e,
                "failed to push plugin-hook job"
            );
        }
    }

    /// Enqueue a `ProcessMediaItemJob`. Bypasses `push_deduped` because the
    /// dedup key is per-step (`process-media-item:{step}:{id}`) — the job
    /// re-pushes itself with a different step at every transition, and we
    /// always want the new step to land. Inter-step protection comes from
    /// each child flow's own dedup (`scrape:{id}`, `download:{id}`, …).
    pub async fn push_process_media_item(&self, job: ProcessMediaItemJob) {
        if let Err(e) = self.process_media_item_storage.clone().push(job).await {
            tracing::error!(error = %e, "failed to push ProcessMediaItemJob");
        }
    }

    /// Re-acquire a media item: delete its media filesystem entries so it is no
    /// longer "completed" (state is derived from having a media entry), recompute
    /// state, then re-process. The re-scrape's ingest availability probe skips
    /// any incomplete/dead release, so a complete one is picked. Shared by the
    /// manual "Re-grab" mutation and the usenet auto-repair worker.
    pub async fn regrab_media_item(&self, media_item_id: i64) -> anyhow::Result<()> {
        use riven_core::entities::filesystem_entries;
        let entries: Vec<(i64, Option<String>)> = filesystem_entries::Entity::find()
            .filter(filesystem_entries::Column::MediaItemId.eq(media_item_id))
            .filter(
                filesystem_entries::Column::EntryType
                    .eq(riven_core::types::FileSystemEntryType::Media),
            )
            .select_only()
            .column(filesystem_entries::Column::Id)
            .column(filesystem_entries::Column::UsenetInfoHash)
            .into_tuple::<(i64, Option<String>)>()
            .all(riven_db::orm())
            .await?;

        let info_hashes: Vec<&str> = entries
            .iter()
            .filter_map(|(_, info_hash)| info_hash.as_deref())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        if let Err(error) =
            riven_db::repo::blacklist_streams_permanent_by_hashes(media_item_id, &info_hashes).await
        {
            tracing::warn!(
                %error,
                hashes = info_hashes.len(),
                "regrab: failed to blacklist releases"
            );
        }

        let entry_ids: Vec<i64> = entries.iter().map(|(id, _)| *id).collect();
        let state_recomputed = match riven_db::repo::delete_filesystem_entries(&entry_ids).await {
            Ok(affected) => affected.contains(&media_item_id),
            Err(error) => {
                tracing::warn!(
                    %error,
                    entries = entry_ids.len(),
                    "regrab: failed to delete filesystem entries"
                );
                false
            }
        };
        if !state_recomputed {
            riven_db::repo::recompute(&[media_item_id]).await?;
        }

        self.push_process_media_item(ProcessMediaItemJob::new(media_item_id))
            .await;
        Ok(())
    }

    /// Re-push a `ProcessMediaItemJob` with a future `run_at`. Used by the
    /// `Scrape` step when `next_scrape_attempt_at` is in the future.
    pub async fn push_process_media_item_at(
        &self,
        job: ProcessMediaItemJob,
        run_at: DateTime<Utc>,
    ) {
        let now = Utc::now();
        if run_at <= now {
            self.push_process_media_item(job).await;
            return;
        }
        let delay = (run_at - now).to_std().unwrap_or_default();
        let task = TaskBuilder::new(job).run_after(delay).build();
        if let Err(e) = self
            .process_media_item_storage
            .clone()
            .push_task(task)
            .await
        {
            tracing::error!(error = %e, "failed to push delayed ProcessMediaItemJob");
        }
    }

    /// Enqueue the download flow starting at rank-streams, if at least one
    /// non-blacklisted stream exists. Returns `true` when enqueued.
    pub async fn push_download_from_best_stream(&self, id: i64) -> bool {
        let ranks = self.resolution_ranks.read().await.clone();
        let has_any = riven_db::repo::get_best_stream(id, &ranks)
            .await
            .ok()
            .flatten()
            .is_some();
        if !has_any {
            return false;
        }
        self.push_rank_streams(RankStreamsJob {
            id,
            preferred_info_hash: None,
        })
        .await;
        true
    }

    /// Release the dedup key for a job, allowing it to be re-queued.
    pub async fn release_dedup(&self, prefix: &str, id: i64) {
        let mut conn = self.redis.clone();
        if let Err(e) = redis::cmd("DEL")
            .arg(dedup_key(prefix, id))
            .query_async::<()>(&mut conn)
            .await
        {
            tracing::error!(error = %e, prefix, id, "failed to release dedup key");
        }
    }

    /// SET NX with a 30-min safety TTL. Returns `true` if the key was acquired.
    /// TTL fires only on hard process kill; normal path is `DedupGuard::drop`.
    async fn set_nx(&self, key: &str) -> bool {
        let mut conn = self.redis.clone();
        redis::cmd("SET")
            .arg(key)
            .arg(1u8)
            .arg("NX")
            .arg("EX")
            .arg(dedup::DEDUP_KEY_TTL_SECS)
            .query_async::<Option<String>>(&mut conn)
            .await
            .ok()
            .flatten()
            .is_some()
    }

    async fn push_deduped<F, Fut, E>(&self, prefix: &str, id: i64, label: &'static str, push: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        if self.set_nx(&dedup_key(prefix, id)).await
            && let Err(e) = push().await
        {
            self.release_dedup(prefix, id).await;
            tracing::error!(error = %e, label, "failed to push job");
        }
    }

    pub async fn notify(&self, event: RivenEvent) {
        drop(self.event_tx.send(event.clone()));

        let event_type = event.event_type();
        if event_type.is_ui_streamed()
            && let Ok(json) = serde_json::to_string(&event)
        {
            drop(self.notification_tx.send(json));
        }

        let subscribers = self.registry.subscriber_names(event_type).await;
        future::join_all(
            subscribers
                .iter()
                .map(|plugin| self.push_plugin_hook(plugin, event.clone())),
        )
        .await;
    }
}

/// Await a single plugin-hook child's stored task result. Bounded by
/// [`HOOK_COLLECT_TIMEOUT_SECS`] so a child that will never complete (purged
/// by `cancel_items`, lost to a crash mid-rescue) cannot hang the
/// orchestrator; apalis's `wait_for` itself polls forever.
async fn wait_for_hook_outcome(
    storage: &RedisStorage<PluginHookJob>,
    task_id: Ulid,
    plugin: &str,
) -> HookOutcome {
    use futures::StreamExt;
    let mut results =
        WaitForCompletion::<HookOutcome>::wait_for_single(storage, TaskId::new(task_id));
    let timeout = std::time::Duration::from_secs(HOOK_COLLECT_TIMEOUT_SECS);
    match tokio::time::timeout(timeout, results.next()).await {
        Ok(Some(Ok(result))) => match result.take() {
            Ok(outcome) => outcome,
            // The task itself errored (panic, worker timeout): RedisAck stores
            // the raw error string, surfaced here as the Err arm.
            Err(error) => {
                tracing::warn!(plugin, %error, "plugin-hook task failed");
                HookOutcome::Failed
            }
        },
        Ok(Some(Err(error))) => {
            tracing::warn!(plugin, %error, "failed to read plugin-hook task result");
            HookOutcome::Failed
        }
        Ok(None) => {
            tracing::warn!(plugin, "plugin-hook result stream ended without a result");
            HookOutcome::Failed
        }
        Err(_) => {
            tracing::warn!(
                plugin,
                timeout_secs = HOOK_COLLECT_TIMEOUT_SECS,
                "timed out waiting for plugin-hook result"
            );
            HookOutcome::Failed
        }
    }
}

/// Count the outcomes where the plugin never gave an answer — it errored, or
/// deferred with a rate limit. These are infrastructure failures, not a
/// negative domain result, so callers must not record them as one (no streams
/// found, no metadata, content removed upstream).
pub(crate) fn count_infrastructure_failures(outcomes: &[(String, HookOutcome)]) -> usize {
    outcomes
        .iter()
        .filter(|(_, outcome)| matches!(outcome, HookOutcome::Failed | HookOutcome::RateLimited))
        .count()
}

/// Deserialize the `Response` payloads out of collected fan-in outcomes,
/// dropping everything else. Logs and skips payloads that fail to decode.
pub(crate) fn decode_hook_responses<T: DeserializeOwned>(
    outcomes: Vec<(String, HookOutcome)>,
) -> Vec<T> {
    outcomes
        .into_iter()
        .filter_map(|(plugin, outcome)| match outcome {
            HookOutcome::Response(Some(value)) => match serde_json::from_value(value) {
                Ok(decoded) => Some(decoded),
                Err(error) => {
                    tracing::error!(%plugin, %error, "failed to deserialize plugin-hook response");
                    None
                }
            },
            _ => None,
        })
        .collect()
}
