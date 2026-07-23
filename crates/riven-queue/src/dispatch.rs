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
    pub async fn push_parse_scrape_results(&self, job: ParseScrapeResultsJob) {
        self.push_deduped("parse", job.id, "ParseScrapeResultsJob", || async {
            self.parse_storage.clone().push(job).await
        })
        .await;
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

    /// Resolve subscribers for `event`, initialise its fan-in flow, and push a
    /// plugin-hook child job to each subscriber's queue. Returns the number of
    /// children enqueued — `0` means no plugin subscribed, which the caller
    /// usually treats as "skip straight to finalize".
    ///
    /// Caller-provided `scope` namespaces the flow's Redis keys
    /// (`riven:flow:<prefix>:<scope>:...`); for per-item events use the media
    /// item id, for singletons use a fixed value.
    pub async fn fan_out_plugin_hook(&self, event: RivenEvent, scope: i64) -> usize {
        let event_type = event.event_type();
        let DispatchStrategy::FanIn { prefix } = event_type.dispatch_strategy() else {
            tracing::error!(
                ?event_type,
                "fan_out_plugin_hook called for non-FanIn event"
            );
            return 0;
        };
        let subscribers = self.registry.subscriber_names(event_type).await;
        if subscribers.is_empty() {
            return 0;
        }
        self.init_flow(prefix, scope, subscribers.len()).await;
        future::join_all(subscribers.iter().map(|plugin| {
            let event = event.clone();
            async move { self.push_plugin_hook(plugin, event, Some(scope)).await }
        }))
        .await;
        subscribers.len()
    }

    /// Push a per-plugin hook job onto the queue dedicated to
    /// `(plugin_name, event.event_type())`. The plugin-hook worker dispatches
    /// the event to that single plugin and — for fan-in events — stores the
    /// response under the `scope` flow keys, then triggers finalize / signals
    /// the awaiting caller when the last sibling completes.
    pub async fn push_plugin_hook(&self, plugin_name: &str, event: RivenEvent, scope: Option<i64>) {
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
            scope,
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
        // The path comes along only so the failure log below can name the
        // entry being re-grabbed rather than printing a bare hash.
        let entries: Vec<(i64, Option<String>, String)> = filesystem_entries::Entity::find()
            .filter(filesystem_entries::Column::MediaItemId.eq(media_item_id))
            .filter(
                filesystem_entries::Column::EntryType
                    .eq(riven_core::types::FileSystemEntryType::Media),
            )
            .select_only()
            .column(filesystem_entries::Column::Id)
            .column(filesystem_entries::Column::UsenetInfoHash)
            .column(filesystem_entries::Column::Path)
            .into_tuple::<(i64, Option<String>, String)>()
            .all(riven_db::orm())
            .await?;

        for (_, info_hash, path) in &entries {
            if let Some(info_hash) = info_hash
                && let Err(error) =
                    riven_db::repo::blacklist_stream_permanent_by_hash(media_item_id, info_hash)
                        .await
            {
                tracing::warn!(
                    %error,
                    info_hash,
                    file = %path,
                    "regrab: failed to blacklist release"
                );
            }
        }

        for (id, _, _) in &entries {
            if let Err(error) = riven_db::repo::delete_filesystem_entry(*id).await {
                tracing::warn!(%error, entry_id = *id, "regrab: failed to delete filesystem entry");
            }
        }

        riven_db::repo::recompute(&[media_item_id]).await?;
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
                .map(|plugin| self.push_plugin_hook(plugin, event.clone(), None)),
        )
        .await;
    }
}
