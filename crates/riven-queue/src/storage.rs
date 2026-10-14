use super::*;

impl JobQueue {
    pub async fn new(
        redis_url: &str,
        registry: Arc<PluginRegistry>,
        notification_tx: broadcast::Sender<String>,
        attempt_unknown_downloads: bool,
        reindex_config: ReindexConfig,
        filesystem_settings: FilesystemSettings,
        retry_interval_secs: u64,
        maximum_scrape_attempts: u32,
    ) -> Result<Self> {
        let apalis_conn = connect_managed(redis_url).await?;

        let index_storage = queue_storage(&apalis_conn, "riven:index");
        let scrape_storage = queue_storage(&apalis_conn, "riven:scrape");
        let parse_storage = queue_storage(&apalis_conn, "riven:parse");
        let download_storage = queue_storage(&apalis_conn, "riven:download");
        let rank_streams_storage = queue_storage(&apalis_conn, "riven:rank-streams");
        let process_media_item_storage = queue_storage(&apalis_conn, "riven:process-media-item");

        let mut plugin_hook_storages: HashMap<(String, EventType), RedisStorage<PluginHookJob>> =
            HashMap::new();
        for (plugin_name, event_type) in registry.subscribed_event_pairs().await {
            if matches!(event_type.dispatch_strategy(), DispatchStrategy::Inline) {
                continue;
            }
            let namespace = format!("riven:plugin-hook:{}:{plugin_name}", event_type.slug());
            let storage = queue_storage(&apalis_conn, &namespace);
            plugin_hook_storages.insert((plugin_name, event_type), storage);
        }

        let redis = connect_managed(redis_url).await?;

        let resolution_ranks = riven_db::repo::load_resolution_ranks().await;
        let (event_tx, _) = broadcast::channel(4096);

        Ok(Self {
            index_storage,
            scrape_storage,
            parse_storage,
            download_storage,
            rank_streams_storage,
            process_media_item_storage,
            plugin_hook_storages,
            redis,
            registry,
            event_tx,
            notification_tx,
            attempt_unknown_downloads: Arc::new(AtomicBool::new(attempt_unknown_downloads)),
            reindex_config: Arc::new(RwLock::new(reindex_config)),
            vfs_layout: Arc::new(RwLock::new(VfsLibraryLayout::new(
                filesystem_settings.clone(),
            ))),
            filesystem_settings: Arc::new(RwLock::new(filesystem_settings)),
            filesystem_settings_revision: Arc::new(AtomicU64::new(0)),
            retry_interval_secs: Arc::new(AtomicU64::new(retry_interval_secs)),
            maximum_scrape_attempts: Arc::new(AtomicU32::new(maximum_scrape_attempts)),
            resolution_ranks: Arc::new(RwLock::new(resolution_ranks)),
        })
    }

    pub(crate) fn orchestrator_queue_configs(&self) -> [&Config; 6] {
        [
            self.index_storage.get_config(),
            self.scrape_storage.get_config(),
            self.parse_storage.get_config(),
            self.download_storage.get_config(),
            self.rank_streams_storage.get_config(),
            self.process_media_item_storage.get_config(),
        ]
    }

    /// Every apalis-redis queue this `JobQueue` owns — fixed orchestrator queues
    /// plus the dynamic per-(plugin, event) hook queues. Maintenance routines
    /// (orphan purge, stale-worker rescue, history prune) iterate this so a new
    /// queue added in `JobQueue::new` is automatically covered. Missing one
    /// here causes orphaned active job IDs to kill its worker on first poll.
    pub fn queue_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .orchestrator_queue_configs()
            .map(|config| config.queue.as_ref().to_owned())
            .into();
        names.extend(
            self.plugin_hook_storages
                .values()
                .map(|storage| storage.get_config().queue.as_ref().to_owned()),
        );
        names
    }
}

/// A worker that stops heartbeating is presumed dead after
/// `HEARTBEAT_INTERVAL × MISSED_HEARTBEATS`, and its claims go back on the
/// queue. A replacement under the same name can only register once one
/// interval has passed since its predecessor's last heartbeat, so the interval
/// is kept short: it bounds how long a restarted worker waits to come back.
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const MISSED_HEARTBEATS: usize = 6;

fn queue_storage<Args>(conn: &redis::aio::ConnectionManager, queue: &str) -> RedisStorage<Args> {
    RedisStorage::new(conn.clone()).with_config(
        Config::default()
            .queue(queue)
            .heartbeat_interval(HEARTBEAT_INTERVAL)
            .missed_heartbeats(MISSED_HEARTBEATS),
    )
}
