use async_graphql::{Context, Error, Object, Result as GqlResult, SimpleObject};
use async_trait::async_trait;
use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;

#[derive(Default)]
pub struct LogsPlugin;

register_plugin!(LogsPlugin);

#[async_trait]
impl Plugin for LogsPlugin {
    fn name(&self) -> &'static str {
        "logs"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[]
    }

    async fn validate(&self, _settings: &PluginSettings) -> anyhow::Result<bool> {
        Ok(true)
    }

    async fn handle_event(
        &self,
        _event: &RivenEvent,
        _ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        Ok(HookResponse::Empty)
    }
}

/// Context data holding the path to the log directory.
pub struct LogDirectory(pub String);

#[derive(SimpleObject)]
pub struct LogEntry {
    pub timestamp: Option<String>,
    pub level: Option<String>,
    pub message: Option<String>,
    pub target: Option<String>,
}

#[derive(Default)]
pub struct LogsQuery;

#[Object]
impl LogsQuery {
    /// Read recent log entries from the current daily log file.
    /// Returns up to `limit` entries (default 500), most-recent-first.
    /// Optionally filter by `level` (e.g. "INFO", "WARN", "ERROR").
    async fn logs(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
        level: Option<String>,
    ) -> GqlResult<Vec<LogEntry>> {
        use std::io::{BufRead, BufReader};
        use tokio::task;

        let log_dir = ctx.data::<LogDirectory>()?.0.clone();
        let limit = limit.unwrap_or(500).max(1).min(5000) as usize;
        let level_filter = level.map(|l| l.to_uppercase());

        let entries = task::spawn_blocking(move || -> Vec<LogEntry> {
            use std::collections::VecDeque;

            // tracing-appender rolling::daily writes files named
            // "{prefix}.{YYYY-MM-DD}", so we glob for the most recent.
            let dir = std::path::Path::new(&log_dir);
            let mut log_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map(|n| n.starts_with("riven.log"))
                        .unwrap_or(false)
                })
                .collect();

            // Most recent file last alphabetically; iterate newest-first.
            log_files.sort_unstable();

            // Collect lines newest-first into a deque, stopping once we have
            // enough to satisfy the limit (with headroom for level filtering).
            let headroom = limit * 4;
            let mut lines: VecDeque<String> = VecDeque::new();

            'outer: for path in log_files.iter().rev() {
                if let Ok(file) = std::fs::File::open(path) {
                    // Read all lines from this file, then prepend them as a block
                    // so that within each file lines stay in chronological order.
                    let file_lines: Vec<String> = BufReader::new(file)
                        .lines()
                        .flatten()
                        .filter(|l| !l.trim().is_empty())
                        .collect();
                    for line in file_lines.into_iter().rev() {
                        lines.push_back(line);
                        if lines.len() >= headroom {
                            break 'outer;
                        }
                    }
                }
            }

            // Parse JSON, filter by level, take `limit` most-recent entries.
            lines
                .iter()
                .filter_map(|line| {
                    let v: serde_json::Value = serde_json::from_str(line).ok()?;
                    let entry_level = v["level"].as_str().map(|s| s.to_uppercase());
                    if let Some(ref filter) = level_filter {
                        if entry_level.as_deref() != Some(filter.as_str()) {
                            return None;
                        }
                    }
                    // tracing-subscriber JSON format:
                    // { "timestamp": "...", "level": "INFO", "fields": { "message": "..." }, "target": "..." }
                    let message = v["fields"]["message"]
                        .as_str()
                        .or_else(|| v["message"].as_str())
                        .map(String::from);
                    Some(LogEntry {
                        timestamp: v["timestamp"].as_str().map(String::from),
                        level: v["level"].as_str().map(String::from),
                        message,
                        target: v["target"].as_str().map(String::from),
                    })
                })
                .take(limit)
                .collect()
        })
        .await
        .map_err(|e| Error::new(format!("log read error: {e}")))?;

        Ok(entries)
    }
}
