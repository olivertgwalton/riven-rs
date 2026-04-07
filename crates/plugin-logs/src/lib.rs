mod runtime;

use async_graphql::{Context, Error, Object, Result as GqlResult, SimpleObject};
use async_trait::async_trait;
use riven_core::plugin::{Plugin, SettingField};
use riven_core::register_plugin;

pub use runtime::{LogControl, LogSettings, init_logging, load_log_settings};

#[derive(Default)]
pub struct LogsPlugin;

register_plugin!(LogsPlugin);

#[async_trait]
impl Plugin for LogsPlugin {
    fn name(&self) -> &'static str {
        "logs"
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("logging_enabled", "Application logging", "boolean")
                .with_description("Enable or disable runtime logging output."),
            SettingField::new("log_level", "Logging verbosity", "select")
                .with_default("info")
                .with_options(&["error", "warn", "info", "debug", "trace"])
                .with_description("Choose how verbose the application logs should be."),
            SettingField::new("log_rotation", "Log rotation", "select")
                .with_default("hourly")
                .with_options(&["hourly", "daily"])
                .with_description("Rotate log files on this schedule. Takes effect after restart."),
            SettingField::new("log_max_files", "Retained log files", "number")
                .with_default("72")
                .with_description("Maximum number of rotated log files to keep on disk. Takes effect after restart."),
            SettingField::new("vfs_debug_logging", "VFS debug logging", "boolean")
                .with_description("Emit verbose virtual filesystem operation logs."),
        ]
    }
}

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
    async fn logs(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
        level: Option<String>,
    ) -> GqlResult<Vec<LogEntry>> {
        use std::io::{BufRead, BufReader};
        use tokio::task;

        let log_dir = ctx.data::<LogDirectory>()?.0.clone();
        let limit = limit.unwrap_or(500).clamp(1, 5000) as usize;
        let level_filter = level.map(|l| l.to_uppercase());

        let entries = task::spawn_blocking(move || -> Vec<LogEntry> {
            use std::collections::VecDeque;

            // The rolling file appender writes files named with the "riven.log"
            // prefix followed by the rotation timestamp.
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

            log_files.sort_unstable();

            let headroom = limit * 4;
            let mut lines: VecDeque<String> = VecDeque::new();

            'outer: for path in log_files.iter().rev() {
                if let Ok(file) = std::fs::File::open(path) {
                    // Read all lines from this file, then prepend them as a block
                    // so that within each file lines stay in chronological order.
                    let file_lines: Vec<String> = BufReader::new(file)
                        .lines()
                        .map_while(Result::ok)
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

            lines
                .iter()
                .filter_map(|line| {
                    let v: serde_json::Value = serde_json::from_str(line).ok()?;
                    let entry_level = v["level"].as_str().map(|s| s.to_uppercase());
                    if let Some(ref filter) = level_filter
                        && entry_level.as_deref() != Some(filter.as_str())
                    {
                        return None;
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
