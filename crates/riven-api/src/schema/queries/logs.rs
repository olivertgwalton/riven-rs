use async_graphql::{Context, Error, Object, Result as GqlResult, SchemaBuilder, SimpleObject};

use crate::schema::auth::require_settings_access;

pub struct LogDirectory(pub String);

pub fn register_with_schema<Q, M, S>(
    builder: SchemaBuilder<Q, M, S>,
    log_directory: String,
) -> SchemaBuilder<Q, M, S>
where
    Q: async_graphql::ObjectType + 'static,
    M: async_graphql::ObjectType + 'static,
    S: async_graphql::SubscriptionType + 'static,
{
    builder.data(LogDirectory(log_directory))
}

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
        require_settings_access(ctx)?;

        use std::io::{BufRead, BufReader};
        use tokio::task;

        let log_dir = ctx.data::<LogDirectory>()?.0.clone();
        let limit = usize::try_from(limit.unwrap_or(500).clamp(1, 5000)).unwrap_or(500);
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
