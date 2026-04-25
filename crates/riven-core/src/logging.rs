use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Local;
use log::LevelFilter;
use tokio::sync::broadcast;
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    filter::filter_fn,
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    layer::SubscriberExt,
    registry::LookupSpan,
    reload,
    util::SubscriberInitExt,
};

use crate::settings::{PluginSettings, RivenSettings};

#[derive(Debug, Clone)]
pub struct LogSettings {
    pub enabled: bool,
    pub level: String,
    pub rotation: String,
    pub max_files: usize,
    pub vfs_debug_logging: bool,
}

impl Default for LogSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            level: "info".to_string(),
            rotation: "hourly".to_string(),
            max_files: 5,
            vfs_debug_logging: false,
        }
    }
}

/// Precedence: plugin DB override > `RIVEN_SETTING_LOGS_*` env > top-level `RivenSettings`.
pub async fn load_log_settings(
    pool: &sqlx::PgPool,
    core: &RivenSettings,
) -> anyhow::Result<LogSettings> {
    let mut settings = PluginSettings::load("LOGS");
    if let Some(db_value) = riven_db_get_setting(pool, "plugin.logs").await? {
        settings.merge_db_override(&db_value);
    }

    let plugin_enabled = riven_db_get_plugin_enabled_setting(pool, "logs")
        .await
        .unwrap_or(None)
        .unwrap_or(true);

    Ok(LogSettings {
        enabled: plugin_enabled
            && settings
                .get("logging_enabled")
                .map(is_truthy)
                .unwrap_or(core.logging_enabled),
        level: settings.get_or("log_level", core.log_level.as_str()),
        rotation: settings.get_or("log_rotation", "hourly"),
        max_files: settings
            .get("log_max_files")
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(5),
        vfs_debug_logging: settings
            .get("vfs_debug_logging")
            .map(is_truthy)
            .unwrap_or(core.vfs_debug_logging),
    })
}

// Inline sqlx mirrors of `riven_db::repo::get_setting` and `get_plugin_enabled_setting`
// — riven-db depends on riven-core, so we can't depend back.
async fn riven_db_get_setting(
    pool: &sqlx::PgPool,
    key: &str,
) -> anyhow::Result<Option<serde_json::Value>> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT value FROM settings WHERE key = $1")
            .bind(key)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(v,)| v))
}

async fn riven_db_get_plugin_enabled_setting(
    pool: &sqlx::PgPool,
    plugin: &str,
) -> anyhow::Result<Option<bool>> {
    let value = riven_db_get_setting(pool, &format!("plugin.{plugin}")).await?;
    Ok(value
        .as_ref()
        .and_then(|v| v.get("__enabled"))
        .and_then(serde_json::Value::as_bool))
}

pub struct LogControl {
    handle: reload::Handle<EnvFilter, Registry>,
    enabled: Arc<AtomicBool>,
    _file_guard: WorkerGuard,
}

impl LogControl {
    pub fn apply(&self, settings: &LogSettings) -> anyhow::Result<()> {
        self.enabled.store(settings.enabled, Ordering::Relaxed);
        self.handle
            .reload(build_level_filter(settings)?)
            .map_err(|error| anyhow::anyhow!("failed to reload log filter: {error}"))?;
        log::set_max_level(log_max_level(settings));
        Ok(())
    }
}

pub fn init_logging(
    settings: &LogSettings,
    log_directory: &str,
    log_tx: broadcast::Sender<String>,
) -> anyhow::Result<Arc<LogControl>> {
    let enabled = Arc::new(AtomicBool::new(settings.enabled));
    let (filter_layer, handle) = reload::Layer::new(build_level_filter(settings)?);

    let gate = |enabled: &Arc<AtomicBool>| {
        let flag = enabled.clone();
        filter_fn(move |_| flag.load(Ordering::Relaxed))
    };

    let registry = tracing_subscriber::registry().with(filter_layer);
    let console_layer = tracing_subscriber::fmt::layer()
        .event_format(RivenFormatter)
        .with_filter(gate(&enabled));
    let file_appender = build_file_appender(settings, log_directory)?;
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .json()
        .with_filter(gate(&enabled));
    let broadcast_layer = tracing_subscriber::fmt::layer()
        .event_format(RivenFormatter)
        .with_ansi(false)
        .with_writer(BroadcastMakeWriter { tx: log_tx })
        .with_filter(gate(&enabled));

    registry
        .with(console_layer)
        .with(file_layer)
        .with(broadcast_layer)
        .init();

    log::set_max_level(log_max_level(settings));

    Ok(Arc::new(LogControl {
        handle,
        enabled,
        _file_guard: file_guard,
    }))
}

fn log_max_level(settings: &LogSettings) -> LevelFilter {
    if !settings.enabled {
        return LevelFilter::Off;
    }
    let configured = match settings.level.as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };
    // When VFS debug logging is off, cap at Info so fuser's log::debug!() calls
    // are stopped at the log-crate level before any record is created.
    if settings.vfs_debug_logging {
        configured
    } else {
        configured.min(LevelFilter::Info)
    }
}

fn build_level_filter(settings: &LogSettings) -> anyhow::Result<EnvFilter> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&settings.level))
        .map(|filter| filter.add_directive("apalis_core=info".parse().unwrap()))
        .map_err(|error| anyhow::anyhow!("invalid log level '{}': {error}", settings.level))?;

    // "streaming" target: riven VFS/media-stream debug logs.
    // "log" target: tracing-log 0.2 bridges all log-crate records (including
    // fuser FUSE kernel traces) under this fixed target.
    // Both are suppressed together when VFS debug logging is off.
    if !settings.vfs_debug_logging {
        Ok(filter
            .add_directive("streaming=off".parse().unwrap())
            .add_directive("log=info".parse().unwrap()))
    } else {
        Ok(filter)
    }
}

fn build_file_appender(
    settings: &LogSettings,
    log_directory: &str,
) -> anyhow::Result<RollingFileAppender> {
    let rotation = match settings.rotation.to_ascii_lowercase().as_str() {
        "daily" => Rotation::DAILY,
        "hourly" => Rotation::HOURLY,
        other => anyhow::bail!("invalid log rotation '{other}'"),
    };

    tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(rotation)
        .filename_prefix("riven.log")
        .max_log_files(settings.max_files)
        .build(log_directory)
        .map_err(|error| anyhow::anyhow!("failed to initialize log file appender: {error}"))
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn target_display(target: &str) -> String {
    if target.starts_with("plugin_") {
        let crate_name = target.split("::").next().unwrap_or(target);
        crate_name.replace('_', "-")
    } else if target.starts_with("riven") {
        "core".to_string()
    } else {
        target.to_string()
    }
}

fn level_colored(level: &tracing::Level, ansi: bool) -> String {
    let label = match *level {
        tracing::Level::ERROR => "error",
        tracing::Level::WARN => "warn",
        tracing::Level::INFO => "info",
        tracing::Level::DEBUG => "debug",
        tracing::Level::TRACE => "trace",
    };

    if !ansi {
        return format!("{label}:");
    }

    let color = match *level {
        tracing::Level::ERROR => "\x1b[31m",
        tracing::Level::WARN => "\x1b[33m",
        tracing::Level::INFO => "\x1b[32m",
        tracing::Level::DEBUG => "\x1b[36m",
        tracing::Level::TRACE => "\x1b[2m",
    };
    format!("{color}{label}:\x1b[0m")
}

struct RivenFormatter;

impl<S, N> FormatEvent<S, N> for RivenFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
        let target = target_display(meta.target());
        let level = level_colored(meta.level(), writer.has_ansi_escapes());

        write!(writer, "{timestamp} - {target} - {level} ")?;
        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// A `MakeWriter` that buffers each log event and sends the completed line to a broadcast channel.
#[derive(Clone)]
struct BroadcastMakeWriter {
    tx: broadcast::Sender<String>,
}

struct BroadcastWriter {
    tx: broadcast::Sender<String>,
    buf: Vec<u8>,
}

impl std::io::Write for BroadcastWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for BroadcastWriter {
    fn drop(&mut self) {
        if let Ok(s) = String::from_utf8(std::mem::take(&mut self.buf)) {
            let line = s.trim_end_matches('\n').trim_end_matches('\r').to_string();
            if !line.is_empty() {
                let _ = self.tx.send(line);
            }
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BroadcastMakeWriter {
    type Writer = BroadcastWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BroadcastWriter {
            tx: self.tx.clone(),
            buf: Vec::new(),
        }
    }
}
