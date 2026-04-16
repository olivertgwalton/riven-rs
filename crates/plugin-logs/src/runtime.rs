use std::fmt;
use std::sync::Arc;

use chrono::Local;
use riven_core::settings::PluginSettings;
use tokio::sync::broadcast;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    layer::SubscriberExt,
    registry::LookupSpan,
    reload,
    util::SubscriberInitExt,
};

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
            max_files: 72,
            vfs_debug_logging: false,
        }
    }
}

pub async fn load_log_settings(pool: &sqlx::PgPool) -> anyhow::Result<LogSettings> {
    let mut settings = PluginSettings::load("LOGS");
    if let Some(db_value) = riven_db::repo::get_setting(pool, "plugin.logs").await? {
        settings.merge_db_override(&db_value);
    }

    Ok(LogSettings {
        enabled: settings
            .get("logging_enabled")
            .map(is_truthy)
            .unwrap_or(true),
        level: settings.get_or("log_level", "info"),
        rotation: settings.get_or("log_rotation", "hourly"),
        max_files: settings
            .get("log_max_files")
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(72),
        vfs_debug_logging: settings
            .get("vfs_debug_logging")
            .map(is_truthy)
            .unwrap_or(false),
    })
}

pub struct LogControl {
    handle: reload::Handle<EnvFilter, Registry>,
    _file_guard: WorkerGuard,
}

impl LogControl {
    pub fn apply(&self, settings: &LogSettings) -> anyhow::Result<()> {
        self.handle
            .reload(build_filter(settings)?)
            .map_err(|error| anyhow::anyhow!("failed to reload log filter: {error}"))
    }
}

pub fn init_logging(
    settings: &LogSettings,
    log_directory: &str,
    log_tx: broadcast::Sender<String>,
) -> anyhow::Result<Arc<LogControl>> {
    let (filter_layer, handle) = reload::Layer::new(build_filter(settings)?);

    let registry = tracing_subscriber::registry().with(filter_layer);
    let console_layer = tracing_subscriber::fmt::layer().event_format(RivenFormatter);
    let file_appender = build_file_appender(settings, log_directory)?;
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .json();
    let broadcast_layer = BroadcastLogLayer { tx: log_tx };

    registry
        .with(console_layer)
        .with(file_layer)
        .with(broadcast_layer)
        .init();

    Ok(Arc::new(LogControl {
        handle,
        _file_guard: file_guard,
    }))
}

fn build_filter(settings: &LogSettings) -> anyhow::Result<EnvFilter> {
    if !settings.enabled {
        return Ok(EnvFilter::new("off"));
    }

    EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&settings.level))
        .map(|filter| filter.add_directive("apalis_core=info".parse().unwrap()))
        .map_err(|error| anyhow::anyhow!("invalid log level '{}': {error}", settings.level))
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

pub struct BroadcastLogLayer {
    pub tx: broadcast::Sender<String>,
}

impl<S: tracing::Subscriber> Layer<S> for BroadcastLogLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        let raw_target = meta.target();
        if !raw_target.starts_with("riven") && !raw_target.starts_with("plugin_") {
            return;
        }

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        let entry = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "level": meta.level().to_string().to_lowercase(),
            "message": visitor.message,
            "target": target_display(raw_target),
        });

        if let Ok(json) = serde_json::to_string(&entry) {
            let _ = self.tx.send(json);
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let s = format!("{value:?}");
            self.message = if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                s[1..s.len() - 1]
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\")
            } else {
                s
            };
        }
    }
}
