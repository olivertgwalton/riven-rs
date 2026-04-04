use std::fmt;

use chrono::Local;
use tokio::sync::broadcast;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    fmt::{format::Writer, FmtContext, FormatEvent, FormatFields},
    layer::SubscriberExt,
    registry::LookupSpan,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

use riven_core::settings::RivenSettings;

// ── Target / level helpers ──

/// Maps a tracing target (module path) to a display name.
/// - `riven_*` → `core`
/// - `plugin_tmdb::*` → `@repo/plugin-tmdb`
/// - everything else → as-is
pub fn target_display(target: &str) -> String {
    if target.starts_with("plugin_") {
        let crate_name = target.split("::").next().unwrap_or(target);
        let name = crate_name.replace('_', "-");
        format!("@repo/{name}")
    } else if target.starts_with("riven") {
        "core".to_string()
    } else {
        target.to_string()
    }
}

/// Returns the level label, optionally wrapped in ANSI colour codes.
fn level_colored(level: &tracing::Level, ansi: bool) -> String {
    let label = match *level {
        tracing::Level::ERROR => "error",
        tracing::Level::WARN => "warn",
        tracing::Level::INFO => "info",
        tracing::Level::DEBUG => "verbose",
        tracing::Level::TRACE => "trace",
    };

    if !ansi {
        return format!("{label}:");
    }

    let color = match *level {
        tracing::Level::ERROR => "\x1b[31m", // red
        tracing::Level::WARN => "\x1b[33m",  // yellow
        tracing::Level::INFO => "\x1b[32m",  // green
        tracing::Level::DEBUG => "\x1b[36m", // cyan
        tracing::Level::TRACE => "\x1b[2m",  // dim
    };
    format!("{color}{label}:\x1b[0m")
}

// ── Custom console formatter ──

/// Produces lines like:
/// `2026-03-21 23:38:29 - core - info: message`
/// `2026-03-21 23:38:29 - @repo/plugin-tmdb - verbose: message field=value`
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

// ── Live-log broadcast layer ──

/// A tracing Layer that serialises each log event to JSON and sends it to a
/// broadcast channel so SSE clients can receive live logs.
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

        // Only broadcast INFO and above from riven/plugin crates.
        if *meta.level() > tracing::Level::INFO {
            return;
        }
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

/// Visitor that extracts the `message` field from a tracing event.
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

// ── Init ──

pub fn init_logging(settings: &RivenSettings, log_tx: broadcast::Sender<String>) {
    if !settings.logging_enabled {
        return;
    }

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&settings.log_level))
        // Apalis emits heartbeat events every second on DEBUG/TRACE;
        // this suppresses that spam while keeping user levels intact.
        .add_directive("apalis_core=info".parse().unwrap());

    let registry = tracing_subscriber::registry().with(filter);

    // Console layer — custom format
    let console_layer = tracing_subscriber::fmt::layer().event_format(RivenFormatter);

    // File layer (JSON, rolling daily)
    let log_dir = &settings.log_directory;
    let file_appender = tracing_appender::rolling::daily(log_dir, "riven.log");
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(file_appender)
        .with_ansi(false)
        .json();

    // Live-stream broadcast layer
    let broadcast_layer = BroadcastLogLayer { tx: log_tx };

    registry
        .with(console_layer)
        .with(file_layer)
        .with(broadcast_layer)
        .init();
}
