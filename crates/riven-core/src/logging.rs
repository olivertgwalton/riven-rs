use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Local;
use log::LevelFilter;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use sentry::ClientInitGuard;
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
};

use crate::settings::RivenSettings;

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

impl From<&RivenSettings> for LogSettings {
    fn from(core: &RivenSettings) -> Self {
        Self {
            enabled: core.logging_enabled,
            level: core.log_level.clone(),
            rotation: core.log_rotation.clone(),
            max_files: core.log_max_files.max(1),
            vfs_debug_logging: core.vfs_debug_logging,
        }
    }
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

/// Held for the lifetime of the process. Dropping the Sentry guard flushes
/// pending events; calling `shutdown` on the OTEL provider flushes spans.
pub struct ObservabilityHandles {
    pub log_control: Arc<LogControl>,
    pub sentry: Option<ClientInitGuard>,
    pub otel_provider: Option<SdkTracerProvider>,
}

impl ObservabilityHandles {
    pub fn shutdown(&self) {
        if let Some(provider) = &self.otel_provider
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!(error = %e, "OTEL provider shutdown error");
        }
    }
}

/// Initialize Sentry, OTEL, and the tracing subscriber.
///
/// Sentry activates when `SENTRY_DSN` is set; OTEL activates when
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Service name comes from
/// `OTEL_SERVICE_NAME`, defaulting to `riven`.
pub fn init_logging(
    settings: &LogSettings,
    log_directory: &str,
    log_tx: broadcast::Sender<String>,
) -> anyhow::Result<ObservabilityHandles> {
    let sentry_guard = init_sentry();
    let otel_provider = init_otel()?;

    let enabled = Arc::new(AtomicBool::new(settings.enabled));
    let (filter_layer, handle) = reload::Layer::new(build_level_filter(settings)?);

    let gate = |enabled: &Arc<AtomicBool>| {
        let flag = enabled.clone();
        filter_fn(move |_| flag.load(Ordering::Relaxed))
    };

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

    let sentry_layer = sentry_guard
        .as_ref()
        .map(|_| sentry::integrations::tracing::layer());
    let otel_layer = otel_provider
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("riven")));

    let subscriber = tracing_subscriber::registry()
        .with(filter_layer)
        .with(sentry_layer)
        .with(otel_layer)
        .with(console_layer)
        .with(file_layer)
        .with(broadcast_layer);

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|error| anyhow::anyhow!("failed to install tracing subscriber: {error}"))?;

    // Install the `log` → `tracing` bridge ourselves rather than letting
    // `SubscriberInitExt::init` do it, so we can drop the crates whose records
    // arrive as `log` (see `NOISY_LOG_CRATES`). Every `log` record shares the
    // single `log` tracing target, so `EnvFilter` cannot tell them apart — this
    // is the only place they can be filtered per-crate.
    if let Err(error) = tracing_log::LogTracer::builder()
        .with_max_level(log_max_level(settings))
        .ignore_all(NOISY_LOG_CRATES)
        .init()
    {
        // Not fatal: everything that logs through `tracing` still works, we
        // just lose records from dependencies that use the `log` crate.
        tracing::warn!(
            %error,
            "could not install the log-crate bridge, so logs from dependencies using `log` will be missing"
        );
    }

    log::set_max_level(log_max_level(settings));

    Ok(ObservabilityHandles {
        log_control: Arc::new(LogControl {
            handle,
            enabled,
            _file_guard: file_guard,
        }),
        sentry: sentry_guard,
        otel_provider,
    })
}

fn init_sentry() -> Option<ClientInitGuard> {
    let dsn = std::env::var("SENTRY_DSN")
        .ok()
        .filter(|v| !v.trim().is_empty())?;
    let environment = std::env::var("SENTRY_ENVIRONMENT").ok().map(Into::into);
    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment,
            attach_stacktrace: true,
            ..Default::default()
        },
    ));
    Some(guard)
}

fn init_otel() -> anyhow::Result<Option<SdkTracerProvider>> {
    let endpoint = match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return Ok(None),
    };
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "riven".into());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", service_name))
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    Ok(Some(provider))
}

fn log_max_level(settings: &LogSettings) -> LevelFilter {
    if !settings.enabled {
        return LevelFilter::Off;
    }
    match settings.level.as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    }
}

/// Crates that log through the `log` crate and only ever emit transport-level
/// chatter — TLS handshakes, socket readiness. They arrive on the single `log`
/// tracing target, so raising Riven's own level to `debug` used to bury the
/// pipeline in "Resuming session" / "Sending warning alert CloseNotify" lines.
/// Matched by target prefix; dropped at the bridge, before any filter runs.
const NOISY_LOG_CRATES: [&str; 4] = ["rustls", "mio", "want", "tungstenite"];

/// Third-party crates pinned to `info` so that turning Riven's level up to
/// `debug` shows Riven's own work rather than DNS packet dumps and per-job
/// queue bookkeeping. Setting `RUST_LOG` bypasses this list entirely.
const NOISY_TRACING_TARGETS: [&str; 7] = [
    "apalis",
    "apalis_core",
    "hickory_resolver",
    "hickory_proto",
    "hickory_net",
    "hyper_util",
    "h2",
];

fn build_level_filter(settings: &LogSettings) -> anyhow::Result<EnvFilter> {
    // An explicit `RUST_LOG` is a deliberate act — hand it over untouched so it
    // can be used to turn any of the defaults below back on.
    if let Ok(filter) = EnvFilter::try_from_default_env() {
        return Ok(filter);
    }

    let mut filter = EnvFilter::try_new(&settings.level)
        .map_err(|error| anyhow::anyhow!("invalid log level '{}': {error}", settings.level))?;

    for target in NOISY_TRACING_TARGETS {
        filter = filter.add_directive(format!("{target}=info").parse()?);
    }

    if !settings.vfs_debug_logging {
        filter = filter
            .add_directive("streaming=off".parse()?)
            .add_directive("log=info".parse()?)
            .add_directive("fuser=info".parse()?);
    }

    Ok(filter)
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
                drop(self.tx.send(line));
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

#[cfg(test)]
mod tests {
    use super::{LogSettings, build_level_filter};

    fn directives(settings: &LogSettings) -> String {
        build_level_filter(settings).unwrap().to_string()
    }

    #[test]
    fn noisy_third_party_targets_stay_at_info_when_riven_goes_to_debug() {
        let settings = LogSettings {
            level: "debug".into(),
            ..LogSettings::default()
        };
        let directives = directives(&settings);

        // Riven's own level is what the operator asked for...
        assert!(directives.contains("debug"), "{directives}");
        // ...but DNS, queue bookkeeping and the `log` bridge stay quiet, so the
        // pipeline's own lines aren't buried.
        for target in ["hickory_net=info", "apalis=info", "log=info"] {
            assert!(
                directives.contains(target),
                "missing {target} in {directives}"
            );
        }
    }

    #[test]
    fn vfs_debug_logging_keeps_the_log_bridge_verbose() {
        let settings = LogSettings {
            level: "debug".into(),
            vfs_debug_logging: true,
            ..LogSettings::default()
        };
        let directives = directives(&settings);

        assert!(!directives.contains("log=info"), "{directives}");
        assert!(!directives.contains("fuser=info"), "{directives}");
    }
}
