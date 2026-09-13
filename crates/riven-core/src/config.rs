/// Tuning for outbound API calls — metadata providers, scrapers, notification
/// targets. Deliberately separate from [`vfs`]: those requests are small JSON
/// bodies to chatty hosts, where a whole-request deadline and HTTP/2 both make
/// sense. Streaming wants the opposite of each.
pub mod api {
    /// Total deadline for one API request, body included. Sound here precisely
    /// because the bodies are small and bounded — the same setting applied to a
    /// multi-megabyte ranged read would cap throughput rather than detect a
    /// fault.
    pub const REQUEST_TIMEOUT_SECS: u64 = 30;

    /// Timeout for establishing a connection to an API host.
    pub const CONNECT_TIMEOUT_SECS: u64 = 10;

    /// How long an idle connection is kept for reuse. Longer than the streaming
    /// pool's: metadata polls recur on a schedule, so a warm connection usually
    /// gets used again.
    pub const POOL_IDLE_TIMEOUT_SECS: u64 = 90;
}

pub mod vfs {
    /// Kernel block size — the byte length the OS reads/writes at a time.
    pub const BLOCK_SIZE: u64 = 131_072;

    /// Timeout for detecting stalled streams. Wired to reqwest's
    /// `read_timeout`, which resets on every successful read, so this bounds
    /// *inactivity* rather than total transfer time. It must not be applied as
    /// a whole-request deadline: a healthy 8 MiB chunk fetched over a slow or
    /// heavily-shared link legitimately exceeds it while never stalling.
    pub const ACTIVITY_TIMEOUT_SECS: u64 = 60;

    /// Timeout for establishing a connection to the streaming service.
    pub const CONNECT_TIMEOUT_SECS: u64 = 10;
}
