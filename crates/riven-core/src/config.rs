/// VFS configuration constants matching the TypeScript implementation.
pub mod vfs {
    /// Kernel block size — the byte length the OS reads/writes at a time.
    pub const BLOCK_SIZE: u64 = 131_072; // 128 KB

    /// Default header size for scanning purposes.
    pub const HEADER_SIZE: u64 = 262_144; // 256 KB

    /// Minimum footer size for scanning purposes.
    pub const MIN_FOOTER_SIZE: u64 = 16_384; // 16 KB

    /// Maximum footer size for scanning purposes.
    pub const MAX_FOOTER_SIZE: u64 = 10_485_760; // 10 MB

    /// Target footer size as a percentage of the file size.
    pub const TARGET_FOOTER_PERCENTAGE: f64 = 0.02; // 2%

    /// Chunk size (in bytes) used for streaming calculations.
    pub const CHUNK_SIZE: u64 = 1_048_576; // 1 MB

    /// Per-handle RAM budget for sequential playback buffering.
    pub const STREAM_BUFFER_SIZE: u64 = 32 * 1_048_576; // 32 MB

    /// Timeout for detecting stalled streams.
    pub const ACTIVITY_TIMEOUT_SECS: u64 = 60;

    /// Timeout for establishing a connection to the streaming service.
    pub const CONNECT_TIMEOUT_SECS: u64 = 10;

    /// Timeout for waiting for a chunk to become available.
    pub const CHUNK_TIMEOUT_SECS: u64 = 10;

    /// Tolerance for detecting scan reads (in blocks).
    pub const SCAN_TOLERANCE_BLOCKS: u64 = 25;

    /// Tolerance for interleaved sequential reads (in blocks).
    pub const SEQUENTIAL_READ_TOLERANCE_BLOCKS: u64 = 10;

    /// Scan tolerance in bytes.
    pub const SCAN_TOLERANCE_BYTES: u64 = SCAN_TOLERANCE_BLOCKS * BLOCK_SIZE;

    /// Sequential read tolerance in bytes.
    pub const SEQUENTIAL_READ_TOLERANCE_BYTES: u64 = SEQUENTIAL_READ_TOLERANCE_BLOCKS * BLOCK_SIZE;

    /// Calculate footer size for a given file size.
    pub fn footer_size(file_size: u64) -> u64 {
        let target = (file_size as f64 * TARGET_FOOTER_PERCENTAGE) as u64;
        let clamped = target.clamp(MIN_FOOTER_SIZE, MAX_FOOTER_SIZE);
        // Align to block boundary
        (clamped / BLOCK_SIZE) * BLOCK_SIZE
    }
}
