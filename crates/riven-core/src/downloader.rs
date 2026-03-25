/// Runtime-configurable bitrate filter for downloaded files.
///
/// Thresholds are computed as `runtime_seconds × bitrate_mbps × 125_000` (bytes).
/// If a threshold is `None` or the item has no runtime, that check is skipped.
#[derive(Clone, Default)]
pub struct DownloaderConfig {
    /// Minimum average bitrate for movies (Mbps). `None` = disabled.
    pub minimum_average_bitrate_movies: Option<u32>,
    /// Minimum average bitrate for episodes (Mbps). `None` = disabled.
    pub minimum_average_bitrate_episodes: Option<u32>,
    /// Maximum average bitrate for movies (Mbps). `None` = disabled.
    pub maximum_average_bitrate_movies: Option<u32>,
    /// Maximum average bitrate for episodes (Mbps). `None` = disabled.
    pub maximum_average_bitrate_episodes: Option<u32>,
}

impl DownloaderConfig {
    pub fn threshold_bytes(mbps: u32, runtime_minutes: i32) -> u64 {
        runtime_minutes as u64 * 60 * mbps as u64 * 125_000
    }

    /// Returns `true` if the file passes both the min and max bitrate gates for movies.
    pub fn movie_passes(&self, file_size: u64, runtime_minutes: Option<i32>) -> bool {
        self.passes(
            self.minimum_average_bitrate_movies,
            self.maximum_average_bitrate_movies,
            file_size,
            runtime_minutes,
        )
    }

    /// Returns `true` if the file passes both the min and max bitrate gates for episodes.
    pub fn episode_passes(&self, file_size: u64, runtime_minutes: Option<i32>) -> bool {
        self.passes(
            self.minimum_average_bitrate_episodes,
            self.maximum_average_bitrate_episodes,
            file_size,
            runtime_minutes,
        )
    }

    fn passes(
        &self,
        min_mbps: Option<u32>,
        max_mbps: Option<u32>,
        file_size: u64,
        runtime_minutes: Option<i32>,
    ) -> bool {
        let Some(mins) = runtime_minutes else {
            return true;
        };
        if let Some(min) = min_mbps {
            if file_size < Self::threshold_bytes(min, mins) {
                return false;
            }
        }
        if let Some(max) = max_mbps {
            if file_size > Self::threshold_bytes(max, mins) {
                return false;
            }
        }
        true
    }
}
