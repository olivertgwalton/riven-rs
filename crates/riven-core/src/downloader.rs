/// Runtime-configurable bitrate filter for downloaded files.
#[derive(Clone, Default)]
pub struct DownloaderConfig {
    pub minimum_average_bitrate_movies: Option<u32>,
    pub minimum_average_bitrate_episodes: Option<u32>,
    pub maximum_average_bitrate_movies: Option<u32>,
    pub maximum_average_bitrate_episodes: Option<u32>,
}

impl From<&crate::settings::RivenSettings> for DownloaderConfig {
    fn from(s: &crate::settings::RivenSettings) -> Self {
        Self {
            minimum_average_bitrate_movies: s.minimum_average_bitrate_movies,
            minimum_average_bitrate_episodes: s.minimum_average_bitrate_episodes,
            maximum_average_bitrate_movies: s.maximum_average_bitrate_movies,
            maximum_average_bitrate_episodes: s.maximum_average_bitrate_episodes,
        }
    }
}

impl DownloaderConfig {
    pub fn threshold_bytes(mbps: u32, runtime_minutes: i32) -> u64 {
        runtime_minutes as u64 * 60 * mbps as u64 * 125_000
    }

    pub fn movie_passes(&self, file_size: u64, runtime_minutes: Option<i32>) -> bool {
        self.passes(
            self.minimum_average_bitrate_movies,
            self.maximum_average_bitrate_movies,
            file_size,
            runtime_minutes,
        )
    }

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
