/// Runtime-configurable downloader settings.
#[derive(Clone, Default)]
pub struct DownloaderConfig {
    pub minimum_average_bitrate_movies: Option<u32>,
    pub minimum_average_bitrate_episodes: Option<u32>,
    pub maximum_average_bitrate_movies: Option<u32>,
    pub maximum_average_bitrate_episodes: Option<u32>,
    pub attempt_unknown_downloads: bool,
}

impl From<&crate::settings::RivenSettings> for DownloaderConfig {
    fn from(s: &crate::settings::RivenSettings) -> Self {
        Self {
            minimum_average_bitrate_movies: s.minimum_average_bitrate_movies,
            minimum_average_bitrate_episodes: s.minimum_average_bitrate_episodes,
            maximum_average_bitrate_movies: s.maximum_average_bitrate_movies,
            maximum_average_bitrate_episodes: s.maximum_average_bitrate_episodes,
            attempt_unknown_downloads: s.attempt_unknown_downloads,
        }
    }
}

impl DownloaderConfig {
    pub fn threshold_bytes(mbps: u32, runtime_minutes: i32) -> u64 {
        let mins = u64::from(runtime_minutes.max(0).cast_unsigned());
        mins * 60 * u64::from(mbps) * 125_000
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
        if let Some(min) = min_mbps
            && file_size < Self::threshold_bytes(min, mins)
        {
            return false;
        }
        if let Some(max) = max_mbps
            && file_size > Self::threshold_bytes(max, mins)
        {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::DownloaderConfig;

    #[test]
    fn threshold_bytes_scales_with_runtime_and_bitrate() {
        assert_eq!(DownloaderConfig::threshold_bytes(10, 60), 4_500_000_000);
    }

    #[test]
    fn movie_passes_returns_true_without_runtime() {
        let config = DownloaderConfig {
            minimum_average_bitrate_movies: Some(10),
            maximum_average_bitrate_movies: Some(40),
            ..DownloaderConfig::default()
        };

        assert!(config.movie_passes(1, None));
    }

    #[test]
    fn movie_passes_enforces_minimum_and_maximum_thresholds() {
        let config = DownloaderConfig {
            minimum_average_bitrate_movies: Some(10),
            maximum_average_bitrate_movies: Some(20),
            ..DownloaderConfig::default()
        };

        let min_bytes = DownloaderConfig::threshold_bytes(10, 120);
        let max_bytes = DownloaderConfig::threshold_bytes(20, 120);

        assert!(!config.movie_passes(min_bytes - 1, Some(120)));
        assert!(config.movie_passes(min_bytes, Some(120)));
        assert!(config.movie_passes(max_bytes, Some(120)));
        assert!(!config.movie_passes(max_bytes + 1, Some(120)));
    }

    #[test]
    fn episode_passes_uses_episode_specific_limits() {
        let config = DownloaderConfig {
            minimum_average_bitrate_episodes: Some(4),
            maximum_average_bitrate_episodes: Some(8),
            ..DownloaderConfig::default()
        };

        let ok_size = DownloaderConfig::threshold_bytes(6, 30);
        let too_small = DownloaderConfig::threshold_bytes(3, 30);
        let too_large = DownloaderConfig::threshold_bytes(9, 30);

        assert!(!config.episode_passes(too_small, Some(30)));
        assert!(config.episode_passes(ok_size, Some(30)));
        assert!(!config.episode_passes(too_large, Some(30)));
    }
}
