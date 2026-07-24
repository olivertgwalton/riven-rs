/// Runtime-configurable downloader settings.
#[derive(Clone, Default)]
pub struct DownloaderConfig {
    pub attempt_unknown_downloads: bool,
}

impl From<&crate::settings::RivenSettings> for DownloaderConfig {
    fn from(s: &crate::settings::RivenSettings) -> Self {
        Self {
            attempt_unknown_downloads: s.attempt_unknown_downloads,
        }
    }
}

/// Minimum/maximum average-bitrate limits for a single ranking profile.
///
/// Each limit is expressed in Mbps and is optional (`None` = no limit). The
/// checks convert a limit into a size threshold using the item's runtime, so a
/// stream's file size stands in for its average bitrate.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct BitrateLimits {
    pub minimum_average_bitrate_movies: Option<u32>,
    pub minimum_average_bitrate_episodes: Option<u32>,
    pub maximum_average_bitrate_movies: Option<u32>,
    pub maximum_average_bitrate_episodes: Option<u32>,
}

impl BitrateLimits {
    #[must_use]
    pub fn threshold_bytes(mbps: u32, runtime_minutes: i32) -> u64 {
        let mins = u64::from(runtime_minutes.max(0).cast_unsigned());
        mins * 60 * u64::from(mbps) * 125_000
    }

    #[must_use]
    pub fn movie_passes(&self, file_size: u64, runtime_minutes: Option<i32>) -> bool {
        self.passes(
            self.minimum_average_bitrate_movies,
            self.maximum_average_bitrate_movies,
            file_size,
            runtime_minutes,
        )
    }

    #[must_use]
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
    use super::BitrateLimits;

    #[test]
    fn threshold_bytes_scales_with_runtime_and_bitrate() {
        assert_eq!(BitrateLimits::threshold_bytes(10, 60), 4_500_000_000);
    }

    #[test]
    fn movie_passes_returns_true_without_runtime() {
        let limits = BitrateLimits {
            minimum_average_bitrate_movies: Some(10),
            maximum_average_bitrate_movies: Some(40),
            ..BitrateLimits::default()
        };

        assert!(limits.movie_passes(1, None));
    }

    #[test]
    fn movie_passes_enforces_minimum_and_maximum_thresholds() {
        let limits = BitrateLimits {
            minimum_average_bitrate_movies: Some(10),
            maximum_average_bitrate_movies: Some(20),
            ..BitrateLimits::default()
        };

        let min_bytes = BitrateLimits::threshold_bytes(10, 120);
        let max_bytes = BitrateLimits::threshold_bytes(20, 120);

        assert!(!limits.movie_passes(min_bytes - 1, Some(120)));
        assert!(limits.movie_passes(min_bytes, Some(120)));
        assert!(limits.movie_passes(max_bytes, Some(120)));
        assert!(!limits.movie_passes(max_bytes + 1, Some(120)));
    }

    #[test]
    fn episode_passes_uses_episode_specific_limits() {
        let limits = BitrateLimits {
            minimum_average_bitrate_episodes: Some(4),
            maximum_average_bitrate_episodes: Some(8),
            ..BitrateLimits::default()
        };

        let ok_size = BitrateLimits::threshold_bytes(6, 30);
        let too_small = BitrateLimits::threshold_bytes(3, 30);
        let too_large = BitrateLimits::threshold_bytes(9, 30);

        assert!(!limits.episode_passes(too_small, Some(30)));
        assert!(limits.episode_passes(ok_size, Some(30)));
        assert!(!limits.episode_passes(too_large, Some(30)));
    }
}
