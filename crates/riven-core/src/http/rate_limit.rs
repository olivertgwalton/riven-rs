use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::time::sleep;

use super::HttpServiceProfile;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimit {
    pub max: u32,
    pub per: Duration,
}

impl RateLimit {
    /// Minimum gap between consecutive requests: `per / max`.
    /// Jobs are spread evenly over the
    /// window rather than bursting to the cap and then stalling.
    fn min_interval(self) -> Duration {
        self.per / self.max
    }
}

#[derive(Debug)]
pub(super) struct ServiceState {
    pub(super) profile: HttpServiceProfile,
    limiter: Mutex<LimiterState>,
}

impl ServiceState {
    pub(super) fn new(profile: HttpServiceProfile) -> Self {
        Self {
            profile,
            limiter: Mutex::new(LimiterState::default()),
        }
    }

    pub(super) async fn acquire_slot(&self) {
        loop {
            let wait = self.limiter.lock().next_wait(&self.profile);
            match wait {
                Some(d) => sleep(d).await,
                None => return,
            }
        }
    }

    pub(super) fn register_retry_after(&self, delay: Duration) {
        self.limiter.lock().pause_for(delay);
    }
}

#[derive(Debug, Default)]
struct LimiterState {
    /// When the last request was allowed through.
    last_sent: Option<Instant>,
    /// Hard pause set by a `Retry-After` response header.
    paused_until: Option<Instant>,
}

impl LimiterState {
    fn next_wait(&mut self, profile: &HttpServiceProfile) -> Option<Duration> {
        let now = Instant::now();

        // Honour any explicit Retry-After pause first.
        if let Some(paused_until) = self.paused_until {
            if paused_until > now {
                return Some(paused_until - now);
            }
            self.paused_until = None;
        }

        let rate_limit = profile.rate_limit?;
        let min_interval = rate_limit.min_interval();

        let elapsed = self
            .last_sent
            .map(|t| now.duration_since(t))
            .unwrap_or(min_interval); // first request: always allowed immediately

        if elapsed >= min_interval {
            self.last_sent = Some(now);
            None
        } else {
            Some(min_interval.saturating_sub(elapsed))
        }
    }

    fn pause_for(&mut self, delay: Duration) {
        let until = Instant::now() + delay;
        self.paused_until = Some(
            self.paused_until
                .map_or(until, |current| current.max(until)),
        );
    }
}
