use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::time::sleep;

use super::HttpServiceProfile;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimit {
    pub max: u32,
    pub per: Duration,
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
            let wait = self.limiter.lock().next_wait(self.profile);
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
    window_started: Option<Instant>,
    used_in_window: u32,
    paused_until: Option<Instant>,
}

impl LimiterState {
    fn next_wait(&mut self, profile: HttpServiceProfile) -> Option<Duration> {
        let now = Instant::now();

        if let Some(paused_until) = self.paused_until {
            if paused_until > now {
                return Some(paused_until - now);
            }
            self.paused_until = None;
        }

        let rate_limit = profile.rate_limit?;

        let window_started = self.window_started.get_or_insert(now);
        if now.duration_since(*window_started) >= rate_limit.per {
            *window_started = now;
            self.used_in_window = 0;
        }

        if self.used_in_window < rate_limit.max {
            self.used_in_window += 1;
            return None;
        }

        Some(
            rate_limit
                .per
                .saturating_sub(now.duration_since(*window_started)),
        )
    }

    fn pause_for(&mut self, delay: Duration) {
        let until = Instant::now() + delay;
        self.paused_until = Some(
            self.paused_until
                .map_or(until, |current| current.max(until)),
        );
    }
}
