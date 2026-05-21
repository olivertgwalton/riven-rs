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
    /// Sustained refill rate in tokens per second: `max / per`.
    fn refill_per_sec(self) -> f64 {
        f64::from(self.max) / self.per.as_secs_f64()
    }

    /// Bucket capacity — the maximum burst allowed before the limiter starts
    /// pacing. Equal to the window cap (`max`).
    fn capacity(self) -> f64 {
        f64::from(self.max)
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

/// Token-bucket limiter. Unlike a strict `min_interval` gate (one request every
/// `per/max`, fully serial), the bucket lets up to `max` requests through in a
/// burst and then refills continuously at `max/per`. This is what lets the many
/// concurrent flow workers (e.g. every episode of a season fanned out at once)
/// actually run in parallel within a service's budget instead of being pinned
/// to one-at-a-time. The long-run average still tracks the configured rate.
#[derive(Debug, Default)]
struct LimiterState {
    /// Available tokens. `None` until the first request seeds a full bucket.
    tokens: Option<f64>,
    /// Last time `tokens` was refilled.
    last_refill: Option<Instant>,
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
        let capacity = rate_limit.capacity();
        let refill = rate_limit.refill_per_sec();

        // Refill: add tokens accrued since the last check, capped at capacity.
        // First request seeds a full bucket so an idle service bursts freely.
        let mut tokens = match (self.tokens, self.last_refill) {
            (Some(t), Some(last)) => {
                (t + now.duration_since(last).as_secs_f64() * refill).min(capacity)
            }
            _ => capacity,
        };
        self.last_refill = Some(now);

        if tokens >= 1.0 {
            tokens -= 1.0;
            self.tokens = Some(tokens);
            None
        } else {
            // Not enough for a whole token yet — wait for the deficit to refill.
            self.tokens = Some(tokens);
            let deficit = 1.0 - tokens;
            Some(Duration::from_secs_f64(deficit / refill))
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
