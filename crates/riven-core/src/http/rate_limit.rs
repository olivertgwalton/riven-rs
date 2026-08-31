use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::time::sleep;

use super::HttpServiceProfile;

/// A proactive cap, for the rare service whose own signaling is too
/// undocumented or unreliable to react to — see [`HttpServiceProfile::with_rate_limit`].
/// Every other service is reactive-only: paced purely by pauses registered
/// from an actual 429/Retry-After or a provider-specific quota signal.
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

/// Longest a caller will block waiting for a token, or waiting out a reactive
/// pause, before being told to requeue. Short enough to stay far inside the
/// tightest job deadline (the plugin-hook worker's 180s), long enough that
/// ordinary sub-second pacing or a short `Retry-After` still resolves
/// in-place rather than churning the queue.
const MAX_LIMITER_WAIT: Duration = Duration::from_secs(10);

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

    /// Take a token, or report that the caller should hand its job back to the
    /// queue. `false` means either a reactive pause (set by an actual
    /// 429/Retry-After or a provider-specific quota signal) or, for the
    /// handful of profiles configured with `with_rate_limit`, a proactive
    /// token-bucket wait would cost more than [`MAX_LIMITER_WAIT`]. A profile
    /// with no proactive limit and no pause registered against it never waits
    /// at all, however many requests are in flight.
    ///
    /// riven-ts never has to make this choice: BullMQ's limiter is queue-level,
    /// so a worker does not *pick up* a job while the queue is limited and a
    /// rate-limited job is never sitting in a slot. riven's limiter is
    /// per-request and inside the handler, so the equivalent is "wait briefly,
    /// otherwise defer" — a job that keeps waiting holds its worker slot and
    /// spends its own deadline doing nothing, which is how a saturated bucket
    /// turns into every job dying at its timeout having made no request.
    pub(super) async fn acquire_slot(&self) -> bool {
        let deadline = Instant::now() + MAX_LIMITER_WAIT;
        loop {
            let wait = self.limiter.lock().next_wait(&self.profile);
            match wait {
                Some(d) => {
                    if Instant::now() + d > deadline {
                        return false;
                    }
                    sleep(d).await;
                }
                None => return true,
            }
        }
    }

    pub(super) fn register_retry_after(&self, delay: Duration) {
        self.limiter.lock().pause_for(delay);
    }
}

/// Reactive by default: nothing is throttled unless the service has actually
/// told us to back off. `tokens`/`last_refill` stay `None` and are never
/// consulted for a profile with no `rate_limit` configured — only
/// `paused_until` gates those. For the few profiles that do carry a
/// `rate_limit` (a proactive token bucket, letting up to `max` requests burst
/// through before pacing at `max/per`), it layers on top of the same reactive
/// pause.
#[derive(Debug, Default)]
struct LimiterState {
    tokens: Option<f64>,
    last_refill: Option<Instant>,
    paused_until: Option<Instant>,
}

impl LimiterState {
    fn next_wait(&mut self, profile: &HttpServiceProfile) -> Option<Duration> {
        let now = Instant::now();

        if let Some(paused_until) = self.paused_until {
            if paused_until > now {
                return Some(paused_until - now);
            }
            self.paused_until = None;
        }

        let rate_limit = profile.rate_limit?;
        let capacity = rate_limit.capacity();
        let refill = rate_limit.refill_per_sec();

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
