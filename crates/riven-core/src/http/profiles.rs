use std::borrow::Cow;
use std::time::Duration;

use super::RateLimit;

pub const DEFAULT_ATTEMPTS: u32 = 3;

/// `name` is `Cow<'static, str>` so that well-known services can use zero-cost
/// `&'static str` literals while unknown runtime stores (e.g. from config) can
/// supply an owned `String` without leaking memory or losing identity in logs.
/// The type does not implement `Copy` because `Cow::Owned` contains a `String`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpServiceProfile {
    pub name: Cow<'static, str>,
    pub attempts: u32,
    pub rate_limit: Option<RateLimit>,
}

impl HttpServiceProfile {
    /// Create a profile with a `'static` name (zero-cost borrow).
    pub const fn new(name: &'static str) -> Self {
        Self {
            name: Cow::Borrowed(name),
            attempts: DEFAULT_ATTEMPTS,
            rate_limit: None,
        }
    }

    /// Create a profile with a runtime-owned name for stores not covered by a
    /// named constant.
    pub fn new_owned(name: String) -> Self {
        Self {
            name: Cow::Owned(name),
            attempts: DEFAULT_ATTEMPTS,
            rate_limit: None,
        }
    }

    pub const fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// A proactive cap on top of reactive handling. Reserve this for a
    /// service whose own rate-limit signal is too undocumented or
    /// inconsistent to react to — every other service should stay
    /// reactive-only (no `rate_limit` set) and rely purely on 429/Retry-After
    /// or a provider-specific quota pause.
    pub const fn with_rate_limit(mut self, max: u32, per: Duration) -> Self {
        self.rate_limit = Some(RateLimit { max, per });
        self
    }
}

pub const DISCORD_WEBHOOK: HttpServiceProfile = HttpServiceProfile::new("discord_webhook");
pub const WEBHOOK_JSON: HttpServiceProfile = HttpServiceProfile::new("json_webhook");
