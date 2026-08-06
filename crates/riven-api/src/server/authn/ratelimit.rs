//! Rate limiting for `/auth`, modelled on better-auth's built-in limiter.
//!
//! A fixed window per `(client, path)`: 100 requests per 60 seconds by
//! default, tightened to 3 per 10 seconds on the endpoints that verify a
//! credential — the same numbers better-auth applies to `/sign-in/email` and
//! `/two-factor/verify`. Over-limit answers `429` with `Retry-After`.
//!
//! This matters more here than the request count suggests: every password
//! attempt costs an Argon2 hash (~19 MiB and two passes), so an unthrottled
//! login endpoint is both unlimited guessing and a cheap way to exhaust the
//! server's CPU from off-machine.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

struct Rule {
    window: Duration,
    max: u32,
}

/// Endpoints where a request is an attempt at a credential — password
/// sign-in, sign-up, reset, passkey assertion, and the two endpoints that
/// re-check the current password. Paths are matched exactly, so a new route
/// gets the (safe, looser) default rather than silently inheriting this.
const STRICT_PATHS: [&str; 8] = [
    "/sign-in/email",
    "/sign-in/username",
    "/sign-up/email",
    "/request-password-reset",
    "/reset-password",
    "/passkey/verify-authentication",
    "/change-password",
    "/delete-user",
];

const STRICT: Rule = Rule {
    window: Duration::from_secs(10),
    max: 3,
};

const DEFAULT: Rule = Rule {
    window: Duration::from_secs(60),
    max: 100,
};

fn rule_for(path: &str) -> &'static Rule {
    if STRICT_PATHS.contains(&path) {
        &STRICT
    } else {
        &DEFAULT
    }
}

struct Counter {
    started: Instant,
    hits: u32,
}

static WINDOWS: LazyLock<Mutex<HashMap<String, Counter>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Check and increment under one lock, so concurrent requests cannot all read
/// a stale count before any increment lands. Returns the seconds to wait when
/// the caller is over its limit.
fn consume(key: String, rule: &Rule) -> Result<(), u64> {
    let mut windows = WINDOWS.lock().unwrap_or_else(|e| e.into_inner());

    // Sweeping on every call keeps the map bounded by live windows rather
    // than by the number of distinct clients ever seen, which is what stops a
    // caller rotating source addresses from growing it without limit.
    windows.retain(|_, counter| counter.started.elapsed() < DEFAULT.window.max(STRICT.window));

    let counter = windows.entry(key).or_insert_with(|| Counter {
        started: Instant::now(),
        hits: 0,
    });
    if counter.started.elapsed() >= rule.window {
        counter.started = Instant::now();
        counter.hits = 0;
    }
    if counter.hits >= rule.max {
        return Err(rule
            .window
            .saturating_sub(counter.started.elapsed())
            .as_secs()
            + 1);
    }
    counter.hits += 1;
    Ok(())
}

/// Identify the caller by socket address — the one identity a client cannot
/// forge.
///
/// No forwarded-IP header is consulted, deliberately. Believing one would let
/// any caller mint an unlimited supply of fresh budgets by inventing header
/// values, which turns the limiter off for exactly the attacker it exists to
/// stop. The cost is that behind a reverse proxy every request carries the
/// *proxy's* address and all callers share one bucket — blunter, but it fails
/// in the safe direction.
fn client_key(request: &Request) -> String {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map_or_else(
            || "shared".to_string(),
            |ConnectInfo(addr)| addr.ip().to_string(),
        )
}

/// Middleware over the whole `/auth` router.
pub async fn limit(request: Request, next: Next) -> Response {
    // `nest` strips the mount prefix before the inner router sees the URI,
    // but strip it defensively so the rule table keys on one stable spelling
    // whichever way the router is composed.
    let path = request.uri().path().to_string();
    let path = path.strip_prefix("/auth").unwrap_or(&path);

    let rule = rule_for(path);
    let key = format!("{}:{path}", client_key(&request));

    match consume(key, rule) {
        Ok(()) => next.run(request).await,
        Err(retry_after) => {
            tracing::warn!(%path, %retry_after, "rate limit exceeded");
            (
                StatusCode::TOO_MANY_REQUESTS,
                [
                    ("retry-after", retry_after.to_string()),
                    // better-auth's clients read this spelling; the standard
                    // header above is what proxies and browsers understand.
                    ("x-retry-after", retry_after.to_string()),
                ],
                axum::Json(serde_json::json!({
                    "message": "Too many requests. Try again shortly."
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_endpoints_are_strict_and_everything_else_is_not() {
        assert_eq!(rule_for("/sign-in/email").max, STRICT.max);
        assert_eq!(rule_for("/reset-password").max, STRICT.max);
        assert_eq!(rule_for("/passkey/verify-authentication").max, STRICT.max);
        // A route not in the table must not accidentally inherit the strict
        // budget — `/get-session` is polled on every page load.
        assert_eq!(rule_for("/get-session").max, DEFAULT.max);
        assert_eq!(rule_for("/oidc-providers").max, DEFAULT.max);
    }

    #[test]
    fn a_caller_is_cut_off_after_the_maximum_and_told_when_to_retry() {
        let key = format!("test-{}:/sign-in/email", super::super::random_token());
        for attempt in 0..STRICT.max {
            assert!(
                consume(key.clone(), &STRICT).is_ok(),
                "attempt {attempt} should be allowed"
            );
        }
        let retry_after =
            consume(key.clone(), &STRICT).expect_err("the request past the limit must be refused");
        assert!(
            retry_after > 0 && retry_after <= STRICT.window.as_secs() + 1,
            "implausible retry-after: {retry_after}"
        );
    }

    /// Buckets are per key, so one client exhausting its budget must not
    /// lock out everyone else.
    #[test]
    fn limits_are_isolated_per_caller() {
        let noisy = format!("noisy-{}:/sign-in/email", super::super::random_token());
        let quiet = format!("quiet-{}:/sign-in/email", super::super::random_token());
        for _ in 0..STRICT.max {
            assert!(consume(noisy.clone(), &STRICT).is_ok());
        }
        assert!(consume(noisy, &STRICT).is_err());
        assert!(consume(quiet, &STRICT).is_ok());
    }
}
