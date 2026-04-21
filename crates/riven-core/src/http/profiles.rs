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

    pub const fn with_rate_limit(mut self, max: u32, per: Duration) -> Self {
        self.rate_limit = Some(RateLimit { max, per });
        self
    }
}

pub const AIOSTREAMS: HttpServiceProfile =
    HttpServiceProfile::new("aiostreams").with_rate_limit(120, Duration::from_secs(60));
pub const ANILIST: HttpServiceProfile =
    HttpServiceProfile::new("anilist").with_rate_limit(2, Duration::from_secs(1));
pub const ANIZIP: HttpServiceProfile =
    HttpServiceProfile::new("anizip").with_rate_limit(2, Duration::from_secs(1));
pub const COMET: HttpServiceProfile =
    HttpServiceProfile::new("comet").with_rate_limit(150, Duration::from_secs(60));
pub const DISCORD_WEBHOOK: HttpServiceProfile = HttpServiceProfile::new("discord_webhook");
pub const EMBY: HttpServiceProfile = HttpServiceProfile::new("emby");
pub const JELLYFIN: HttpServiceProfile = HttpServiceProfile::new("jellyfin");
pub const LISTRR: HttpServiceProfile =
    HttpServiceProfile::new("listrr").with_rate_limit(50, Duration::from_secs(1));
pub const MDBLIST: HttpServiceProfile =
    HttpServiceProfile::new("mdblist").with_rate_limit(50, Duration::from_secs(1));
pub const PLEX: HttpServiceProfile = HttpServiceProfile::new("plex");
pub const SEERR: HttpServiceProfile =
    HttpServiceProfile::new("seerr").with_rate_limit(20, Duration::from_secs(1));
pub const STREMTHRU: HttpServiceProfile = HttpServiceProfile::new("stremthru");
pub const TMDB: HttpServiceProfile =
    HttpServiceProfile::new("tmdb").with_rate_limit(40, Duration::from_secs(1));
pub const TORRENTIO: HttpServiceProfile =
    HttpServiceProfile::new("torrentio").with_rate_limit(150, Duration::from_secs(60));
pub const TRAKT: HttpServiceProfile =
    HttpServiceProfile::new("trakt").with_rate_limit(1000, Duration::from_secs(300));
pub const TVDB: HttpServiceProfile =
    HttpServiceProfile::new("tvdb").with_rate_limit(25, Duration::from_secs(1));
pub const TVMAZE: HttpServiceProfile =
    HttpServiceProfile::new("tvmaze").with_rate_limit(20, Duration::from_secs(10));
pub const WEBHOOK_JSON: HttpServiceProfile = HttpServiceProfile::new("json_webhook");

pub const REALDEBRID: HttpServiceProfile = HttpServiceProfile::new("realdebrid");
pub const TORBOX: HttpServiceProfile = HttpServiceProfile::new("torbox");
pub const ALLDEBRID: HttpServiceProfile = HttpServiceProfile::new("alldebrid");
pub const DEBRIDLINK: HttpServiceProfile = HttpServiceProfile::new("debridlink");
pub const PREMIUMIZE: HttpServiceProfile = HttpServiceProfile::new("premiumize");

pub fn media_server(plugin: &'static str) -> HttpServiceProfile {
    match plugin {
        "emby" => EMBY,
        "jellyfin" => JELLYFIN,
        _ => HttpServiceProfile::new(plugin),
    }
}

pub fn debrid_service(store: &str) -> HttpServiceProfile {
    match store {
        "realdebrid" => REALDEBRID,
        "torbox" => TORBOX,
        "alldebrid" => ALLDEBRID,
        "debridlink" => DEBRIDLINK,
        "premiumize" => PREMIUMIZE,
        _ => HttpServiceProfile::new_owned(store.to_owned()),
    }
}
