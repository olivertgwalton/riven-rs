use std::time::Duration;

use riven_core::http::HttpServiceProfile;

pub(crate) const TMDB: HttpServiceProfile =
    HttpServiceProfile::new("tmdb").with_rate_limit(40, Duration::from_secs(1));
pub(crate) const TVDB: HttpServiceProfile =
    HttpServiceProfile::new("tvdb").with_rate_limit(25, Duration::from_secs(1));
pub(crate) const TRAKT: HttpServiceProfile =
    HttpServiceProfile::new("trakt").with_rate_limit(1000, Duration::from_secs(300));
pub(crate) const ANILIST: HttpServiceProfile =
    HttpServiceProfile::new("anilist").with_rate_limit(2, Duration::from_secs(1));
pub(crate) const ANIZIP: HttpServiceProfile =
    HttpServiceProfile::new("anizip").with_rate_limit(2, Duration::from_secs(1));
