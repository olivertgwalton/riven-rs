use std::time::Duration;

use riven_core::http::HttpServiceProfile;

pub(crate) const TMDB: HttpServiceProfile = HttpServiceProfile::new("tmdb");
/// TVDB v4 does not document a rate limit or a reliable way to react to
/// one — kept on a conservative proactive cap; see `plugin-tvdb`.
pub(crate) const TVDB: HttpServiceProfile =
    HttpServiceProfile::new("tvdb").with_rate_limit(25, Duration::from_secs(1));
pub(crate) const TRAKT: HttpServiceProfile = HttpServiceProfile::new("trakt");
pub(crate) const ANILIST: HttpServiceProfile = HttpServiceProfile::new("anilist");
pub(crate) const ANIZIP: HttpServiceProfile = HttpServiceProfile::new("anizip");
