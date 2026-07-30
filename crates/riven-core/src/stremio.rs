//! Stremio addon identifier helpers. Series requests are episode-addressed —
//! Show falls back to `:1:1`, Season to `:N:1`, since most addons don't expose
//! Show- or Season-level endpoints.
//!
//! Also home to the addon-token derivation used when Riven *serves* a Stremio
//! addon, kept here so the HTTP layer and the settings schema derive it
//! identically from one implementation.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::events::ScrapeRequest;
use crate::types::MediaItemType;

/// Domain separator for the addon token, so the derived value can never collide
/// with another HMAC computed from the same API key.
const ADDON_TOKEN_CONTEXT: &[u8] = b"riven-stremio-addon-v1";

/// The credential embedded in Stremio addon URLs.
///
/// Stremio fetches manifests and streams with no way to attach a header, so the
/// credential has to travel in the URL. Rather than paste the real API key into
/// a third-party client (and its logs, and its account sync), this is an HMAC of
/// the API key under a fixed context: it authenticates the same holder, reveals
/// nothing about the key, and is revoked by rotating the key.
///
/// `None` means no API key is configured, i.e. the API is already unauthenticated.
pub fn addon_token(api_key: &str) -> Option<String> {
    if api_key.is_empty() {
        return None;
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(api_key.as_bytes()).ok()?;
    mac.update(ADDON_TOKEN_CONTEXT);
    Some(hex::encode(mac.finalize().into_bytes()))
}

/// Verify a token taken from an addon URL. Comparison goes through
/// `verify_slice` so it stays constant-time. An empty `api_key` means auth is
/// disabled, so anything is accepted.
pub fn verify_addon_token(api_key: &str, token: &str) -> bool {
    if api_key.is_empty() {
        return true;
    }
    let Ok(provided) = hex::decode(token) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(api_key.as_bytes()) else {
        return false;
    };
    mac.update(ADDON_TOKEN_CONTEXT);
    mac.verify_slice(&provided).is_ok()
}

/// The manifest URL to paste into Stremio. `base_url` is the public origin Riven
/// is reachable at; a trailing slash is tolerated. Returns `None` when no base
/// URL is configured, since a relative manifest URL is useless to Stremio.
pub fn manifest_url(base_url: &str, token: Option<&str>) -> Option<String> {
    let base = base_url.trim().trim_end_matches('/');
    if base.is_empty() {
        return None;
    }
    Some(match token {
        Some(token) if !token.is_empty() => format!("{base}/stremio/{token}/manifest.json"),
        _ => format!("{base}/stremio/manifest.json"),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StremioKind {
    Movie,
    Series,
}

impl StremioKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Movie => "movie",
            Self::Series => "series",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StremioScrapeConfig<'a> {
    pub imdb_id: &'a str,
    pub kind: StremioKind,
    pub episode_id: Option<(i32, i32)>,
}

impl<'a> StremioScrapeConfig<'a> {
    pub fn from_request(req: &ScrapeRequest<'a>) -> Option<Self> {
        let imdb_id = req.imdb_id?;
        let (kind, episode_id) = match req.item_type {
            MediaItemType::Movie => (StremioKind::Movie, None),
            MediaItemType::Show => (StremioKind::Series, Some((1, 1))),
            MediaItemType::Season => (StremioKind::Series, Some((req.season_or_1(), 1))),
            MediaItemType::Episode => (
                StremioKind::Series,
                Some((req.season_or_1(), req.episode_or_1())),
            ),
        };
        Some(Self {
            imdb_id,
            kind,
            episode_id,
        })
    }

    /// `:S:E` for series, empty for movies — appended to the imdb id in URL paths.
    pub fn id_suffix(&self) -> String {
        match self.episode_id {
            Some((s, e)) => format!(":{s}:{e}"),
            None => String::new(),
        }
    }

    /// `imdb_id` for movies, `imdb_id:S:E` for series — colon-joined single token.
    pub fn full_id(&self) -> String {
        match self.episode_id {
            Some((s, e)) => format!("{}:{s}:{e}", self.imdb_id),
            None => self.imdb_id.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(
        item_type: MediaItemType,
        season: Option<i32>,
        episode: Option<i32>,
    ) -> ScrapeRequest<'static> {
        ScrapeRequest {
            id: 0,
            item_type,
            imdb_id: Some("tt123"),
            tvdb_id: None,
            title: "",
            season,
            episode,
        }
    }

    #[test]
    fn movie_has_no_episode_suffix() {
        let cfg = StremioScrapeConfig::from_request(&req(MediaItemType::Movie, Some(2), Some(3)))
            .unwrap();
        assert_eq!(cfg.kind, StremioKind::Movie);
        assert_eq!(cfg.id_suffix(), "");
        assert_eq!(cfg.full_id(), "tt123");
    }

    #[test]
    fn show_falls_back_to_season_1_episode_1() {
        let cfg = StremioScrapeConfig::from_request(&req(MediaItemType::Show, None, None)).unwrap();
        assert_eq!(cfg.kind, StremioKind::Series);
        assert_eq!(cfg.id_suffix(), ":1:1");
        assert_eq!(cfg.full_id(), "tt123:1:1");
    }

    #[test]
    fn season_uses_episode_1() {
        let cfg =
            StremioScrapeConfig::from_request(&req(MediaItemType::Season, Some(2), None)).unwrap();
        assert_eq!(cfg.id_suffix(), ":2:1");
        assert_eq!(cfg.full_id(), "tt123:2:1");
    }

    #[test]
    fn episode_uses_provided_season_and_episode() {
        let cfg = StremioScrapeConfig::from_request(&req(MediaItemType::Episode, Some(3), Some(7)))
            .unwrap();
        assert_eq!(cfg.id_suffix(), ":3:7");
        assert_eq!(cfg.full_id(), "tt123:3:7");
    }

    #[test]
    fn addon_token_is_stable_and_key_dependent() {
        let a = addon_token("secret").unwrap();
        assert_eq!(a, addon_token("secret").unwrap());
        assert_ne!(a, addon_token("secret2").unwrap());
        // Hex-encoded SHA-256 output.
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        // No API key means no token — auth is disabled.
        assert_eq!(addon_token(""), None);
    }

    #[test]
    fn addon_token_verifies_only_against_its_own_key() {
        let token = addon_token("secret").unwrap();
        assert!(verify_addon_token("secret", &token));
        assert!(!verify_addon_token("other", &token));
        assert!(!verify_addon_token("secret", "not-hex"));
        assert!(!verify_addon_token("secret", ""));
        // Auth disabled accepts anything, matching `check_api_key`.
        assert!(verify_addon_token("", "whatever"));
    }

    #[test]
    fn manifest_url_normalises_the_base_and_needs_one() {
        assert_eq!(
            manifest_url("https://riven.example.uk/", Some("abc")).as_deref(),
            Some("https://riven.example.uk/stremio/abc/manifest.json")
        );
        assert_eq!(
            manifest_url("  https://riven.example.uk  ", None).as_deref(),
            Some("https://riven.example.uk/stremio/manifest.json")
        );
        assert_eq!(manifest_url("", Some("abc")), None);
        assert_eq!(manifest_url("   ", Some("abc")), None);
    }

    #[test]
    fn missing_imdb_id_returns_none() {
        let mut request = req(MediaItemType::Movie, None, None);
        request.imdb_id = None;
        assert!(StremioScrapeConfig::from_request(&request).is_none());
    }
}
