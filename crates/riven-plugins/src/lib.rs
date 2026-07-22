//! The set of plugins compiled into the binary.
//!
//! This explicit list is the single source of truth for which plugins are
//! built in. Adding a plugin means adding its crate to `Cargo.toml` and one
//! line here. The order is the order plugins are registered and dispatched in.
//!
//! The list lives in its own crate rather than in `riven-app` so the docs
//! generator (`cargo run --bin gen-docs`) can read every plugin's settings
//! schema without pulling in the binary's FUSE and database dependencies.

use riven_core::plugin::Plugin;

pub fn all_plugins() -> Vec<Box<dyn Plugin>> {
    vec![
        Box::new(plugin_tmdb::TmdbPlugin),
        Box::new(plugin_tvdb::TvdbPlugin::default()),
        Box::new(plugin_comet::CometPlugin),
        Box::new(plugin_torrentio::TorrentioPlugin),
        Box::new(plugin_aiostreams::AioStreamsPlugin),
        Box::new(plugin_stremthru::StremthruPlugin),
        Box::new(plugin_plex::PlexPlugin::default()),
        Box::new(plugin_seerr::SeerrPlugin),
        Box::new(plugin_listrr::ListrrPlugin),
        Box::new(plugin_mdblist::MdblistPlugin),
        Box::new(plugin_subdl::SubdlPlugin),
        Box::new(plugin_notifications::NotificationsPlugin),
        Box::new(plugin_webhooks::WebhooksPlugin),
        Box::new(plugin_emby_jellyfin::EmbyPlugin),
        Box::new(plugin_emby_jellyfin::JellyfinPlugin),
        Box::new(plugin_calendar::CalendarPlugin),
        Box::new(plugin_trakt::TraktPlugin),
        Box::new(plugin_dashboard::DashboardPlugin),
        Box::new(plugin_newznab::NewznabPlugin),
        Box::new(plugin_usenet::UsenetPlugin),
    ]
}
