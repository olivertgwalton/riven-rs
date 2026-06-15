//! The set of plugins compiled into the binary.
//!
//! This explicit list is the single source of truth for which plugins are
//! built in. Adding a plugin means adding its crate to `Cargo.toml` and one
//! line here. The order is the order plugins are registered and dispatched in.

use riven_core::plugin::Plugin;

pub fn all_plugins() -> Vec<Box<dyn Plugin>> {
    vec![
        Box::new(plugin_tmdb::TmdbPlugin::default()),
        Box::new(plugin_tvdb::TvdbPlugin::default()),
        Box::new(plugin_comet::CometPlugin::default()),
        Box::new(plugin_torrentio::TorrentioPlugin::default()),
        Box::new(plugin_aiostreams::AioStreamsPlugin::default()),
        Box::new(plugin_stremthru::StremthruPlugin::default()),
        Box::new(plugin_plex::PlexPlugin::default()),
        Box::new(plugin_seerr::SeerrPlugin::default()),
        Box::new(plugin_listrr::ListrrPlugin::default()),
        Box::new(plugin_mdblist::MdblistPlugin::default()),
        Box::new(plugin_subdl::SubdlPlugin::default()),
        Box::new(plugin_notifications::NotificationsPlugin::default()),
        Box::new(plugin_webhooks::WebhooksPlugin::default()),
        Box::new(plugin_emby_jellyfin::EmbyPlugin::default()),
        Box::new(plugin_emby_jellyfin::JellyfinPlugin::default()),
        Box::new(plugin_calendar::CalendarPlugin::default()),
        Box::new(plugin_trakt::TraktPlugin::default()),
        Box::new(plugin_dashboard::DashboardPlugin::default()),
        Box::new(plugin_newznab::NewznabPlugin::default()),
        Box::new(plugin_usenet::UsenetPlugin::default()),
    ]
}
