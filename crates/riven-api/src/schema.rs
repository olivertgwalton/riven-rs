use crate::vfs_mount::VfsMountManager;
use async_graphql::{MergedObject, Schema};
use plugin_calendar::CalendarQuery;
use plugin_dashboard::DashboardQuery;
use riven_core::downloader::DownloaderConfig;
use riven_core::http::HttpClient;
use riven_core::logging::LogControl;
use riven_core::plugin::PluginRegistry;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) mod auth;
pub mod discovery;
mod helpers;
mod metadata;
mod mutations;
mod queries;
mod subscriptions;
pub mod typed_items;
pub mod types;
mod vfs;

pub use mutations::MutationRoot;
pub use queries::CoreQuery;
pub use subscriptions::SubscriptionRoot;
pub use vfs::VfsQuery;

#[derive(MergedObject, Default)]
pub struct QueryRoot(CoreQuery, DashboardQuery, CalendarQuery, VfsQuery);

pub type AppSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

/// The Stremio addon token for this instance, carried in the GraphQL context so
/// settings resolvers can render the manifest URL without reaching back into
/// HTTP state. `None` means no API key is configured.
#[derive(Clone, Default)]
pub struct StremioAddonToken(pub Option<String>);

impl StremioAddonToken {
    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

/// This instance's own GraphQL/HTTP listen port, carried in the GraphQL
/// context so a manual-NZB-URL resolver can tell a genuine
/// `store_nzb_upload` loopback URL (same port this server is actually
/// listening on) apart from an attacker-supplied `http://127.0.0.1:<other
/// port>/internal/nzb-uploads/...` — which has the right shape but would
/// otherwise smuggle an SSRF request to any other port on the container's
/// loopback interface. See `validate_nzb_fetch_target`.
#[derive(Clone, Copy)]
pub struct GqlPort(pub u16);

pub fn build_schema(
    registry: Arc<PluginRegistry>,
    job_queue: Arc<riven_queue::JobQueue>,
    redis_conn: redis::aio::ConnectionManager,
    http_client: HttpClient,
    log_directory: String,
    downloader_config: Arc<RwLock<DownloaderConfig>>,
    log_control: Arc<LogControl>,
    log_tx: tokio::sync::broadcast::Sender<String>,
    vfs_mount_manager: Arc<VfsMountManager>,
    stremio_addon_token: StremioAddonToken,
    gql_port: GqlPort,
) -> AppSchema {
    let builder = Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .data(registry)
    .data(job_queue)
    .data(redis_conn)
    .data(http_client)
    .data(downloader_config)
    .data(log_control)
    .data(log_tx)
    .data(vfs_mount_manager)
    .data(stremio_addon_token)
    .data(gql_port);
    let builder = queries::logs::register_with_schema(builder, log_directory);
    let builder = plugin_dashboard::register_with_schema(builder);
    builder
        .limit_depth(MAX_QUERY_DEPTH)
        .limit_complexity(MAX_QUERY_COMPLEXITY)
        .finish()
}

/// The type graph is cyclic — `Show.seasons → Season.show → Show.seasons`, and
/// the same through `Season.episodes → Episode.season` — and every hop is a
/// database round trip. Without a ceiling, one authenticated request at modest
/// nesting expands into an exponential number of queries, which is a denial of
/// service available to the lowest role on the instance.
///
/// Both limits are set well above what the frontend actually asks for: the
/// deepest real query is the media-detail page at roughly eight levels. They are
/// a backstop against a pathological query, not a budget the UI has to live
/// within — if a legitimate page ever trips one, raise it rather than reshaping
/// the page.
const MAX_QUERY_DEPTH: usize = 15;
const MAX_QUERY_COMPLEXITY: usize = 2000;

/// The schema as SDL, for the frontend's type generation.
///
/// Built from the bare roots: every `.data(…)` in [`build_schema`] supplies
/// values that resolvers read at execution time, none of which changes a type,
/// so this needs no database, queue or plugin registry to produce.
pub fn sdl() -> String {
    Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .finish()
    .sdl()
}

#[cfg(test)]
mod tests {
    /// Every mutation, and the capability guarding it.
    ///
    /// This list is the review checkpoint: a resolver that reaches the schema
    /// without an entry here fails `every_mutation_is_classified`. It exists
    /// because `regrabUsenetTitle` and `rescanUsenetHealth` shipped with no
    /// guard at all — a low-privilege caller could permanently blacklist and
    /// delete any library item — and nothing caught it, because "did the author
    /// remember?" was the only control.
    ///
    /// The test cannot prove the guard is *correct*; it forces a deliberate
    /// decision, which is the part that was missing.
    const MUTATION_GUARDS: &[(&str, &str)] = &[
        // Requesting — the only action an ordinary user holds.
        ("requestMovie", "RequestItems"),
        ("requestShow", "RequestItems"),
        ("requestItems", "RequestItems"),
        ("seerrHandleWebhook", "RequestItems"),
        // Adding straight to the library.
        ("addItem", "AddItems"),
        ("discoverItem", "AddItems"),
        // Item actions.
        ("pauseItems", "PauseItems"),
        ("unpauseItems", "PauseItems"),
        ("retryItems", "RetryItems"),
        ("resetItems", "ResetItems"),
        ("removeItems", "DeleteItems"),
        ("deleteFilesystemEntry", "DeleteItems"),
        ("blacklistFilesystemEntry", "DeleteItems"),
        // Finding and committing a release.
        ("scrapeItem", "ScrapeItems"),
        ("scrapeMediaItem", "ScrapeItems"),
        ("downloadMediaItem", "ScrapeItems"),
        ("discoverStreams", "ScrapeItems"),
        ("downloadDiscoveredStream", "ScrapeItems"),
        ("downloadExplicitNzb", "ScrapeItems"),
        ("previewManualMagnet", "ScrapeItems"),
        ("previewManualNzb", "ScrapeItems"),
        ("saveStreamUrl", "ScrapeItems"),
        ("rescanUsenetHealth", "ScrapeItems"),
        // Deletes *and* re-scrapes, so it requires both.
        ("regrabUsenetTitle", "DeleteItems+ScrapeItems"),
        // Settings.
        ("resetLibrary", "ManageSettings"),
        ("saveCustomProfile", "ManageSettings"),
        ("deleteCustomProfile", "ManageSettings"),
        ("setProfileEnabled", "ManageSettings"),
        ("updateProfileSettings", "ManageSettings"),
        ("updateRankSettings", "ManageSettings"),
        ("updateAllSettings", "ManageSettings"),
        ("updateSettings", "ManageSettings"),
        ("completeInitialSetup", "ManageSettings"),
        ("rematchFilesystemLibraryProfiles", "ManageSettings"),
        ("indexMovie", "ManageSettings"),
        ("indexShow", "ManageSettings"),
    ];

    /// Field names on `MutationRoot`, read from the SDL rather than from source,
    /// so plugin-contributed mutations are included.
    fn mutation_fields(sdl: &str) -> Vec<String> {
        let body = sdl
            .split("\ntype MutationRoot {")
            .nth(1)
            .and_then(|rest| rest.split("\n}").next())
            .expect("MutationRoot missing from the schema");

        let mut fields = Vec::new();
        let mut in_description = false;
        for line in body.lines() {
            let trimmed = line.trim();
            // Descriptions are `"""…"""` blocks whose prose would otherwise be
            // mistaken for field names. An odd number of delimiters on a line
            // opens or closes a block; an even number is a one-line description.
            let delimiters = trimmed.matches("\"\"\"").count();
            if delimiters > 0 {
                if delimiters % 2 == 1 {
                    in_description = !in_description;
                }
                continue;
            }
            if in_description || trimmed.is_empty() {
                continue;
            }
            let name: String = trimmed
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if name.is_empty() {
                continue;
            }
            if trimmed[name.len()..].trim_start().starts_with(['(', ':']) {
                fields.push(name);
            }
        }
        fields
    }

    #[test]
    fn every_mutation_is_classified() {
        let sdl = super::sdl();
        let actual: std::collections::BTreeSet<String> =
            mutation_fields(&sdl).into_iter().collect();
        let classified: std::collections::BTreeSet<String> = MUTATION_GUARDS
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect();

        let unclassified: Vec<_> = actual.difference(&classified).collect();
        assert!(
            unclassified.is_empty(),
            "mutation(s) reached the schema with no entry in MUTATION_GUARDS — \
             add the resolver's guard, then list it here: {unclassified:?}"
        );

        let stale: Vec<_> = classified.difference(&actual).collect();
        assert!(
            stale.is_empty(),
            "MUTATION_GUARDS names mutations that no longer exist: {stale:?}"
        );
    }

    /// Fails when `schema.graphql` is out of date with the code.
    ///
    /// That file is the frontend's only source of types, and it lives in this
    /// repo because it is generated from here. Without this check a resolver
    /// change would compile, ship, and leave the frontend generating types for
    /// an API that no longer exists.
    #[test]
    fn the_checked_in_schema_matches_the_code() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../schema.graphql");
        let committed = std::fs::read_to_string(path).unwrap_or_default();

        assert_eq!(
            committed.trim(),
            super::sdl().trim(),
            "schema.graphql is stale — regenerate it with:\n\
             \tcargo run -p riven-api --example dump_schema"
        );
    }
}
