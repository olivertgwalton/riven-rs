mod emby;
mod jellyfin;

pub use emby::EmbyPlugin;
pub use jellyfin::JellyfinPlugin;

use serde::Serialize;

#[derive(Serialize)]
struct LibraryUpdate<'a> {
    #[serde(rename = "Updates")]
    updates: Vec<PathUpdate<'a>>,
}

#[derive(Serialize)]
struct PathUpdate<'a> {
    #[serde(rename = "Path")]
    path: &'a str,
    #[serde(rename = "UpdateType")]
    update_type: &'static str,
}

/// Notify a Jellyfin/Emby server that the given VFS paths were created.
/// All paths are sent in a single request.
pub(crate) async fn notify_paths(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    paths: &[String],
    plugin: &'static str,
) -> anyhow::Result<()> {
    let updates = paths
        .iter()
        .map(|p| PathUpdate { path: p, update_type: "Created" })
        .collect();

    let resp = client
        .post(format!("{base_url}/Library/Media/Updated"))
        .query(&[("api_key", api_key)])
        .json(&LibraryUpdate { updates })
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("{plugin} notify failed: {}", resp.status());
    }

    tracing::info!(plugin, paths = paths.len(), "library paths notified");
    Ok(())
}
