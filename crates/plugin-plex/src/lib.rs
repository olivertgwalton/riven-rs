use async_trait::async_trait;
use serde::Deserialize;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::settings::PluginSettings;
use riven_core::register_plugin;
use riven_db::repo;

#[derive(Default)]
pub struct PlexPlugin;

register_plugin!(PlexPlugin);

#[async_trait]
impl Plugin for PlexPlugin {
    fn name(&self) -> &'static str {
        "plex"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemDownloadSuccess]
    }

    async fn validate(&self, settings: &PluginSettings) -> anyhow::Result<bool> {
        Ok(settings.has("plextoken") && settings.has("plexserverurl"))
    }


    fn settings_schema(&self) -> Vec<riven_core::plugin::SettingField> {
        use riven_core::plugin::SettingField;
        vec![
            SettingField::new("plextoken", "Plex Token", "password").required(),
            SettingField::new("plexserverurl", "Server URL", "url")
                .required()
                .with_placeholder("http://localhost:32400"),
            SettingField::new("plexlibrarypath", "Library Path", "text")
                .with_default("/mount")
                .with_placeholder("/mount")
                .with_description("Path Plex uses to reference the VFS mount."),
        ]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::MediaItemDownloadSuccess { id, .. } => {
                let plex_token = ctx.require_setting("plextoken")?;
                let plex_url = ctx.require_setting("plexserverurl")?
                    .trim_end_matches('/');
                let library_path = ctx.settings.get_or("plexlibrarypath", "/mount");

                // Get filesystem entries for this media item
                let entries = repo::get_media_entries(&ctx.db_pool, *id).await?;
                if entries.is_empty() {
                    anyhow::bail!("no filesystem entries found for media item {id}");
                }

                // Get library sections
                let sections = get_library_sections(&ctx.http_client, plex_url, plex_token).await?;

                // Refresh matching sections
                for entry in &entries {
                    let dir_path = entry
                        .path
                        .rsplit_once('/')
                        .map(|(dir, _)| dir)
                        .unwrap_or(&entry.path);

                    let full_path = format!("{library_path}{dir_path}");

                    for section in &sections {
                        for location in &section.locations {
                            if full_path.starts_with(&location.path) {
                                refresh_section(
                                    &ctx.http_client,
                                    plex_url,
                                    plex_token,
                                    &section.key,
                                    &full_path,
                                )
                                .await?;
                                tracing::info!(
                                    section = section.key,
                                    path = full_path,
                                    "plex library section refreshed"
                                );
                            }
                        }
                    }
                }

                Ok(HookResponse::Empty)
            }
            _ => Ok(HookResponse::Empty),
        }
    }
}

async fn get_library_sections(
    client: &reqwest::Client,
    plex_url: &str,
    token: &str,
) -> anyhow::Result<Vec<PlexSection>> {
    let url = format!("{plex_url}/library/sections");
    let resp: PlexSectionsResponse = client
        .get(&url)
        .header("x-plex-token", token)
        .header("accept", "application/json")
        .send()
        .await?
        .json()
        .await?;

    Ok(resp.media_container.directory)
}

async fn refresh_section(
    client: &reqwest::Client,
    plex_url: &str,
    token: &str,
    section_key: &str,
    path: &str,
) -> anyhow::Result<()> {
    let encoded_path = urlencoding::encode(path);
    let url = format!(
        "{plex_url}/library/sections/{section_key}/refresh?path={encoded_path}"
    );
    client
        .post(&url)
        .header("x-plex-token", token)
        .header("accept", "application/json")
        .send()
        .await?;
    Ok(())
}

#[derive(Deserialize)]
struct PlexSectionsResponse {
    #[serde(rename = "MediaContainer")]
    media_container: PlexMediaContainer,
}

#[derive(Deserialize)]
struct PlexMediaContainer {
    #[serde(rename = "Directory", default)]
    directory: Vec<PlexSection>,
}

#[derive(Deserialize)]
struct PlexSection {
    key: String,
    #[serde(rename = "Location", default)]
    locations: Vec<PlexLocation>,
}

#[derive(Deserialize)]
struct PlexLocation {
    path: String,
}

// Plex plugin needs urlencoding
mod urlencoding {
    pub fn encode(s: &str) -> String {
        let mut result = String::new();
        for c in s.chars() {
            match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' | '/' => {
                    result.push(c)
                }
                _ => {
                    for b in c.to_string().as_bytes() {
                        result.push_str(&format!("%{b:02X}"));
                    }
                }
            }
        }
        result
    }
}
