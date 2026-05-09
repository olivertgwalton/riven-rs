//! SubDL subtitle plugin.
//!
//! Subscribes to `MediaItemDownloadSuccess`. When a download lands, queries
//! SubDL for matching subtitles in the configured languages, picks the best
//! match per language, downloads the ZIP, extracts the inner `.srt`, and
//! writes a subtitle filesystem entry next to the media file. The VFS picks
//! up subtitle entries automatically (`{name}.{lang}.srt` siblings).

use async_trait::async_trait;
use riven_core::events::{DownloadSuccessInfo, EventType, HookResponse};
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::{Plugin, PluginContext, SettingField};
use riven_core::register_plugin;
use riven_core::settings::PluginSettings;
use riven_core::types::MediaItemType;
use riven_db::entities::FileSystemEntry;
use serde::Deserialize;
use std::time::Duration;

const DEFAULT_BASE_URL: &str = "https://api.subdl.com/api/v1/";
const DEFAULT_DL_BASE: &str = "https://dl.subdl.com";
const DEFAULT_LANGUAGES: &str = "en";

pub(crate) const PROFILE: HttpServiceProfile =
    HttpServiceProfile::new("subdl").with_rate_limit(5, Duration::from_secs(1));

#[derive(Default)]
pub struct SubdlPlugin;

register_plugin!(SubdlPlugin);

#[async_trait]
impl Plugin for SubdlPlugin {
    fn name(&self) -> &'static str {
        "subdl"
    }

    fn subscribed_events(&self) -> &[EventType] {
        &[EventType::MediaItemDownloadSuccess]
    }

    async fn validate(
        &self,
        settings: &PluginSettings,
        http: &riven_core::http::HttpClient,
    ) -> anyhow::Result<bool> {
        let api_key = match settings.get("apikey") {
            Some(k) if !k.is_empty() => k.to_string(),
            _ => return Ok(false),
        };
        // SubDL has no /me endpoint; ping a known TMDB id with a tiny page size.
        let url = format!("{DEFAULT_BASE_URL}subtitles?api_key={api_key}&tmdb_id=27205&type=movie&subs_per_page=1");
        let resp: SearchResponse = match http
            .get_json(PROFILE, url.clone(), |client| client.get(&url))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(error = %e, "subdl validate failed");
                return Ok(false);
            }
        };
        Ok(resp.status)
    }

    fn settings_schema(&self) -> Vec<SettingField> {
        vec![
            SettingField::new("apikey", "API Key", "password").required(),
            SettingField::new("languages", "Languages", "text")
                .with_default(DEFAULT_LANGUAGES)
                .with_placeholder("en, es")
                .with_description(
                    "Comma-separated list of subtitle languages to download (e.g. en, es).",
                ),
        ]
    }

    async fn on_download_success(
        &self,
        info: &DownloadSuccessInfo<'_>,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        let api_key = ctx.require_setting("apikey")?.to_string();
        let languages: Vec<String> =
            parse_languages(&ctx.settings.get_or("languages", DEFAULT_LANGUAGES));
        if languages.is_empty() {
            return Ok(HookResponse::Empty);
        }

        // Resolve TMDB / IMDB ids and (for episodes) season/episode numbers
        // from the media item — the download event only carries movie ids.
        let meta = match resolve_item_metadata(&ctx.db_pool, info).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                tracing::debug!(item_id = info.id, "subdl: unsupported item type, skipping");
                return Ok(HookResponse::Empty);
            }
            Err(e) => {
                tracing::warn!(item_id = info.id, error = %e, "subdl: failed to resolve item metadata");
                return Ok(HookResponse::Empty);
            }
        };

        if meta.tmdb_id.is_none() && meta.imdb_id.is_none() {
            tracing::debug!(item_id = info.id, "subdl: item has no TMDB/IMDB id, skipping");
            return Ok(HookResponse::Empty);
        }

        // Find the media filesystem entry so we know where to put the
        // subtitle file. Without it, the subtitle has nowhere to live in the
        // VFS — the show might have multiple recently-downloaded episodes
        // racing through this hook, so we look up the specific item.
        let media_entry =
            match find_media_entry(&ctx.db_pool, info.id).await {
                Ok(Some(e)) => e,
                Ok(None) => {
                    tracing::debug!(item_id = info.id, "subdl: no media filesystem entry yet, skipping");
                    return Ok(HookResponse::Empty);
                }
                Err(e) => {
                    tracing::warn!(item_id = info.id, error = %e, "subdl: failed to look up media entry");
                    return Ok(HookResponse::Empty);
                }
            };

        let results = match search_subtitles(&ctx.http, &api_key, &meta, &languages).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(item_id = info.id, error = %e, "subdl: search failed");
                return Ok(HookResponse::Empty);
            }
        };

        // Best-of-language selection: first match per language. For TV we
        // additionally require the entry's season/episode to match (when the
        // API returns those fields) to filter season-pack-but-wrong-episode
        // hits.
        use std::collections::HashMap;
        let mut best_per_lang: HashMap<String, SubtitleEntry> = HashMap::new();
        for sub in results {
            if let (Some(s), Some(e)) = (meta.season_number, meta.episode_number) {
                if let (Some(ss), Some(ee)) = (sub.season, sub.episode) {
                    if ss != s || ee != e {
                        continue;
                    }
                }
            }
            let key = sub.lang.to_ascii_lowercase();
            best_per_lang.entry(key).or_insert(sub);
        }

        let mut saved = 0usize;
        for (language, sub) in best_per_lang {
            match download_and_save(
                &ctx.http,
                &ctx.db_pool,
                info.id,
                &media_entry,
                &language,
                &sub,
            )
            .await
            {
                Ok(()) => saved += 1,
                Err(e) => tracing::warn!(
                    item_id = info.id,
                    lang = %language,
                    error = %e,
                    "subdl: failed to save subtitle"
                ),
            }
        }

        if saved > 0 {
            tracing::info!(item_id = info.id, count = saved, "subdl: saved subtitles");
        }
        Ok(HookResponse::Empty)
    }
}

fn parse_languages(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

struct ItemMetadata {
    media_type: SubMediaType,
    tmdb_id: Option<String>,
    imdb_id: Option<String>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
}

#[derive(Clone, Copy)]
enum SubMediaType {
    Movie,
    Tv,
}

impl SubMediaType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Movie => "movie",
            Self::Tv => "tv",
        }
    }
}

async fn resolve_item_metadata(
    pool: &sqlx::PgPool,
    info: &DownloadSuccessInfo<'_>,
) -> anyhow::Result<Option<ItemMetadata>> {
    let item = match riven_db::repo::get_media_item(pool, info.id).await? {
        Some(i) => i,
        None => return Ok(None),
    };
    match item.item_type {
        MediaItemType::Movie => Ok(Some(ItemMetadata {
            media_type: SubMediaType::Movie,
            tmdb_id: item.tmdb_id,
            imdb_id: item.imdb_id,
            season_number: None,
            episode_number: None,
        })),
        MediaItemType::Episode => {
            let hierarchy = riven_db::repo::get_media_item_hierarchy(pool, info.id).await?;
            let Some(hierarchy) = hierarchy else {
                return Ok(None);
            };
            // Episodes carry their own number; season comes via the join.
            Ok(Some(ItemMetadata {
                media_type: SubMediaType::Tv,
                tmdb_id: hierarchy
                    .item
                    .tmdb_id
                    .clone()
                    .or_else(|| info.tmdb_id.map(str::to_string)),
                imdb_id: hierarchy
                    .resolved_show_imdb_id
                    .clone()
                    .or(hierarchy.item.imdb_id.clone()),
                season_number: hierarchy.resolved_season_number.or(item.season_number),
                episode_number: hierarchy.item.episode_number,
            }))
        }
        // Shows/seasons don't directly produce a downloaded file — they
        // expand into per-episode downloads which fire this hook each.
        _ => Ok(None),
    }
}

async fn find_media_entry(
    pool: &sqlx::PgPool,
    item_id: i64,
) -> anyhow::Result<Option<FileSystemEntry>> {
    let entries = riven_db::repo::get_filesystem_entries(pool, item_id).await?;
    Ok(entries
        .into_iter()
        .find(|e| matches!(e.entry_type, riven_core::types::FileSystemEntryType::Media)))
}

async fn search_subtitles(
    http: &riven_core::http::HttpClient,
    api_key: &str,
    meta: &ItemMetadata,
    languages: &[String],
) -> anyhow::Result<Vec<SubtitleEntry>> {
    let mut params = vec![
        ("api_key".to_string(), api_key.to_string()),
        ("type".to_string(), meta.media_type.as_str().to_string()),
        ("subs_per_page".to_string(), "30".to_string()),
        ("languages".to_string(), languages.join(",")),
    ];
    if let Some(tmdb) = &meta.tmdb_id {
        params.push(("tmdb_id".to_string(), tmdb.clone()));
    } else if let Some(imdb) = &meta.imdb_id {
        params.push(("imdb_id".to_string(), imdb.clone()));
    }
    if let Some(s) = meta.season_number {
        params.push(("season_number".to_string(), s.to_string()));
    }
    if let Some(e) = meta.episode_number {
        params.push(("episode_number".to_string(), e.to_string()));
    }

    let qs = serde_urlencoded::to_string(&params)?;
    let url = format!("{DEFAULT_BASE_URL}subtitles?{qs}");

    let resp: SearchResponse = http
        .get_json(PROFILE, url.clone(), |client| client.get(&url))
        .await?;

    if !resp.status {
        return Err(anyhow::anyhow!(
            resp.error.unwrap_or_else(|| "subdl: unknown error".to_string())
        ));
    }
    Ok(resp.subtitles.unwrap_or_default())
}

async fn download_and_save(
    http: &riven_core::http::HttpClient,
    pool: &sqlx::PgPool,
    item_id: i64,
    media_entry: &FileSystemEntry,
    language: &str,
    sub: &SubtitleEntry,
) -> anyhow::Result<()> {
    let url = if sub.url.starts_with("http") {
        sub.url.clone()
    } else if sub.url.starts_with('/') {
        format!("{DEFAULT_DL_BASE}{}", sub.url)
    } else {
        format!("{DEFAULT_DL_BASE}/{}", sub.url)
    };

    let resp = http
        .send(PROFILE, |client| client.get(&url))
        .await?
        .error_for_status()?;
    let bytes = resp.bytes().await?;

    let srt = extract_srt_from_zip(&bytes)?
        .ok_or_else(|| anyhow::anyhow!("no .srt found in downloaded ZIP"))?;

    // Sit the subtitle next to the media file: strip the trailing extension
    // and append `.{lang}.srt`. Falls back to a sane default if the path has
    // no extension.
    let subtitle_path = subtitle_path_from(&media_entry.path, language);

    riven_db::repo::upsert_subtitle_entry(
        pool,
        item_id,
        &subtitle_path,
        language,
        &srt,
        "subdl",
        Some(&sub.url),
        media_entry.original_filename.as_deref(),
    )
    .await?;
    Ok(())
}

fn subtitle_path_from(media_path: &str, language: &str) -> String {
    // Find the last segment, strip its extension if any.
    let (dir, file) = match media_path.rsplit_once('/') {
        Some((d, f)) => (d, f),
        None => ("", media_path),
    };
    let stem = file.rsplit_once('.').map(|(s, _)| s).unwrap_or(file);
    if dir.is_empty() {
        format!("{stem}.{language}.srt")
    } else {
        format!("{dir}/{stem}.{language}.srt")
    }
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    #[serde(default)]
    status: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    subtitles: Option<Vec<SubtitleEntry>>,
}

#[derive(Debug, Deserialize, Clone)]
struct SubtitleEntry {
    lang: String,
    url: String,
    #[serde(default)]
    season: Option<i32>,
    #[serde(default)]
    episode: Option<i32>,
}

/// Extract the first `.srt` file from a ZIP archive. Supports stored (0) and
/// deflate (8) compression methods, which is what SubDL produces in practice.
fn extract_srt_from_zip(buf: &[u8]) -> anyhow::Result<Option<String>> {
    use std::io::Cursor;
    use std::io::Read;
    let mut archive = zip::ZipArchive::new(Cursor::new(buf))?;
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let name = file.name().to_string();
        if !name.to_ascii_lowercase().ends_with(".srt") {
            continue;
        }
        // Subtitle files come in many encodings; accept valid UTF-8, fall
        // back to lossy conversion rather than dropping the subtitle.
        let mut bytes = Vec::with_capacity(file.size() as usize);
        file.read_to_end(&mut bytes)?;
        let content = String::from_utf8(bytes)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
        let _ = name;
        return Ok(Some(content));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_languages_strips_and_lowercases() {
        assert_eq!(parse_languages(" EN, Es ,fr"), vec!["en", "es", "fr"]);
        assert_eq!(parse_languages(""), Vec::<String>::new());
    }

    #[test]
    fn subtitle_path_replaces_extension() {
        assert_eq!(
            subtitle_path_from("/movies/Foo (2020)/Foo.mkv", "en"),
            "/movies/Foo (2020)/Foo.en.srt"
        );
        assert_eq!(subtitle_path_from("Foo.mp4", "es"), "Foo.es.srt");
        assert_eq!(
            subtitle_path_from("/shows/Bar/Season 01/Bar S01E02.mkv", "en"),
            "/shows/Bar/Season 01/Bar S01E02.en.srt"
        );
    }
}
