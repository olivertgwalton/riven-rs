use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use riven_core::events::ScrapeRequest;
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::PluginContext;
use riven_core::types::{MediaItemType, ScrapeResponse, ScrapeStream};
use serde::Deserialize;
use url::Url;

use crate::first_line;
use crate::preflight::{best_preflight_file, preflight_payload};
use crate::storage::store_payload;
use crate::types::{UsenetFile, UsenetPayload};

const PROFILE: HttpServiceProfile = HttpServiceProfile::new("usenet");
const URI_COMPONENT_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

#[derive(Debug, Deserialize)]
struct DiscoveryResponse {
    #[serde(default)]
    streams: Vec<DiscoveryStream>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveryStream {
    name: Option<String>,
    title: Option<String>,
    #[serde(default, alias = "description")]
    description: Option<String>,
    url: Option<String>,
    #[serde(rename = "externalUrl")]
    external_url: Option<String>,
    #[serde(rename = "nzbUrl")]
    nzb_url: Option<String>,
    #[serde(rename = "nzbUrls", default)]
    nzb_urls: Vec<String>,
    #[serde(default)]
    servers: Vec<String>,
    #[serde(default)]
    behavior_hints: BehaviorHints,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BehaviorHints {
    filename: Option<String>,
    video_size: Option<u64>,
    #[serde(default)]
    binge_group: Option<String>,
}

pub(crate) async fn discover_streams(
    ctx: &PluginContext,
    source_url: &str,
    fallback_servers: &[String],
    req: &ScrapeRequest<'_>,
    preflight_on_scrape: bool,
) -> anyhow::Result<ScrapeResponse> {
    let stream_url = stream_manifest_url(source_url, req)?;
    let response = ctx
        .http
        .send_data(PROFILE, Some(stream_url.clone()), |client| {
            client.get(&stream_url)
        })
        .await?;
    response.error_for_status_ref()?;
    let response: DiscoveryResponse = response.json()?;

    let mut out = ScrapeResponse::new();
    for stream in response.streams {
        let Some(payload) = payload_from_stream(&stream, fallback_servers) else {
            continue;
        };

        let hash = payload.stable_id()?;
        let mut title = stream
            .title
            .as_deref()
            .or(stream.name.as_deref())
            .or(stream.description.as_deref())
            .and_then(first_line)
            .unwrap_or(&payload.filename)
            .to_string();

        let mut payload = payload;
        if preflight_on_scrape {
            match preflight_payload(ctx, &hash, &payload).await {
                Ok(preflight) => {
                    if let Some(file) = best_preflight_file(&preflight.files, req) {
                        title = file.name.clone();
                        payload.filename = file.name.clone();
                        payload.video_size = file.size.or(payload.video_size);
                    }
                    payload.files = preflight.files;
                }
                Err(error) => {
                    tracing::debug!(hash, error = %error, "usenet scrape preflight failed");
                }
            }
        }

        store_payload(ctx, &hash, &payload).await;
        out.insert(hash, ScrapeStream::with_magnet(title, payload.to_magnet()?));
    }

    Ok(out)
}

fn payload_from_stream(
    stream: &DiscoveryStream,
    fallback_servers: &[String],
) -> Option<UsenetPayload> {
    let mut nzb_urls = stream.nzb_urls.clone();
    if let Some(nzb_url) = stream.nzb_url.clone() {
        nzb_urls.insert(0, nzb_url);
    }
    if nzb_urls.is_empty() {
        let nzb_url = stream.url.as_deref().or(stream.external_url.as_deref())?;
        if !looks_like_nzb_url(nzb_url) {
            return None;
        }
        nzb_urls.push(nzb_url.to_string());
    }
    let mut seen_nzb_urls = Vec::new();
    nzb_urls.retain(|url| {
        if seen_nzb_urls.contains(url) {
            false
        } else {
            seen_nzb_urls.push(url.clone());
            true
        }
    });

    let mut servers = if stream.servers.is_empty() {
        fallback_servers.to_vec()
    } else {
        stream.servers.clone()
    };
    if servers.is_empty() {
        if let Some(group) = &stream.behavior_hints.binge_group {
            servers.extend(crate::split_setting(group));
        }
    }

    let filename = stream
        .behavior_hints
        .filename
        .clone()
        .or_else(|| stream.title.clone())
        .or_else(|| stream.description.clone())
        .or_else(|| stream.name.clone())
        .and_then(|value| first_line(&value).map(ToOwned::to_owned))
        .unwrap_or_else(|| "usenet-stream.mkv".to_string());

    Some(UsenetPayload {
        nzb_url: nzb_urls.first().cloned(),
        nzb_urls,
        servers,
        filename,
        video_size: stream.behavior_hints.video_size,
        files: Vec::<UsenetFile>::new(),
    })
}

fn stream_manifest_url(source_url: &str, req: &ScrapeRequest) -> anyhow::Result<String> {
    let mut url = Url::parse(source_url)?;
    let mut path = url.path().trim_end_matches('/').to_string();
    if path.ends_with("/manifest.json") {
        path.truncate(path.len() - "/manifest.json".len());
    }

    let stream_type = match req.item_type {
        MediaItemType::Movie => "movie",
        MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode => "series",
    };
    let id = match req.item_type {
        MediaItemType::Movie => req
            .imdb_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| req.id.to_string()),
        MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode => req
            .imdb_id
            .map(|id| format!("{id}:{}:{}", req.season_or_1(), req.episode_or_1()))
            .unwrap_or_else(|| format!("{}:{}:{}", req.id, req.season_or_1(), req.episode_or_1())),
    };

    let resource = utf8_percent_encode("stream", URI_COMPONENT_ENCODE_SET);
    let stream_type = utf8_percent_encode(stream_type, URI_COMPONENT_ENCODE_SET);
    let id = utf8_percent_encode(&id, URI_COMPONENT_ENCODE_SET);
    path.push_str(&format!("/{resource}/{stream_type}/{id}.json"));
    url.set_path(&path);
    Ok(url.to_string())
}

fn looks_like_nzb_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.contains(".nzb") || lower.contains("getnzb") || lower.contains("nzb")
}
