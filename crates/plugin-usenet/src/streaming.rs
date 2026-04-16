use riven_core::plugin::PluginContext;

use crate::storage::store_payload;
use crate::types::UsenetPayload;

const DEFAULT_RIVEN_STREAM_BASE_URL: &str = "http://127.0.0.1:8080";

pub(crate) async fn build_stream_url(
    ctx: &PluginContext,
    hash: &str,
    payload: &UsenetPayload,
    stream_base_url: Option<&str>,
) -> anyhow::Result<String> {
    if payload.servers.is_empty() {
        anyhow::bail!("No NNTP server URLs configured for this Usenet stream");
    }

    let stable_hash = if hash.is_empty() {
        payload.stable_id()?
    } else {
        hash.to_string()
    };
    store_payload(ctx, &stable_hash, payload).await;

    let base = stream_base_url
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(DEFAULT_RIVEN_STREAM_BASE_URL)
        .trim()
        .trim_end_matches('/');
    let filename =
        url::form_urlencoded::byte_serialize(payload.filename.as_bytes()).collect::<String>();

    Ok(format!("{base}/stream/usenet/{stable_hash}/{filename}"))
}
