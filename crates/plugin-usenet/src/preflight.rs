use riven_core::events::ScrapeRequest;
use riven_core::http::HttpServiceProfile;
use riven_core::plugin::PluginContext;

use crate::nntp::check_nntp_availability;
use crate::setting_bool_default;
use crate::storage::{load_preflight, store_preflight};
use crate::types::{ParsedPreflight, PreflightResult, UsenetFile, UsenetPayload};

const PROFILE: HttpServiceProfile = HttpServiceProfile::new("usenet");

const MEDIA_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "mov", "m4v", "webm", "ts", "m2ts", "wmv", "flv",
];
const ARCHIVE_EXTENSIONS: &[&str] = &["rar", "r00", "r01", "zip", "7z"];
const IGNORED_EXTENSIONS: &[&str] = &[
    "nfo", "txt", "srt", "sub", "idx", "jpg", "jpeg", "png", "sfv", "par2", "url",
];

pub(crate) async fn preflight_payload(
    ctx: &PluginContext,
    hash: &str,
    payload: &UsenetPayload,
) -> anyhow::Result<PreflightResult> {
    if !payload.files.is_empty() {
        return Ok(PreflightResult {
            files: payload.files.clone(),
        });
    }

    if let Some(preflight) = load_preflight(ctx, hash).await {
        return Ok(preflight);
    }

    let nzb = fetch_nzb(ctx, payload).await?;
    let parsed = parse_nzb_preflight(&nzb)?;
    if parsed.result.files.is_empty() {
        anyhow::bail!("NZB did not expose a streamable media file");
    }

    if setting_bool_default(&ctx.settings, "nntpcheckenabled", true) {
        let sample_percent = ctx.settings.get_parsed_or("nntpsamplepercent", 10usize);
        let sample = sample_message_ids(&parsed.message_ids, sample_percent);
        check_nntp_availability(&payload.servers, &sample).await?;
    }

    store_preflight(ctx, hash, &parsed.result).await;
    Ok(parsed.result)
}

async fn fetch_nzb(ctx: &PluginContext, payload: &UsenetPayload) -> anyhow::Result<String> {
    let urls = if payload.nzb_urls.is_empty() {
        payload.nzb_url.iter().cloned().collect::<Vec<_>>()
    } else {
        payload.nzb_urls.clone()
    };

    let mut errors = Vec::new();
    for url in urls {
        match ctx
            .http
            .send_data(PROFILE, Some(format!("nzb:{url}")), |client| {
                client.get(&url)
            })
            .await
        {
            Ok(response) => {
                if let Err(error) = response.error_for_status_ref() {
                    errors.push(format!("{url}: {error}"));
                    continue;
                }
                return response.text();
            }
            Err(error) => errors.push(format!("{url}: {error}")),
        }
    }

    anyhow::bail!("failed to fetch NZB: {}", errors.join("; "))
}

pub(crate) fn best_preflight_file<'a>(
    files: &'a [UsenetFile],
    req: &ScrapeRequest,
) -> Option<&'a UsenetFile> {
    let episode_hint = match (req.season, req.episode) {
        (Some(season), Some(episode)) => Some(format!("s{season:02}e{episode:02}")),
        _ => None,
    };

    files
        .iter()
        .filter(|file| {
            episode_hint
                .as_ref()
                .is_none_or(|hint| file.name.to_ascii_lowercase().contains(hint))
        })
        .max_by_key(|file| file.size.unwrap_or_default())
        .or_else(|| {
            files
                .iter()
                .max_by_key(|file| file.size.unwrap_or_default())
        })
}

fn parse_nzb_preflight(xml: &str) -> anyhow::Result<ParsedPreflight> {
    if !xml.to_ascii_lowercase().contains("<nzb") {
        anyhow::bail!("response is not an NZB document");
    }

    let mut files = Vec::new();
    let mut message_ids = Vec::new();
    let mut cursor = 0usize;
    let lower = xml.to_ascii_lowercase();

    while let Some(relative_start) = lower[cursor..].find("<file") {
        let start = cursor + relative_start;
        let Some(tag_end_relative) = lower[start..].find('>') else {
            break;
        };
        let tag_end = start + tag_end_relative;
        let file_tag = &xml[start..=tag_end];
        let Some(end_relative) = lower[tag_end..].find("</file>") else {
            break;
        };
        let end = tag_end + end_relative;
        let body = &xml[tag_end + 1..end];

        message_ids.extend(segment_message_ids(body));
        let subject = attr_value(file_tag, "subject");
        let name = subject
            .as_deref()
            .and_then(filename_from_subject)
            .or_else(|| subject.as_deref().map(clean_filename))
            .unwrap_or_else(|| "usenet-file".to_string());
        let size = sum_segment_bytes(body);

        if is_media_file(&name) {
            files.push(UsenetFile {
                name,
                size: size.filter(|value| *value > 0),
            });
        }

        cursor = end + "</file>".len();
    }

    if files.is_empty() {
        files = parse_nzb_fallback_files(xml);
    }

    files.sort_by(|a, b| {
        b.size
            .unwrap_or_default()
            .cmp(&a.size.unwrap_or_default())
            .then_with(|| a.name.cmp(&b.name))
    });

    Ok(ParsedPreflight {
        result: PreflightResult { files },
        message_ids,
    })
}

fn parse_nzb_fallback_files(xml: &str) -> Vec<UsenetFile> {
    let mut files = Vec::new();
    for token in xml.split(|ch: char| ch.is_whitespace() || matches!(ch, '"' | '\'' | '<' | '>')) {
        let cleaned = clean_filename(token);
        if is_media_file(&cleaned) {
            files.push(UsenetFile {
                name: cleaned,
                size: None,
            });
        }
    }
    files
}

fn attr_value(tag: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=");
    let start = tag.find(&needle)? + needle.len();
    let quote = tag[start..].chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = start + quote.len_utf8();
    let value_end = tag[value_start..].find(quote)? + value_start;
    Some(decode_xml_entities(&tag[value_start..value_end]))
}

fn sum_segment_bytes(body: &str) -> Option<u64> {
    let mut total = 0u64;
    let mut found = false;
    let mut cursor = 0usize;
    let lower = body.to_ascii_lowercase();

    while let Some(relative_start) = lower[cursor..].find("<segment") {
        let start = cursor + relative_start;
        let Some(tag_end_relative) = lower[start..].find('>') else {
            break;
        };
        let tag_end = start + tag_end_relative;
        let tag = &body[start..=tag_end];
        if let Some(bytes) = attr_value(tag, "bytes").and_then(|value| value.parse::<u64>().ok()) {
            total += bytes;
            found = true;
        }
        cursor = tag_end + 1;
    }

    found.then_some(total)
}

fn segment_message_ids(body: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut cursor = 0usize;
    let lower = body.to_ascii_lowercase();

    while let Some(relative_start) = lower[cursor..].find("<segment") {
        let start = cursor + relative_start;
        let Some(tag_end_relative) = lower[start..].find('>') else {
            break;
        };
        let tag_end = start + tag_end_relative;
        let Some(end_relative) = lower[tag_end..].find("</segment>") else {
            break;
        };
        let end = tag_end + end_relative;
        let value = decode_xml_entities(body[tag_end + 1..end].trim());
        if !value.is_empty() {
            ids.push(value);
        }
        cursor = end + "</segment>".len();
    }

    ids
}

fn filename_from_subject(subject: &str) -> Option<String> {
    let decoded = decode_xml_entities(subject);
    for separator in ['"', '\''] {
        let parts = decoded.split(separator).collect::<Vec<_>>();
        for part in parts {
            let cleaned = clean_filename(part);
            if looks_like_filename(&cleaned) {
                return Some(cleaned);
            }
        }
    }

    decoded
        .split_whitespace()
        .map(clean_filename)
        .find(|part| looks_like_filename(part))
}

fn looks_like_filename(value: &str) -> bool {
    let Some(ext) = value.rsplit('.').next() else {
        return false;
    };
    MEDIA_EXTENSIONS
        .iter()
        .chain(ARCHIVE_EXTENSIONS)
        .chain(IGNORED_EXTENSIONS)
        .any(|candidate| ext.eq_ignore_ascii_case(candidate))
}

fn clean_filename(value: &str) -> String {
    decode_xml_entities(value)
        .trim()
        .trim_matches(['"', '\'', '(', ')', '[', ']'])
        .trim()
        .to_string()
}

fn decode_xml_entities(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn is_media_file(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    if is_ignored_file(&lower) {
        return false;
    }
    MEDIA_EXTENSIONS
        .iter()
        .any(|ext| lower.ends_with(&format!(".{ext}")))
}

fn is_ignored_file(value: &str) -> bool {
    IGNORED_EXTENSIONS
        .iter()
        .any(|ext| value.ends_with(&format!(".{ext}")))
}

fn sample_message_ids(message_ids: &[String], percent: usize) -> Vec<String> {
    if message_ids.len() <= 2 || percent >= 100 {
        return message_ids.to_vec();
    }

    let target = ((message_ids.len() * percent.max(1)) / 100).max(2);
    let last_index = message_ids.len() - 1;
    let mut indexes = vec![0usize, last_index];
    if target > 2 {
        let step = last_index as f64 / (target - 1) as f64;
        for i in 1..target - 1 {
            indexes.push((i as f64 * step).round() as usize);
        }
    }
    indexes.sort_unstable();
    indexes.dedup();
    indexes
        .into_iter()
        .filter_map(|index| message_ids.get(index).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_extracts_media_files_and_sizes() {
        let xml = r#"
            <nzb>
              <file subject='"Movie.2024.1080p.WEB-DL.mkv" yEnc (1/2)'>
                <segments>
                  <segment bytes="100">one@example.com</segment>
                  <segment bytes="150">two@example.com</segment>
                </segments>
              </file>
              <file subject='"Movie.2024.nfo" yEnc'>
                <segments><segment bytes="10">nfo@example.com</segment></segments>
              </file>
            </nzb>
        "#;

        let parsed = parse_nzb_preflight(xml).unwrap();
        assert_eq!(parsed.result.files.len(), 1);
        assert_eq!(parsed.result.files[0].name, "Movie.2024.1080p.WEB-DL.mkv");
        assert_eq!(parsed.result.files[0].size, Some(250));
        assert_eq!(parsed.message_ids.len(), 3);
    }

    #[test]
    fn filename_from_subject_reads_yenc_name() {
        assert_eq!(
            filename_from_subject(r#""Show.S01E02.2160p.WEB-DL.mkv" yEnc (1/42)"#),
            Some("Show.S01E02.2160p.WEB-DL.mkv".to_string())
        );
    }

    #[test]
    fn message_id_sampling_keeps_edges() {
        let ids = (0..10)
            .map(|i| format!("{i}@example.com"))
            .collect::<Vec<_>>();
        let sample = sample_message_ids(&ids, 30);
        assert_eq!(sample.first(), ids.first());
        assert_eq!(sample.last(), ids.last());
    }
}
