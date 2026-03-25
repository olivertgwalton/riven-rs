pub(super) fn resolution_to_dims(res: &str) -> (Option<i64>, Option<i64>) {
    match res.to_lowercase().trim_end_matches('p') {
        "2160" | "4k" | "uhd" => (Some(3840), Some(2160)),
        "1440" | "2k" | "qhd" => (Some(2560), Some(1440)),
        "1080" | "fhd" => (Some(1920), Some(1080)),
        "720" | "hd" => (Some(1280), Some(720)),
        "480" | "sd" => (Some(854), Some(480)),
        _ => (None, None),
    }
}

/// Parse `original_filename` using riven-rank and return a `media_metadata` JSON value
/// matching the shape the frontend's `MediaMetadata` type expects.
pub fn derive_media_metadata(filename: &str) -> serde_json::Value {
    let parsed = riven_rank::parse(filename);

    let (width, height) = resolution_to_dims(&parsed.resolution);
    let hdr_type = parsed.hdr.first().cloned();
    let bit_depth: Option<i64> = parsed.bit_depth.as_deref().and_then(|b| {
        b.trim_end_matches("-bit")
            .trim_end_matches("bit")
            .trim()
            .parse()
            .ok()
    });

    let audio_tracks: Vec<serde_json::Value> = parsed
        .audio
        .iter()
        .map(|codec| serde_json::json!({ "codec": codec }))
        .collect();

    let container_formats: Vec<String> = parsed.container.into_iter().collect();

    serde_json::json!({
        "filename": filename,
        "parsed_title": parsed.parsed_title,
        "year": parsed.year,
        "video": {
            "codec": parsed.codec,
            "resolution_width": width,
            "resolution_height": height,
            "bit_depth": bit_depth,
            "hdr_type": hdr_type,
            "frame_rate": null
        },
        "audio_tracks": audio_tracks,
        "subtitle_tracks": [],
        "quality_source": parsed.quality,
        "bitrate": null,
        "duration": null,
        "is_remux": false,
        "is_proper": parsed.proper,
        "is_repack": parsed.repack,
        "container_format": container_formats,
        "data_source": "parsed"
    })
}
