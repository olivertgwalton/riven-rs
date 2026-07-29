//! Projection from a parsed release name into the `media_metadata` JSON shape
//! the frontend's `MediaMetadata` type expects.
//!
//! Lives here rather than in `riven-db` or `riven-api` because both produce it
//! — `riven-db` when persisting a stream, `riven-api` when deriving it on the
//! fly for an entry that predates persistence — and the two must agree. They
//! previously held byte-identical copies, which is a UI bug waiting to happen:
//! nothing fails loudly when one side drifts.

/// Map a parsed resolution token onto nominal pixel dimensions.
pub fn resolution_to_dims(resolution: &str) -> (Option<i64>, Option<i64>) {
    match resolution.to_lowercase().trim_end_matches('p') {
        "2160" | "4k" | "uhd" => (Some(3840), Some(2160)),
        "1440" | "2k" | "qhd" => (Some(2560), Some(1440)),
        "1080" | "fhd" => (Some(1920), Some(1080)),
        "720" | "hd" => (Some(1280), Some(720)),
        "480" | "sd" => (Some(854), Some(480)),
        _ => (None, None),
    }
}

/// Parse `filename` and return the `media_metadata` JSON document.
pub fn derive_media_metadata(filename: &str) -> serde_json::Value {
    let parsed = crate::parse(filename);

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

    let is_remux = matches!(parsed.quality.as_deref(), Some("BluRay REMUX" | "REMUX"));
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
        "is_remux": is_remux,
        "is_proper": parsed.proper,
        "is_repack": parsed.repack,
        "container_format": container_formats,
        "data_source": "parsed"
    })
}

#[cfg(test)]
mod tests {
    use super::{derive_media_metadata, resolution_to_dims};

    #[test]
    fn maps_known_resolutions() {
        assert_eq!(resolution_to_dims("1080p"), (Some(1920), Some(1080)));
        assert_eq!(resolution_to_dims("4K"), (Some(3840), Some(2160)));
        assert_eq!(resolution_to_dims("nonsense"), (None, None));
    }

    #[test]
    fn emits_the_frontend_shape() {
        let value =
            derive_media_metadata("Some.Movie.2019.2160p.UHD.BluRay.x265.10bit.HDR-GROUP.mkv");
        assert_eq!(value["video"]["resolution_width"], 3840);
        assert_eq!(value["video"]["bit_depth"], 10);
        assert_eq!(value["data_source"], "parsed");
        assert!(value["subtitle_tracks"].is_array());
    }

    #[test]
    fn marks_remux_releases() {
        let value = derive_media_metadata(
            "Some.Movie.2019.2160p.UHD.BluRay.REMUX.HEVC.TrueHD.7.1-GROUP.mkv",
        );
        assert_eq!(value["quality_source"], "BluRay REMUX");
        assert_eq!(value["is_remux"], true);
    }
}
