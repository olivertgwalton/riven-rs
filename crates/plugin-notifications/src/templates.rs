use std::collections::HashMap;

use riven_core::types::MediaItemType;

use super::NotificationPayload;
use super::dispatch::format_duration;

/// Which set of custom templates applies to a payload's `item_type`.
pub(crate) enum TemplateCategory {
    Movie,
    Show,
}

pub(crate) fn template_category(item_type: MediaItemType) -> TemplateCategory {
    match item_type {
        MediaItemType::Movie => TemplateCategory::Movie,
        MediaItemType::Show | MediaItemType::Season | MediaItemType::Episode => {
            TemplateCategory::Show
        }
    }
}

/// Build the `{{variable}}` → value map for a payload. Every variable listed
/// in the plugin's settings schema has an entry here, even when the
/// underlying field is absent — those substitute to an empty string rather
/// than leaving the literal `{{placeholder}}` in the rendered text.
pub(crate) fn template_variables(payload: &NotificationPayload) -> HashMap<&'static str, String> {
    let tmdb_link = payload.tmdb_id.as_deref().map(|id| {
        let path = if payload.item_type == MediaItemType::Movie {
            "movie"
        } else {
            "tv"
        };
        format!("https://www.themoviedb.org/{path}/{id}")
    });
    let imdb_link = payload
        .imdb_id
        .as_deref()
        .map(|id| format!("https://www.imdb.com/title/{id}"));
    let tvdb_link = payload
        .tvdb_slug
        .as_deref()
        .map(|slug| format!("https://thetvdb.com/series/{slug}"));

    HashMap::from([
        ("title", payload.title.clone()),
        ("full_title", payload.full_title.clone()),
        (
            "year",
            payload.year.map(|y| y.to_string()).unwrap_or_default(),
        ),
        (
            "season",
            payload.season.map(|s| s.to_string()).unwrap_or_default(),
        ),
        (
            "episode",
            payload.episode.map(|e| e.to_string()).unwrap_or_default(),
        ),
        (
            "episode_title",
            payload.episode_title.clone().unwrap_or_default(),
        ),
        ("quality", payload.quality.clone().unwrap_or_default()),
        ("resolution", payload.resolution.clone().unwrap_or_default()),
        (
            "release_group",
            payload.release_group.clone().unwrap_or_default(),
        ),
        ("downloader", payload.downloader.clone()),
        ("provider", payload.provider.clone().unwrap_or_default()),
        (
            "rating",
            payload
                .rating
                .map(|r| format!("{r:.1}"))
                .unwrap_or_default(),
        ),
        ("overview", payload.overview.clone().unwrap_or_default()),
        ("poster", payload.poster_path.clone().unwrap_or_default()),
        ("duration", format_duration(payload.duration_seconds)),
        ("tmdb_link", tmdb_link.unwrap_or_default()),
        ("imdb_link", imdb_link.unwrap_or_default()),
        ("tvdb_link", tvdb_link.unwrap_or_default()),
    ])
}

/// Plain `{{variable}}` substitution — no conditionals or loops. A variable
/// with no value (see [`template_variables`]) substitutes to an empty
/// string rather than being left as a literal placeholder or removing the
/// surrounding line, so templates that reference optional fields should
/// account for that (e.g. avoid a lone "Group: {{release_group}}" line if
/// the release group is commonly absent).
pub(crate) fn render_template(template: &str, vars: &HashMap<&'static str, String>) -> String {
    let mut result = template.to_string();
    for (key, value) in vars {
        result = result.replace(&format!("{{{{{key}}}}}"), value);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> NotificationPayload {
        NotificationPayload {
            event: "riven.media-item.download.success".to_string(),
            title: "Movie".to_string(),
            full_title: "Movie".to_string(),
            item_type: MediaItemType::Movie,
            year: Some(2024),
            imdb_id: Some("tt123".to_string()),
            tmdb_id: Some("456".to_string()),
            tvdb_id: None,
            poster_path: Some("https://image.test/poster.jpg".to_string()),
            downloader: "stremthru".to_string(),
            provider: Some("realdebrid".to_string()),
            duration_seconds: 125.0,
            timestamp: "2026-04-16T12:00:00Z".to_string(),
            is_anime: false,
            rating: Some(8.25),
            overview: Some("Short overview".to_string()),
            tvdb_slug: None,
            resolution: Some("1080p".to_string()),
            quality: Some("WEB-DL".to_string()),
            release_group: Some("GROUP".to_string()),
            season: None,
            episode: None,
            episode_title: None,
        }
    }

    #[test]
    fn renders_known_variables_and_builds_external_links() {
        let vars = template_variables(&payload());
        let rendered = render_template(
            "{{title}} ({{year}}) {{quality}} {{resolution}} by {{release_group}} via {{downloader}}/{{provider}} — {{tmdb_link}} {{imdb_link}}",
            &vars,
        );
        assert_eq!(
            rendered,
            "Movie (2024) WEB-DL 1080p by GROUP via stremthru/realdebrid — https://www.themoviedb.org/movie/456 https://www.imdb.com/title/tt123"
        );
    }

    #[test]
    fn missing_optional_variables_render_as_empty_not_left_as_placeholders() {
        let mut data = payload();
        data.tmdb_id = None;
        data.imdb_id = None;
        data.release_group = None;
        let vars = template_variables(&data);
        let rendered = render_template("[{{tmdb_link}}][{{imdb_link}}][{{release_group}}]", &vars);
        assert_eq!(rendered, "[][][]");
    }

    #[test]
    fn tmdb_link_uses_tv_path_for_non_movie_items() {
        let mut data = payload();
        data.item_type = MediaItemType::Episode;
        data.season = Some(1);
        data.episode = Some(3);
        data.episode_title = Some("Pilot".to_string());
        let vars = template_variables(&data);
        let rendered = render_template(
            "{{title}} S{{season}}E{{episode}} {{episode_title}} {{tmdb_link}}",
            &vars,
        );
        assert_eq!(
            rendered,
            "Movie S1E3 Pilot https://www.themoviedb.org/tv/456"
        );
    }

    #[test]
    fn movie_and_show_item_types_select_the_right_category() {
        assert!(matches!(
            template_category(MediaItemType::Movie),
            TemplateCategory::Movie
        ));
        for item_type in [
            MediaItemType::Show,
            MediaItemType::Season,
            MediaItemType::Episode,
        ] {
            assert!(matches!(
                template_category(item_type),
                TemplateCategory::Show
            ));
        }
    }
}
