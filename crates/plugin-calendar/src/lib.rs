use async_graphql::{Context, Object, Result as GqlResult, SimpleObject};
use async_trait::async_trait;
use redis::AsyncCommands;

use riven_core::events::{EventType, HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginContext};
use riven_core::register_plugin;
use riven_core::types::{MediaItemState, MediaItemType};
use riven_db::entities::MediaItem;
use riven_db::repo;

/// Redis key under which the generated iCal feed is stored.
/// The API layer (or any other consumer) can read this key to serve the feed.
///
/// Environment: `GET riven:calendar:ical` → `text/calendar` content.
pub const CALENDAR_REDIS_KEY: &str = "riven:calendar:ical";

/// Maximum number of upcoming items included in the calendar feed.
const CALENDAR_ITEM_LIMIT: i64 = 1000;

#[derive(Default)]
pub struct CalendarPlugin;

register_plugin!(CalendarPlugin);

#[async_trait]
impl Plugin for CalendarPlugin {
    fn name(&self) -> &'static str {
        "calendar"
    }

    fn show_in_settings(&self) -> bool {
        false
    }

    fn subscribed_events(&self) -> &[EventType] {
        // Regenerate on startup and whenever item metadata is refreshed,
        // which is when air dates are first written or updated.
        &[EventType::CoreStarted, EventType::MediaItemIndexSuccess]
    }

    async fn handle_event(
        &self,
        event: &RivenEvent,
        ctx: &PluginContext,
    ) -> anyhow::Result<HookResponse> {
        match event {
            RivenEvent::CoreStarted | RivenEvent::MediaItemIndexSuccess { .. } => {
                regenerate_feed(ctx).await?;
            }
            _ => {}
        }
        Ok(HookResponse::Empty)
    }
}

async fn regenerate_feed(ctx: &PluginContext) -> anyhow::Result<()> {
    let items = repo::get_upcoming_unreleased(&ctx.db_pool, CALENDAR_ITEM_LIMIT).await?;

    let ical = build_ical(&items);

    let mut redis = ctx.redis.clone();
    redis
        .set::<_, _, ()>(CALENDAR_REDIS_KEY, ical.as_bytes())
        .await?;

    tracing::info!("calendar feed regenerated count={}", items.len());

    Ok(())
}

fn build_ical(items: &[MediaItem]) -> String {
    let mut out = String::with_capacity(items.len() * 256);

    out.push_str("BEGIN:VCALENDAR\r\n");
    out.push_str("VERSION:2.0\r\n");
    out.push_str("PRODID:-//Riven//Upcoming Releases//EN\r\n");
    out.push_str("CALSCALE:GREGORIAN\r\n");
    out.push_str("METHOD:PUBLISH\r\n");
    out.push_str("X-WR-CALNAME:Riven Upcoming Releases\r\n");
    out.push_str("X-WR-TIMEZONE:UTC\r\n");

    for item in items {
        let Some(air_date) = item.aired_at else {
            continue;
        };

        let dtstart = air_date.format("%Y%m%d").to_string();
        let uid = format!("riven-{}@riven", item.id);

        let summary = build_summary(item);
        let description = build_description(item);

        out.push_str("BEGIN:VEVENT\r\n");
        out.push_str(&format!("UID:{uid}\r\n"));
        out.push_str(&format!("DTSTART;VALUE=DATE:{dtstart}\r\n"));
        out.push_str(&format!("DTEND;VALUE=DATE:{dtstart}\r\n"));
        out.push_str(&fold_line(&format!("SUMMARY:{}", escape_text(&summary))));
        out.push_str(&fold_line(&format!("DESCRIPTION:{description}")));
        out.push_str("END:VEVENT\r\n");
    }

    out.push_str("END:VCALENDAR\r\n");
    out
}

fn build_summary(item: &MediaItem) -> String {
    match item.item_type {
        MediaItemType::Episode => {
            let s = item
                .season_number
                .map(|n| format!("S{n:02}"))
                .unwrap_or_default();
            let e = item
                .episode_number
                .map(|n| format!("E{n:02}"))
                .unwrap_or_default();
            let show = item.full_title.as_deref().unwrap_or(&item.title);
            format!("{show} - {s}{e}: {}", item.title)
        }
        MediaItemType::Movie => {
            let year = item.year.map(|y| format!(" ({y})")).unwrap_or_default();
            format!("{}{year}", item.title)
        }
        _ => item.title.clone(),
    }
}

fn build_description(item: &MediaItem) -> String {
    let mut parts: Vec<String> = vec![format!("Type: {:?}", item.item_type)];

    if let Some(ref id) = item.imdb_id {
        parts.push(format!("IMDB: {id}"));
    }
    if let Some(ref id) = item.tmdb_id {
        parts.push(format!("TMDB: {id}"));
    }
    if let Some(ref id) = item.tvdb_id {
        parts.push(format!("TVDB: {id}"));
    }

    // Use iCal escaped newlines so clients render each field on its own line.
    parts.join("\\n")
}

/// Escape special characters per RFC 5545 §3.3.11.
fn escape_text(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace(';', "\\;")
        .replace(',', "\\,")
        .replace('\n', "\\n")
        .replace('\r', "")
}

/// Fold long iCal lines at 75 octets per RFC 5545 §3.1.
/// Continuation lines begin with a single SPACE.
fn fold_line(line: &str) -> String {
    const MAX: usize = 75;

    if line.len() <= MAX {
        return format!("{line}\r\n");
    }

    let mut out = String::with_capacity(line.len() + line.len() / MAX * 3);
    let bytes = line.as_bytes();
    let mut pos = 0;

    while pos < bytes.len() {
        let end = (pos + MAX).min(bytes.len());

        // Walk back to a UTF-8 char boundary so we never split a multi-byte sequence.
        let mut end = end;
        while !line.is_char_boundary(end) {
            end -= 1;
        }

        out.push_str(&line[pos..end]);
        out.push_str("\r\n");
        pos = end;

        if pos < bytes.len() {
            out.push(' '); // RFC 5545 continuation marker
        }
    }

    out
}

// ── GraphQL types and query ──

#[derive(SimpleObject)]
pub struct CalendarEntry {
    pub item_id: i64,
    pub show_title: String,
    pub item_type: String,
    pub aired_at: Option<String>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    pub tmdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub last_state: String,
}

#[derive(Default)]
pub struct CalendarQuery;

#[Object]
impl CalendarQuery {
    /// Get upcoming unreleased items (calendar feed), with show title resolved in a single query.
    async fn calendar(
        &self,
        ctx: &Context<'_>,
        limit: Option<i64>,
    ) -> GqlResult<Vec<CalendarEntry>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let rows = repo::get_calendar_entries(pool, limit.unwrap_or(100)).await?;

        let entries = rows
            .into_iter()
            .map(|r| {
                let item_type = match r.item_type {
                    MediaItemType::Movie => "movie",
                    MediaItemType::Show => "show",
                    MediaItemType::Season => "season",
                    MediaItemType::Episode => "episode",
                };
                let last_state = match r.state {
                    MediaItemState::Completed => "Completed",
                    MediaItemState::Indexed => "Indexed",
                    MediaItemState::Scraped => "Scraped",
                    MediaItemState::Ongoing => "Ongoing",
                    MediaItemState::PartiallyCompleted => "PartiallyCompleted",
                    MediaItemState::Unreleased => "Unreleased",
                    MediaItemState::Paused => "Paused",
                    MediaItemState::Failed => "Failed",
                };
                CalendarEntry {
                    item_id: r.id,
                    show_title: r.show_title,
                    item_type: item_type.to_string(),
                    aired_at: r.aired_at.map(|d| d.to_string()),
                    season: r.season_number,
                    episode: r.episode_number,
                    tmdb_id: r.tmdb_id,
                    tvdb_id: r.tvdb_id,
                    last_state: last_state.to_string(),
                }
            })
            .collect();

        Ok(entries)
    }
}

#[cfg(test)]
mod tests;
