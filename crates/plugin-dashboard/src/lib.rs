use async_graphql::{Context, Object, Result as GqlResult, SimpleObject};
use async_trait::async_trait;
use riven_core::events::{HookResponse, RivenEvent};
use riven_core::plugin::{Plugin, PluginRegistry};
use riven_core::register_plugin;
use riven_core::types::{ActivePlaybackSession, DebridUserInfo};
use riven_db::repo;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct DashboardPlugin;

register_plugin!(DashboardPlugin);

#[async_trait]
impl Plugin for DashboardPlugin {
    fn name(&self) -> &'static str {
        "dashboard"
    }

    fn show_in_settings(&self) -> bool {
        false
    }
}

#[derive(SimpleObject)]
pub struct LibraryStats {
    pub total_movies: i64,
    pub total_shows: i64,
    pub total_seasons: i64,
    pub total_episodes: i64,
    pub completed: i64,
    pub scraped: i64,
    pub indexed: i64,
    pub failed: i64,
    pub paused: i64,
    pub ongoing: i64,
    pub partially_completed: i64,
    pub unreleased: i64,
}

#[derive(SimpleObject)]
pub struct YearRelease {
    pub year: i32,
    pub count: i64,
}

#[derive(Default)]
pub struct DashboardQuery;

const ACTIVE_PLAYBACK_CACHE_TTL: Duration = Duration::from_secs(10);

#[derive(Default)]
pub struct PlaybackSessionsCache {
    state: Mutex<PlaybackSessionsCacheState>,
}

#[derive(Default)]
struct PlaybackSessionsCacheState {
    fetched_at: Option<Instant>,
    sessions: Vec<ActivePlaybackSession>,
}

impl PlaybackSessionsCache {
    pub async fn get_or_refresh(&self, registry: &PluginRegistry) -> Vec<ActivePlaybackSession> {
        let mut state = self.state.lock().await;
        if state
            .fetched_at
            .is_some_and(|fetched_at| fetched_at.elapsed() < ACTIVE_PLAYBACK_CACHE_TTL)
        {
            return state.sessions.clone();
        }

        let results = registry
            .dispatch(&RivenEvent::ActivePlaybackSessionsRequested)
            .await;
        let mut sessions = Vec::new();
        for (_, result) in results {
            if let Ok(HookResponse::ActivePlaybackSessions(items)) = result {
                sessions.extend(items);
            }
        }
        sessions.sort_by(|a, b| {
            a.server
                .cmp(&b.server)
                .then_with(|| a.user_name.cmp(&b.user_name))
                .then_with(|| a.item_title.cmp(&b.item_title))
        });

        state.fetched_at = Some(Instant::now());
        state.sessions = sessions.clone();
        sessions
    }
}

#[Object]
impl DashboardQuery {
    async fn stats(&self, ctx: &Context<'_>) -> GqlResult<LibraryStats> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let s = repo::get_stats(pool).await?;
        Ok(LibraryStats {
            total_movies: s.total_movies,
            total_shows: s.total_shows,
            total_seasons: s.total_seasons,
            total_episodes: s.total_episodes,
            completed: s.completed,
            scraped: s.scraped,
            indexed: s.indexed,
            failed: s.failed,
            paused: s.paused,
            ongoing: s.ongoing,
            partially_completed: s.partially_completed,
            unreleased: s.unreleased,
        })
    }

    /// Get completed-item activity counts grouped by date (past year).
    /// Returns a JSON object mapping ISO date strings (YYYY-MM-DD) to counts.
    async fn activity(&self, ctx: &Context<'_>) -> GqlResult<serde_json::Value> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let map = repo::get_activity(pool).await?;
        Ok(serde_json::to_value(map)?)
    }

    /// Count of movies and shows per release year.
    async fn year_releases(&self, ctx: &Context<'_>) -> GqlResult<Vec<YearRelease>> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        Ok(repo::get_year_releases(pool)
            .await?
            .into_iter()
            .map(|(year, count)| YearRelease { year, count })
            .collect())
    }

    /// Get debrid account information for all configured stores.
    async fn debrid_account_info(&self, ctx: &Context<'_>) -> GqlResult<Vec<DebridUserInfo>> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let results = registry
            .dispatch(&RivenEvent::DebridUserInfoRequested)
            .await;
        let mut infos = Vec::new();
        for (_, result) in results {
            if let Ok(HookResponse::UserInfo(user_infos)) = result {
                infos.extend(user_infos);
            }
        }
        Ok(infos)
    }

    /// Get active playback sessions from configured media-server plugins.
    async fn active_playback_sessions(
        &self,
        ctx: &Context<'_>,
    ) -> GqlResult<Vec<ActivePlaybackSession>> {
        let registry = ctx.data::<Arc<PluginRegistry>>()?;
        let cache = ctx.data::<Arc<PlaybackSessionsCache>>()?;
        Ok(cache.get_or_refresh(registry).await)
    }
}

#[cfg(test)]
mod tests;
