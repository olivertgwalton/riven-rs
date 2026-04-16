use async_graphql::*;
use riven_db::entities::FileSystemEntry;
use riven_db::repo;
use sqlx::PgPool;

use crate::schema::auth::require_library_access;

use super::MutationStatusText;

// ── Response types ──

/// Structured response returned by `saveStreamUrl`.
#[derive(SimpleObject)]
pub(super) struct SaveStreamUrlMutationResponse {
    success: bool,
    message: String,
    status_text: MutationStatusText,
    item: Option<FileSystemEntry>,
}

// ── Resolver ──

#[derive(Default)]
pub struct MediaEntryMutations;

#[Object]
impl MediaEntryMutations {
    /// Save a stream URL on a filesystem entry (media entry).
    ///
    /// Used by players and integrations to store the resolved playback URL
    /// alongside the downloaded file record.
    async fn save_stream_url(
        &self,
        ctx: &Context<'_>,
        id: i64,
        url: String,
    ) -> Result<SaveStreamUrlMutationResponse> {
        require_library_access(ctx)?;
        let pool = ctx.data::<PgPool>()?;

        repo::update_stream_url(pool, id, &url).await?;

        let entry = repo::get_media_entry_by_id(pool, id)
            .await?
            .ok_or_else(|| Error::new("Filesystem entry not found"))?;

        Ok(SaveStreamUrlMutationResponse {
            success: true,
            message: "Stream URL saved successfully.".to_string(),
            status_text: MutationStatusText::Ok,
            item: Some(entry),
        })
    }
}
