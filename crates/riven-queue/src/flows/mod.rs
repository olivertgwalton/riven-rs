pub mod download_item;
pub mod index_item;
pub mod request_content;
pub mod scrape_item;

use riven_db::{entities::MediaItem, repo};

/// Load a media item by id, logging an error and returning `None` on failure.
pub(crate) async fn load_item_or_log(id: i64, db_pool: &sqlx::PgPool, context: &str) -> Option<MediaItem> {
    match repo::get_media_item(db_pool, id).await {
        Ok(Some(item)) => Some(item),
        Ok(None) => { tracing::error!(id, "media item not found for {context}"); None }
        Err(e) => { tracing::error!(id, error = %e, "failed to load media item for {context}"); None }
    }
}
