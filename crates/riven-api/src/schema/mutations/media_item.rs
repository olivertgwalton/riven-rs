use async_graphql::*;
use riven_core::events::RivenEvent;
use riven_core::types::{MediaItemState, build_magnet_uri};
use riven_db::repo;
use riven_queue::JobQueue;
use riven_queue::application::download::{
    ManualDownloadErrorKind, ManualDownloadTorrentInput, persist_manual_download,
};
use riven_queue::context::build_parse_item_context;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

use crate::schema::typed_items::MediaItemUnion;

use super::MutationStatusText;

#[derive(Enum, Copy, Clone, PartialEq, Eq)]
enum ScrapeMediaItemMutationErrorCode {
    NoNewStreams,
    IncorrectState,
    ScrapeError,
}

#[derive(InputObject)]
struct ScrapeMediaItemMutationInput {
    id: i64,
    results: serde_json::Value,
}

#[derive(SimpleObject)]
struct ScrapeMediaItemMutationResponse {
    success: bool,
    message: String,
    status_text: MutationStatusText,
    error_code: Option<ScrapeMediaItemMutationErrorCode>,
    item: Option<MediaItemUnion>,
    new_streams_count: Option<i32>,
}

#[derive(InputObject)]
struct DownloadMediaItemMutationInput {
    id: i64,
    torrent: serde_json::Value,
    processed_by: String,
}

#[derive(SimpleObject)]
struct DownloadMediaItemMutationResponse {
    success: bool,
    message: String,
    status_text: MutationStatusText,
    item: Option<MediaItemUnion>,
}

#[derive(Default)]
pub struct MediaItemMutations;

#[Object]
impl MediaItemMutations {
    async fn scrape_media_item(
        &self,
        ctx: &Context<'_>,
        input: ScrapeMediaItemMutationInput,
    ) -> Result<ScrapeMediaItemMutationResponse> {
        let pool = ctx.data::<sqlx::PgPool>()?;
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let Some(item) = repo::get_media_item(pool, input.id).await? else {
            return Ok(ScrapeMediaItemMutationResponse {
                success: false,
                message: "Media item not found.".to_string(),
                status_text: MutationStatusText::NotFound,
                error_code: Some(ScrapeMediaItemMutationErrorCode::ScrapeError),
                item: None,
                new_streams_count: None,
            });
        };

        if !matches!(
            item.state,
            MediaItemState::Indexed
                | MediaItemState::Ongoing
                | MediaItemState::Scraped
                | MediaItemState::PartiallyCompleted
        ) {
            return Ok(ScrapeMediaItemMutationResponse {
                success: false,
                message: "Media item is not in a scrapeable state.".to_string(),
                status_text: MutationStatusText::BadRequest,
                error_code: Some(ScrapeMediaItemMutationErrorCode::IncorrectState),
                item: Some(MediaItemUnion::from(item)),
                new_streams_count: None,
            });
        }

        let results = match input.results {
            Value::Object(map) => map,
            _ => {
                return Ok(ScrapeMediaItemMutationResponse {
                    success: false,
                    message: "results must be an object keyed by info hash".to_string(),
                    status_text: MutationStatusText::BadRequest,
                    error_code: Some(ScrapeMediaItemMutationErrorCode::ScrapeError),
                    item: Some(MediaItemUnion::from(item)),
                    new_streams_count: None,
                });
            }
        };

        let existing_stream_ids: HashSet<i64> = repo::get_streams_for_item(pool, input.id)
            .await?
            .into_iter()
            .map(|stream| stream.id)
            .collect();

        for (info_hash, parsed_data) in results {
            let stream = repo::upsert_stream(
                pool,
                &info_hash,
                &build_magnet_uri(&info_hash),
                Some(parsed_data),
                None,
            )
            .await?;
            repo::link_stream_to_item(pool, input.id, stream.id).await?;
        }

        repo::update_scraped(pool, input.id).await?;
        repo::refresh_state_cascade(pool, &item).await?;

        let fresh = repo::get_media_item(pool, input.id).await?.unwrap_or(item);
        let new_streams_count = repo::get_streams_for_item(pool, input.id)
            .await?
            .into_iter()
            .filter(|stream| !existing_stream_ids.contains(&stream.id))
            .count() as i32;

        let parse_ctx = build_parse_item_context(pool, fresh.clone()).await;
        if new_streams_count == 0 {
            let _ = repo::increment_failed_attempts(pool, input.id).await;
            job_queue
                .notify(RivenEvent::MediaItemScrapeErrorNoNewStreams {
                    id: input.id,
                    title: parse_ctx.item_title,
                    item_type: parse_ctx.item_type,
                })
                .await;

            return Ok(ScrapeMediaItemMutationResponse {
                success: true,
                message: "No new streams were added.".to_string(),
                status_text: MutationStatusText::Ok,
                error_code: Some(ScrapeMediaItemMutationErrorCode::NoNewStreams),
                item: Some(MediaItemUnion::from(fresh)),
                new_streams_count: Some(0),
            });
        }

        let _ = repo::reset_failed_attempts(pool, input.id).await;
        job_queue
            .notify(RivenEvent::MediaItemScrapeSuccess {
                id: input.id,
                title: parse_ctx.item_title,
                item_type: parse_ctx.item_type,
                stream_count: new_streams_count as usize,
            })
            .await;

        Ok(ScrapeMediaItemMutationResponse {
            success: true,
            message: "Media item scraped successfully.".to_string(),
            status_text: MutationStatusText::Ok,
            error_code: None,
            item: Some(MediaItemUnion::from(fresh)),
            new_streams_count: Some(new_streams_count),
        })
    }

    async fn download_media_item(
        &self,
        ctx: &Context<'_>,
        input: DownloadMediaItemMutationInput,
    ) -> Result<DownloadMediaItemMutationResponse> {
        let job_queue = ctx.data::<Arc<JobQueue>>()?;

        let torrent: ManualDownloadTorrentInput = serde_json::from_value(input.torrent)
            .map_err(|error| Error::new(format!("invalid torrent payload: {error}")))?;

        match persist_manual_download(input.id, torrent, &input.processed_by, job_queue).await {
            Ok(item) => Ok(DownloadMediaItemMutationResponse {
                success: true,
                message: "Media item download results processed successfully.".to_string(),
                status_text: MutationStatusText::Ok,
                item: Some(MediaItemUnion::from(item)),
            }),
            Err(error) => Ok(DownloadMediaItemMutationResponse {
                success: false,
                message: error.message,
                status_text: match error.kind {
                    ManualDownloadErrorKind::IncorrectState => MutationStatusText::BadRequest,
                    ManualDownloadErrorKind::DownloadError => {
                        MutationStatusText::InternalServerError
                    }
                },
                item: error.item.map(MediaItemUnion::from),
            }),
        }
    }
}
