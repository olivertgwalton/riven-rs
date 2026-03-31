use anyhow::Result;
use riven_core::types::*;
use sqlx::PgPool;

use crate::entities::*;

/// Collect the non-null `external_request_id`s for the item_requests linked to
/// the given media_item IDs.  Used to notify content services before deletion.
pub async fn get_external_request_ids_for_items(
    pool: &PgPool,
    media_item_ids: &[i64],
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT ir.external_request_id
           FROM media_items mi
           JOIN item_requests ir ON ir.id = mi.item_request_id
           WHERE mi.id = ANY($1)
             AND ir.external_request_id IS NOT NULL"#,
        media_item_ids
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .flatten()
    .collect())
}

pub async fn get_item_request_by_id(pool: &PgPool, id: i64) -> Result<Option<ItemRequest>> {
    Ok(
        sqlx::query_as::<_, ItemRequest>("SELECT * FROM item_requests WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?,
    )
}

/// Find an existing item request by any matching external ID.
pub async fn find_existing_item_request(
    pool: &PgPool,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
) -> Result<Option<ItemRequest>> {
    Ok(sqlx::query_as::<_, ItemRequest>(
        "SELECT * FROM item_requests
         WHERE (imdb_id = $1 AND $1 IS NOT NULL)
            OR (tmdb_id = $2 AND $2 IS NOT NULL)
            OR (tvdb_id = $3 AND $3 IS NOT NULL)
         LIMIT 1",
    )
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .fetch_optional(pool)
    .await?)
}

pub async fn create_item_request(
    pool: &PgPool,
    imdb_id: Option<&str>,
    tmdb_id: Option<&str>,
    tvdb_id: Option<&str>,
    request_type: ItemRequestType,
    requested_by: Option<&str>,
    external_request_id: Option<&str>,
    seasons: Option<&[i32]>,
) -> Result<ItemRequest> {
    if let Some(existing) =
        find_existing_item_request(pool, imdb_id, tmdb_id, tvdb_id).await?
    {
        // For shows: if new seasons are specified, update the request.
        // - If the existing request has a seasons list, merge (Request More adds seasons).
        // - If the existing request has seasons = NULL (whole show), replace with the
        //   specific seasons so the indexer honours the user's selection.
        if let Some(new_seasons) = seasons {
            if !new_seasons.is_empty() {
                let updated_seasons: Vec<i32> =
                    if let Some(ref existing_seasons_val) = existing.seasons {
                        let existing_vec: Vec<i32> =
                            serde_json::from_value(existing_seasons_val.clone())
                                .unwrap_or_default();
                        let mut seen: std::collections::HashSet<i32> =
                            existing_vec.iter().copied().collect();
                        let mut merged = existing_vec;
                        for &s in new_seasons {
                            if seen.insert(s) {
                                merged.push(s);
                            }
                        }
                        merged.sort_unstable();
                        merged
                    } else {
                        // Existing request covered the whole show; narrow to requested seasons.
                        let mut v = new_seasons.to_vec();
                        v.sort_unstable();
                        v
                    };
                let merged_json = serde_json::to_value(&updated_seasons).unwrap_or_default();
                let updated = sqlx::query_as::<_, ItemRequest>(
                    "UPDATE item_requests SET seasons = $1 WHERE id = $2 RETURNING *",
                )
                .bind(merged_json)
                .bind(existing.id)
                .fetch_one(pool)
                .await?;
                return Ok(updated);
            }
        }
        return Ok(existing);
    }

    let seasons_json = seasons.map(|s| serde_json::to_value(s).unwrap_or_default());
    let request = sqlx::query_as::<_, ItemRequest>(
        "INSERT INTO item_requests (imdb_id, tmdb_id, tvdb_id, request_type, requested_by, external_request_id, seasons) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         RETURNING *",
    )
    .bind(imdb_id)
    .bind(tmdb_id)
    .bind(tvdb_id)
    .bind(request_type as ItemRequestType)
    .bind(requested_by)
    .bind(external_request_id)
    .bind(seasons_json)
    .fetch_one(pool)
    .await?;
    Ok(request)
}
