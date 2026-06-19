//! ORM-native migrations via `sea-orm-migration`.

use sea_orm::ConnectionTrait;
use sea_orm_migration::{MigrationName, MigrationTrait, MigratorTrait, SchemaManager};

macro_rules! sql_migrations {
    ($(($ty:ident, $name:literal, $file:literal)),* $(,)?) => {
        $(
            struct $ty;
            impl MigrationName for $ty {
                fn name(&self) -> &str { $name }
            }
            #[async_trait::async_trait]
            impl MigrationTrait for $ty {
                async fn up(&self, manager: &SchemaManager) -> Result<(), sea_orm::DbErr> {
                    manager
                        .get_connection()
                        .execute_unprepared(include_str!(concat!("../migrations/", $file)))
                        .await?;
                    Ok(())
                }
            }
        )*

        pub struct Migrator;

        impl MigratorTrait for Migrator {
            fn migrations() -> Vec<Box<dyn MigrationTrait>> {
                vec![$(Box::new($ty)),*]
            }
        }
    };
}

sql_migrations![
    (M001, "m001_initial", "001_initial.sql"),
    (M002, "m002_add_stream_rank", "002_add_stream_rank.sql"),
    (
        M003,
        "m003_add_settings_table",
        "003_add_settings_table.sql"
    ),
    (M004, "m004_retry_and_upsert", "004_retry_and_upsert.sql"),
    (M005, "m005_stream_file_size", "005_stream_file_size.sql"),
    (M006, "m006_scheduler_index", "006_scheduler_index.sql"),
    (M007, "m007_ranking_profiles", "007_ranking_profiles.sql"),
    (M008, "m008_multi_version", "008_multi_version.sql"),
    (M009, "m009_profile_versions", "009_profile_versions.sql"),
    (M010, "m010_profile_enabled", "010_profile_enabled.sql"),
    (M011, "m011_perf_indexes", "011_perf_indexes.sql"),
    (
        M012,
        "m012_item_request_state_alignment",
        "012_item_request_state_alignment.sql"
    ),
    (M013, "m013_flow_artifacts", "013_flow_artifacts.sql"),
    (
        M014,
        "m014_media_item_is_anime",
        "014_media_item_is_anime.sql"
    ),
    (M015, "m015_stream_magnet", "015_stream_magnet.sql"),
    (
        M016,
        "m016_drop_flow_artifacts",
        "016_drop_flow_artifacts.sql"
    ),
    (M017, "m017_stream_timestamps", "017_stream_timestamps.sql"),
    (
        M018,
        "m018_enable_hd_profile_by_default",
        "018_enable_hd_profile_by_default.sql"
    ),
    (
        M019,
        "m019_media_item_network_timezone",
        "019_media_item_network_timezone.sql"
    ),
    (
        M020,
        "m020_episode_aired_at_utc",
        "020_episode_aired_at_utc.sql"
    ),
    (
        M021,
        "m021_scrape_attempt_timestamp",
        "021_scrape_attempt_timestamp.sql"
    ),
    (
        M022,
        "m022_backfill_parent_aired_at",
        "022_backfill_parent_aired_at.sql"
    ),
    (M023, "m023_state_triggers", "023_state_triggers.sql"),
    (
        M024,
        "m024_state_recompute_batching",
        "024_state_recompute_batching.sql"
    ),
    (
        M025,
        "m025_drop_state_triggers",
        "025_drop_state_triggers.sql"
    ),
    (
        M026,
        "m026_subtitle_source_columns",
        "026_subtitle_source_columns.sql"
    ),
    (
        M027,
        "m027_item_request_partial_request",
        "027_item_request_partial_request.sql"
    ),
    (M028, "m028_usenet_meta", "028_usenet_meta.sql"),
    (
        M029,
        "m029_usenet_entry_identity",
        "029_usenet_entry_identity.sql"
    ),
    (
        M030,
        "m030_usenet_file_health",
        "030_usenet_file_health.sql"
    ),
    (M031, "m031_usenet_traffic", "031_usenet_traffic.sql"),
    (M032, "m032_usenet_repair", "032_usenet_repair.sql"),
    (
        M033,
        "m033_permanent_blacklist",
        "033_permanent_blacklist.sql"
    ),
    (
        M034,
        "m034_updated_at_orphan_gc_identity",
        "034_updated_at_orphan_gc_identity.sql"
    ),
];
