pub mod entities;
pub mod migrations;
pub mod repo;

use anyhow::Result;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub async fn connect(database_url: &str) -> Result<PgPool> {
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(4);
    // Peak demand: IO-bound workers (scrape-plugin, index-plugin, download) run at 8× CPU
    // concurrency each, so size the pool to cover them plus parse + API headroom.
    let max_connections = (parallelism * 16).max(64);

    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(database_url)
        .await?;

    tracing::info!("database connection established");
    Ok(pool)
}

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations").run(pool).await?;
    tracing::info!("database migrations applied");
    Ok(())
}
