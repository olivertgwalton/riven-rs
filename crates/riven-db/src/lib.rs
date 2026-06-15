pub mod entities;
pub mod migrations;
pub mod repo;

use anyhow::Result;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub async fn connect(database_url: &str) -> Result<PgPool> {
    let parallelism = std::thread::available_parallelism().map_or(4, |n| n.get() as u32);
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
