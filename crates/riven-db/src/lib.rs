pub mod entities;
pub mod migrations;
pub mod repo;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub async fn connect(database_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .min_connections(1)
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
