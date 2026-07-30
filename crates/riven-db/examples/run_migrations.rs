//! Run every migration against a database of your choosing, then print the
//! resulting schema.
//!
//! Intended for exercising migrations against a throwaway Postgres before they
//! touch a real library — `cargo check` proves the DDL compiles, not that
//! Postgres accepts it.
//!
//! ```sh
//! docker run -d --name pg-test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=riven \
//!   -e POSTGRES_DB=riven_test -p 55432:5432 postgres:18-alpine
//! MIGRATION_TEST_DATABASE_URL=postgres://riven:test@localhost:55432/riven_test \
//!   cargo run -p riven-db --example run_migrations
//! ```
//!
//! Deliberately reads its own env var rather than `DATABASE_URL`, so a stray
//! invocation cannot pick up a production connection string from the shell.

use sea_orm::{ConnectionTrait, Statement};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("MIGRATION_TEST_DATABASE_URL").map_err(|_| {
        anyhow::anyhow!(
            "set MIGRATION_TEST_DATABASE_URL (this tool refuses to read DATABASE_URL, \
             so it cannot be pointed at production by accident)"
        )
    })?;

    println!("connecting…");
    let db = riven_db::connect(&url).await?;

    println!("running migrations…");
    riven_db::run_migrations(&db).await?;
    println!("migrations applied\n");

    let rows = db
        .query_all_raw(Statement::from_string(
            db.get_database_backend(),
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' ORDER BY table_name",
        ))
        .await?;
    println!("tables ({}):", rows.len());
    for row in &rows {
        let name: String = row.try_get_by_index(0)?;
        println!("  {name}");
    }

    Ok(())
}
