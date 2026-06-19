pub mod entities;
pub mod migration;
pub mod repo;

use std::collections::HashSet;
use std::sync::OnceLock;

use anyhow::Result;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
};
use sea_orm_migration::MigratorTrait;

use migration::Migrator;

/// Process-wide SeaORM connection (a module-level singleton). Set once by
/// [`connect`]. Repo functions read it via [`orm`].
static ORM: OnceLock<DatabaseConnection> = OnceLock::new();

/// The global SeaORM connection. Panics if [`connect`] hasn't run — that is a
/// startup-ordering bug, not a runtime condition.
pub fn orm() -> &'static DatabaseConnection {
    ORM.get()
        .expect("SeaORM connection used before connect() initialised it")
}

fn pool_size() -> u32 {
    let parallelism = std::thread::available_parallelism().map_or(4, |n| n.get() as u32);
    (parallelism * 16).max(64)
}

/// Open the database connection and publish it as the global [`orm`] handle.
pub async fn connect(database_url: &str) -> Result<DatabaseConnection> {
    let mut opt = ConnectOptions::new(database_url.to_owned());
    opt.max_connections(pool_size())
        .min_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(10));

    let db = Database::connect(opt).await?;
    if ORM.set(db.clone()).is_err() {
        tracing::warn!("database connection already initialised; keeping the first one");
    }
    tracing::info!("database connection established");
    Ok(db)
}

/// Apply pending migrations via `sea-orm-migration`.
///
/// Tracking lives in sea-orm's `seaql_migrations` table. On a database that was
/// migrated by the previous tooling (tracked in the `_sqlx_migrations` table),
/// the applied state is imported once so those migrations are not re-run, after
/// which the legacy table is dropped.
pub async fn run_migrations(db: &DatabaseConnection) -> Result<()> {
    Migrator::install(db).await?;

    if table_exists(db, "_sqlx_migrations").await? {
        import_legacy_migration_state(db).await?;
        db.execute_unprepared("DROP TABLE IF EXISTS _sqlx_migrations")
            .await?;
        tracing::info!("imported legacy migration state; dropped _sqlx_migrations");
    }

    Migrator::up(db, None).await?;
    tracing::info!("database migrations up to date");
    Ok(())
}

async fn table_exists(db: &DatabaseConnection, name: &str) -> Result<bool> {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "SELECT to_regclass($1) IS NOT NULL AS present",
            [format!("public.{name}").into()],
        ))
        .await?;
    Ok(row
        .and_then(|row| row.try_get::<bool>("", "present").ok())
        .unwrap_or(false))
}

/// Map each applied `_sqlx_migrations.version` (1-based, in file order) to the
/// matching `seaql_migrations` name and record it so `Migrator::up` skips it.
async fn import_legacy_migration_state(db: &DatabaseConnection) -> Result<()> {
    let names: Vec<String> = Migrator::migrations()
        .iter()
        .map(|m| m.name().to_owned())
        .collect();

    let applied: HashSet<i64> = db
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            "SELECT version FROM _sqlx_migrations WHERE success",
        ))
        .await?
        .iter()
        .filter_map(|row| row.try_get::<i64>("", "version").ok())
        .collect();

    for (idx, name) in names.iter().enumerate() {
        let version = i64::try_from(idx + 1).unwrap_or(0);
        if applied.contains(&version) {
            db.execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO seaql_migrations (version, applied_at) \
                 VALUES ($1, EXTRACT(EPOCH FROM now())::bigint) \
                 ON CONFLICT (version) DO NOTHING",
                [name.clone().into()],
            ))
            .await?;
        }
    }
    Ok(())
}
