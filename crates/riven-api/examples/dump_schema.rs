//! Writes the GraphQL schema to `schema.graphql` at the workspace root.
//!
//! That file is what the frontend generates its types from, so it is committed
//! rather than built on demand — a frontend build should not need a Rust
//! toolchain. `schema::tests::the_checked_in_schema_matches_the_code` fails when
//! it drifts.
//!
//! ```sh
//! cargo run -p riven-api --example dump_schema
//! ```

fn main() -> std::io::Result<()> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../schema.graphql");
    std::fs::write(path, format!("{}\n", riven_api::schema::sdl().trim()))?;
    println!("wrote {path}");
    Ok(())
}
