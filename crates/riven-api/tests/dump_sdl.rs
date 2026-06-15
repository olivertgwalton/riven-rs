// Run with: cargo test -p riven-api --test dump_sdl -- --nocapture
// Writes the full GraphQL SDL to target/schema.graphql
use async_graphql::Schema;
use riven_api::schema::{MutationRoot, QueryRoot, SubscriptionRoot};

#[test]
fn dump_sdl() {
    let schema = Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .finish();
    let sdl = schema.sdl();

    // Strip description blocks so the emitted schema is structure-only.
    // async-graphql writes every Rust `///` doc comment as a `"""…"""`
    // block; toggle on the delimiter lines and drop everything between.
    let mut out = String::with_capacity(sdl.len());
    let mut in_desc = false;
    for line in sdl.lines() {
        if line.trim() == "\"\"\"" {
            in_desc = !in_desc;
            continue;
        }
        if in_desc {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }

    std::fs::write("schema.graphql", &out).unwrap();
    eprintln!(
        "wrote {} bytes to crates/riven-api/schema.graphql",
        out.len()
    );
}
