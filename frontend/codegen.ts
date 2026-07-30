import type { CodegenConfig } from "@graphql-codegen/cli";

/**
 * Generates TypeScript types from riven's GraphQL schema.
 *
 * The schema is produced by the backend (`cargo run -p riven-api --example
 * dump_schema`) and committed at the repo root, so regenerating needs no
 * network and no running server — a normal build just uses the committed
 * output in `src/lib/gql/`.
 *
 * Types only, no operation documents: the queries in this codebase are plain
 * template strings rather than `gql` tags, so codegen cannot find them. The
 * duplication worth removing is the hand-written mirrors of backend *types*.
 */
const config: CodegenConfig = {
    schema: process.env.RIVEN_SCHEMA ?? "../schema.graphql",
    generates: {
        "src/lib/gql/schema.ts": {
            plugins: ["typescript"],
            config: {
                // String unions rather than TS enums: these are wire values, and
                // a `const enum` would need importing at every comparison.
                enumsAsTypes: true,
                skipTypename: true,
                useTypeImports: true,
                scalars: { JSON: "unknown" }
            }
        }
    }
};

export default config;
