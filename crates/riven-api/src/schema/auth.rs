//! GraphQL-facing authorization types.
//!
//! The ladder, capabilities and guards themselves live in [`riven_core::auth`]
//! so that plugin crates can use them; this module re-exports them unchanged so
//! every existing `crate::schema::auth::…` import keeps working, and adds the
//! `viewer` query, which is schema-specific.

use async_graphql::{Context, Object, Result, SimpleObject};

pub use riven_core::auth::{Capability, RequestAuth, UserRole, require, require_settings_access};

/// What the caller may do, as the backend understands it.
///
/// A list rather than a field per capability: adding one becomes a single edit
/// here, and clients receive it without a change on their side. The frontend
/// tests membership; it never derives anything from `role`.
#[derive(SimpleObject)]
pub struct Viewer {
    pub role: UserRole,
    pub capabilities: Vec<Capability>,
}

#[derive(Default)]
pub struct ViewerQuery;

#[Object]
impl ViewerQuery {
    async fn viewer(&self, ctx: &Context<'_>) -> Result<Viewer> {
        let role = riven_core::auth::request_role(ctx)?;
        Ok(Viewer {
            role,
            capabilities: Capability::ALL
                .into_iter()
                .filter(|capability| capability.granted_to(role))
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the names the frontend matches on. `async-graphql` screams enum
    /// variants, so `Manager` goes over the wire as `MANAGER` — renaming a
    /// variant here would silently change the API without this.
    #[test]
    fn the_role_enum_is_part_of_the_schema() {
        let sdl = async_graphql::Schema::build(
            ViewerQuery,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        )
        .finish()
        .sdl();

        assert!(sdl.contains("enum UserRole"), "{sdl}");
        for name in ["USER", "MANAGER", "ADMIN"] {
            assert!(sdl.contains(name), "missing {name} in:\n{sdl}");
        }
    }

    /// The ladder every capability threshold depends on.
    #[test]
    fn roles_are_ordered_least_to_most_privileged() {
        assert!(UserRole::User < UserRole::Manager);
        assert!(UserRole::Manager < UserRole::Admin);
    }

    /// A new variant breaks `minimum_role`'s match and so cannot be forgotten
    /// there — but it *can* be left out of `ALL`, which would silently hide it
    /// from every client. This catches that.
    #[test]
    fn every_capability_is_advertised() {
        let sdl = async_graphql::Schema::build(
            ViewerQuery,
            async_graphql::EmptyMutation,
            async_graphql::EmptySubscription,
        )
        .finish()
        .sdl();

        // Counts value lines specifically. Variant doc comments are emitted as
        // `"""…"""` descriptions, so anything looser counts those too.
        let declared = sdl
            .split("enum Capability {")
            .nth(1)
            .and_then(|rest| rest.split('}').next())
            .expect("Capability enum missing from the schema")
            .lines()
            .map(str::trim)
            .filter(|line| {
                !line.is_empty()
                    && line
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
            })
            .count();

        assert_eq!(
            declared,
            Capability::ALL.len(),
            "a capability is in the enum but missing from Capability::ALL"
        );
    }

    /// The property the whole design exists for: what `viewer` advertises and
    /// what `require` enforces are the same decision, for every role.
    #[test]
    fn advertised_capabilities_match_what_is_enforced() {
        for role in [UserRole::User, UserRole::Manager, UserRole::Admin] {
            for capability in Capability::ALL {
                assert_eq!(
                    capability.granted_to(role),
                    role >= capability.minimum_role(),
                    "{capability:?} disagrees with itself for {role:?}"
                );
            }
        }

        // And the ladder is actually a ladder: an admin holds everything.
        assert!(
            Capability::ALL
                .into_iter()
                .all(|capability| capability.granted_to(UserRole::Admin))
        );
    }
}
