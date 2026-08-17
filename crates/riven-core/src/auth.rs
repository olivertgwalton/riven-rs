//! Riven's authorization vocabulary.
//!
//! Lives in `riven-core` rather than `riven-api` because plugins define GraphQL
//! mutations too (`plugin-seerr`'s webhook, for one) and cannot depend on
//! `riven-api` — it depends on them. Before this move a plugin resolver had no
//! way to express a permission check at all, which is precisely how
//! `seerrHandleWebhook` shipped with no guard.

use async_graphql::{Context, Enum, Error, Result};

/// Riven's privilege ladder. Ordered: every check is `role >= minimum`.
///
/// Exposed to GraphQL as an enum rather than a string so the set of roles is
/// part of the schema — a client cannot invent a fourth one, and adding one here
/// is a visible schema change rather than a new magic string.
#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UserRole {
    User,
    Manager,
    Admin,
}

/// Something a caller may be allowed to do.
///
/// One variant per *action*, following riven-ts's access-control statements
/// (`item: ["request", "delete", "reset", "pause", "retry", "scrape"]`) rather
/// than a single "manage the library" lump. The distinction earns its keep the
/// moment you want a role that can retry a failed grab but not delete anything.
///
/// riven-ts declared these in the frontend and enforced none of them — its
/// GraphQL server had no auth at all. Here they gate the resolvers.
///
/// [`Capability::minimum_role`] is the *only* place a threshold is written down.
/// Both halves of authorisation read it: the guards that reject a mutation, and
/// the `viewer` query the UI renders from. They cannot disagree, because there
/// is nothing to disagree with.
#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    /// Ask for something to be added. The only action an ordinary user has.
    RequestItems,
    /// Put an item straight into the library, bypassing the request queue.
    /// No riven-ts counterpart — `addItem`/`discoverItem` are riven-rs's.
    AddItems,
    PauseItems,
    RetryItems,
    ResetItems,
    /// Find and choose a release: manual scrape, stream discovery, and the
    /// download that commits the chosen one.
    ScrapeItems,
    DeleteItems,
    /// Settings, profiles, indexing and setup — riven-ts's `adminAc` statements.
    ManageSettings,
}

impl Capability {
    /// Every capability. Kept in step with the enum by
    /// `tests::every_capability_is_advertised`.
    pub const ALL: [Self; 8] = [
        Self::RequestItems,
        Self::AddItems,
        Self::PauseItems,
        Self::RetryItems,
        Self::ResetItems,
        Self::ScrapeItems,
        Self::DeleteItems,
        Self::ManageSettings,
    ];

    /// The role table, matching riven-ts's: `user` holds only `request`,
    /// `manager` holds every item action, `admin` additionally holds settings.
    pub const fn minimum_role(self) -> UserRole {
        match self {
            Self::RequestItems => UserRole::User,
            Self::AddItems
            | Self::PauseItems
            | Self::RetryItems
            | Self::ResetItems
            | Self::ScrapeItems
            | Self::DeleteItems => UserRole::Manager,
            Self::ManageSettings => UserRole::Admin,
        }
    }

    pub fn granted_to(self, role: UserRole) -> bool {
        role >= self.minimum_role()
    }
}

/// The caller's resolved role, injected into the GraphQL context by
/// `authorize_request` before execution.
#[derive(Clone, Debug)]
pub struct RequestAuth {
    pub role: UserRole,
    /// The signed-in user's display handle (`display_username`, falling back
    /// to `username` — the same preference order the frontend already uses
    /// for this account, see `sidebar.svelte`/`user-management.svelte`).
    /// `None` for a trusted-API-key caller, which has no user behind it, or
    /// for the rare account with neither field set.
    ///
    /// `None` alone doesn't say *why* — that's [`Self::is_trusted_api_key`].
    /// A caller that wants to fall back to some other source of identity
    /// (e.g. a caller-supplied string) only when there's no session at all
    /// must check that flag first: treating "no username" as "no session"
    /// would let a signed-in user with an unset username masquerade as
    /// whatever identity they supply themselves.
    pub username: Option<String>,
    /// Whether this request authenticated via the configured API key rather
    /// than a real session. See [`Self::username`]'s doc for why this needs
    /// to be checked separately from it being `None`.
    pub is_trusted_api_key: bool,
}

impl RequestAuth {
    pub fn trusted_api_key() -> Self {
        Self {
            role: UserRole::Admin,
            username: None,
            is_trusted_api_key: true,
        }
    }
}

fn get_request_auth<'ctx>(ctx: &'ctx Context<'_>) -> Result<&'ctx RequestAuth> {
    ctx.data::<RequestAuth>()
        .map_err(|_e| Error::new("Missing request auth context"))
}

/// The caller's role, for resolvers that need to project it rather than gate on
/// it (the `viewer` query).
pub fn request_role(ctx: &Context<'_>) -> Result<UserRole> {
    Ok(get_request_auth(ctx)?.role)
}

/// Reject the request unless the caller holds `capability`.
pub fn require(ctx: &Context<'_>, capability: Capability) -> Result<()> {
    if capability.granted_to(get_request_auth(ctx)?.role) {
        Ok(())
    } else {
        Err(Error::new("Forbidden"))
    }
}

/// Settings has its own helper only because it guards twelve resolvers and
/// `require(ctx, Capability::ManageSettings)?` at every one of them is noise.
/// Item actions name their capability at the call site — that is the point.
pub fn require_settings_access(ctx: &Context<'_>) -> Result<()> {
    require(ctx, Capability::ManageSettings)
}
