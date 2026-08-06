//! Authentication entities.
//!
//! Column set and table names are preserved from the `better-auth` era — the
//! TypeScript frontend created this schema and the Rust port kept it — so
//! existing rows (users, password hashes, linked accounts, sessions, passkeys)
//! keep working with the native auth implementation in `riven-api`. Tables are
//! prefixed `auth_` because `user` and `session` are reserved words in
//! Postgres. `passkeys` keeps its unprefixed name: better-auth owned that
//! table and its name, and renaming it would orphan every registered
//! credential.

use sea_orm::entity::prelude::*;

pub mod user {
    use super::*;

    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel)]
    #[sea_orm(table_name = "auth_users")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub name: Option<String>,
        #[sea_orm(unique)]
        pub email: Option<String>,
        pub email_verified: bool,
        pub image: Option<String>,
        pub created_at: DateTimeUtc,
        pub updated_at: DateTimeUtc,
        /// Login handle, stored lowercased; `display_username` keeps the case
        /// as typed. Migration `m037` keeps `name` equal to `username`.
        #[sea_orm(unique)]
        pub username: Option<String>,
        pub display_username: Option<String>,
        /// Riven's `UserRole` ladder is derived from this free-text column;
        /// unknown values map to the least privilege.
        pub role: Option<String>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod session {
    use super::*;

    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel)]
    #[sea_orm(table_name = "auth_sessions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub expires_at: DateTimeUtc,
        /// SHA-256 of the bearer token, never the token itself — a database
        /// dump therefore yields nothing replayable. `serde(skip)` because no
        /// endpoint has a reason to hand even the hash back out.
        #[sea_orm(unique)]
        #[serde(skip)]
        pub token: String,
        pub created_at: DateTimeUtc,
        pub updated_at: DateTimeUtc,
        pub ip_address: Option<String>,
        pub user_agent: Option<String>,
        pub user_id: String,
        /// Revocation flips this rather than deleting the row, so a session
        /// list can still show revoked entries.
        pub active: bool,
        /// Admin impersonation trail. Unused by the native implementation but
        /// kept so existing rows and the column survive.
        pub impersonated_by: Option<String>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod account {
    use super::*;

    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel)]
    #[sea_orm(table_name = "auth_accounts")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        /// The identity at the provider: the OIDC `sub`, the Plex account id,
        /// or the user id itself for the `credential` provider.
        pub account_id: String,
        /// `credential`, `plex`, or a configured OIDC provider id.
        pub provider_id: String,
        pub user_id: String,
        pub access_token: Option<String>,
        pub refresh_token: Option<String>,
        pub id_token: Option<String>,
        pub access_token_expires_at: Option<DateTimeUtc>,
        pub refresh_token_expires_at: Option<DateTimeUtc>,
        pub scope: Option<String>,
        /// Password hash for the `credential` provider. NULL for OAuth/Plex
        /// accounts, which is what keeps them out of the password flow.
        pub password: Option<String>,
        pub created_at: DateTimeUtc,
        pub updated_at: DateTimeUtc,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod verification {
    use super::*;

    /// Short-lived one-time tokens — today only password-reset tokens, keyed
    /// `reset-password:{token}` with the user id as `value`.
    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel)]
    #[sea_orm(table_name = "auth_verifications")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub identifier: String,
        pub value: String,
        pub expires_at: DateTimeUtc,
        pub created_at: DateTimeUtc,
        pub updated_at: DateTimeUtc,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod passkey {
    use super::*;

    /// WebAuthn credentials. `credential` holds the `webauthn-rs` `Passkey`
    /// serialized as JSON — the same format better-auth-rs wrote, so
    /// credentials registered before the native rewrite keep verifying.
    /// `credential_id` is the base64url (no padding) credential id, which is
    /// how an assertion is matched back to its row.
    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel)]
    #[sea_orm(table_name = "passkeys")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub name: Option<String>,
        /// Informational only; verification uses `credential`. better-auth
        /// stored the raw COSE key here, the native implementation writes an
        /// empty string.
        pub public_key: String,
        pub user_id: String,
        pub credential_id: String,
        pub counter: i64,
        /// `"singleDevice"` or `"multiDevice"`, from the backup-eligible flag.
        pub device_type: String,
        pub backed_up: bool,
        /// Comma-separated authenticator transports (`"internal,hybrid"`).
        pub transports: Option<String>,
        pub credential: String,
        pub aaguid: Option<String>,
        pub created_at: DateTimeUtc,
        pub updated_at: DateTimeUtc,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
