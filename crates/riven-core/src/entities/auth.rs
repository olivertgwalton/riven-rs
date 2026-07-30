//! App-owned authentication entities, backing `better-auth-rs`.
//!
//! better-auth's `AuthSchema` has exactly four app-owned slots — user, session,
//! account, verification — and the library owns the plugin tables (`api_keys`,
//! `passkeys`, `two_factor`, `organization`, …) as its own entities. That split
//! is why these four live here in riven's model graph: they are ours to migrate
//! and to join against, while the plugin tables are the library's business.
//!
//! Column set mirrors the schema the TypeScript better-auth frontend created, so
//! existing rows port across by copy rather than by re-registration. Tables are
//! prefixed `auth_` for two reasons: `user` and `session` are reserved words in
//! Postgres, and the library's own entities already claim the unprefixed
//! `users`/`sessions`/`accounts`/`verifications` names.
//!
//! `better-auth` is pinned to a git branch (`v1`) that self-describes as alpha:
//! wire formats and schemas may change between alpha releases. Treat this schema
//! as tracking that branch rather than as settled.

use better_auth::AuthSchema;
use better_auth::seaorm::AuthEntity;
use sea_orm::entity::prelude::*;

pub mod user {
    use super::*;

    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel, AuthEntity)]
    #[auth(role = "user")]
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
        /// Username plugin.
        #[sea_orm(unique)]
        pub username: Option<String>,
        pub display_username: Option<String>,
        /// Admin plugin. Riven's own `UserRole` is derived from this.
        pub role: Option<String>,
        pub banned: bool,
        pub ban_reason: Option<String>,
        pub ban_expires: Option<DateTimeUtc>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod session {
    use super::*;

    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel, AuthEntity)]
    #[auth(role = "session")]
    #[sea_orm(table_name = "auth_sessions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub expires_at: DateTimeUtc,
        #[sea_orm(unique)]
        pub token: String,
        pub created_at: DateTimeUtc,
        pub updated_at: DateTimeUtc,
        pub ip_address: Option<String>,
        pub user_agent: Option<String>,
        pub user_id: String,
        /// Required by the session role: revocation flips this rather than
        /// deleting the row, so `list-sessions` can still show it.
        pub active: bool,
        /// Admin plugin's impersonation trail.
        pub impersonated_by: Option<String>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod account {
    use super::*;

    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel, AuthEntity)]
    #[auth(role = "account")]
    #[sea_orm(table_name = "auth_accounts")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub account_id: String,
        pub provider_id: String,
        pub user_id: String,
        pub access_token: Option<String>,
        pub refresh_token: Option<String>,
        pub id_token: Option<String>,
        pub access_token_expires_at: Option<DateTimeUtc>,
        pub refresh_token_expires_at: Option<DateTimeUtc>,
        pub scope: Option<String>,
        /// Password hash for the credential provider. Empty for OAuth accounts.
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

    #[derive(Clone, Debug, serde::Serialize, DeriveEntityModel, AuthEntity)]
    #[auth(role = "verification")]
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

/// Binds riven's four entities into the shape `better-auth` expects. Every
/// `BetterAuth`, `SeaOrmStore` and session extractor in the codebase is
/// parameterised by this type.
pub struct RivenAuthSchema;

impl AuthSchema for RivenAuthSchema {
    type User = user::Model;
    type Session = session::Model;
    type Account = account::Model;
    type Verification = verification::Model;
}
