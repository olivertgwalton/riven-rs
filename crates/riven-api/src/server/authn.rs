//! `better-auth` wiring: the backend's own authentication surface.
//!
//! This exists to move authentication out of the SvelteKit frontend and into
//! riven. Today the frontend owns sessions and *signs the role headers riven
//! trusts* (`server/auth.rs`'s HMAC path) — which means the trust boundary sits
//! in a Node process that also proxies media. Once every route derives its role
//! from a session verified here, that HMAC path can go, and the frontend can be
//! a static bundle.
//!
//! Pinned to better-auth-rs's `v1` branch, which is unpublished and
//! self-described as alpha: "APIs, wire formats, and database schemas may change
//! without notice between alpha releases". The schema lives in
//! `riven_core::entities::auth` and its tables are created by migration `m036`.

use std::sync::Arc;

use chrono::TimeDelta;

use better_auth::plugins::{
    AccountManagementPlugin, AdminPlugin, ApiKeyPlugin, EmailPasswordPlugin,
    EmailVerificationPlugin, OAuthPlugin, PasskeyPlugin, PasswordManagementPlugin,
    SessionManagementPlugin, TwoFactorPlugin, UserManagementPlugin,
};
use better_auth::seaorm::SeaOrmStore;
use better_auth::{AuthConfig, BetterAuth};
use better_auth_core::{AccountConfig, AccountLinkingConfig, PasswordHasher};
use riven_core::entities::auth::RivenAuthSchema;
use riven_core::settings::OidcProviderSettings;

use super::first_user::FirstUserIsAdmin;
use super::legacy_password::DualFormatHasher;
use super::oidc;

/// An OIDC provider that resolved successfully, for the login page to render
/// a button for. Carries no secrets.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OidcProviderSummary {
    pub id: String,
    pub name: String,
}

/// Sessions last a week, refreshed daily. Matches what the frontend's
/// better-auth was configured with, so migrated sessions don't all expire at
/// once on cutover.
const SESSION_EXPIRES_IN_DAYS: i64 = 7;
const SESSION_UPDATE_AGE_DAYS: i64 = 1;

/// How long a WebAuthn challenge stays valid. The challenge is held in a cookie
/// between `generate-*-options` and `verify-*`, so this is the window the user
/// has to touch their authenticator. Five minutes matches better-auth's own
/// default and the platform prompts' own timeouts.
const PASSKEY_CHALLENGE_TTL_SECS: i64 = 300;

/// The instance's auth handle, shared by the router and the session extractors.
pub type RivenAuth = BetterAuth<RivenAuthSchema>;

/// Build the auth stack.
///
/// `secret` must be at least 32 bytes — it keys session token signing, so
/// rotating it invalidates every session. `base_url` is the public origin the
/// browser reaches riven at; better-auth uses it for cookie scope and for
/// deciding which redirect targets are trusted, so a wrong value here is a
/// login loop rather than a loud error.
pub async fn build(
    secret: &str,
    base_url: &str,
    trusted_origins: Vec<String>,
    oidc_providers: &[OidcProviderSettings],
) -> anyhow::Result<(Arc<RivenAuth>, Vec<OidcProviderSummary>)> {
    anyhow::ensure!(
        secret.len() >= 32,
        "auth secret must be at least 32 characters (got {})",
        secret.len()
    );

    // Logged rather than validated: a wrong relying-party ID is not something
    // this function can detect, only something the operator can recognise. It
    // shows up as passkeys that register fine and then refuse to authenticate,
    // which is otherwise a miserable thing to diagnose.
    match passkey_rp_id(base_url) {
        Some(rp_id) => tracing::info!(
            %rp_id,
            "passkeys are bound to this relying-party ID; browsers reaching riven \
             at any other hostname will not be offered them"
        ),
        None => tracing::warn!(
            %base_url,
            "public URL has no host — passkeys will fail; set RIVEN_SETTING__PUBLIC_URL"
        ),
    }

    warn_if_session_cookie_will_not_be_secure(base_url);

    let trusted_oidc_providers = trusted_oidc_provider_ids(oidc_providers);

    let config = AuthConfig::new(secret)
        .app_name("Riven")
        .base_url(base_url)
        .base_path("/auth")
        .trusted_origins(trusted_origins)
        .session_expires_in(TimeDelta::days(SESSION_EXPIRES_IN_DAYS))
        .session_update_age(TimeDelta::days(SESSION_UPDATE_AGE_DAYS))
        .password_min_length(8)
        .account(AccountConfig {
            account_linking: AccountLinkingConfig {
                trusted_providers: trusted_oidc_providers,
                ..Default::default()
            },
            ..Default::default()
        });

    // Shares riven's existing pool rather than opening a second one — the whole
    // reason for the sea-orm 2 upgrade, which put better-auth's entities and
    // riven's in one model graph.
    let store = SeaOrmStore::<RivenAuthSchema>::new(config.clone(), riven_db::orm().clone())
        .hook(FirstUserIsAdmin);

    // Verifies both the frontend's scrypt hashes and better-auth-rs's own
    // Argon2 — without it every existing user is locked out at cutover. Shared
    // by both plugins that touch passwords, so sign-in and password-change agree
    // on what a valid hash looks like.
    let hasher: Arc<dyn PasswordHasher> = Arc::new(DualFormatHasher::new(riven_db::orm().clone()));

    let auth = BetterAuth::<RivenAuthSchema>::new(config)
        .store(store)
        // Sign-up is open at the route level and closed by `FirstUserIsAdmin`
        // once an account exists — riven is a private media server, but a fresh
        // install still needs a way to create its first admin. Every later user
        // is created by that admin through the user-management plugin.
        .plugin(
            EmailPasswordPlugin::new()
                .enable_signup(true)
                .password_hasher(hasher.clone()),
        )
        .plugin(SessionManagementPlugin::new())
        .plugin(PasswordManagementPlugin::new().password_hasher(hasher.clone()))
        .plugin(AccountManagementPlugin::new())
        // Both halves default to off in better-auth-rs, and both were on in the
        // frontend's better-auth before the cutover — the profile page's
        // "Change Email" and "Delete Account" sections have no route otherwise.
        //
        // Verification is skipped in both cases because riven has no email
        // provider configured: `send_email_or_log` would only write the
        // confirmation link to the log, so requiring it would make the two
        // features permanently unreachable rather than merely unverified. That
        // matches what the TypeScript side did with the same empty mail config.
        // Deletion still demands the account's password at the call site.
        .plugin(
            UserManagementPlugin::new()
                .change_email_enabled(true)
                .update_without_verification(true)
                .delete_user_enabled(true)
                .require_delete_verification(false),
        )
        .plugin(EmailVerificationPlugin::new())
        .plugin(TwoFactorPlugin::new())
        .plugin(
            PasskeyPlugin::new()
                // Shown by the OS credential picker ("Save a passkey for …"),
                // so leaving the library's "Better Auth" default would name the
                // wrong product on the user's device — and the name is baked
                // into the credential at registration, not read back later.
                .rp_name("Riven")
                // `rp_id` and `origin` are deliberately left empty. Empty means
                // "derive from `base_url`": the relying-party ID becomes its
                // host, and registration options are generated against it, while
                // *verification* accepts the request's own `Origin`. That is the
                // behaviour we want — `public_url` is already the one setting
                // that says where browsers reach riven, and a second knob could
                // only ever disagree with it. See `passkey_rp_id` below for the
                // consequence when they do.
                .challenge_ttl_secs(PASSKEY_CHALLENGE_TTL_SECS),
        )
        // `ApiKeyPlugin` carries a `bon` builder rather than the plain `new()`
        // the other plugins get from the `PluginConfig` derive.
        .plugin(ApiKeyPlugin::builder().build())
        .plugin(AdminPlugin::new());

    // Every configured OIDC provider whose issuer resolved via discovery —
    // see `oidc::resolve_providers`. A provider that fails discovery is
    // logged there and simply absent here, so it gets neither a plugin
    // registration nor a login button.
    let resolved = oidc::resolve_providers(oidc_providers).await;
    let summaries = oidc_provider_summaries(&resolved, oidc_providers);

    // Skipped entirely rather than registered empty when nothing resolved —
    // most deployments configure no OIDC provider at all, and there is no
    // reason for `/sign-in/social` and friends to exist on the router just to
    // 404 everything.
    let auth = if resolved.is_empty() {
        auth
    } else {
        let mut oauth_plugin = OAuthPlugin::new();
        for (id, provider) in resolved {
            oauth_plugin = oauth_plugin.add_provider(&id, provider);
        }
        auth.plugin(oauth_plugin)
    };

    let auth = auth.build().await?;

    Ok((Arc::new(auth), summaries))
}

/// The ids of every configured provider that opted into
/// `trust_unverified_email`.
///
/// An OIDC sign-in whose email matches an existing account only auto-links
/// when either the provider is "trusted" or the provider itself reports the
/// email verified — otherwise a stranger who gets an *unconfirmed* address to
/// match an existing account could take it over. Trusting a provider
/// regardless of its `email_verified` claim is therefore only as safe as the
/// operator's confidence that every account on it is one they vetted, which is
/// why it's an explicit per-provider opt-in rather than implied by being
/// configured at all — see `OidcProviderSettings::trust_unverified_email`.
fn trusted_oidc_provider_ids(configured: &[OidcProviderSettings]) -> Vec<String> {
    configured
        .iter()
        .filter(|settings| settings.trust_unverified_email)
        .map(|settings| settings.id.clone())
        .collect()
}

/// Pairs each successfully-resolved provider with the display name from its
/// original settings entry — `resolved` only carries `(id, OAuthProvider)`,
/// which has no name field, so this looks it back up by id. Falls back to the
/// id itself in the (unreachable in practice) case a resolved id has no
/// matching settings entry.
fn oidc_provider_summaries(
    resolved: &[(String, better_auth::plugins::oauth::OAuthProvider)],
    configured: &[OidcProviderSettings],
) -> Vec<OidcProviderSummary> {
    resolved
        .iter()
        .map(|(id, _)| OidcProviderSummary {
            id: id.clone(),
            name: configured
                .iter()
                .find(|settings| &settings.id == id)
                .map(|settings| settings.display_name().to_string())
                .unwrap_or_else(|| id.clone()),
        })
        .collect()
}

/// Warn when the session cookie will go out without `Secure`.
///
/// `AuthConfig::base_url` sets `session.cookie_secure` from the scheme, so a
/// `public_url` of `http://…` yields a session cookie any network path can read
/// — and it does so silently. The common way to land here is a TLS-terminating
/// reverse proxy with `public_url` pointed at the internal address rather than
/// the public one, which looks like it works.
///
/// Loopback is exempt: browsers treat it as a secure context, and a local `http`
/// URL is the normal way to run riven on your own machine.
fn warn_if_session_cookie_will_not_be_secure(base_url: &str) {
    let Ok(url) = url::Url::parse(base_url) else {
        return;
    };
    if url.scheme() != "http" {
        return;
    }
    let is_loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    if is_loopback {
        return;
    }

    tracing::warn!(
        %base_url,
        "RIVEN_SETTING__PUBLIC_URL is http, so the session cookie will be sent \
         without the Secure flag and is readable by anything on the network path. \
         If riven sits behind a TLS-terminating proxy, set this to the https URL \
         browsers actually use"
    );
}

/// The relying-party ID better-auth will derive for passkeys — the host of
/// `base_url`, with no port and no scheme, exactly as `PasskeyPlugin` computes
/// it when `rp_id` is left empty.
///
/// A passkey is sealed to this value by the authenticator itself, so it is the
/// one piece of auth config that cannot be changed after the fact without
/// invalidating every credential already registered.
fn passkey_rp_id(base_url: &str) -> Option<String> {
    url::Url::parse(base_url)
        .ok()?
        .host_str()
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oidc_settings(id: &str, name: &str) -> OidcProviderSettings {
        OidcProviderSettings {
            id: id.to_string(),
            name: name.to_string(),
            issuer: "https://idp.example.com".to_string(),
            client_id: "client-id".to_string(),
            client_secret: "client-secret".to_string(),
            scopes: Vec::new(),
            disable_sign_up: false,
            trust_unverified_email: false,
        }
    }

    fn fake_provider() -> better_auth::plugins::oauth::OAuthProvider {
        better_auth::plugins::oauth::OAuthProvider {
            client_id: String::new(),
            client_secret: String::new(),
            auth_url: String::new(),
            token_url: String::new(),
            user_info_url: None,
            scopes: Vec::new(),
            authorization_params: Vec::new(),
            map_user_info: None,
            get_user_info: None,
            refresh_access_token: None,
            verify_id_token: None,
            disable_implicit_sign_up: false,
            disable_sign_up: false,
            override_user_info_on_sign_in: false,
        }
    }

    #[test]
    fn oidc_provider_summaries_uses_the_configured_display_name() {
        let configured = vec![oidc_settings("pocketid", "PocketID")];
        let resolved = vec![("pocketid".to_string(), fake_provider())];

        let summaries = oidc_provider_summaries(&resolved, &configured);

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, "pocketid");
        assert_eq!(summaries[0].name, "PocketID");
    }

    #[test]
    fn oidc_provider_summaries_only_includes_resolved_providers() {
        // Two configured, only one resolved (the other presumably failed
        // discovery in `oidc::resolve_providers`) — the unresolved one must
        // not appear, or its login button would 404 every click.
        let configured = vec![
            oidc_settings("pocketid", "PocketID"),
            oidc_settings("broken", "Broken"),
        ];
        let resolved = vec![("pocketid".to_string(), fake_provider())];

        let summaries = oidc_provider_summaries(&resolved, &configured);

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, "pocketid");
    }

    #[test]
    fn trusted_oidc_provider_ids_excludes_providers_by_default() {
        let configured = vec![oidc_settings("pocketid", "PocketID")];

        assert!(trusted_oidc_provider_ids(&configured).is_empty());
    }

    #[test]
    fn trusted_oidc_provider_ids_includes_only_opted_in_providers() {
        let mut trusted = oidc_settings("pocketid", "PocketID");
        trusted.trust_unverified_email = true;
        let configured = vec![trusted, oidc_settings("authelia", "Authelia")];

        assert_eq!(trusted_oidc_provider_ids(&configured), vec!["pocketid"]);
    }

    /// End-to-end sign-in against a real Postgres, exercising migrations, the
    /// `SeaOrmStore`, [`DualFormatHasher`] and session creation together.
    ///
    /// Skipped unless `MIGRATION_TEST_DATABASE_URL` is set, because it needs a
    /// database and will write to it. Point it at a throwaway:
    ///
    /// ```sh
    /// docker run -d --name pg-test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=riven \
    ///   -e POSTGRES_DB=riven_test -p 55432:5432 postgres:18-alpine
    /// MIGRATION_TEST_DATABASE_URL=postgres://riven:test@localhost:55432/riven_test \
    ///   cargo test -p riven-api -- --ignored signs_in
    /// ```
    ///
    /// The hash below is scrypt in the *TypeScript* library's format, so a pass
    /// means a user migrated from the old frontend can still log in — the single
    /// most important property of this whole migration.
    #[tokio::test]
    #[ignore = "needs MIGRATION_TEST_DATABASE_URL"]
    async fn signs_in_a_user_whose_password_was_hashed_by_the_typescript_frontend() {
        use better_auth::prelude::{AuthRequest, HttpMethod};
        use sea_orm::{ConnectionTrait, Statement};

        let Ok(url) = std::env::var("MIGRATION_TEST_DATABASE_URL") else {
            return;
        };

        let db = riven_db::connect(&url).await.expect("connect");
        riven_db::run_migrations(&db).await.expect("migrate");

        // A scrypt hash produced by the TypeScript `better-auth` package, for
        // the password below. Same provenance as the vector in
        // `legacy_password`, so this is the real migrated-user shape.
        const EMAIL: &str = "legacy-user@example.test";
        const PASSWORD: &str = "correct horse battery staple";
        const SCRYPT_HASH: &str = "f75f61efb8db90adf62b0110c3112c29:bbf6b844a6a40007b1a7acb9d650db802601ad6dc5ffe63317a9ba1bf653475bdc10846af2f1228b672ea4ddf7c03b74ca77004a8893e9562d0a4cbdb3c4bf97";

        for sql in [
            format!(
                "INSERT INTO auth_users (id, name, email, email_verified, created_at, updated_at, banned) \
                 VALUES ('legacy-test-user', 'Legacy', '{EMAIL}', true, now(), now(), false) \
                 ON CONFLICT (id) DO NOTHING"
            ),
            format!(
                "INSERT INTO auth_accounts (id, account_id, provider_id, user_id, password, created_at, updated_at) \
                 VALUES ('legacy-test-account', 'legacy-test-user', 'credential', 'legacy-test-user', \
                 '{SCRYPT_HASH}', now(), now()) ON CONFLICT (id) DO NOTHING"
            ),
        ] {
            db.execute_raw(Statement::from_string(db.get_database_backend(), sql))
                .await
                .expect("seed");
        }

        let (auth, _) = build(
            "a-test-secret-that-is-at-least-32-bytes-long",
            "http://localhost:8080",
            Vec::new(),
            &[],
        )
        .await
        .expect("build auth");

        let body = serde_json::json!({ "email": EMAIL, "password": PASSWORD });
        let mut request = AuthRequest::new(HttpMethod::Post, "/auth/sign-in/email");
        request.body = Some(serde_json::to_vec(&body).expect("body"));
        request
            .headers
            .insert("content-type".to_string(), "application/json".to_string());

        let response = auth.handle_request(request).await.expect("sign-in call");
        let text = String::from_utf8_lossy(&response.body);
        assert_eq!(
            response.status, 200,
            "sign-in with a TypeScript-hashed password failed: {text}"
        );
        assert!(
            text.contains("token") || response.headers.get_all("set-cookie").next().is_some(),
            "sign-in returned no session: {text}"
        );
    }

    /// The port is deliberately absent: WebAuthn relying-party IDs are domains,
    /// so `localhost:8080` would be rejected by every browser.
    #[test]
    fn the_passkey_relying_party_is_the_public_url_host_without_its_port() {
        assert_eq!(
            passkey_rp_id("https://riven.example.com/").as_deref(),
            Some("riven.example.com")
        );
        assert_eq!(
            passkey_rp_id("http://localhost:8080").as_deref(),
            Some("localhost")
        );
        assert_eq!(passkey_rp_id("riven.example.com"), None);
    }

    #[tokio::test]
    async fn a_short_secret_is_rejected_before_any_database_work() {
        // `BetterAuth` is not `Debug`, so the Ok side can't be unwrapped for a
        // message — match instead.
        match build("too-short", "http://localhost:8080", Vec::new(), &[]).await {
            Ok(_) => panic!("a 9-character secret must not be accepted"),
            Err(error) => assert!(
                error.to_string().contains("at least 32 characters"),
                "unexpected error: {error}"
            ),
        }
    }
}
