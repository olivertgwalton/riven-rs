//! Generic OIDC provider wiring.
//!
//! Riven never hardcodes a provider's endpoint layout. Each configured
//! provider is resolved from its issuer via the standard OIDC discovery
//! document (`{issuer}/.well-known/openid-configuration`), which is what lets
//! one implementation cover PocketID, Authelia, Keycloak, Authentik, Zitadel
//! or any other spec-compliant issuer — their endpoint *paths* differ
//! (PocketID's token endpoint is `/api/oidc/token`; others use `/oauth2/token`,
//! `/token`, ...) but discovery always points at the right one.

use better_auth::plugins::oauth::{OAuthProvider, OAuthUserInfo};
use riven_core::settings::OidcProviderSettings;
use serde::Deserialize;
use serde_json::Value;
use url::Url;

/// The subset of an OIDC discovery document riven actually needs.
#[derive(Debug, Deserialize)]
struct DiscoveryDocument {
    authorization_endpoint: String,
    token_endpoint: String,
    userinfo_endpoint: String,
}

/// Fetches and parses `{issuer}/.well-known/openid-configuration`, rejecting
/// a cleartext issuer or a document whose `authorization_endpoint`,
/// `token_endpoint` or `userinfo_endpoint` isn't `https://` —
/// `client_secret` and access tokens go out to those endpoints, so any of
/// this over plain HTTP (a misconfigured issuer, or a request or redirect
/// intercepted by an on-path attacker) would send credentials over the wire
/// in the open, or hand discovery itself to a document the attacker
/// controls. Plain `http://` is accepted only to a loopback address, since
/// that traffic never leaves the machine — this module's own tests run
/// discovery against a local mock server with no certificate. Redirects are
/// refused outright rather than validated one by one: a discovery document
/// has no legitimate reason to live behind one.
async fn discover(issuer: &str) -> anyhow::Result<DiscoveryDocument> {
    let issuer = issuer.trim_end_matches('/');
    anyhow::ensure!(
        is_https_or_loopback(issuer),
        "issuer is not https and not a loopback address: {issuer}"
    );
    let url = format!("{issuer}/.well-known/openid-configuration");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let response = client.get(&url).send().await?.error_for_status()?;
    let doc = response.json::<DiscoveryDocument>().await?;

    for endpoint in [
        &doc.authorization_endpoint,
        &doc.token_endpoint,
        &doc.userinfo_endpoint,
    ] {
        anyhow::ensure!(
            is_https_or_loopback(endpoint),
            "discovery document endpoint is not https and not a loopback address: {endpoint}"
        );
    }
    Ok(doc)
}

/// True for `https://` URLs, and for `http://` URLs to a loopback address —
/// the one case a cleartext endpoint is acceptable, since that traffic never
/// leaves the machine.
fn is_https_or_loopback(url: &str) -> bool {
    let Ok(parsed) = Url::parse(url) else {
        return false;
    };
    match parsed.scheme() {
        "https" => true,
        // `Url::host_str` keeps the brackets on an IPv6 literal.
        "http" => matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")),
        _ => false,
    }
}

/// Resolves every configured provider via OIDC discovery and returns the
/// `(id, OAuthProvider)` pairs that succeeded.
///
/// A provider that fails discovery — unreachable, wrong issuer, not actually
/// OIDC-compliant — is logged and skipped rather than failing the whole auth
/// stack: these are optional sign-in methods layered on top of the built-in
/// password/passkey/Plex ones, not something a typo should be able to take
/// riven's login page down over.
pub async fn resolve_providers(
    configured: &[OidcProviderSettings],
) -> Vec<(String, OAuthProvider)> {
    let mut resolved = Vec::with_capacity(configured.len());
    for settings in configured {
        match discover(&settings.issuer).await {
            Ok(doc) => resolved.push((
                settings.id.clone(),
                OAuthProvider {
                    client_id: settings.client_id.clone(),
                    client_secret: settings.client_secret.clone(),
                    auth_url: doc.authorization_endpoint,
                    token_url: doc.token_endpoint,
                    user_info_url: Some(doc.userinfo_endpoint),
                    scopes: settings.effective_scopes(),
                    authorization_params: Vec::new(),
                    map_user_info: Some(map_user_info),
                    get_user_info: None,
                    refresh_access_token: None,
                    verify_id_token: None,
                    disable_implicit_sign_up: false,
                    disable_sign_up: settings.disable_sign_up,
                    override_user_info_on_sign_in: false,
                },
            )),
            Err(error) => tracing::warn!(
                provider = %settings.id,
                issuer = %settings.issuer,
                %error,
                "OIDC discovery failed; this provider will not be offered for sign-in"
            ),
        }
    }
    resolved
}

/// Maps an OIDC provider's standard userinfo claims to better-auth's
/// provider-agnostic shape. Works for any spec-compliant issuer — `sub`,
/// `email` and `email_verified` are defined by the OIDC core spec, and `name`
/// falls back to `preferred_username` for providers (PocketID included) whose
/// account setup only guarantees the latter is non-empty.
fn map_user_info(claims: Value) -> Result<OAuthUserInfo, String> {
    let id = claims
        .get("sub")
        .and_then(Value::as_str)
        .ok_or("missing sub")?
        .to_string();
    let email = claims
        .get("email")
        .and_then(Value::as_str)
        .ok_or("missing email")?
        .to_string();
    let name = claims
        .get("name")
        .and_then(Value::as_str)
        .or_else(|| claims.get("preferred_username").and_then(Value::as_str))
        .map(String::from);
    let image = claims
        .get("picture")
        .and_then(Value::as_str)
        .map(String::from);
    let email_verified = claims
        .get("email_verified")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    Ok(OAuthUserInfo {
        id,
        email,
        name,
        image,
        email_verified,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_providers_skips_a_provider_whose_issuer_is_unreachable() {
        let configured = vec![OidcProviderSettings {
            id: "broken".to_string(),
            name: "Broken".to_string(),
            issuer: "http://127.0.0.1:1".to_string(), // nothing listens on port 1
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            scopes: Vec::new(),
            disable_sign_up: false,
            trust_unverified_email: false,
        }];

        let resolved = resolve_providers(&configured).await;

        assert!(resolved.is_empty());
    }

    #[tokio::test]
    async fn resolve_providers_skips_a_non_loopback_cleartext_issuer() {
        // Never sends a request — a cleartext, non-loopback issuer is refused
        // before the fetch, since an on-path attacker intercepting that
        // request could substitute their own discovery document (whose
        // endpoints could themselves be `https://`, so validating only the
        // returned endpoints isn't enough).
        let configured = vec![OidcProviderSettings {
            id: "insecure-issuer".to_string(),
            name: "Insecure Issuer".to_string(),
            issuer: "http://idp.example.com".to_string(),
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            scopes: Vec::new(),
            disable_sign_up: false,
            trust_unverified_email: false,
        }];

        let resolved = resolve_providers(&configured).await;

        assert!(resolved.is_empty());
    }

    /// Serves a fixed discovery document off a real localhost socket for
    /// exactly one request. Returns the issuer origin to configure against.
    ///
    /// Doesn't bother reading the request — it's a single `GET` with no body,
    /// and the response doesn't depend on anything in it, so there is nothing
    /// to gain from parsing it.
    #[expect(
        clippy::let_underscore_must_use,
        reason = "best-effort mock server; a write/flush failure just means the \
                   test's HTTP request times out and fails loudly on its own"
    )]
    async fn start_discovery_mock(body: Value) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;

            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };

            let payload = body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len(),
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.flush().await;
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    async fn resolve_providers_builds_a_provider_from_a_real_discovery_document() {
        let issuer = start_discovery_mock(serde_json::json!({
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/api/oidc/token",
            "userinfo_endpoint": "https://idp.example.com/api/oidc/userinfo",
        }))
        .await;

        let configured = vec![OidcProviderSettings {
            id: "pocketid".to_string(),
            name: "PocketID".to_string(),
            issuer,
            client_id: "client-id".to_string(),
            client_secret: "client-secret".to_string(),
            scopes: Vec::new(),
            disable_sign_up: true,
            trust_unverified_email: false,
        }];

        let resolved = resolve_providers(&configured).await;

        assert_eq!(resolved.len(), 1);
        let (id, provider) = &resolved[0];
        assert_eq!(id, "pocketid");
        assert_eq!(provider.auth_url, "https://idp.example.com/authorize");
        assert_eq!(provider.token_url, "https://idp.example.com/api/oidc/token");
        assert_eq!(
            provider.user_info_url.as_deref(),
            Some("https://idp.example.com/api/oidc/userinfo")
        );
        assert_eq!(provider.scopes, vec!["openid", "profile", "email"]);
        assert!(
            provider.disable_sign_up,
            "settings.disable_sign_up must carry through to the resolved OAuthProvider"
        );
    }

    #[tokio::test]
    async fn resolve_providers_skips_a_discovery_document_with_a_cleartext_endpoint() {
        // A non-loopback `http://` endpoint would send `client_secret` and
        // access tokens over the wire in the open — same outcome as an
        // unreachable issuer: log it and don't offer the provider, rather
        // than fail the whole auth stack over one bad/compromised discovery
        // document.
        let issuer = start_discovery_mock(serde_json::json!({
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "http://idp.example.com/api/oidc/token",
            "userinfo_endpoint": "https://idp.example.com/api/oidc/userinfo",
        }))
        .await;

        let configured = vec![OidcProviderSettings {
            id: "insecure".to_string(),
            name: "Insecure".to_string(),
            issuer,
            client_id: "client-id".to_string(),
            client_secret: "client-secret".to_string(),
            scopes: Vec::new(),
            disable_sign_up: false,
            trust_unverified_email: false,
        }];

        let resolved = resolve_providers(&configured).await;

        assert!(resolved.is_empty());
    }

    #[test]
    fn is_https_or_loopback_accepts_https_and_loopback_http() {
        assert!(is_https_or_loopback("https://idp.example.com/token"));
        assert!(is_https_or_loopback("http://127.0.0.1:8080/token"));
        assert!(is_https_or_loopback("http://localhost:8080/token"));
        assert!(is_https_or_loopback("http://[::1]:8080/token"));
    }

    #[test]
    fn is_https_or_loopback_rejects_cleartext_non_loopback_and_other_schemes() {
        assert!(!is_https_or_loopback("http://idp.example.com/token"));
        assert!(!is_https_or_loopback("ftp://idp.example.com/token"));
        assert!(!is_https_or_loopback("not a url"));
    }

    #[test]
    fn map_user_info_reads_standard_oidc_claims() {
        let claims = serde_json::json!({
            "sub": "user-123",
            "email": "person@example.com",
            "name": "Person Name",
            "picture": "https://idp.example.com/avatar.png",
            "email_verified": true,
        });

        let info = map_user_info(claims).unwrap();

        assert_eq!(info.id, "user-123");
        assert_eq!(info.email, "person@example.com");
        assert_eq!(info.name.as_deref(), Some("Person Name"));
        assert_eq!(
            info.image.as_deref(),
            Some("https://idp.example.com/avatar.png")
        );
        assert!(info.email_verified);
    }

    #[test]
    fn map_user_info_falls_back_to_preferred_username_for_name() {
        let claims = serde_json::json!({
            "sub": "user-123",
            "email": "person@example.com",
            "preferred_username": "person",
        });

        let info = map_user_info(claims).unwrap();

        assert_eq!(info.name.as_deref(), Some("person"));
        assert!(!info.email_verified);
    }

    #[test]
    fn map_user_info_rejects_missing_sub() {
        let claims = serde_json::json!({ "email": "person@example.com" });

        assert_eq!(map_user_info(claims).unwrap_err(), "missing sub");
    }

    #[test]
    fn map_user_info_rejects_missing_email() {
        let claims = serde_json::json!({ "sub": "user-123" });

        assert_eq!(map_user_info(claims).unwrap_err(), "missing email");
    }
}
