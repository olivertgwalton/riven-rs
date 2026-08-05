use serde::{Deserialize, Deserializer, Serialize};

/// One configured OIDC identity provider (PocketID, Authelia, Keycloak,
/// Authentik, Zitadel, or any other spec-compliant issuer).
///
/// Riven never hardcodes a provider's endpoint layout — `issuer` is resolved
/// to `authorization_endpoint`/`token_endpoint`/`userinfo_endpoint` via
/// `{issuer}/.well-known/openid-configuration` at startup, which is what
/// makes one implementation work across providers whose endpoint paths
/// differ (PocketID's token endpoint is `/api/oidc/token`; others use
/// `/oauth2/token`, `/token`, and so on).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OidcProviderSettings {
    /// Becomes both the callback path segment (`/auth/callback/{id}`) and the
    /// linked account's `provider_id` — changing it after users have signed
    /// in orphans their existing links.
    pub id: String,
    /// Shown on the login button, e.g. "PocketID". Falls back to `id` when
    /// empty.
    #[serde(default)]
    pub name: String,
    /// The provider's issuer origin, e.g. `https://pocketid.example.com`
    /// (no trailing slash or path).
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    /// Defaults to `["openid", "profile", "email"]` when empty — the claims
    /// riven actually reads (`sub`, `email`, `name`, `picture`,
    /// `email_verified`) all come from those three.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// When `true`, a sign-in from this provider only succeeds if it matches
    /// an *existing* riven account (by email) — it never creates one.
    /// Default `false`: a first-time sign-in registers a new account, the
    /// same way the built-in password/Plex sign-in has always worked. Flip
    /// this on for a provider whose own user base is broader than who should
    /// have riven access, so provisioning goes through an admin (Users →
    /// Create User) rather than "anyone who can authenticate against the
    /// issuer gets in."
    #[serde(default)]
    pub disable_sign_up: bool,
}

impl OidcProviderSettings {
    /// The button label: `name` when set, otherwise `id`.
    pub fn display_name(&self) -> &str {
        if self.name.trim().is_empty() {
            &self.id
        } else {
            &self.name
        }
    }

    /// `scopes`, or the OIDC-standard default when unset.
    pub fn effective_scopes(&self) -> Vec<String> {
        if self.scopes.is_empty() {
            vec![
                "openid".to_string(),
                "profile".to_string(),
                "email".to_string(),
            ]
        } else {
            self.scopes.clone()
        }
    }
}

/// Accepts a native sequence (from `Serialized::defaults` and the like) or a
/// JSON-encoded string (from `figment`'s `Env` provider: its value coercion
/// speaks TOML, not JSON, so a JSON object's `"key": value` never matches
/// TOML's `key = value` inline-table syntax and arrives here as an
/// undeserialized string instead of a sequence).
pub(super) fn deserialize_providers<'de, D>(
    deserializer: D,
) -> Result<Vec<OidcProviderSettings>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrSeq {
        String(String),
        Seq(Vec<OidcProviderSettings>),
    }

    match StringOrSeq::deserialize(deserializer)? {
        StringOrSeq::String(s) if s.trim().is_empty() => Ok(Vec::new()),
        StringOrSeq::String(s) => serde_json::from_str(&s).map_err(serde::de::Error::custom),
        StringOrSeq::Seq(providers) => Ok(providers),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(name: &str) -> OidcProviderSettings {
        OidcProviderSettings {
            id: "pocketid".to_string(),
            name: name.to_string(),
            issuer: "https://pocketid.example.com".to_string(),
            client_id: "client-id".to_string(),
            client_secret: "client-secret".to_string(),
            scopes: Vec::new(),
            disable_sign_up: false,
        }
    }

    #[test]
    fn display_name_falls_back_to_id_when_name_is_blank() {
        assert_eq!(provider("").display_name(), "pocketid");
        assert_eq!(provider("   ").display_name(), "pocketid");
        assert_eq!(provider("PocketID").display_name(), "PocketID");
    }

    #[test]
    fn effective_scopes_defaults_to_oidc_standard_set() {
        assert_eq!(
            provider("PocketID").effective_scopes(),
            vec!["openid", "profile", "email"]
        );
    }

    #[test]
    fn effective_scopes_respects_explicit_list() {
        let mut settings = provider("PocketID");
        settings.scopes = vec!["openid".to_string(), "groups".to_string()];

        assert_eq!(settings.effective_scopes(), vec!["openid", "groups"]);
    }
}
