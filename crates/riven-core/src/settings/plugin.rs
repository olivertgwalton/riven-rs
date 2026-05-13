use std::collections::HashMap;
use std::str::FromStr;

/// Per-plugin settings, loaded from environment variables.
/// Prefix: RIVEN_PLUGIN_SETTING__{PLUGIN_PREFIX}__{KEY}
#[derive(Debug, Clone)]
pub struct PluginSettings {
    prefix: String,
    values: HashMap<String, String>,
}

impl PluginSettings {
    pub fn load(prefix: &str) -> Self {
        let env_prefix = format!("RIVEN_PLUGIN_SETTING__{prefix}__");
        let values = std::env::vars()
            .filter_map(|(key, value)| {
                key.strip_prefix(&env_prefix)
                    .map(|suffix| (normalize_key(suffix), value))
            })
            .collect();

        Self {
            prefix: prefix.to_string(),
            values,
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.values
            .get(&normalize_key(key))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
    }

    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or(default).to_string()
    }

    pub fn get_bool(&self, key: &str) -> bool {
        self.get(key)
            .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
    }

    pub fn get_parsed<T>(&self, key: &str) -> Option<T>
    where
        T: FromStr,
    {
        self.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_parsed_or<T>(&self, key: &str, default: T) -> T
    where
        T: FromStr,
    {
        self.get_parsed(key).unwrap_or(default)
    }

    pub fn get_list(&self, key: &str) -> Vec<String> {
        self.get(key)
            .map(|v| {
                serde_json::from_str::<Vec<String>>(v)
                    .unwrap_or_else(|_| v.split(',').map(|s| s.trim().to_string()).collect())
            })
            .unwrap_or_default()
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn has(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn has_effective_values(&self) -> bool {
        self.values.keys().any(|key| self.get(key).is_some())
    }

    /// Merge DB-stored settings (JSON object of string values) on top of env vars.
    /// DB values win for any key they provide.
    pub fn merge_db_override(&mut self, db_value: &serde_json::Value) {
        let Some(obj) = db_value.as_object() else {
            return;
        };

        for (key, value) in obj {
            if let Some(value) = setting_value_to_string(value) {
                self.values.insert(normalize_key(key), value);
            }
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        let map = self
            .values
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect();
        serde_json::Value::Object(map)
    }
}

#[cfg(test)]
impl PluginSettings {
    pub(super) fn from_pairs(prefix: &str, values: &[(&str, &str)]) -> Self {
        Self {
            prefix: prefix.to_string(),
            values: values
                .iter()
                .map(|(key, value)| (normalize_key(key), value.to_string()))
                .collect(),
        }
    }
}

fn normalize_key(key: &str) -> String {
    key.to_lowercase()
}

fn setting_value_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) if !s.is_empty() => {
            // Transparent decryption: `enc:v1:` envelopes are decrypted; legacy
            // plaintext-on-disk values pass through unchanged.
            Some(crate::secret::decrypt_if_encrypted(s).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "failed to decrypt setting value; passing ciphertext through");
                s.clone()
            }))
        }
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        // Arrays + objects are kept as JSON-encoded strings so plugin
        // settings can carry structured values (e.g. dictionary fields
        // representing a list of NNTP providers). Encrypted password
        // sub-fields inside these objects are decrypted at the same
        // time so callers don't have to know about the envelope.
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Some(crate::secret::decrypt_nested(value).to_string())
        }
        _ => None,
    }
}
