//! Process-wide symmetric encryption for credentials at rest.
//!
//! Used to keep plugin-settings password fields out of plaintext in
//! Postgres. Threat model:
//!   - Protects DB dumps, backups, casual `psql` / `jsonb_pretty` reads.
//!   - Does NOT protect against an attacker with both DB read and host
//!     read access — the master key file or env var would be reachable.
//!   - Plaintext lives in process memory at use-time (NNTP credentials
//!     must be sent in the clear); this is unavoidable.
//!
//! Key sourcing, in priority order:
//!   1. `RIVEN_SECRET_KEY` env var (hex-encoded 32 bytes — 64 hex chars).
//!   2. The path in `RIVEN_SECRET_KEY_PATH` (defaults to
//!      `./.riven-secret-key`). On first start the file is auto-created
//!      with a fresh random 32-byte key. Permissions are set to 0600.
//!
//! On-disk envelope: `enc:v1:<base64(nonce ‖ ciphertext ‖ tag)>`.
//! Nonce is 12 bytes (AES-GCM standard).

use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngCore;

const ENVELOPE_PREFIX: &str = "enc:v1:";
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const DEFAULT_KEY_PATH: &str = "./.riven-secret-key";

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("invalid envelope format")]
    InvalidEnvelope,
    #[error("decryption failed (wrong key, corrupt ciphertext, or tampered)")]
    DecryptionFailed,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid key length: expected {KEY_LEN}, got {got}")]
    InvalidKeyLength { got: usize },
    #[error("invalid hex in RIVEN_SECRET_KEY")]
    InvalidHex,
}

fn key() -> &'static [u8; KEY_LEN] {
    static KEY: OnceLock<[u8; KEY_LEN]> = OnceLock::new();
    KEY.get_or_init(|| {
        let loaded = load_or_create_key().unwrap_or_else(|e| {
            // We want a hard failure rather than silently disabling
            // encryption — that would write plaintext-looking strings
            // into the DB and silently regress security.
            panic!("riven-core: failed to initialize secret key: {e}");
        });
        loaded
    })
}

fn load_or_create_key() -> Result<[u8; KEY_LEN], SecretError> {
    if let Ok(hex_str) = std::env::var("RIVEN_SECRET_KEY") {
        return parse_hex_key(hex_str.trim());
    }

    let path = std::env::var("RIVEN_SECRET_KEY_PATH")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_KEY_PATH));

    if path.exists() {
        let bytes = fs::read(&path)?;
        let trimmed: Vec<u8> = bytes
            .into_iter()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        let s = std::str::from_utf8(&trimmed).map_err(|_e| SecretError::InvalidHex)?;
        return parse_hex_key(s);
    }

    let mut key = [0u8; KEY_LEN];
    rand::rng().fill_bytes(&mut key);
    let hex_str = hex_encode(&key);
    fs::write(&path, hex_str.as_bytes())?;
    set_owner_read_write_only(&path)?;
    tracing::info!(
        path = %path.display(),
        "generated new riven secret key for at-rest encryption"
    );
    Ok(key)
}

fn parse_hex_key(s: &str) -> Result<[u8; KEY_LEN], SecretError> {
    let s = s.trim();
    if s.len() != KEY_LEN * 2 {
        return Err(SecretError::InvalidKeyLength { got: s.len() / 2 });
    }
    let mut out = [0u8; KEY_LEN];
    for i in 0..KEY_LEN {
        let byte_hex = s.get(i * 2..i * 2 + 2).ok_or(SecretError::InvalidHex)?;
        out[i] = u8::from_str_radix(byte_hex, 16).map_err(|_e| SecretError::InvalidHex)?;
    }
    Ok(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(unix)]
fn set_owner_read_write_only(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_owner_read_write_only(_path: &std::path::Path) -> std::io::Result<()> {
    Ok(())
}

/// Encrypt a UTF-8 string into the `enc:v1:<base64>` envelope. Safe to
/// call before the key is initialized — the first call lazily creates
/// (or loads) it. If you want to detect encryption-already-applied to
/// avoid double-encrypting, use `is_encrypted_envelope`.
pub fn encrypt(plaintext: &str) -> Result<String, SecretError> {
    let key_arr = key();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key_arr));
    let mut nonce_bytes = [0u8; NONCE_LEN];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|_e| SecretError::DecryptionFailed)?;
    let mut combined = Vec::with_capacity(NONCE_LEN + ct.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ct);
    let b64 = BASE64.encode(&combined);
    Ok(format!("{ENVELOPE_PREFIX}{b64}"))
}

/// Decrypt an `enc:v1:<base64>` envelope. Returns the wrapped plaintext.
pub fn decrypt(envelope: &str) -> Result<String, SecretError> {
    let body = envelope
        .strip_prefix(ENVELOPE_PREFIX)
        .ok_or(SecretError::InvalidEnvelope)?;
    let combined = BASE64.decode(body).map_err(|_e| SecretError::InvalidEnvelope)?;
    if combined.len() < NONCE_LEN {
        return Err(SecretError::InvalidEnvelope);
    }
    let (nonce_bytes, ct) = combined.split_at(NONCE_LEN);
    let key_arr = key();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key_arr));
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ct)
        .map_err(|_e| SecretError::DecryptionFailed)?;
    String::from_utf8(plaintext).map_err(|_e| SecretError::DecryptionFailed)
}

/// True if `s` looks like our encryption envelope. Used by load paths to
/// decide whether a stored value needs `decrypt`-ing or is legacy
/// plaintext.
pub fn is_encrypted_envelope(s: &str) -> bool {
    s.starts_with(ENVELOPE_PREFIX)
}

/// Decrypt if encrypted, otherwise pass through. Convenient for callers
/// that handle both new and migration-pending settings without branching.
pub fn decrypt_if_encrypted(s: &str) -> Result<String, SecretError> {
    if is_encrypted_envelope(s) {
        decrypt(s)
    } else {
        Ok(s.to_string())
    }
}

/// Walk a JSON value, decrypting any string in the `enc:v1:` envelope
/// shape. Returns a fresh `Value` with the same structure but with
/// encrypted leaves replaced. Used by the settings loader so callers
/// observing structured (`dictionary` / `object`) plugin settings see
/// plaintext sub-values.
pub fn decrypt_nested(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::String(s) => {
            serde_json::Value::String(decrypt_if_encrypted(s).unwrap_or_else(|_e| s.clone()))
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(decrypt_nested).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), decrypt_nested(v)))
                .collect(),
        ),
        _ => v.clone(),
    }
}

/// Walk a JSON value and encrypt every string leaf whose path's terminal
/// key matches one of the password field names. Used by the settings
/// save path. `password_keys` is the set of field keys declared as
/// `type: "password"` in the plugin's schema (e.g. `pass`,
/// `archivepassword`). For dictionary entries the sub-field key is what
/// we match against, not the entry's name.
pub fn encrypt_password_fields(
    v: &serde_json::Value,
    password_keys: &std::collections::HashSet<String>,
) -> serde_json::Value {
    encrypt_password_fields_inner(v, password_keys, None)
}

fn encrypt_password_fields_inner(
    v: &serde_json::Value,
    password_keys: &std::collections::HashSet<String>,
    last_key: Option<&str>,
) -> serde_json::Value {
    match v {
        serde_json::Value::String(s) => {
            let is_password_slot = last_key
                .map(|k| password_keys.contains(k))
                .unwrap_or(false);
            if is_password_slot && !s.is_empty() && !is_encrypted_envelope(s) {
                match encrypt(s) {
                    Ok(enc) => serde_json::Value::String(enc),
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to encrypt password field; storing plaintext");
                        serde_json::Value::String(s.clone())
                    }
                }
            } else {
                serde_json::Value::String(s.clone())
            }
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|x| encrypt_password_fields_inner(x, password_keys, last_key))
                .collect(),
        ),
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        encrypt_password_fields_inner(v, password_keys, Some(k)),
                    )
                })
                .collect(),
        ),
        _ => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Tests share the global key, so guard them.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn with_test_key<F: FnOnce()>(f: F) {
        let _g = TEST_LOCK.lock().unwrap();
        // Force the OnceLock to initialize with a deterministic key.
        unsafe {
            std::env::set_var(
                "RIVEN_SECRET_KEY",
                "0000000000000000000000000000000000000000000000000000000000000001",
            );
        }
        f();
    }

    #[test]
    fn round_trip() {
        with_test_key(|| {
            let envelope = encrypt("hunter2").unwrap();
            assert!(envelope.starts_with(ENVELOPE_PREFIX));
            let plaintext = decrypt(&envelope).unwrap();
            assert_eq!(plaintext, "hunter2");
        });
    }

    #[test]
    fn detects_envelope() {
        assert!(is_encrypted_envelope("enc:v1:abcd"));
        assert!(!is_encrypted_envelope("plain password"));
        assert!(!is_encrypted_envelope(""));
    }

    #[test]
    fn passthrough_for_plaintext() {
        with_test_key(|| {
            assert_eq!(decrypt_if_encrypted("plain").unwrap(), "plain");
        });
    }

    #[test]
    fn rejects_tampered_envelope() {
        with_test_key(|| {
            let envelope = encrypt("hunter2").unwrap();
            let mut chars: Vec<char> = envelope.chars().collect();
            let last = chars.len() - 5;
            chars[last] = if chars[last] == 'A' { 'B' } else { 'A' };
            let tampered: String = chars.into_iter().collect();
            assert!(decrypt(&tampered).is_err());
        });
    }
}
