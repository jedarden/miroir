//! Admin session cookie sealing (plan §9, §13.19).
//!
//! Seals the admin session ID using XChaCha20-Poly1305 AEAD so that:
//! - The session ID is encrypted (confidentiality).
//! - The ciphertext is authenticated (integrity — any tampering is detected).
//!
//! Cookie wire format (base64 of):
//!   [24-byte XNonce][ciphertext + 16-byte Poly1305 tag]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    XChaCha20Poly1305, XNonce,
};
use rand::RngCore;
use subtle::ConstantTimeEq;
use tracing::warn;

/// Cookie name for the sealed admin session.
pub const COOKIE_NAME: &str = "miroir_admin_session";

/// Required key length: 32 bytes for XChaCha20-Poly1305.
pub const KEY_LEN: usize = 32;

/// Nonce length for XChaCha20-Poly1305.
const NONCE_LEN: usize = 24;

/// Tag length appended by XChaCha20-Poly1305.
const TAG_LEN: usize = 16;

/// Admin session seal key — 32 bytes loaded from env or generated randomly.
#[derive(Clone)]
pub struct SealKey {
    key: [u8; KEY_LEN],
    /// Whether the key was generated at startup (not from env).
    generated: bool,
}

impl std::fmt::Debug for SealKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealKey")
            .field("generated", &self.generated)
            .finish_non_exhaustive()
    }
}

impl SealKey {
    /// Load the seal key from a raw 32-byte value.
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self {
            key: bytes,
            generated: false,
        }
    }

    /// Load from a base64-encoded string (the env var value).
    /// Returns `None` if the value is invalid (wrong length after decoding).
    pub fn from_base64(value: &str) -> Option<Self> {
        let decoded = URL_SAFE_NO_PAD.decode(value).ok()?;
        if decoded.len() != KEY_LEN {
            return None;
        }
        let mut key = [0u8; KEY_LEN];
        key.copy_from_slice(&decoded);
        Some(Self {
            key,
            generated: false,
        })
    }

    /// Generate a random key at startup. Logs a warning about multi-pod deployments.
    pub fn generate_random() -> Self {
        let mut key = [0u8; KEY_LEN];
        rand::rngs::OsRng.fill_bytes(&mut key);
        warn!(
            "generated random ADMIN_SESSION_SEAL_KEY; multi-pod deployments must set this \
             manually to a shared value"
        );
        Self {
            key,
            generated: true,
        }
    }

    /// Load from environment variable, falling back to random generation.
    pub fn from_env_or_generate() -> Self {
        match std::env::var("ADMIN_SESSION_SEAL_KEY") {
            Ok(val) if !val.is_empty() => {
                if let Some(key) = Self::from_base64(&val) {
                    key
                } else {
                    warn!(
                        "ADMIN_SESSION_SEAL_KEY is set but not valid base64-encoded {}-byte key; \
                         generating random key",
                        KEY_LEN
                    );
                    Self::generate_random()
                }
            }
            _ => Self::generate_random(),
        }
    }

    /// Whether the key was generated at startup rather than loaded from env.
    pub fn is_generated(&self) -> bool {
        self.generated
    }

    /// Constant-time equality check between two seal keys.
    pub fn ct_eq(&self, other: &Self) -> bool {
        self.key.ct_eq(&other.key).into()
    }
}

/// Sealed cookie value — the opaque blob stored in the cookie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedCookie {
    nonce: [u8; NONCE_LEN],
    ciphertext: Vec<u8>,
}

impl SealedCookie {
    /// Seal a session ID using the given key.
    pub fn seal(session_id: &str, key: &SealKey) -> Result<Self, SealError> {
        let cipher = XChaCha20Poly1305::new_from_slice(&key.key)
            .map_err(|_| SealError::KeyError)?;

        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, session_id.as_bytes())
            .map_err(|_| SealError::EncryptError)?;

        Ok(Self {
            nonce: nonce_bytes,
            ciphertext,
        })
    }

    /// Unseal a cookie value, returning the plaintext session ID.
    pub fn unseal(&self, key: &SealKey) -> Result<String, SealError> {
        let cipher = XChaCha20Poly1305::new_from_slice(&key.key)
            .map_err(|_| SealError::KeyError)?;

        let nonce = XNonce::from_slice(&self.nonce);
        let plaintext = cipher
            .decrypt(nonce, self.ciphertext.as_slice())
            .map_err(|_| SealError::DecryptError)?;

        String::from_utf8(plaintext).map_err(|_| SealError::InvalidUtf8)
    }

    /// Encode to base64 for cookie storage.
    pub fn encode(&self) -> String {
        let mut buf = Vec::with_capacity(NONCE_LEN + self.ciphertext.len());
        buf.extend_from_slice(&self.nonce);
        buf.extend_from_slice(&self.ciphertext);
        URL_SAFE_NO_PAD.encode(&buf)
    }

    /// Decode from base64 cookie value.
    pub fn decode(value: &str) -> Result<Self, SealError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| SealError::MalformedCookie)?;

        // Minimum size: nonce + 1 byte plaintext + tag
        if bytes.len() < NONCE_LEN + 1 + TAG_LEN {
            return Err(SealError::MalformedCookie);
        }

        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&bytes[..NONCE_LEN]);
        let ciphertext = bytes[NONCE_LEN..].to_vec();

        Ok(Self { nonce, ciphertext })
    }
}

/// Seal a session ID into a base64 cookie value.
pub fn seal_session(session_id: &str, key: &SealKey) -> Result<String, SealError> {
    let sealed = SealedCookie::seal(session_id, key)?;
    Ok(sealed.encode())
}

/// Unseal a base64 cookie value into a session ID.
pub fn unseal_session(cookie_value: &str, key: &SealKey) -> Result<String, SealError> {
    let sealed = SealedCookie::decode(cookie_value)?;
    sealed.unseal(key)
}

/// Errors during seal/unseal operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealError {
    /// The seal key is invalid.
    KeyError,
    /// Encryption failed.
    EncryptError,
    /// Decryption failed — wrong key or tampered ciphertext.
    DecryptError,
    /// The cookie value is malformed (wrong length, bad base64).
    MalformedCookie,
    /// The decrypted plaintext is not valid UTF-8.
    InvalidUtf8,
}

impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SealError::KeyError => write!(f, "invalid seal key"),
            SealError::EncryptError => write!(f, "encryption failed"),
            SealError::DecryptError => write!(f, "decryption failed — wrong key or tampered cookie"),
            SealError::MalformedCookie => write!(f, "malformed cookie value"),
            SealError::InvalidUtf8 => write!(f, "decrypted value is not valid UTF-8"),
        }
    }
}

impl std::error::Error for SealError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> SealKey {
        SealKey::from_bytes([42u8; KEY_LEN])
    }

    fn different_key() -> SealKey {
        SealKey::from_bytes([99u8; KEY_LEN])
    }

    #[test]
    fn seal_unseal_roundtrip() {
        let key = test_key();
        let session_id = "sess_abc123def456";
        let sealed = seal_session(session_id, &key).unwrap();
        let unsealed = unseal_session(&sealed, &key).unwrap();
        assert_eq!(unsealed, session_id);
    }

    #[test]
    fn seal_produces_different_ciphertexts() {
        let key = test_key();
        let session_id = "sess_same_value";
        let sealed1 = seal_session(session_id, &key).unwrap();
        let sealed2 = seal_session(session_id, &key).unwrap();
        // Different nonces produce different ciphertexts
        assert_ne!(sealed1, sealed2);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = test_key();
        let key2 = different_key();
        let sealed = seal_session("sess_test", &key1).unwrap();
        let result = unseal_session(&sealed, &key2);
        assert_eq!(result.unwrap_err(), SealError::DecryptError);
    }

    #[test]
    fn tampered_cookie_fails() {
        let key = test_key();
        let sealed = seal_session("sess_test", &key).unwrap();
        // Tamper with one byte
        let decoded = URL_SAFE_NO_PAD.decode(&sealed).unwrap();
        let mut tampered = decoded;
        tampered[30] ^= 0xFF;
        let tampered_b64 = URL_SAFE_NO_PAD.encode(&tampered);
        let result = unseal_session(&tampered_b64, &key);
        assert_eq!(result.unwrap_err(), SealError::DecryptError);
    }

    #[test]
    fn malformed_cookie_fails() {
        let key = test_key();
        assert_eq!(
            unseal_session("not-valid-base64!!!", &key).unwrap_err(),
            SealError::MalformedCookie
        );
        assert_eq!(
            unseal_session("", &key).unwrap_err(),
            SealError::MalformedCookie
        );
    }

    #[test]
    fn too_short_cookie_fails() {
        let key = test_key();
        // Only 10 bytes — shorter than nonce + tag
        let short = URL_SAFE_NO_PAD.encode(&[0u8; 10]);
        assert_eq!(
            unseal_session(&short, &key).unwrap_err(),
            SealError::MalformedCookie
        );
    }

    #[test]
    fn cookie_structure_is_nonce_plus_ciphertext() {
        let key = test_key();
        let session_id = "sess_12345";
        let sealed = SealedCookie::seal(session_id, &key).unwrap();
        let encoded = sealed.encode();
        let decoded_bytes = URL_SAFE_NO_PAD.decode(&encoded).unwrap();

        // Structure: [24-byte nonce][ciphertext + 16-byte tag]
        assert_eq!(decoded_bytes.len(), NONCE_LEN + session_id.len() + TAG_LEN);

        // First 24 bytes are the nonce
        let nonce = &decoded_bytes[..NONCE_LEN];
        assert_eq!(nonce.len(), NONCE_LEN);
        assert_ne!(nonce, &[0u8; NONCE_LEN]); // should be random
    }

    #[test]
    fn seal_key_from_base64() {
        let raw = [77u8; KEY_LEN];
        let b64 = URL_SAFE_NO_PAD.encode(raw);
        let key = SealKey::from_base64(&b64).unwrap();
        assert!(!key.is_generated());
        assert!(key.ct_eq(&SealKey::from_bytes(raw)));
    }

    #[test]
    fn seal_key_from_base64_wrong_length() {
        let b64 = URL_SAFE_NO_PAD.encode([0u8; 16]);
        assert!(SealKey::from_base64(&b64).is_none());
    }

    #[test]
    fn seal_key_from_base64_invalid() {
        assert!(SealKey::from_base64("!!!not-base64!!!").is_none());
    }

    #[test]
    fn generated_key_is_flagged() {
        let key = SealKey::generate_random();
        assert!(key.is_generated());
    }

    #[test]
    fn seal_unseal_long_session_id() {
        let key = test_key();
        let session_id = "sess_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let sealed = seal_session(session_id, &key).unwrap();
        let unsealed = unseal_session(&sealed, &key).unwrap();
        assert_eq!(unsealed, session_id);
    }

    #[test]
    fn cross_pod_same_key_succeeds() {
        // Simulate two pods sharing the same key
        let raw = [123u8; KEY_LEN];
        let pod_a_key = SealKey::from_bytes(raw);
        let pod_b_key = SealKey::from_bytes(raw);

        let sealed = seal_session("sess_cross_pod", &pod_a_key).unwrap();
        let unsealed = unseal_session(&sealed, &pod_b_key).unwrap();
        assert_eq!(unsealed, "sess_cross_pod");
    }

    #[test]
    fn cross_pod_different_keys_fails() {
        let pod_a_key = SealKey::from_bytes([1u8; KEY_LEN]);
        let pod_b_key = SealKey::from_bytes([2u8; KEY_LEN]);

        let sealed = seal_session("sess_cross_pod", &pod_a_key).unwrap();
        let result = unseal_session(&sealed, &pod_b_key);
        assert_eq!(result.unwrap_err(), SealError::DecryptError);
    }
}
