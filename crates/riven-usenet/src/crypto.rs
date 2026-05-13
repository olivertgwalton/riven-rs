//! RAR5 password-derived AES-256-CBC decryption.
//!
//! RAR5 encrypts the file data area as a single AES-256-CBC stream
//! starting with the per-file IV in the encryption record. The key is
//! derived from the user's password via PBKDF2-HMAC-SHA256 over the
//! per-file salt, with `1 << log2_count` iterations.
//!
//! For random-access reads we exploit CBC's "decrypting block N needs
//! ciphertext block N-1" property: to decrypt the contiguous ciphertext
//! range starting at offset `start`, the predecessor ciphertext block at
//! `start - 16` serves as the chaining IV. For `start == 0` we use the
//! encryption record's initial IV.

use aes::Aes256;
use cbc::Decryptor;
use cbc::cipher::{Array, BlockModeDecrypt, KeyIvInit};
use hmac::Hmac;
use sha2::Sha256;

/// AES-256 block size in bytes.
pub const AES_BLOCK: usize = 16;

/// PBKDF2-HMAC-SHA256(password, salt, 1 << log2_count) → 32-byte key.
pub fn derive_key(password: &str, salt: &[u8; 16], log2_count: u8) -> [u8; 32] {
    let iterations = 1u32 << log2_count;
    let mut key = [0u8; 32];
    pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), salt, iterations, &mut key)
        .expect("pbkdf2 with non-zero iterations and 32-byte output");
    key
}

/// Decrypt `ciphertext` (whose length must be a multiple of `AES_BLOCK`)
/// using AES-256-CBC with the given key and `iv`. The decrypted plaintext
/// is written back into `ciphertext` in place.
pub fn decrypt_blocks_in_place(
    key: &[u8; 32],
    iv: &[u8; 16],
    ciphertext: &mut [u8],
) -> Result<(), CryptoError> {
    if ciphertext.len() % AES_BLOCK != 0 {
        return Err(CryptoError::UnalignedCiphertext {
            len: ciphertext.len(),
        });
    }
    if ciphertext.is_empty() {
        return Ok(());
    }
    let mut dec = Decryptor::<Aes256>::new(key.into(), iv.into());
    // Reinterpret &mut [u8] as &mut [Array<u8, U16>]. Layout is identical
    // (Array is `#[repr(transparent)]` wrapping `[u8; N]`).
    let block_count = ciphertext.len() / AES_BLOCK;
    let blocks: &mut [Array<u8, _>] = unsafe {
        std::slice::from_raw_parts_mut(ciphertext.as_mut_ptr().cast(), block_count)
    };
    dec.decrypt_blocks(blocks);
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("ciphertext length {len} is not a multiple of {AES_BLOCK}")]
    UnalignedCiphertext { len: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use cbc::Encryptor;
    use cbc::cipher::BlockModeEncrypt;

    #[test]
    fn cbc_round_trip() {
        let key = derive_key("hunter2", &[7u8; 16], 4); // 2^4 = 16 iters
        let iv = [3u8; 16];
        let plaintext = b"sixteen-byte-blo".to_vec();
        assert_eq!(plaintext.len() % AES_BLOCK, 0);

        let mut ct = plaintext.clone();
        {
            let mut enc = Encryptor::<Aes256>::new((&key).into(), (&iv).into());
            let count = ct.len() / AES_BLOCK;
            let blocks: &mut [Array<u8, _>] = unsafe {
                std::slice::from_raw_parts_mut(ct.as_mut_ptr().cast(), count)
            };
            enc.encrypt_blocks(blocks);
        }
        assert_ne!(ct, plaintext);

        decrypt_blocks_in_place(&key, &iv, &mut ct).unwrap();
        assert_eq!(ct, plaintext);
    }

    #[test]
    fn rejects_unaligned() {
        let key = [0u8; 32];
        let iv = [0u8; 16];
        let mut data = vec![0u8; 17];
        let err = decrypt_blocks_in_place(&key, &iv, &mut data).unwrap_err();
        assert!(matches!(err, CryptoError::UnalignedCiphertext { len: 17 }));
    }
}
