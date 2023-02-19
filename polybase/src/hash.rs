use winter_crypto::hashers::Rp64_256;
use winter_crypto::{Digest, Hasher};

pub fn hash(b: Vec<u8>) -> <Rp64_256 as Hasher>::Digest {
    winter_crypto::hashers::Rp64_256::hash(&pad_byte_vector(b))
}

pub fn hash_bytes(b: Vec<u8>) -> [u8; 32] {
    winter_crypto::hashers::Rp64_256::hash(&pad_byte_vector(b)).as_bytes()
}

fn pad_byte_vector(mut bytes: Vec<u8>) -> Vec<u8> {
    let len = bytes.len();
    let rem = len % 7;
    if rem != 0 {
        let pad_len = 7 - rem;
        bytes.resize(len + pad_len, 0);
    }
    bytes
}
