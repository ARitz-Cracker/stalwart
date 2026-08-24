/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncSeek, AsyncSeekExt};

pub const BLOB_HASH_LEN: usize = 32;

#[derive(
    rkyv::Archive,
    rkyv::Deserialize,
    rkyv::Serialize,
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
#[repr(transparent)]
pub struct BlobHash(pub [u8; BLOB_HASH_LEN]);

impl BlobHash {
    pub fn new_max() -> Self {
        BlobHash([u8::MAX; BLOB_HASH_LEN])
    }

    pub fn generate(value: impl AsRef<[u8]>) -> Self {
        BlobHash(blake3::hash(value.as_ref()).into())
    }
    async fn generate_from_stream_inner<R: AsyncRead + AsyncSeek + Unpin>(
        readable: &mut R,
    ) -> std::io::Result<Self> {
        readable.rewind().await?;
        let mut hasher = blake3::Hasher::new();
        let mut read_buffer = [0u8; 8192];
        loop {
            let bytes_read = readable.read(&mut read_buffer).await?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&read_buffer[0..bytes_read]);
        }
        readable.rewind().await?;
        Ok(BlobHash(hasher.finalize().into()))
    }
    pub async fn generate_from_stream<R: AsyncRead + AsyncSeek + Unpin>(
        readable: &mut R,
    ) -> trc::Result<Self> {
        Self::generate_from_stream_inner(readable)
            .await
            .map_err(|err| trc::StoreEvent::BlobWrite.reason(err))
    }

    pub fn try_from_hash_slice(value: &[u8]) -> Result<BlobHash, std::array::TryFromSliceError> {
        value.try_into().map(BlobHash)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(BLOB_HASH_LEN * 2);
        for byte in self.0.iter() {
            hex.push_str(&format!("{:02x}", byte));
        }
        hex
    }

    pub fn is_empty(&self) -> bool {
        self.0 == [0; BLOB_HASH_LEN]
    }
}

impl From<&ArchivedBlobHash> for BlobHash {
    fn from(value: &ArchivedBlobHash) -> Self {
        BlobHash(value.0)
    }
}

impl AsRef<BlobHash> for BlobHash {
    fn as_ref(&self) -> &BlobHash {
        self
    }
}

impl From<BlobHash> for Vec<u8> {
    fn from(value: BlobHash) -> Self {
        value.0.to_vec()
    }
}

impl AsRef<[u8]> for BlobHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for BlobHash {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}
