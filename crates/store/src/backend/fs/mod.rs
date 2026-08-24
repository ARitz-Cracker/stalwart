/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{BlobStore, stream::BlobReadStream};
use registry::schema::structs;
use std::{ops::Range, path::PathBuf, sync::Arc};
use tokio::fs::{self, File};
use utils::{codec::base32_custom::Base32Writer, jumbo_bytes::JumboBytesMut};

pub struct FsStore {
    path: PathBuf,
    hash_levels: usize,
}

impl FsStore {
    pub async fn open(config: structs::FileSystemStore) -> Result<BlobStore, String> {
        let path = PathBuf::from(&config.path);
        if !path.exists() {
            fs::create_dir_all(&path)
                .await
                .map_err(|e| format!("Failed to create directory: {e}"))?;
        }

        Ok(BlobStore::Fs(Arc::new(FsStore {
            path,
            hash_levels: std::cmp::min(config.depth as usize, 5),
        })))
    }

    pub(crate) async fn get_blob(
        &self,
        key: &[u8],
        range: Range<u64>,
    ) -> trc::Result<Option<BlobReadStream>> {
        let blob_path = self.build_path(key);
        let file = File::open(&blob_path).await.map_err(into_error)?;
        if range.start == 0 && range.end == u64::MAX {
            Ok(Some(BlobReadStream::File(file)))
        } else {
            Ok(Some(BlobReadStream::file_range(file, range).await?))
        }
    }

    pub(crate) async fn get_blob_length(&self, key: &[u8]) -> trc::Result<Option<u64>> {
        let blob_path = self.build_path(key);
        match fs::metadata(&blob_path).await {
            Ok(m) => Ok(Some(m.len())),
            Err(_) => return Ok(None),
        }
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: JumboBytesMut) -> trc::Result<()> {
        let blob_path = self.build_path(key);

        if fs::metadata(&blob_path)
            .await
            .map_or(true, |m| m.len() != data.len())
        {
            fs::create_dir_all(blob_path.parent().unwrap())
                .await
                .map_err(into_error)?;
            data.move_into_file(&blob_path).await.map_err(into_error)?;
        }

        Ok(())
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let blob_path = self.build_path(key);
        if fs::metadata(&blob_path).await.is_ok() {
            fs::remove_file(&blob_path).await.map_err(into_error)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn build_path(&self, key: &[u8]) -> PathBuf {
        let mut path = self.path.clone();

        for byte in key.iter().take(self.hash_levels) {
            path.push(format!("{:x}", byte));
        }
        path.push(Base32Writer::from_bytes(key).finalize());
        path
    }
}

fn into_error(err: std::io::Error) -> trc::Error {
    trc::StoreEvent::FilesystemError.reason(err)
}
