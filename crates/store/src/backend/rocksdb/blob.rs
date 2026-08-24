/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::ops::Range;

use crate::stream::BlobReadStream;

use super::{CF_BLOBS, RocksDbStore, into_error};

impl RocksDbStore {
    async fn get_blob_inner(
        &self,
        key: &[u8],
        range: Range<usize>,
    ) -> trc::Result<Option<Vec<u8>>> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            db.get_pinned_cf(&db.cf_handle(CF_BLOBS).unwrap(), key)
                .map(|obj| {
                    obj.map(|bytes| {
                        if range.start == 0 && range.end == usize::MAX {
                            bytes.to_vec()
                        } else {
                            bytes
                                .get(range.start..std::cmp::min(bytes.len(), range.end))
                                .unwrap_or_default()
                                .to_vec()
                        }
                    })
                })
                .map_err(into_error)
        })
        .await
    }

    pub(crate) async fn get_blob(
        &self,
        key: &[u8],
        mut range: Range<u64>,
    ) -> trc::Result<Option<BlobReadStream>> {
        range.start = range.start.min(usize::MAX as u64);
        range.end = range.end.min(usize::MAX as u64);
        let range = (range.start as usize)..(range.end as usize);
        Ok(self
            .get_blob_inner(key, range)
            .await?
            .map(|blob_data| BlobReadStream::Bytes(blob_data.into())))
    }

    pub(crate) async fn get_blob_length(&self, key: &[u8]) -> trc::Result<Option<u64>> {
        self.get_blob_inner(key, 0..usize::MAX)
            .await
            .map(|maybe_bytes| maybe_bytes.map(|bytes| bytes.len() as u64))
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: &[u8]) -> trc::Result<()> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            db.put_cf(&db.cf_handle(CF_BLOBS).unwrap(), key, data)
                .map_err(into_error)
        })
        .await
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            db.delete_cf(&db.cf_handle(CF_BLOBS).unwrap(), key)
                .map_err(into_error)
                .map(|_| true)
        })
        .await
    }
}
