/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::ops::Range;

use rusqlite::OptionalExtension;

use crate::stream::BlobReadStream;

use super::{SqliteStore, into_error};

impl SqliteStore {
    pub(crate) async fn get_blob(
        &self,
        key: &[u8],
        mut range: Range<u64>,
    ) -> trc::Result<Option<BlobReadStream>> {
        range.start = range.start.min(usize::MAX as u64);
        range.end = range.end.min(usize::MAX as u64);
        let range = (range.start as usize)..(range.end as usize);
        let manager = self.conn_pool.clone();
        let maybe_bytes = self
            .spawn_worker(move || {
                let conn = manager.get().map_err(into_error)?;
                let mut result = conn
                    .prepare_cached("SELECT v FROM t WHERE k = ?")
                    .map_err(into_error)?;
                result
                    .query_row([&key], |row| {
                        Ok({
                            let bytes = row.get_ref(0)?.as_bytes()?;
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
                    .optional()
                    .map_err(into_error)
            })
            .await?;
        Ok(maybe_bytes.map(|bytes| BlobReadStream::Bytes(bytes.into())))
    }

    pub(crate) async fn get_blob_length(&self, key: &[u8]) -> trc::Result<Option<u64>> {
        let manager = self.conn_pool.clone();
        self.spawn_worker(move || {
            let conn = manager.get().map_err(into_error)?;
            let mut result = conn
                .prepare_cached("SELECT OCTET_LENGTH(v) FROM t WHERE k = ?")
                .map_err(into_error)?;
            result
                .query_row([&key], |row| Ok(row.get_ref(0)?.as_i64()? as u64))
                .optional()
                .map_err(into_error)
        })
        .await
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: &[u8]) -> trc::Result<()> {
        let manager = self.conn_pool.clone();
        self.spawn_worker(move || {
            let conn = manager.get().map_err(into_error)?;
            conn.prepare_cached("INSERT OR REPLACE INTO t (k, v) VALUES (?, ?)")
                .map_err(into_error)?
                .execute([key, data])
                .map_err(into_error)
                .map(|_| ())
        })
        .await
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let manager = self.conn_pool.clone();
        self.spawn_worker(move || {
            let conn = manager.get().map_err(into_error)?;
            conn.prepare_cached("DELETE FROM t WHERE k = ?")
                .map_err(into_error)?
                .execute([key])
                .map_err(into_error)
                .map(|_| true)
        })
        .await
    }
}
