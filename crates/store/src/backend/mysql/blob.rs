/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::ops::Range;

use mysql_async::prelude::Queryable;

use crate::stream::BlobReadStream;

use super::{MysqlStore, into_error};

impl MysqlStore {
    pub(crate) async fn get_blob(
        &self,
        key: &[u8],
        mut range: Range<u64>,
    ) -> trc::Result<Option<BlobReadStream>> {
        range.start = range.start.min(usize::MAX as u64);
        range.end = range.end.min(usize::MAX as u64);
        let range = (range.start as usize)..(range.end as usize);
        let mut conn = self.conn_pool.get_conn().await.map_err(into_error)?;
        let s = conn
            .prep("SELECT v FROM t WHERE k = ?")
            .await
            .map_err(into_error)?;
        let maybe_bytes = conn
            .exec_first::<Vec<u8>, _, _>(&s, (key,))
            .await
            .map(|bytes| {
                if range.start == 0 && range.end == usize::MAX {
                    bytes
                } else {
                    bytes.map(|bytes| {
                        bytes
                            .get(range.start..std::cmp::min(bytes.len(), range.end))
                            .unwrap_or_default()
                            .to_vec()
                    })
                }
            })
            .map_err(into_error)?;
        Ok(maybe_bytes.map(|bytes| BlobReadStream::Bytes(bytes.into())))
    }

    pub(crate) async fn get_blob_length(&self, key: &[u8]) -> trc::Result<Option<u64>> {
        let mut conn = self.conn_pool.get_conn().await.map_err(into_error)?;
        let s = conn
            .prep("SELECT OCTET_LENGTH(v) FROM t WHERE k = ?")
            .await
            .map_err(into_error)?;
        conn.exec_first::<i64, _, _>(&s, (key,))
            .await
            .map(|maybe_length| maybe_length.map(|length| length as u64))
            .map_err(into_error)
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: &[u8]) -> trc::Result<()> {
        let mut conn = self.conn_pool.get_conn().await.map_err(into_error)?;
        let s = conn
            .prep("INSERT INTO t (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)")
            .await
            .map_err(into_error)?;
        conn.exec_drop(&s, (key, data))
            .await
            .map_err(into_error)
            .map(|_| ())
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let mut conn = self.conn_pool.get_conn().await.map_err(into_error)?;
        let s = conn
            .prep("DELETE FROM t WHERE k = ?")
            .await
            .map_err(into_error)?;
        conn.exec_iter(&s, (key,))
            .await
            .map_err(into_error)
            .map(|hits| hits.affected_rows() > 0)
    }
}
