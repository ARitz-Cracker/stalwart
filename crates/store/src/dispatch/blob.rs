/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{
    BlobStore, CompressionAlgo, Store, U32_LEN, async_lz4::AsyncFrameEncoder,
    stream::BlobReadStream,
};
use std::{ops::Range, time::Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use trc::{AddContext, StoreEvent};
use utils::jumbo_bytes::JumboBytesMut;

const MAGIC_MARKER: u8 = 0xa0;
const LZ4_MARKER: u8 = MAGIC_MARKER | 0x01;
//const ZSTD_MARKER: u8 = MAGIC_MARKER | 0x02;
const LZ4_STREAM_MARKER: u8 = MAGIC_MARKER | 0x03;
const NONE_MARKER: u8 = 0x00;

impl BlobStore {
    async fn get_maybe_compressed_blob(
        &self,
        key: &[u8],
        range: Range<u64>,
    ) -> trc::Result<Option<BlobReadStream>> {
        match &self {
            BlobStore::Store(store) => match store {
                #[cfg(feature = "sqlite")]
                Store::SQLite(store) => store.get_blob(key, range).await,
                #[cfg(feature = "foundation")]
                Store::FoundationDb(store) => store.get_blob(key, range).await,
                #[cfg(feature = "postgres")]
                Store::PostgreSQL(store) => store.get_blob(key, range).await,
                #[cfg(feature = "mysql")]
                Store::MySQL(store) => store.get_blob(key, range).await,
                #[cfg(feature = "rocks")]
                Store::RocksDb(store) => store.get_blob(key, range).await,
                Store::Ephemeral(store) => store.get_blob(key, range).await,
                // SPDX-SnippetBegin
                // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
                // SPDX-License-Identifier: LicenseRef-SEL
                #[cfg(all(feature = "enterprise", any(feature = "postgres", feature = "mysql")))]
                Store::SQLReadReplica(store) => store.get_blob(key, range).await,
                // SPDX-SnippetEnd
                Store::None => Err(trc::StoreEvent::NotConfigured.into()),
            },
            BlobStore::Fs(store) => store.get_blob(key, range).await,
            #[cfg(feature = "s3")]
            BlobStore::S3(store) => store.get_blob(key, range).await,
            #[cfg(feature = "azure")]
            BlobStore::Azure(store) => store.get_blob(key, range).await,
            // SPDX-SnippetBegin
            // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
            // SPDX-License-Identifier: LicenseRef-SEL
            #[cfg(feature = "enterprise")]
            BlobStore::Sharded(store) => store.get_blob(key, range).await,
            // SPDX-SnippetEnd
        }
        .caused_by(trc::location!())
    }

    async fn get_maybe_compressed_blob_length(&self, key: &[u8]) -> trc::Result<Option<u64>> {
        let start_time = Instant::now();
        let result = match &self {
            BlobStore::Store(store) => match store {
                #[cfg(feature = "sqlite")]
                Store::SQLite(store) => store.get_blob_length(key).await,
                #[cfg(feature = "foundation")]
                Store::FoundationDb(store) => store.get_blob_length(key).await,
                #[cfg(feature = "postgres")]
                Store::PostgreSQL(store) => store.get_blob_length(key).await,
                #[cfg(feature = "mysql")]
                Store::MySQL(store) => store.get_blob_length(key).await,
                #[cfg(feature = "rocks")]
                Store::RocksDb(store) => store.get_blob_length(key).await,
                Store::Ephemeral(store) => store.get_blob_length(key).await,
                // SPDX-SnippetBegin
                // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
                // SPDX-License-Identifier: LicenseRef-SEL
                #[cfg(all(feature = "enterprise", any(feature = "postgres", feature = "mysql")))]
                Store::SQLReadReplica(store) => store.get_blob_length(key).await,
                // SPDX-SnippetEnd
                Store::None => Err(trc::StoreEvent::NotConfigured.into()),
            },
            BlobStore::Fs(store) => store.get_blob_length(key).await,
            #[cfg(feature = "s3")]
            BlobStore::S3(store) => store.get_blob_length(key).await,
            #[cfg(feature = "azure")]
            BlobStore::Azure(store) => store.get_blob_length(key).await,
            // SPDX-SnippetBegin
            // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
            // SPDX-License-Identifier: LicenseRef-SEL
            #[cfg(feature = "enterprise")]
            BlobStore::Sharded(store) => store.get_blob_length(key).await,
            // SPDX-SnippetEnd
        }
        .caused_by(trc::location!())?;

        trc::event!(
            Store(StoreEvent::BlobRead),
            Key = key,
            Elapsed = start_time.elapsed(),
            Size = result.unwrap_or_default(),
        );
        Ok(result)
    }

    async fn get_blob_compression_method_and_raw_length(
        &self,
        key: &[u8],
    ) -> trc::Result<Option<(u8, u64)>> {
        // Maybe we could put an LRU here?
        let Some(raw_length) = self.get_maybe_compressed_blob_length(key).await? else {
            return Ok(None);
        };
        if raw_length == 0 {
            // Adding 1 here since later code assumes that a NONE_MARKER should always subtract 1 to get the actual
            // size
            return Ok(Some((NONE_MARKER, raw_length + 1)));
        }
        let Some(single_byte_stream) = self
            .get_maybe_compressed_blob(key, (raw_length - 1)..u64::MAX)
            .await?
        else {
            trc::event!(Store(StoreEvent::BlobMissingMarker), Key = key);
            // Adding 1 here since later code assumes that a NONE_MARKER should always subtract 1 to get the actual
            // size
            return Ok(Some((NONE_MARKER, raw_length + 1)));
        };
        Ok(single_byte_stream
            .into_vec()
            .await?
            .last()
            .copied()
            .map(|cmethod| (cmethod, raw_length)))
    }

    pub async fn get_blob_length(&self, key: &[u8]) -> trc::Result<Option<u64>> {
        match self.get_blob_compression_method_and_raw_length(key).await? {
            None => Ok(None),
            Some((NONE_MARKER, raw_length)) => Ok(Some(raw_length.saturating_sub(1))),
            Some((LZ4_MARKER, raw_length)) => {
                if raw_length < 6 {
                    // impossible. 4 bytes size prefix + minimal lz4 data (1 byte when encoding &[]) + marker is
                    // 6 bytes
                    return Ok(Some(raw_length));
                }
                let Some(length_bytes) = self.get_maybe_compressed_blob(key, 0..4).await? else {
                    trc::event!(Store(StoreEvent::BlobMissingMarker), Key = key);
                    return Ok(None);
                };
                let Ok(length_bytes) = <[u8; 4]>::try_from(length_bytes.into_vec().await?) else {
                    trc::event!(Store(StoreEvent::BlobMissingMarker), Key = key);
                    return Ok(None);
                };
                Ok(Some(u32::from_le_bytes(length_bytes) as u64))
            }
            Some((LZ4_STREAM_MARKER, raw_length)) => {
                if raw_length < 10 {
                    // impossible. 8 bytes size prefix + minimal lz4 data (1 byte when encoding &[]) + marker is
                    // 10 bytes
                    return Ok(Some(raw_length));
                }
                let Some(length_bytes) = self.get_maybe_compressed_blob(key, 0..8).await? else {
                    trc::event!(Store(StoreEvent::BlobMissingMarker), Key = key);
                    return Ok(None);
                };
                let Ok(length_bytes) = <[u8; 8]>::try_from(length_bytes.into_vec().await?) else {
                    trc::event!(Store(StoreEvent::BlobMissingMarker), Key = key);
                    return Ok(None);
                };
                Ok(Some(u64::from_le_bytes(length_bytes)))
            }
            Some((_, raw_length)) => {
                trc::event!(Store(StoreEvent::BlobMissingMarker), Key = key);
                Ok(Some(raw_length))
            }
        }
    }

    pub async fn get_blob(
        &self,
        key: &[u8],
        mut range: Range<u64>,
    ) -> trc::Result<Option<(BlobReadStream, u64)>> {
        let start_time = Instant::now();
        let blob_and_size: Option<(BlobReadStream, u64)> =
            match self.get_blob_compression_method_and_raw_length(key).await? {
                None => None,
                Some((NONE_MARKER, raw_length)) => {
                    if range.end >= raw_length {
                        range.end = raw_length.saturating_sub(1);
                    }
                    if range.start >= range.end {
                        Some((BlobReadStream::Bytes(Vec::new().into()), 0))
                    } else {
                        let blob_length = range.end.saturating_sub(range.start);
                        self.get_maybe_compressed_blob(key, range.clone())
                            .await?
                            .map(|blob| (blob, blob_length))
                    }
                }
                Some((LZ4_MARKER, raw_length)) if raw_length >= 6 => {
                    // We need to decompress the whole thing in one go
                    let Some(compressed_blob) = self
                        .get_maybe_compressed_blob(key, 0..(raw_length - 1))
                        .await?
                    else {
                        return Ok(None);
                    };
                    let decompressed_blob =
                        lz4_flex::decompress_size_prepended(&compressed_blob.into_vec().await?)
                            .map_err(|err| {
                                trc::StoreEvent::DecompressError
                                    .reason(err)
                                    .ctx(trc::Key::Key, key)
                                    .ctx(trc::Key::CausedBy, trc::location!())
                            })?;
                    let result_length = range
                        .end
                        .saturating_sub(range.start)
                        .min(decompressed_blob.len() as u64);
                    Some((
                        BlobReadStream::byte_range(decompressed_blob.into(), range).await?,
                        result_length,
                    ))
                }
                Some((LZ4_STREAM_MARKER, raw_length)) if raw_length >= 10 => {
                    // Even when a range is requested, we have to get the whole blob to decompress it properly.
                    // Also, skip over the first 8 bytes since that represents the decompressed length, which the streaming
                    // decoder doesn't really need.
                    let Some(mut compressed_blob) = self
                        .get_maybe_compressed_blob(key, 0..(raw_length - 1))
                        .await?
                    else {
                        return Ok(None);
                    };

                    let mut decompressed_length_bytes = [0u8; 8];
                    compressed_blob
                        .read_exact(&mut decompressed_length_bytes)
                        .await
                        .map_err(|err| {
                            trc::StoreEvent::DecompressError
                                .reason(err)
                                .ctx(trc::Key::Key, key)
                                .ctx(trc::Key::CausedBy, trc::location!())
                        })?;

                    let result_length = range
                        .end
                        .saturating_sub(range.start)
                        .min(u64::from_le_bytes(decompressed_length_bytes));
                    Some((
                        BlobReadStream::lz4_stream_range(compressed_blob, range),
                        result_length,
                    ))
                }
                Some((_, raw_length)) => {
                    trc::event!(Store(StoreEvent::BlobMissingMarker), Key = key);
                    if range.end > raw_length {
                        range.end = raw_length;
                    }
                    if range.start >= range.end {
                        Some((BlobReadStream::Bytes(Vec::new().into()), 0))
                    } else {
                        let blob_length = range.end.saturating_sub(range.start);
                        self.get_maybe_compressed_blob(key, range.clone())
                            .await?
                            .map(|blob| (blob, blob_length))
                    }
                }
            };

        trc::event!(
            Store(StoreEvent::BlobRead),
            Key = key,
            Elapsed = start_time.elapsed(),
            Size = blob_and_size
                .as_ref()
                .map_or(0, |blob_and_size| blob_and_size.1),
        );
        Ok(blob_and_size)
    }

    /// You should be **very sure** that not streaming the blob will not eat all the RAM
    pub async fn get_blob_vec(
        &self,
        key: &[u8],
        range: Range<u64>,
    ) -> trc::Result<Option<Vec<u8>>> {
        let Some((stream, _)) = self.get_blob(key, range).await? else {
            return Ok(None);
        };
        Ok(Some(stream.into_vec().await?))
    }

    pub async fn put_blob(
        &self,
        key: &[u8],
        mut data: JumboBytesMut,
        compression: CompressionAlgo,
    ) -> trc::Result<()> {
        // Some stored items may equal the max file size. However, we add a compression marker at the end, so we have
        // to do this otherwise ostensibly legal writes would fail with a "file too big" error.
        data.max_size = u64::MAX;
        data.rewind()
            .await
            .map_err(jumbo_bytes_into_error)
            .caused_by(trc::location!())?;

        let data = match compression {
            CompressionAlgo::None => {
                data.write_all(&[NONE_MARKER])
                    .await
                    .map_err(jumbo_bytes_into_error)
                    .caused_by(trc::location!())?;
                data
            }
            CompressionAlgo::Lz4 => {
                if data.len() <= u32::MAX as u64
                    && let Some(data_inner) = data.take_vec()
                {
                    let mut compressed = vec![
                        LZ4_MARKER;
                        lz4_flex::block::get_maximum_output_size(
                            data.len() as usize
                        ) + U32_LEN
                            + 1
                    ];

                    // Compress the data
                    let compressed_len =
                        lz4_flex::compress_into(&data_inner, &mut compressed[U32_LEN..]).unwrap();

                    // Prepend the length of the uncompressed data
                    compressed[..U32_LEN].copy_from_slice(&(data_inner.len() as u32).to_le_bytes());

                    // Truncate to the actual size
                    compressed.truncate(compressed_len + U32_LEN + 1);
                    JumboBytesMut::from(compressed)
                } else {
                    let mut compressed = JumboBytesMut::new(u64::MAX);
                    // preprend the compressed payload with the length. (consistent with lz4 block behaviour)
                    compressed
                        .write_all(&data.len().to_le_bytes())
                        .await
                        .map_err(jumbo_bytes_into_error)
                        .caused_by(trc::location!())?;

                    let mut compressor = AsyncFrameEncoder::new(compressed);
                    tokio::io::copy(&mut data, &mut compressor)
                        .await
                        .map_err(jumbo_bytes_into_error)
                        .caused_by(trc::location!())?;
                    let mut compressed = compressor
                        .shutdown_and_take_writer()
                        .await
                        .map_err(jumbo_bytes_into_error)
                        .caused_by(trc::location!())?;

                    compressed
                        .write_all(&[LZ4_STREAM_MARKER])
                        .await
                        .map_err(jumbo_bytes_into_error)
                        .caused_by(trc::location!())?;

                    compressed
                        .rewind()
                        .await
                        .map_err(jumbo_bytes_into_error)
                        .caused_by(trc::location!())?;

                    compressed
                }
            }
        };
        let data_len = data.len();

        let start_time = Instant::now();
        let result = match &self {
            BlobStore::Store(store) => {
                let data = data
                    .into_vec(u64::MAX)
                    .await
                    .map_err(jumbo_bytes_into_error)
                    .caused_by(trc::location!())?;
                match store {
                    #[cfg(feature = "sqlite")]
                    Store::SQLite(store) => store.put_blob(key, &data).await,
                    #[cfg(feature = "foundation")]
                    Store::FoundationDb(store) => store.put_blob(key, &data).await,
                    #[cfg(feature = "postgres")]
                    Store::PostgreSQL(store) => store.put_blob(key, &data).await,
                    #[cfg(feature = "mysql")]
                    Store::MySQL(store) => store.put_blob(key, &data).await,
                    #[cfg(feature = "rocks")]
                    Store::RocksDb(store) => store.put_blob(key, &data).await,
                    Store::Ephemeral(store) => store.put_blob(key, &data).await,
                    // SPDX-SnippetBegin
                    // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
                    // SPDX-License-Identifier: LicenseRef-SEL
                    #[cfg(all(
                        feature = "enterprise",
                        any(feature = "postgres", feature = "mysql")
                    ))]
                    Store::SQLReadReplica(store) => store.put_blob(key, &data).await,
                    // SPDX-SnippetEnd
                    Store::None => Err(trc::StoreEvent::NotConfigured.into()),
                }
            }
            BlobStore::Fs(store) => store.put_blob(key, data).await,
            #[cfg(feature = "s3")]
            BlobStore::S3(store) => store.put_blob(key, data).await,
            #[cfg(feature = "azure")]
            BlobStore::Azure(store) => store.put_blob(key, data).await,
            // SPDX-SnippetBegin
            // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
            // SPDX-License-Identifier: LicenseRef-SEL
            #[cfg(feature = "enterprise")]
            BlobStore::Sharded(store) => store.put_blob(key, data).await,
            // SPDX-SnippetEnd
        }
        .caused_by(trc::location!());

        trc::event!(
            Store(StoreEvent::BlobWrite),
            Key = key,
            Elapsed = start_time.elapsed(),
            Size = data_len,
        );

        result
    }

    pub async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let start_time = Instant::now();
        let result = match &self {
            BlobStore::Store(store) => match store {
                #[cfg(feature = "sqlite")]
                Store::SQLite(store) => store.delete_blob(key).await,
                #[cfg(feature = "foundation")]
                Store::FoundationDb(store) => store.delete_blob(key).await,
                #[cfg(feature = "postgres")]
                Store::PostgreSQL(store) => store.delete_blob(key).await,
                #[cfg(feature = "mysql")]
                Store::MySQL(store) => store.delete_blob(key).await,
                #[cfg(feature = "rocks")]
                Store::RocksDb(store) => store.delete_blob(key).await,
                Store::Ephemeral(store) => store.delete_blob(key).await,
                // SPDX-SnippetBegin
                // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
                // SPDX-License-Identifier: LicenseRef-SEL
                #[cfg(all(feature = "enterprise", any(feature = "postgres", feature = "mysql")))]
                Store::SQLReadReplica(store) => store.delete_blob(key).await,
                // SPDX-SnippetEnd
                Store::None => Err(trc::StoreEvent::NotConfigured.into()),
            },
            BlobStore::Fs(store) => store.delete_blob(key).await,
            #[cfg(feature = "s3")]
            BlobStore::S3(store) => store.delete_blob(key).await,
            #[cfg(feature = "azure")]
            BlobStore::Azure(store) => store.delete_blob(key).await,
            // SPDX-SnippetBegin
            // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
            // SPDX-License-Identifier: LicenseRef-SEL
            #[cfg(feature = "enterprise")]
            BlobStore::Sharded(store) => store.delete_blob(key).await,
            // SPDX-SnippetEnd
        }
        .caused_by(trc::location!());

        trc::event!(
            Store(StoreEvent::BlobWrite),
            Key = key,
            Elapsed = start_time.elapsed(),
        );

        result
    }
}

fn jumbo_bytes_into_error(err: std::io::Error) -> trc::Error {
    trc::StoreEvent::BlobWrite.reason(err)
}
