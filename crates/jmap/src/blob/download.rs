/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use common::{Server, auth::AccessToken};
use email::cache::MessageCacheFetch;
use email::cache::email::MessageCacheAccess;
use email::message::metadata::MessageMetadata;
use groupware::cache::GroupwareCache;
use registry::schema::enums::Permission;
use std::future::Future;
use store::ValueKey;
use store::stream::BlobReadStream;
use store::write::{AlignedBytes, Archive};
use trc::AddContext;
use types::acl::Acl;
use types::blob::{BlobClass, BlobId};
use types::collection::{Collection, SyncCollection};
use types::field::EmailField;
use utils::chained_bytes::ChainedBytes;

pub trait BlobDownload: Sync + Send {
    fn blob_download(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> impl Future<Output = trc::Result<Option<(BlobReadStream, u64)>>> + Send;

    /// You must be very sure that the data requested won't eat all the RAM
    fn blob_download_vec(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> impl Future<Output = trc::Result<Option<Vec<u8>>>> + Send;

    fn has_access_blob(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> impl Future<Output = trc::Result<bool>> + Send;
}

impl BlobDownload for Server {
    #[allow(clippy::blocks_in_conditions)]
    async fn blob_download(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> trc::Result<Option<(BlobReadStream, u64)>> {
        if !self.has_access_blob(blob_id, access_token).await? {
            return Ok(None);
        }
        match &blob_id.section {
            Some(section) if section.encoding == mail_parser::Encoding::None as u8 => {
                // Pass the stream directly if we don't need to decode
                self.blob_store()
                    .get_blob(
                        &blob_id.hash.as_slice(),
                        (section.offset_start as u64)..(if section.size == usize::MAX {
                            u64::MAX
                        } else {
                            (section.offset_start as u64).saturating_add(section.size as u64)
                        }),
                    )
                    .await
                    .caused_by(trc::location!())
            }
            Some(section) => self
                .get_blob_section(&blob_id.hash, section)
                .await
                .map(|maybe_bytes| {
                    maybe_bytes.map(|bytes| {
                        let bytes_len = bytes.len() as u64;
                        (bytes.into(), bytes_len)
                    })
                })
                .caused_by(trc::location!()),
            None => {
                let blob = self
                    .blob_store()
                    .get_blob(blob_id.hash.as_slice(), 0..u64::MAX)
                    .await
                    .caused_by(trc::location!());
                match (&blob_id.class, blob) {
                    (
                        BlobClass::Linked {
                            account_id,
                            collection,
                            document_id,
                        },
                        Ok(Some(data)),
                    ) if *collection == Collection::Email as u8 => {
                        let Some(archive) = self
                            .store()
                            .get_value::<Archive<AlignedBytes>>(ValueKey::property(
                                *account_id,
                                Collection::Email,
                                *document_id,
                                EmailField::Metadata,
                            ))
                            .await
                            .caused_by(trc::location!())?
                        else {
                            return Ok(Some(data));
                        };
                        let metadata = archive
                            .to_unarchived::<MessageMetadata>()
                            .caused_by(trc::location!())?;
                        let body_offset = metadata.inner.blob_body_offset.to_native();
                        if metadata.inner.root_part().offset_body.to_native() != body_offset {
                            let data = data.0.into_vec().await.caused_by(trc::location!())?;
                            let raw_message = ChainedBytes::new(
                                metadata.inner.raw_headers.as_ref(),
                            )
                            .with_last(data.get(body_offset as usize..).unwrap_or_default());
                            let raw_message = raw_message.to_bytes();
                            let raw_message_len = raw_message.len() as u64;
                            Ok(Some((raw_message.into(), raw_message_len)))
                        } else {
                            Ok(Some(data))
                        }
                    }
                    (_, blob) => blob,
                }
            }
        }
    }

    async fn blob_download_vec(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> trc::Result<Option<Vec<u8>>> {
        let Some((stream, _)) = self.blob_download(blob_id, access_token).await? else {
            return Ok(None);
        };
        Ok(Some(stream.into_vec().await?))
    }

    async fn has_access_blob(
        &self,
        blob_id: &BlobId,
        access_token: &AccessToken,
    ) -> trc::Result<bool> {
        Ok(
            (blob_id.class.is_superuser() && access_token.has_permission(Permission::FetchAnyBlob))
                || (self
                    .store()
                    .blob_has_access(&blob_id.hash, &blob_id.class)
                    .await
                    .caused_by(trc::location!())?
                    && match &blob_id.class {
                        BlobClass::Linked {
                            account_id,
                            collection,
                            document_id,
                        } => {
                            if access_token.is_member(*account_id) {
                                true
                            } else {
                                match Collection::from(*collection) {
                                    Collection::Email => self
                                        .get_cached_messages(*account_id)
                                        .await
                                        .caused_by(trc::location!())?
                                        .shared_messages(access_token, Acl::ReadItems)
                                        .contains(*document_id),
                                    collection @ (Collection::FileNode
                                    | Collection::ContactCard
                                    | Collection::CalendarEvent) => self
                                        .fetch_dav_resources(
                                            access_token.account_id(),
                                            *account_id,
                                            SyncCollection::from(collection),
                                        )
                                        .await
                                        .caused_by(trc::location!())?
                                        .shared_items(access_token, [Acl::ReadItems], true)
                                        .contains(*document_id),
                                    _ => false,
                                }
                            }
                        }
                        BlobClass::Reserved { account_id, .. } => {
                            access_token.is_member(*account_id)
                        }
                    }),
        )
    }
}
