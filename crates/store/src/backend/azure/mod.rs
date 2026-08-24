/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use azure_core::error::ErrorKind;
use azure_core::{Body, ExponentialRetryOptions, RetryOptions, StatusCode};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};
use registry::schema::structs::{self};
use std::sync::Arc;
use std::{fmt::Display, io::Write, ops::Range};
use utils::codec::base32_custom::Base32Writer;
use utils::jumbo_bytes::JumboBytesMut;

use crate::BlobStore;
use crate::stream::BlobReadStream;

pub struct AzureStore {
    client: ContainerClient,
    prefix: Option<String>,
}

impl AzureStore {
    pub async fn open(config: structs::AzureStore) -> Result<BlobStore, String> {
        let credentials = match (
            config.access_key.secret().await?.map(|v| v.into_owned()),
            config.sas_token.secret().await?.map(|v| v.into_owned()),
        ) {
            (Some(access_key), None) => {
                StorageCredentials::access_key(config.storage_account.clone(), access_key)
            }
            (None, Some(sas_token)) => match StorageCredentials::sas_token(sas_token) {
                Ok(cred) => cred,
                Err(err) => {
                    return Err(format!("Failed to create credentials: {err:?}"));
                }
            },
            _ => {
                return Err(concat!(
                    "Failed to create credentials: exactly one of ",
                    "'azure-access-key' and 'sas-token' must be specified"
                )
                .to_string());
            }
        };

        Ok(BlobStore::Azure(Arc::new(AzureStore {
            client: ClientBuilder::new(config.storage_account, credentials)
                .retry(RetryOptions::exponential(
                    ExponentialRetryOptions::default().max_retries(config.max_retries as u32 * 2),
                ))
                .container_client(config.container),
            prefix: config.key_prefix,
        })))
    }

    pub(crate) async fn get_blob(
        &self,
        key: &[u8],
        range: Range<u64>,
    ) -> trc::Result<Option<BlobReadStream>> {
        let blob_client = self.client.blob_client(self.build_key(key));
        let mut stream = blob_client.get();
        if range.end == u64::MAX {
            // Let's turn this into a proper RangeFrom.
            stream = stream.range(range.start..);
        } else {
            stream = stream.range(range.clone());
        };
        BlobReadStream::azure_stream(stream.into_stream()).await
    }

    pub(crate) async fn get_blob_length(&self, key: &[u8]) -> trc::Result<Option<u64>> {
        let blob_client = self.client.blob_client(self.build_key(key));
        match blob_client.get_properties().await {
            Err(e)
                if matches!(
                    e.kind(),
                    ErrorKind::HttpResponse {
                        status: StatusCode::NotFound,
                        ..
                    }
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(trc::StoreEvent::AzureError.reason(e)),
            // Now this is an adventure
            Ok(response) => Ok(Some(response.blob.properties.content_length)),
        }
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: JumboBytesMut) -> trc::Result<()> {
        let mut data = data.into_read_only().await;
        let blob_client = self.client.blob_client(self.build_key(key));

        let data = match data.take_bytes() {
            Some(bytes) => Body::Bytes(bytes),
            None => Body::SeekableStream(Box::new(data)),
        };

        blob_client
            .put_block_blob(data)
            .into_future()
            .await
            .map_err(into_error)?;

        Ok(())
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let blob_client = self.client.blob_client(self.build_key(key));

        if let Err(e) = blob_client.delete().into_future().await {
            if matches!(
                e.kind(),
                ErrorKind::HttpResponse {
                    status: StatusCode::NotFound,
                    ..
                }
            ) {
                Ok(false)
            } else {
                Err(trc::StoreEvent::AzureError.reason(e))
            }
        } else {
            Ok(true)
        }
    }

    fn build_key(&self, key: &[u8]) -> String {
        if let Some(prefix) = &self.prefix {
            let mut writer =
                Base32Writer::with_raw_capacity(prefix.len() + (key.len().div_ceil(4) * 5));
            writer.push_string(prefix);
            writer.write_all(key).unwrap();
            writer.finalize()
        } else {
            Base32Writer::from_bytes(key).finalize()
        }
    }
}

#[inline(always)]
fn into_error(err: impl Display) -> trc::Error {
    trc::StoreEvent::AzureError.reason(err)
}
