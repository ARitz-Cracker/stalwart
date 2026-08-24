/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::download::BlobDownload;
use common::{Server, auth::AccessToken};
use email::message::metadata::MessageData;
use jmap_proto::{
    method::{
        get::{GetRequest, GetResponse},
        lookup::{BlobInfo, BlobLookupRequest, BlobLookupResponse},
    },
    object::blob::{Blob, BlobProperty, BlobValue, DataProperty, DigestProperty},
    request::{IntoValid, MaybeInvalid},
};
use jmap_tools::{Map, Value};
use mail_builder::encoders::Base64Encoder;
use sha1::{Digest, Sha1};
use sha2::{Sha256, Sha512};
use std::future::Future;
use store::{
    ValueKey,
    stream::BlobReadStream,
    write::{AlignedBytes, Archive},
};
use tokio::io::AsyncReadExt as _;
use trc::AddContext;
use types::{blob::BlobClass, collection::Collection, id::Id, type_state::DataType};
use utils::map::vec_map::VecMap;

pub trait BlobOperations: Sync + Send {
    fn blob_get(
        &self,
        request: GetRequest<Blob>,
        access_token: &AccessToken,
    ) -> impl Future<Output = trc::Result<GetResponse<Blob>>> + Send;

    fn blob_lookup(
        &self,
        request: BlobLookupRequest,
    ) -> impl Future<Output = trc::Result<BlobLookupResponse>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlobDataRequest {
    None,
    Hash,
    Contents,
}
enum BlobDataResponse {
    None,
    Stream(BlobReadStream),
    Bytes(Vec<u8>),
}

impl BlobOperations for Server {
    async fn blob_get(
        &self,
        mut request: GetRequest<Blob>,
        access_token: &AccessToken,
    ) -> trc::Result<GetResponse<Blob>> {
        let (ids, not_found_ids) = request.unwrap_ids(self.core.jmap.get_max_objects)?;
        let ids = ids.unwrap_or_default();
        let properties = request.unwrap_properties(&[
            BlobProperty::Id,
            BlobProperty::Data(DataProperty::Default),
            BlobProperty::Size,
        ]);
        let mut response = GetResponse {
            account_id: request.account_id.into(),
            state: None,
            list: Vec::with_capacity(ids.len()),
            not_found: not_found_ids,
        };

        // We shouldn't fetch the underlying blob if we don't need to,
        let blob_data_requested = if properties
            .iter()
            .any(|property| matches!(property, BlobProperty::Data(_)))
        {
            BlobDataRequest::Contents
        } else if properties
            .iter()
            .any(|property| matches!(property, BlobProperty::Digest(_)))
        {
            BlobDataRequest::Hash
        } else {
            BlobDataRequest::None
        };

        let range_from = request.arguments.offset.unwrap_or(0) as u64;
        let range_to = request
            .arguments
            .length
            .map(|length| range_from.saturating_add(length as u64))
            .unwrap_or(u64::MAX);

        // Check if we can afford to buffer everything before actually consuming the streams
        let mut blob_bytes_buffered = 0u64;
        let mut blob_full_lengths: Vec<Option<u64>> = Vec::with_capacity(ids.len());

        for blob_id in ids.iter() {
            if !self.has_access_blob(&blob_id, access_token).await? {
                blob_full_lengths.push(None);
                continue;
            }
            let Some(blob_full_length) = self
                .blob_store()
                .get_blob_length(blob_id.hash.as_slice())
                .await?
            else {
                blob_full_lengths.push(None);
                continue;
            };
            if blob_data_requested == BlobDataRequest::Contents {
                // We're only buffering the contents if the client requested it. Hashes can be calcualted via streams.
                let stream_length = blob_full_length.min(range_to).saturating_sub(range_from);
                blob_bytes_buffered = blob_bytes_buffered.saturating_add(stream_length);
                if blob_bytes_buffered > self.core.jmap.max_size_blob_set() {
                    return Err(trc::JmapEvent::RequestTooLarge.into_err().details(
                        "Blob content is too large to serve inline. \
                                    Use the download endpoint or request a smaller range.",
                    ));
                }
            }
            blob_full_lengths.push(Some(blob_full_length));
        }

        // After we validated that we can afford to construct the response, let's do it.
        for (blob_id, blob_full_length) in ids.into_iter().zip(blob_full_lengths) {
            let Some(blob_full_length) = blob_full_length else {
                // The blob didn't exist or the user didn't have access
                response.push_not_found(blob_id);
                continue;
            };

            // This is always Some if the blob_fetch_needed checked passed above
            let mut blob_data_response = if blob_data_requested != BlobDataRequest::None {
                let Some((stream, _)) = self
                    .blob_store()
                    .get_blob(blob_id.hash.as_slice(), range_from..range_to)
                    .await?
                else {
                    // this shouldn't happen?
                    response.push_not_found(blob_id);
                    continue;
                };
                if blob_data_requested == BlobDataRequest::Contents {
                    BlobDataResponse::Bytes(stream.into_vec().await.caused_by(trc::location!())?)
                } else {
                    BlobDataResponse::Stream(stream)
                }
            } else {
                BlobDataResponse::None
            };

            // Since we might be streaming, and the stream can only be read once, we have to update all the hashers
            // silmutaniously.
            let mut sha1_hasher: Option<Sha1> = None;
            let mut sha256_hasher: Option<Sha256> = None;
            let mut sha512_hasher: Option<Sha512> = None;
            if blob_data_requested != BlobDataRequest::None {
                for property in &properties {
                    match property {
                        BlobProperty::Digest(DigestProperty::Sha) => {
                            sha1_hasher = Some(Sha1::new())
                        }
                        BlobProperty::Digest(DigestProperty::Sha256) => {
                            sha256_hasher = Some(Sha256::new())
                        }
                        BlobProperty::Digest(DigestProperty::Sha512) => {
                            sha512_hasher = Some(Sha512::new())
                        }
                        _ => {
                            if sha1_hasher.is_some()
                                && sha256_hasher.is_some()
                                && sha512_hasher.is_some()
                            {
                                break;
                            }
                        }
                    }
                }
            }
            match &mut blob_data_response {
                BlobDataResponse::None => {}
                BlobDataResponse::Stream(stream) => {
                    let mut stream_buffer = vec![0u8; 16 * 1024];
                    loop {
                        match stream.read(&mut stream_buffer).await {
                            Ok(0) => break,
                            Ok(bytes_read) => {
                                let bytes = &stream_buffer[0..bytes_read];
                                if let Some(sha1_hasher) = sha1_hasher.as_mut() {
                                    sha1_hasher.update(&bytes);
                                }
                                if let Some(sha256_hasher) = sha256_hasher.as_mut() {
                                    sha256_hasher.update(&bytes);
                                }
                                if let Some(sha512_hasher) = sha512_hasher.as_mut() {
                                    sha512_hasher.update(&bytes);
                                }
                            }
                            Err(err) => {
                                return Err(trc::StoreEvent::BlobRead
                                    .reason(err)
                                    .caused_by(trc::location!()));
                            }
                        }
                    }
                }
                BlobDataResponse::Bytes(bytes) => {
                    if let Some(sha1_hasher) = sha1_hasher.as_mut() {
                        sha1_hasher.update(&bytes);
                    }
                    if let Some(sha256_hasher) = sha256_hasher.as_mut() {
                        sha256_hasher.update(&bytes);
                    }
                    if let Some(sha512_hasher) = sha512_hasher.as_mut() {
                        sha512_hasher.update(&bytes);
                    }
                }
            }

            let mut blob = Map::with_capacity(properties.len());
            // RFC 9404 section 4.2
            let is_truncated = if range_to == u64::MAX {
                range_from > blob_full_length
            } else {
                range_to > blob_full_length
            };
            if is_truncated {
                blob.insert_unchecked(BlobProperty::IsTruncated, true);
            }
            for property in &properties {
                let mut property = property.clone();
                let value: Value<'static, BlobProperty, BlobValue> = match &property {
                    BlobProperty::Id => Value::Element(BlobValue::BlobId(blob_id.clone())),
                    BlobProperty::Size => Value::Number(blob_full_length.into()),
                    BlobProperty::Digest(digest) => match digest {
                        DigestProperty::Sha => String::from_utf8(
                            Base64Encoder::new()
                                .encode(
                                    &sha1_hasher
                                        .as_ref()
                                        .expect("sha1 hasher should have been constructed")
                                        // digest properties aren't guaranteed to be unique. I guess if the client
                                        // wants multiple of the same hash, we can give it to them.
                                        .clone()
                                        .finalize()[..],
                                )
                                .unwrap_or_default(),
                        )
                        .unwrap(),
                        DigestProperty::Sha256 => String::from_utf8(
                            Base64Encoder::new()
                                .encode(
                                    &sha256_hasher
                                        .as_ref()
                                        .expect("sha256 hasher should have been constructed")
                                        .clone() // potentially non-unique digest value
                                        .finalize()[..],
                                )
                                .unwrap_or_default(),
                        )
                        .unwrap(),
                        DigestProperty::Sha512 => {
                            String::from_utf8(
                                Base64Encoder::new()
                                    .encode(
                                        &sha512_hasher
                                            .as_ref()
                                            .expect("sha512 hasher should have been constructed")
                                            .clone() // potentially non-unique digest value
                                            .finalize()[..],
                                    )
                                    .unwrap_or_default(),
                            )
                            .unwrap()
                        }
                    }
                    .into(),
                    BlobProperty::Data(data) => {
                        let BlobDataResponse::Bytes(blob_bytes) = &blob_data_response else {
                            unreachable!(
                                "whether or not the client wanted the blob contents would have been checked"
                            )
                        };
                        match data {
                            DataProperty::AsText => match std::str::from_utf8(&blob_bytes) {
                                Ok(text) => text.to_string().into(),
                                Err(_) => {
                                    blob.insert_unchecked(BlobProperty::IsEncodingProblem, true);
                                    Value::Null
                                }
                            },
                            DataProperty::AsBase64 => String::from_utf8(
                                Base64Encoder::new().encode(&blob_bytes).unwrap_or_default(),
                            )
                            .unwrap()
                            .into(),
                            DataProperty::Default => match std::str::from_utf8(&blob_bytes) {
                                Ok(text) => {
                                    property = BlobProperty::Data(DataProperty::AsText);
                                    text.to_string().into()
                                }
                                Err(_) => {
                                    property = BlobProperty::Data(DataProperty::AsBase64);
                                    blob.insert_unchecked(BlobProperty::IsEncodingProblem, true);
                                    String::from_utf8(
                                        Base64Encoder::new()
                                            .encode(&blob_bytes)
                                            .unwrap_or_default(),
                                    )
                                    .unwrap()
                                    .into()
                                }
                            },
                        }
                    }
                    _ => Value::Null,
                };
                blob.insert_unchecked(property, value);
            }

            // Add result to response
            response.list.push(blob.into());
        }

        Ok(response)
    }

    async fn blob_lookup(&self, request: BlobLookupRequest) -> trc::Result<BlobLookupResponse> {
        let mut include_email = false;
        let mut include_mailbox = false;
        let mut include_thread = false;

        let type_names = request
            .type_names
            .into_iter()
            .map(|tn| match tn {
                MaybeInvalid::Value(value) => {
                    match &value {
                        DataType::Email => {
                            include_email = true;
                        }
                        DataType::Mailbox => {
                            include_mailbox = true;
                        }
                        DataType::Thread => {
                            include_thread = true;
                        }
                        _ => (),
                    }

                    Ok(value)
                }
                MaybeInvalid::Invalid(_) => Err(trc::JmapEvent::UnknownDataType.into_err()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let req_account_id = request.account_id.document_id();
        let mut response = BlobLookupResponse {
            account_id: request.account_id,
            list: Vec::with_capacity(request.ids.len()),
            not_found: vec![],
        };

        for id in request.ids.into_valid() {
            let mut matched_ids = VecMap::new();

            match &id.class {
                BlobClass::Linked {
                    account_id,
                    collection,
                    document_id,
                } if *account_id == req_account_id => {
                    let collection = Collection::from(*collection);
                    if collection == Collection::Email {
                        if let Some(data_) = self
                            .store()
                            .get_value::<Archive<AlignedBytes>>(ValueKey::archive(
                                req_account_id,
                                Collection::Email,
                                *document_id,
                            ))
                            .await?
                        {
                            let data = data_
                                .unarchive::<MessageData>()
                                .caused_by(trc::location!())?;
                            if include_email {
                                matched_ids.append(
                                    DataType::Email,
                                    vec![Id::from_parts(u32::from(data.thread_id), *document_id)],
                                );
                            }
                            if include_thread {
                                matched_ids.append(
                                    DataType::Thread,
                                    vec![Id::from(u32::from(data.thread_id))],
                                );
                            }
                            if include_mailbox {
                                matched_ids.append(
                                    DataType::Mailbox,
                                    data.mailboxes
                                        .iter()
                                        .map(|m| {
                                            debug_assert!(m.uid != 0);
                                            Id::from(u32::from(m.mailbox_id))
                                        })
                                        .collect::<Vec<_>>(),
                                );
                            }
                        }
                    } else {
                        match DataType::try_from(collection) {
                            Ok(data_type) if type_names.contains(&data_type) => {
                                matched_ids.append(data_type, vec![Id::from(*document_id)]);
                            }
                            _ => (),
                        }
                    }
                }
                BlobClass::Reserved { account_id, .. } if *account_id == req_account_id => {}
                _ => {
                    response.not_found.push(id);
                    continue;
                }
            }

            response.list.push(BlobInfo { id, matched_ids });
        }

        Ok(response)
    }
}
