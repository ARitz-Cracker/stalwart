/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::borrow::Cow;

use compact_str::ToCompactString;
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt as _;
use utils::jumbo_bytes::JumboBytesMut;

use crate::HttpRequest;

#[inline]
pub fn decode_path_element(item: &str) -> Cow<'_, str> {
    percent_encoding::percent_decode_str(item)
        .decode_utf8()
        .unwrap_or_else(|_| item.into())
}

pub async fn fetch_body(
    req: &mut HttpRequest,
    max_size: u64,
    session_id: u64,
) -> Option<JumboBytesMut> {
    let mut body = JumboBytesMut::new(if max_size == 0 { u64::MAX } else { max_size });
    while let Some(Ok(frame)) = req.frame().await {
        if let Some(data) = frame.data_ref() {
            if let Err(_err) = body.write_all(&data).await {
                // ideally we'd log the real error somewhere, sometimes the write may fail due to disk issues, but
                // it will most likely fail due to the body exceeding max_size, and that's what users of this function
                // assume is the case when this returns None
                trc::event!(
                    Http(trc::HttpEvent::RequestBody),
                    SpanId = session_id,
                    Details = req
                        .headers()
                        .iter()
                        .map(|(k, v)| trc::Value::Array(vec![
                            k.as_str().to_compact_string().into(),
                            v.to_str().unwrap_or_default().to_compact_string().into()
                        ]))
                        .collect::<Vec<_>>(),
                    Contents = if let Some(bytes) = body.as_slice() {
                        std::str::from_utf8(bytes)
                            .unwrap_or("[binary data]")
                            .to_string()
                    } else {
                        "[large data]".to_string()
                    },
                    Size = body.len(),
                );
                return None;
            }
        }
    }

    trc::event!(
        Http(trc::HttpEvent::RequestBody),
        SpanId = session_id,
        Details = req
            .headers()
            .iter()
            .map(|(k, v)| trc::Value::Array(vec![
                k.as_str().to_compact_string().into(),
                v.to_str().unwrap_or_default().to_compact_string().into()
            ]))
            .collect::<Vec<_>>(),
        Contents = if let Some(bytes) = body.as_slice() {
            std::str::from_utf8(bytes)
                .unwrap_or("[binary data]")
                .to_string()
        } else {
            "[large data]".to_string()
        },
        Size = body.len(),
    );

    Some(body)
}
