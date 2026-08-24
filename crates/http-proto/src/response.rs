/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use common::manager::application::Resource;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper::{
    StatusCode,
    body::Bytes,
    header::{self, HeaderName, HeaderValue},
};
use serde_json::json;
use tokio::io::{AsyncRead, ReadBuf};

use crate::{
    DownloadResponse, HtmlResponse, HttpResponse, HttpResponseBody, HttpResponseBodyError,
    JsonProblemResponse, JsonResponse, ToHttpResponse,
};

impl HttpResponse {
    pub fn new(status: StatusCode) -> Self {
        HttpResponse {
            status,
            builder: hyper::Response::builder().status(status),
            body: HttpResponseBody::Empty,
        }
    }

    pub fn redirect(location: String) -> Self {
        let mut response = HttpResponse::new(StatusCode::FOUND);
        response.builder = response
            .builder
            .status(StatusCode::FOUND)
            .header(header::LOCATION, location);
        response
    }

    pub fn with_content_type<V>(mut self, content_type: V) -> Self
    where
        V: TryInto<HeaderValue>,
        <V as TryInto<HeaderValue>>::Error: Into<hyper::http::Error>,
    {
        self.builder = self.builder.header(header::CONTENT_TYPE, content_type);
        self
    }

    pub fn with_status_code(mut self, status: StatusCode) -> Self {
        self.status = status;
        self.builder = self.builder.status(status);
        self
    }

    pub fn with_content_length(mut self, content_length: u64) -> Self {
        self.builder = self.builder.header(header::CONTENT_LENGTH, content_length);
        self
    }

    pub fn with_content_range(mut self, content_range: String) -> Self {
        self.builder = self.builder.header(header::CONTENT_RANGE, content_range);
        self
    }

    pub fn with_accept_ranges(mut self) -> Self {
        self.builder = self.builder.header(header::ACCEPT_RANGES, "bytes");
        self
    }

    pub fn with_etag(mut self, etag: String) -> Self {
        self.builder = self.builder.header(header::ETAG, etag);
        self
    }

    pub fn with_etag_opt(self, etag: Option<String>) -> Self {
        if let Some(etag) = etag {
            self.with_etag(etag)
        } else {
            self
        }
    }

    pub fn with_schedule_tag_opt(mut self, tag: Option<u32>) -> Self {
        if let Some(tag) = tag {
            self.builder = self.builder.header("Schedule-Tag", format!("\"{tag}\""));
            self
        } else {
            self
        }
    }

    pub fn with_last_modified(mut self, last_modified: String) -> Self {
        self.builder = self.builder.header(header::LAST_MODIFIED, last_modified);
        self
    }

    pub fn with_lock_token(mut self, token_uri: &str) -> Self {
        self.builder = self.builder.header("Lock-Token", format!("<{token_uri}>"));
        self
    }

    pub fn with_header<K, V>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<HeaderName>,
        <K as TryInto<HeaderName>>::Error: Into<hyper::http::Error>,
        V: TryInto<HeaderValue>,
        <V as TryInto<HeaderValue>>::Error: Into<hyper::http::Error>,
    {
        self.builder = self.builder.header(name, value);
        self
    }

    pub fn with_xml_body(self, body: impl Into<String>) -> Self {
        self.with_text_body(body)
            .with_content_type("application/xml; charset=utf-8")
    }

    pub fn with_text_body(mut self, body: impl Into<String>) -> Self {
        let body = body.into();
        let body_len = body.len();
        self.body = HttpResponseBody::Text(body);
        self.with_content_length(body_len as u64)
    }

    pub fn with_binary_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        let body = body.into();
        let body_len = body.len();
        self.body = HttpResponseBody::Binary(body);
        self.with_content_length(body_len as u64)
    }

    pub fn with_stream_body(
        mut self,
        stream: http_body_util::combinators::UnsyncBoxBody<
            hyper::body::Bytes,
            HttpResponseBodyError,
        >,
    ) -> Self {
        self.body = HttpResponseBody::Stream(stream);
        self
    }

    pub fn with_io_read_body(
        mut self,
        body: Box<dyn AsyncRead + Send + 'static>,
        body_len: Option<u64>,
    ) -> Self {
        use std::pin::Pin;
        use std::task::{Context as AsyncContext, Poll};

        struct IoBodyWrapper {
            inner: Option<Pin<Box<dyn AsyncRead + Send>>>,
            buffer: Vec<u8>,
            body_len: Option<u64>,
        }
        impl http_body::Body for IoBodyWrapper {
            type Data = Bytes;

            type Error = HttpResponseBodyError;

            fn poll_frame(
                self: Pin<&mut Self>,
                cx: &mut AsyncContext<'_>,
            ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
                let self_mut = self.get_mut();
                let mut read_buffer = ReadBuf::new(&mut self_mut.buffer);
                match self_mut.inner.as_mut() {
                    None => Poll::Ready(None),
                    Some(inner) => match AsyncRead::poll_read(inner.as_mut(), cx, &mut read_buffer)
                    {
                        Poll::Ready(Ok(())) => {
                            let bytes_read = read_buffer.capacity() - read_buffer.remaining();
                            if bytes_read == 0 {
                                self_mut.inner = None;
                                self_mut.buffer.truncate(0);
                                Poll::Ready(None)
                            } else {
                                Poll::Ready(Some(Ok(http_body::Frame::data(
                                    Bytes::copy_from_slice(&self_mut.buffer[0..bytes_read]),
                                ))))
                            }
                        }
                        Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err.into()))),
                        Poll::Pending => Poll::Pending,
                    },
                }
            }

            fn is_end_stream(&self) -> bool {
                self.inner.is_none()
            }

            fn size_hint(&self) -> http_body::SizeHint {
                self.body_len
                    .map(http_body::SizeHint::with_exact)
                    .unwrap_or_default()
            }
        }
        self.body = HttpResponseBody::Stream(UnsyncBoxBody::new(IoBodyWrapper {
            inner: Some(Box::into_pin(body)),
            buffer: vec![0u8; 32 * 1024],
            body_len,
        }));
        if let Some(body_len) = body_len {
            self.with_content_length(body_len)
        } else {
            self
        }
    }

    pub fn with_websocket_upgrade(mut self, derived_key: String) -> Self {
        self.body = HttpResponseBody::WebsocketUpgrade(derived_key);
        self
    }

    pub fn with_content_disposition<V>(mut self, content_disposition: V) -> Self
    where
        V: TryInto<HeaderValue>,
        <V as TryInto<HeaderValue>>::Error: Into<hyper::http::Error>,
    {
        self.builder = self
            .builder
            .header(header::CONTENT_DISPOSITION, content_disposition);
        self
    }

    pub fn with_cache_control<V>(mut self, cache_control: V) -> Self
    where
        V: TryInto<HeaderValue>,
        <V as TryInto<HeaderValue>>::Error: Into<hyper::http::Error>,
    {
        self.builder = self.builder.header(header::CACHE_CONTROL, cache_control);
        self
    }

    pub fn with_no_store(mut self) -> Self {
        self.builder = self
            .builder
            .header(header::CACHE_CONTROL, "no-store, no-cache, must-revalidate");
        self
    }

    pub fn with_no_cache(mut self) -> Self {
        self.builder = self.builder.header(header::CACHE_CONTROL, "no-cache");
        self
    }

    pub fn with_immutable_cache(mut self) -> Self {
        self.builder = self
            .builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable");
        self
    }

    pub fn with_location<V>(mut self, location: V) -> Self
    where
        V: TryInto<HeaderValue>,
        <V as TryInto<HeaderValue>>::Error: Into<hyper::http::Error>,
    {
        self.builder = self.builder.header(header::LOCATION, location);
        self
    }

    pub fn with_cors_unrestricted(mut self) -> Self {
        self.builder = self
            .builder
            .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(
                header::ACCESS_CONTROL_ALLOW_HEADERS,
                "Authorization, Content-Type, Accept, X-Requested-With",
            )
            .header(
                header::ACCESS_CONTROL_ALLOW_METHODS,
                "POST, GET, PATCH, PUT, DELETE, HEAD, OPTIONS",
            );
        self
    }

    pub fn size(&self) -> usize {
        match &self.body {
            HttpResponseBody::Text(value) => value.len(),
            HttpResponseBody::Binary(value) => value.len(),
            _ => 0,
        }
    }

    pub fn build(
        self,
    ) -> hyper::Response<
        http_body_util::combinators::UnsyncBoxBody<hyper::body::Bytes, HttpResponseBodyError>,
    > {
        match self.body {
            HttpResponseBody::Text(body) => self.builder.body(
                Full::new(Bytes::from(body))
                    .map_err(|never| match never {})
                    .boxed_unsync(),
            ),
            HttpResponseBody::Binary(body) => self.builder.body(
                Full::new(Bytes::from(body))
                    .map_err(|never| match never {})
                    .boxed_unsync(),
            ),
            HttpResponseBody::Empty => {
                let has_content_length = self
                    .builder
                    .headers_ref()
                    .is_some_and(|headers| headers.contains_key(header::CONTENT_LENGTH));
                let builder = if has_content_length {
                    self.builder
                } else {
                    self.builder.header(header::CONTENT_LENGTH, 0)
                };

                builder.body(
                    Full::new(Bytes::new())
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
            }
            HttpResponseBody::Stream(stream) => self.builder.body(stream),
            HttpResponseBody::WebsocketUpgrade(derived_key) => self
                .builder
                .header(header::CONNECTION, "upgrade")
                .header(header::UPGRADE, "websocket")
                .header("Sec-WebSocket-Accept", &derived_key)
                .header("Sec-WebSocket-Protocol", "jmap")
                .body(
                    Full::new(Bytes::from("Switching to WebSocket protocol"))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                ),
        }
        .unwrap()
    }

    pub fn body(&self) -> &HttpResponseBody {
        &self.body
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> Option<&hyper::HeaderMap<HeaderValue>> {
        self.builder.headers_ref()
    }
}

impl<T: serde::Serialize> ToHttpResponse for JsonResponse<T> {
    fn into_http_response(self) -> HttpResponse {
        let response = HttpResponse::new(self.status)
            .with_content_type("application/json; charset=utf-8")
            .with_text_body(serde_json::to_string(&self.inner).unwrap_or_default());

        if self.no_cache {
            response.with_no_store()
        } else {
            response
        }
    }
}

impl ToHttpResponse for DownloadResponse {
    fn into_http_response(self) -> HttpResponse {
        HttpResponse::new(StatusCode::OK)
            .with_content_type(self.content_type)
            .with_content_disposition(format!(
                "attachment; filename=\"{}\"",
                self.filename.replace('\"', "\\\"")
            ))
            .with_cache_control("private, immutable, max-age=31536000")
            .with_io_read_body(self.blob, Some(self.content_length))
    }
}

impl ToHttpResponse for Resource<Vec<u8>> {
    fn into_http_response(self) -> HttpResponse {
        HttpResponse::new(StatusCode::OK)
            .with_content_type(self.content_type.as_ref())
            .with_binary_body(self.contents)
    }
}

impl ToHttpResponse for HtmlResponse {
    fn into_http_response(self) -> HttpResponse {
        HttpResponse::new(self.status)
            .with_content_type("text/html; charset=utf-8")
            .with_text_body(self.body)
    }
}

impl ToHttpResponse for JsonProblemResponse {
    fn into_http_response(self) -> HttpResponse {
        HttpResponse::new(self.0)
            .with_content_type("application/problem+json")
            .with_text_body(
                serde_json::to_string(&json!(
                    {
                        "type": "about:blank",
                        "title": self.0.canonical_reason().unwrap_or_default(),
                        "status": self.0.as_u16(),
                        "detail": self.0.canonical_reason().unwrap_or_default(),
                    }
                ))
                .unwrap_or_default(),
            )
    }
}
