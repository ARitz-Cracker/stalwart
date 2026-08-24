#[cfg(feature = "azure")]
use azure_core::{
    ResponseBody as AzureResponseBody, StatusCode, error::ErrorKind as AzureErrorKind,
};
#[cfg(feature = "azure")]
use azure_storage_blobs::blob::operations::GetBlobResponse as AzureGetBlobResponse;
use bytes::Bytes;
#[cfg(feature = "azure")]
use futures::StreamExt as _;
#[cfg(feature = "s3")]
use s3::request::ResponseDataStream as S3ResponseDataStream;
#[cfg(feature = "azure")]
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::{
    io::{Result as IoResult, SeekFrom},
    ops::Range,
    pin::Pin,
    task::{Context as AsyncContext, Poll},
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, ReadBuf, Take},
};
use utils::jumbo_bytes::JumboBytesMut;

use crate::async_lz4::AsyncFrameDecoder;

pub enum BlobReadStream {
    Bytes(JumboBytesMut),
    File(File),
    ByteSegment(Take<JumboBytesMut>),
    FileSegment(Take<File>),
    #[cfg(feature = "s3")]
    S3(S3ResponseDataStream),
    #[cfg(feature = "azure")]
    Azure {
        inner: azure_core::Pageable<AzureGetBlobResponse, azure_core::Error>,
        current_response: Option<AzureResponseBody>,
        current_chunk: Bytes,
    },
    Lz4(AsyncFrameDecoder<Box<BlobReadStream>>),
    Lz4Segment {
        inner: Take<AsyncFrameDecoder<Box<BlobReadStream>>>,
        bytes_to_discard: u64,
    },
}
impl From<Vec<u8>> for BlobReadStream {
    fn from(value: Vec<u8>) -> Self {
        BlobReadStream::Bytes(value.into())
    }
}
impl BlobReadStream {
    pub async fn into_vec(mut self) -> trc::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        tokio::io::copy(&mut self, &mut bytes)
            .await
            .map_err(|err| trc::StoreEvent::UnexpectedError.reason(err))?;
        Ok(bytes)
    }

    pub async fn byte_range(mut bytes: JumboBytesMut, range: Range<u64>) -> trc::Result<Self> {
        bytes
            .seek(SeekFrom::Start(range.start))
            .await
            .map_err(|err| trc::StoreEvent::UnexpectedError.reason(err))?;

        if range.end == u64::MAX {
            Ok(Self::Bytes(bytes))
        } else {
            Ok(Self::ByteSegment(
                bytes.take(range.end.saturating_sub(range.start)),
            ))
        }
    }

    pub async fn file_range(mut file: File, range: Range<u64>) -> trc::Result<Self> {
        file.seek(SeekFrom::Start(range.start))
            .await
            .map_err(|err| trc::StoreEvent::FilesystemError.reason(err))?;

        if range.end == u64::MAX {
            Ok(Self::File(file))
        } else {
            Ok(Self::FileSegment(
                file.take(range.end.saturating_sub(range.start)),
            ))
        }
    }

    pub fn lz4_stream_range(inner: Self, range: Range<u64>) -> Self {
        if range.start == 0 && range.end == u64::MAX {
            Self::Lz4(AsyncFrameDecoder::new(Box::new(inner)))
        } else {
            Self::Lz4Segment {
                inner: AsyncFrameDecoder::new(Box::new(inner)).take(range.end),
                bytes_to_discard: range.start,
            }
        }
    }

    #[cfg(feature = "azure_core")]
    /// Attempts to get the first chunk so that the result can be passed as an azure error instead of an io error.
    /// Also returns `Ok(None)` if attempting to grab the first chunk resulted in a 404 error.
    pub async fn azure_stream(
        mut azure_stream: azure_core::Pageable<AzureGetBlobResponse, azure_core::Error>,
    ) -> trc::Result<Option<Self>> {
        let mut first_response = match azure_stream.next().await {
            None => {
                // nothing?
                return Ok(None);
            }
            Some(Err(e)) => {
                if matches!(
                    e.kind(),
                    AzureErrorKind::HttpResponse {
                        status: StatusCode::NotFound,
                        ..
                    }
                ) {
                    return Ok(None);
                } else {
                    return Err(trc::StoreEvent::AzureError.reason(e));
                }
            }
            Some(Ok(first_response)) => first_response.data,
        };
        let first_chunk = match first_response.next().await {
            None => {
                // nothing?
                return Ok(None);
            }
            Some(Err(e)) => {
                if matches!(
                    e.kind(),
                    AzureErrorKind::HttpResponse {
                        status: StatusCode::NotFound,
                        ..
                    }
                ) {
                    return Ok(None);
                } else {
                    return Err(trc::StoreEvent::AzureError.reason(e));
                }
            }
            Some(Ok(first_chunk)) => first_chunk,
        };
        Ok(Some(Self::Azure {
            inner: azure_stream,
            current_response: Some(first_response),
            current_chunk: first_chunk,
        }))
    }
}

impl AsyncRead for BlobReadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        match self.get_mut() {
            Self::Bytes(inner) => Pin::new(inner).poll_read(cx, buf),
            Self::File(inner) => Pin::new(inner).poll_read(cx, buf),
            Self::ByteSegment(inner) => Pin::new(inner).poll_read(cx, buf),
            Self::FileSegment(inner) => Pin::new(inner).poll_read(cx, buf),
            #[cfg(feature = "s3")]
            Self::S3(inner) => Pin::new(inner).poll_read(cx, buf),
            #[cfg(feature = "azure_core")]
            Self::Azure {
                inner,
                current_response,
                current_chunk,
            } => {
                // azure really loves to be special and inconvenient.
                loop {
                    if !current_chunk.is_empty() {
                        let buf_remaining = buf.remaining();
                        if buf_remaining > current_chunk.len() {
                            buf.put_slice(&std::mem::take(current_chunk));
                        } else {
                            // bytes::Bytes is pretty cool as we can use it as a one-way cursor pretty much for free.
                            buf.put_slice(&current_chunk.split_to(buf_remaining));
                        }
                        return Poll::Ready(Ok(()));
                    }
                    if let Some(current_response_inner) = current_response.as_mut() {
                        match current_response_inner.poll_next_unpin(cx) {
                            Poll::Ready(Some(Ok(next_chunk))) => {
                                *current_chunk = next_chunk;
                                continue;
                            }
                            Poll::Ready(Some(Err(err))) => {
                                return Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)));
                            }
                            Poll::Ready(None) => {
                                // this response is done, need to fetch another one from inner.poll_next
                                *current_response = None;
                            }
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                    match inner.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(next_page))) => {
                            *current_response = Some(next_page.data);
                            continue;
                        }
                        Poll::Ready(Some(Err(err))) => {
                            return Poll::Ready(Err(IoError::new(IoErrorKind::Other, err)));
                        }
                        Poll::Ready(None) => {
                            // we done!
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => return Poll::Pending,
                    };
                }
            }
            Self::Lz4(inner) => Pin::new(inner).poll_read(cx, buf),
            Self::Lz4Segment {
                inner,
                bytes_to_discard,
            } => {
                let mut inner = Pin::new(inner);
                let mut discard_vec = vec![0u8; ((*bytes_to_discard).min(16 * 1024)) as usize];
                // Unfortunately it seems like the only way to get a range of decompressed data is to do the work but
                // throw away the bytes we didn't want.
                while *bytes_to_discard > 0 {
                    let mut discard_buffer = ReadBuf::new(&mut discard_vec);
                    match inner.as_mut().poll_read(cx, &mut discard_buffer) {
                        Poll::Ready(Ok(())) => {
                            let bytes_read =
                                (discard_buffer.capacity() - discard_buffer.remaining()) as u64;
                            if bytes_read == 0 {
                                return Poll::Ready(Ok(()));
                            }
                            *bytes_to_discard -= bytes_read;

                            if *bytes_to_discard < 16 * 1024 {
                                discard_vec.truncate(*bytes_to_discard as usize);
                            }
                        }
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                inner.poll_read(cx, buf)
            }
        }
    }
}
