use std::{
    collections::VecDeque,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context as AsyncContext, Poll},
};

use bytes::Bytes;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::mpsc,
};

const STREAM_WRAPPER_BUFFER_SIZE: usize = 16 * 1024;
struct SyncReadWrapper<R: AsyncRead + Unpin> {
    inner: R,
}
impl<R: AsyncRead + Unpin> SyncReadWrapper<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}
impl<R: AsyncRead + Unpin> Read for SyncReadWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        tokio::runtime::Handle::current().block_on(self.inner.read(buf))
    }
}
struct AsyncReadWrapper<R: Read + Send + 'static> {
    // TODO: This could be improved with some kind of sharable VecDeque
    // We can also maybe get rid of the 'static restriction by using stdlib's scoped threads, but that'd come at the
    // cost of not using tokio's spawn_blocking thread pool.
    // ...actually, that might not work due to https://github.com/tokio-rs/tokio/issues/7337 tokio streams don't like
    // being moved to different runtimes, and spawning a new stdlib thread with its own runtime might deadlock us.
    rx: mpsc::Receiver<IoResult<Bytes>>,
    // `Bytes` can basically be used as a `Cursor<Vec<u8>>` except read-only and non-seekable. Splitting it doesn't
    // clone the underlying data.
    data_buffer: Bytes,
    _phantom: PhantomData<R>,
}
impl<R: Read + Send> AsyncReadWrapper<R> {
    pub fn new(mut inner: R) -> Self {
        let (tx, rx) = mpsc::channel::<IoResult<Bytes>>(2);
        let _ = tokio::task::spawn_blocking(move || {
            let mut read_buffer = vec![0u8; STREAM_WRAPPER_BUFFER_SIZE];
            loop {
                let payload = match inner.read(&mut read_buffer) {
                    Ok(0) => break,
                    Ok(bytes_read) => Ok(Bytes::copy_from_slice(&read_buffer[0..bytes_read])),
                    Err(err) => Err(err),
                };
                if tx.blocking_send(payload).is_err() {
                    // the read stream has been cancelled. Guess we'll die.
                    break;
                }
            }
        });
        Self {
            rx,
            data_buffer: Bytes::new(),
            _phantom: PhantomData,
        }
    }
    // not writing any kind of into_inner here since the mpsc::channel would lose data.
}
// The Unpin restriction can only be removed if we remove the PhantomData.
impl<R: Read + Send + Unpin> AsyncRead for AsyncReadWrapper<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let self_mut = self.get_mut();
        if self_mut.data_buffer.is_empty() {
            match self_mut.rx.poll_recv(cx) {
                Poll::Ready(None) => {
                    // a true EOF.
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Ok(data_buffer))) => {
                    // Payload is always non-empty
                    self_mut.data_buffer = data_buffer;
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }
        buf.put_slice(
            &self_mut
                .data_buffer
                .split_to(buf.remaining().min(self_mut.data_buffer.len())),
        );
        Poll::Ready(Ok(()))
    }
}

/// `lz4_flex` tries to be convenient by providing IO streams, but the problem is that it's the only way to use its
/// partial-decoding capabilities. It also doesn't provide a transformer-like API, but instead wraps over the intended
/// io source or destination. Not very useful for us... we need a way to just shove some partial bytes into the black
/// box and get some partial bytes back... Which is where this thing comes in.
#[derive(Debug, Clone)]
struct AsyncHelperPipe {
    // `&self`'s contents are immutable until they aren't.
    shared_buffer: Arc<Mutex<VecDeque<u8>>>,
}
impl AsyncHelperPipe {
    pub fn new() -> Self {
        Self {
            shared_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(
                STREAM_WRAPPER_BUFFER_SIZE,
            ))),
        }
    }
}
impl Write for AsyncHelperPipe {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.shared_buffer.lock().extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        if self.shared_buffer.lock().is_empty() {
            Ok(())
        } else {
            Err(IoError::new(
                IoErrorKind::WouldBlock,
                "AsyncHelperPipe must not block",
            ))
        }
    }
}

pub struct AsyncFrameDecoder<R: AsyncRead + Send + Unpin + 'static> {
    inner: AsyncReadWrapper<FrameDecoder<SyncReadWrapper<R>>>,
}
impl<R: AsyncRead + Send + Unpin + 'static> AsyncFrameDecoder<R> {
    pub fn new(inner: R) -> Self {
        // We can't use something like an `AsyncHelperPipe` here because `FrameDecoder` can't resume on error.
        Self {
            inner: AsyncReadWrapper::new(FrameDecoder::new(SyncReadWrapper::new(inner))),
        }
    }
}
impl<R: AsyncRead + Send + Unpin + 'static> AsyncRead for AsyncFrameDecoder<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        AsyncRead::poll_read(Pin::new(&mut self.get_mut().inner), cx, buf)
    }
}

/// Make sure to call `.shutdown` to be 100% sure that the compression stream actually finished properly.
pub struct AsyncFrameEncoder<W: AsyncWrite + Unpin> {
    // compressed
    destination: W,
    encoder: Option<FrameEncoder<AsyncHelperPipe>>,
    encoder_output: AsyncHelperPipe,
}
impl<W: AsyncWrite + Unpin> AsyncFrameEncoder<W> {
    pub fn new(destination: W) -> Self {
        let shared_buffer = AsyncHelperPipe::new();
        Self {
            destination,
            encoder: Some(FrameEncoder::new(shared_buffer.clone())),
            encoder_output: shared_buffer,
        }
    }
    pub async fn shutdown_and_take_writer(mut self) -> IoResult<W> {
        self.shutdown().await?;
        Ok(self.destination)
    }
    fn poll_flush_destination(&mut self, cx: &mut AsyncContext<'_>) -> Poll<IoResult<()>> {
        let mut encoder_output = self.encoder_output.shared_buffer.lock();
        loop {
            if encoder_output.is_empty() {
                return AsyncWrite::poll_flush(Pin::new(&mut self.destination), cx);
            }
            match AsyncWrite::poll_write(
                Pin::new(&mut self.destination),
                cx,
                encoder_output.as_slices().0,
            ) {
                Poll::Ready(Ok(bytes_written)) => {
                    encoder_output.drain(0..bytes_written);
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
impl<W: AsyncWrite + Unpin> AsyncWrite for AsyncFrameEncoder<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &[u8],
    ) -> Poll<IoResult<usize>> {
        let self_mut = self.get_mut();
        loop {
            if self_mut.encoder_output.shared_buffer.lock().len() >= STREAM_WRAPPER_BUFFER_SIZE {
                match self_mut.poll_flush_destination(cx) {
                    Poll::Ready(Ok(())) => {
                        // poll_flush_destination would only return Ready if the encoder buffer is empty
                        continue;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            break;
        }
        let Some(encoder) = self_mut.encoder.as_mut() else {
            return Poll::Ready(Err(IoError::new(
                IoErrorKind::BrokenPipe,
                "shutdown() was already called",
            )));
        };
        Poll::Ready(encoder.write(buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut AsyncContext<'_>) -> Poll<IoResult<()>> {
        self.get_mut().poll_flush_destination(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut AsyncContext<'_>) -> Poll<IoResult<()>> {
        let self_mut = self.get_mut();
        if let Some(encoder) = self_mut.encoder.take() {
            encoder.finish()?;
        }
        self_mut.poll_flush_destination(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Highly compressible, but position-dependent: filler alone would still match if a chunk were
    /// dropped, duplicated, or emitted at the wrong offset.
    fn payload(len: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(len + 8);
        let mut counter = 0u32;
        while data.len() < len {
            data.extend_from_slice(&counter.to_le_bytes());
            counter = counter.wrapping_add(1);
        }
        data.truncate(len);
        data
    }

    /// Barely compressible, so the frame carries far more bytes than the compressible case and the
    /// block boundaries land in different places.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(len);
        let mut state = 0x2545_f491_4f6c_dd1du64;
        while data.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            data.extend_from_slice(&state.to_le_bytes());
        }
        data.truncate(len);
        data
    }

    async fn encode(data: &[u8]) -> Vec<u8> {
        let mut encoder = AsyncFrameEncoder::new(Vec::new());
        encoder.write_all(data).await.expect("encode must succeed");
        encoder
            .shutdown_and_take_writer()
            .await
            .expect("shutdown must succeed")
    }

    async fn decode(compressed: Vec<u8>) -> IoResult<Vec<u8>> {
        let mut decoder = AsyncFrameDecoder::new(Cursor::new(compressed));
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).await?;
        Ok(out)
    }

    async fn round_trip(data: &[u8]) -> Vec<u8> {
        decode(encode(data).await).await.expect("decode must succeed")
    }

    /// The regression that started this: the decoder used to fail with `BlockTooBig` as soon as a
    /// frame outgrew the internal buffer, so anything at or past 16KiB was written successfully and
    /// then permanently unreadable.
    #[tokio::test]
    async fn round_trips_across_the_internal_buffer_boundary() {
        for len in [
            0,
            1,
            4 * 1024,
            STREAM_WRAPPER_BUFFER_SIZE - 1,
            STREAM_WRAPPER_BUFFER_SIZE,
            STREAM_WRAPPER_BUFFER_SIZE + 1,
            4 * STREAM_WRAPPER_BUFFER_SIZE,
            1024 * 1024,
        ] {
            let data = payload(len);
            assert_eq!(round_trip(&data).await, data, "round trip failed at {len}");
        }
    }

    /// `SyncReadWrapper` calls `block_on` from a `spawn_blocking` thread, which interacts with the
    /// runtime differently depending on flavour. `#[tokio::test]` is single threaded by default, so
    /// the multi-threaded case needs saying out loud.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn round_trips_on_a_multi_thread_runtime() {
        let data = payload(4 * 1024 * 1024);
        assert_eq!(round_trip(&data).await, data);
    }

    #[tokio::test]
    async fn round_trips_incompressible_data() {
        // Compressed output here is larger than the input, so the frame spans many more blocks.
        let data = incompressible(512 * 1024);
        assert_eq!(round_trip(&data).await, data);
    }

    #[tokio::test]
    async fn empty_input_round_trips_to_empty() {
        let compressed = encode(&[]).await;
        assert!(
            !compressed.is_empty(),
            "even an empty payload still needs frame headers"
        );
        assert!(decode(compressed).await.unwrap().is_empty());
    }

    /// A caller reading a few bytes at a time must not lose or reorder data across the channel
    /// hand-off inside `AsyncReadWrapper`.
    #[tokio::test]
    async fn decodes_correctly_through_tiny_reads() {
        let data = payload(128 * 1024);
        let mut decoder = AsyncFrameDecoder::new(Cursor::new(encode(&data).await));

        let mut out = Vec::with_capacity(data.len());
        let mut chunk = [0u8; 7];
        loop {
            let read = decoder.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            out.extend_from_slice(&chunk[..read]);
        }

        assert_eq!(out, data);
    }

    /// Cross-check against lz4_flex directly, so a bug that happened to be symmetric between our
    /// own encoder and decoder cannot hide.
    #[tokio::test]
    async fn encoder_output_is_readable_by_lz4_flex() {
        for len in [1024usize, 64 * 1024, 1024 * 1024] {
            let data = payload(len);
            let compressed = encode(&data).await;

            let mut out = Vec::new();
            FrameDecoder::new(compressed.as_slice())
                .read_to_end(&mut out)
                .unwrap_or_else(|err| panic!("lz4_flex could not read our frame at {len}: {err}"));
            assert_eq!(out, data, "mismatch at {len}");
        }
    }

    #[tokio::test]
    async fn decoder_reads_frames_written_by_lz4_flex() {
        for len in [1024usize, 64 * 1024, 1024 * 1024] {
            let data = payload(len);
            let mut encoder = FrameEncoder::new(Vec::new());
            encoder.write_all(&data).unwrap();
            let compressed = encoder.finish().unwrap();

            assert_eq!(decode(compressed).await.unwrap(), data, "mismatch at {len}");
        }
    }

    /// A read failure partway through the compressed source has to surface rather than being
    /// reported as a clean end of stream, which would silently truncate the blob.
    #[tokio::test]
    async fn source_errors_surface_instead_of_truncating() {
        struct FailAfter {
            data: Cursor<Vec<u8>>,
            remaining: usize,
        }
        impl AsyncRead for FailAfter {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut AsyncContext<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<IoResult<()>> {
                let this = self.get_mut();
                if this.remaining == 0 {
                    return Poll::Ready(Err(IoError::new(IoErrorKind::Other, "boom")));
                }
                let before = buf.filled().len();
                let poll = AsyncRead::poll_read(Pin::new(&mut this.data), cx, buf);
                this.remaining = this
                    .remaining
                    .saturating_sub(buf.filled().len() - before);
                poll
            }
        }

        let compressed = encode(&payload(512 * 1024)).await;
        let mut decoder = AsyncFrameDecoder::new(FailAfter {
            data: Cursor::new(compressed),
            remaining: 1024,
        });

        let mut out = Vec::new();
        let err = decoder
            .read_to_end(&mut out)
            .await
            .expect_err("a failing source must not look like a successful short read");
        assert_ne!(err.kind(), IoErrorKind::UnexpectedEof);
    }
}
