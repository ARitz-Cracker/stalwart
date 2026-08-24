use std::{
    collections::VecDeque,
    io::{Error as IoError, ErrorKind as IoErrorKind, Read, Result as IoResult, Write},
    pin::Pin,
    sync::Arc,
    task::{Context as AsyncContext, Poll},
};

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

const HELPER_PIPE_BUFFER_SIZE: usize = 16 * 1024;
/// `lz4_flex` tries to be convenient by providing IO streams, but the problem is that it's the only way to use its
/// partial-decoding capabilities. It also doesn't provide a transformer-like API, but instead wraps over the intended
/// io source or destination. Not very useful for us... we need a way to just shove some partial bytes into the black
/// box and get some partial bytes back... Which is where this thing comes in.
#[derive(Debug, Clone)]
pub struct AsyncHelperPipe {
    // `&self`'s contents are immutable until they aren't.
    shared_buffer: Arc<Mutex<VecDeque<u8>>>,
}
impl AsyncHelperPipe {
    pub fn new() -> Self {
        Self {
            shared_buffer: Arc::new(Mutex::new(VecDeque::with_capacity(HELPER_PIPE_BUFFER_SIZE))),
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
impl Read for AsyncHelperPipe {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let strong_count = Arc::strong_count(&self.shared_buffer);
        let mut shared_buffer = self.shared_buffer.lock();
        // We're only "done" if we're the only instance of the buffer
        if shared_buffer.is_empty() && strong_count > 1 {
            return Err(IoError::new(
                IoErrorKind::WouldBlock,
                "AsyncHelperPipe must not block",
            ));
        }
        shared_buffer.read(buf)
    }
}

pub struct AsyncFrameDecoder<R: AsyncRead + Unpin> {
    // compressed
    source: R,
    decoder: FrameDecoder<AsyncHelperPipe>,
    decoder_input: Option<AsyncHelperPipe>,
}
impl<R: AsyncRead + Unpin> AsyncFrameDecoder<R> {
    pub fn new(source: R) -> Self {
        let shared_buffer = AsyncHelperPipe::new();
        Self {
            source,
            decoder: FrameDecoder::new(shared_buffer.clone()),
            decoder_input: Some(shared_buffer),
        }
    }
    fn read_inner(
        &mut self,
        cx: &mut AsyncContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let mut decoder_buf = vec![0u8; buf.remaining()];
        match (&mut self.decoder).read(decoder_buf.as_mut_slice()) {
            Ok(bytes_read) => {
                decoder_buf.truncate(bytes_read);
                buf.put_slice(&decoder_buf);
                return Poll::Ready(Ok(()));
            }
            Err(err) if err.kind() == IoErrorKind::WouldBlock => {}
            Err(err) => return Poll::Ready(Err(err)),
        }
        // At this point, the decoder probably doesn't have enough data (it calls .read_exact internally, which would
        // eventually trigger `WouldBlock`) so let's try fetching some more data for it
        if decoder_buf.len() < HELPER_PIPE_BUFFER_SIZE {
            decoder_buf.resize(HELPER_PIPE_BUFFER_SIZE, 0);
        }
        let mut inner_read_buf = ReadBuf::new(decoder_buf.as_mut_slice());
        if AsyncRead::poll_read(Pin::new(&mut self.source), cx, &mut inner_read_buf)?
            == Poll::Pending
        {
            return Poll::Pending;
        }
        let bytes_read = inner_read_buf.capacity() - inner_read_buf.remaining();
        decoder_buf.truncate(bytes_read);
        if decoder_buf.is_empty() {
            // If we read 0 bytes from the original source, we're probably EOF'd.
            // Drop the decoder input so FrameDecoder's AsyncHelperPipe won't return IoErrorKind::WouldBlock on empty
            self.decoder_input = None;
        }
        if let Some(decoder_input) = self.decoder_input.as_mut() {
            decoder_input
                .write(&decoder_buf)
                .expect("write should be infalliable");
        }

        // After we pushed data to the decoder, we should try pulling from it again.
        self.read_inner(cx, buf)
    }
}
impl<R: AsyncRead + Unpin> AsyncRead for AsyncFrameDecoder<R> {
    #[inline]
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        self.get_mut().read_inner(cx, buf)
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
            if self_mut.encoder_output.shared_buffer.lock().len() >= HELPER_PIPE_BUFFER_SIZE {
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
