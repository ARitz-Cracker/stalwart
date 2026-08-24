use std::{
    fs::File as StdFile,
    io::{Cursor, Error as IoError, ErrorKind as IoErrorKind, Read, Result, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context as AsyncContext, Poll},
    time::Instant,
};

use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use parking_lot::RwLock;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf},
};

// These two values are just used for creating a unique-enough file name for this instance
static BYTES_FILE_NONCE: AtomicUsize = AtomicUsize::new(0);
static BYTES_FILE_EPOCH: LazyLock<Instant> = LazyLock::new(|| Instant::now());
static TEMPFILE_CLEANER: LazyLock<mpsc::Sender<(StdFile, Arc<Path>)>> = LazyLock::new(|| {
    let (tx, rx) = mpsc::channel::<(StdFile, Arc<Path>)>();
    std::thread::Builder::new()
        .name("tempBlobCleanup".into())
        .spawn(move || {
            while let Ok((file_handle, file_path)) = rx.recv() {
                drop(file_handle); // Close the file handle
                if let Err(_) = std::fs::remove_file(&file_path) {
                    // maybe log something? The tracing crate isn't a dependency of common.
                };
            }
        })
        .expect("tempBlobCleanup thread must spawn");
    // The cleanup thread always exists as the program does.
    // There is a slight possibility for stalwart to close before the queue is finished, but we might want to handle
    // that with a cleanup on startup anyway.
    tx
});
static SPILLOVER_SIZE: AtomicUsize = AtomicUsize::new(25 * 1024 * 1024); // Default stalwart max file size
static TEMPFILE_DIR: LazyLock<RwLock<Arc<Path>>> =
    LazyLock::new(|| RwLock::new(std::env::temp_dir().into()));

fn new_tempfile_path(dir: &Path) -> PathBuf {
    // create a value that's unique to this instance of stalwart
    let nonce_value = BYTES_FILE_NONCE.fetch_add(1, Ordering::Relaxed);
    let ecpoch_value = Instant::now().duration_since(*BYTES_FILE_EPOCH).as_micros();
    let mut new_path = PathBuf::from(dir);
    new_path.push(format!("stalwart_{ecpoch_value:x}_{nonce_value:x}.temp"));
    new_path
}

enum JumboBytesMutInner {
    Ram {
        bytes: Cursor<Vec<u8>>,
    },
    Transitioning {
        future: BoxFuture<'static, Result<(PathBuf, File)>>,
        original_cursor_pos: usize,
        cursor_pos: Option<usize>,
        file_size: u64,
    },
    File {
        path: PathBuf,
        file: File,
        // one could write to the file after seeking backward, this seems to be the only way correctly update the file
        // size synchronously while writing.
        cursor_pos: u64,
        file_size: u64,
    },
}
impl std::fmt::Debug for JumboBytesMutInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JumboBytesMutInner::Ram { .. } => f.write_str("JumboBytesMutInner::Ram { .. }"),
            JumboBytesMutInner::Transitioning { .. } => {
                f.write_str("JumboBytesMutInner::Transitioning { .. }")
            }
            JumboBytesMutInner::File { path, .. } => f.write_fmt(format_args!(
                "JumboBytesMutInner::File {{ path: {path:?}, .. }}"
            )),
        }
    }
}

/// Represents an array of bytes which spill over to a temporary file on-disk if needed, exposed as a seekable
/// read/write stream.
///
/// Every instance will use [Self::set_spillover_size] to determine when to dump its content onto disk, and will create
/// a temporary file the directory specified [Self::set_file_dir]. When this happens, all buffered data is dumped to
/// disk.
///
/// This is not cloneable as it is meant to represent a unique pointer to the underlying data. If you want a clonable
/// and shareable resource, call [Self::into_read_only]
#[derive(Debug)]
pub struct JumboBytesMut {
    /// The maximum size that the underlying data can be. If the internal data size already exceeds this value, or if a
    /// write to `self` would result in the internal data size exceeding this value, then all future writes will fail
    /// with [IoErrorKind::FileTooLarge].
    pub max_size: u64,
    inner: Option<JumboBytesMutInner>,
    max_ram_size: usize,
    directory: Arc<Path>,
}
impl JumboBytesMut {
    /// Does what the function says. When calling this function, the value will be applied to all _new_ instances of
    /// `Self`.
    ///
    /// This is defined as a u32 as various parts of the codebase effectively assume that anything in-memory is less
    /// than 4 GiB in size.
    ///
    /// This defaults to 25MiB.
    ///
    /// There's also an internally enforced minimum of 1MiB as parts of the codebase are hard-coded to check for these
    /// body sizes for special functionality. It would be inefficient to dump these special requests to disk only for
    /// them to be read back in again.
    pub fn set_spillover_size(size: u32) {
        // There are parts of the codebase that have hard-coded low body sizes. Those should never be dumped to disk
        SPILLOVER_SIZE.store((size as usize).min(1024 * 1024), Ordering::Relaxed);
    }

    /// Sets the directory where the temporary files are written to. When calling this function, the value is applied
    /// to all _new_ instances of `Self`.
    ///
    /// This defaults to [std::env::temp_dir]
    pub fn set_file_dir(tempdir: PathBuf) {
        *TEMPFILE_DIR.write() = tempdir.into();
    }

    /// Creates a new [JumboBytesMut] where the underlying data cannot be larger than `max_size`. If a write attempts
    /// this, it will fail.
    pub fn new(max_size: u64) -> Self {
        Self {
            inner: Some(JumboBytesMutInner::Ram {
                bytes: Cursor::new(Vec::new()),
            }),
            max_size,
            max_ram_size: SPILLOVER_SIZE.load(Ordering::Relaxed),
            directory: TEMPFILE_DIR.read().clone(),
        }
    }

    async fn complete_transition_if_needed(&mut self) -> Result<()> {
        match self.inner.take() {
            Some(JumboBytesMutInner::Transitioning {
                future,
                original_cursor_pos,
                cursor_pos,
                file_size,
            }) => {
                let (path, mut file) = future.await?;
                if let Some(cursor_pos) = cursor_pos {
                    file.seek(SeekFrom::Start(cursor_pos as u64)).await?;
                }
                self.inner = Some(JumboBytesMutInner::File {
                    path,
                    file,
                    file_size,
                    cursor_pos: cursor_pos.unwrap_or(original_cursor_pos) as u64,
                });
            }
            inner => {
                // Put the thing we just took back
                self.inner = inner;
            }
        }
        Ok(())
    }

    /// Returns the inner `Vec<u8>` but only if `self` does not represent a temporary file. If this returns `Some`,
    /// then this JumboBytesMut becomes empty
    pub fn take_vec(&mut self) -> Option<Vec<u8>> {
        match self.inner.as_mut() {
            Some(JumboBytesMutInner::Ram { bytes }) => {
                bytes.set_position(0);
                Some(std::mem::take(bytes.get_mut()))
            }
            _ => None,
        }
    }

    /// Allows you to see the inner data but only if `self` does not represent a temporary file.
    pub fn as_slice(&self) -> Option<&[u8]> {
        match self.inner.as_ref() {
            Some(JumboBytesMutInner::Ram { bytes, .. }) => Some(&bytes.get_ref()),
            _ => None,
        }
    }

    /// Coerces the underlying data into a `Vec<u8>`. Returns`Err` if the underlying data is larger than `max_size`.
    pub async fn into_vec(mut self, max_size: u64) -> Result<Vec<u8>> {
        self.complete_transition_if_needed().await?;
        match self.inner.take() {
            Some(JumboBytesMutInner::Ram { bytes, .. }) => Ok(bytes.into_inner()),
            Some(JumboBytesMutInner::File {
                path,
                mut file,
                cursor_pos: _,
                file_size,
            }) => {
                let mut resulting_bytes = Vec::<u8>::with_capacity(1024);
                if file_size > max_size {
                    return Err(IoError::new(
                        IoErrorKind::FileTooLarge,
                        "file exceeds maximum buffer size",
                    ));
                }
                file.rewind().await?;
                tokio::io::copy(&mut file, &mut resulting_bytes).await?;
                let _ = TEMPFILE_CLEANER.send((file.into_std().await, path.into()));
                Ok(resulting_bytes)
            }
            None => Err(reuse_after_drop_error()),
            _ => unreachable!("complete_transition_if_needed should have worked"),
        }
    }

    #[inline]
    /// Get the size of the underlying data
    pub fn len(&self) -> u64 {
        match self.inner.as_ref() {
            Some(JumboBytesMutInner::File { file_size, .. })
            | Some(JumboBytesMutInner::Transitioning { file_size, .. }) => *file_size,
            Some(JumboBytesMutInner::Ram { bytes, .. }) => bytes.get_ref().len() as u64,
            None => 0,
        }
    }

    #[inline]
    /// `self.len() == 0`
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Moves the underlying data into the specified file path. Will opportunistically do a file rename instead of a
    /// copy if possible.
    pub async fn move_into_file(mut self, to_path: &Path) -> Result<()> {
        self.complete_transition_if_needed().await?;
        match self.inner.take() {
            Some(JumboBytesMutInner::File {
                path: from_path,
                mut file,
                ..
            }) => {
                file.flush().await?;
                drop(file);
                match tokio::fs::rename(&from_path, &to_path).await {
                    Err(err) if err.kind() == IoErrorKind::CrossesDevices => {
                        tokio::fs::copy(&from_path, &to_path).await?;
                        let _ = tokio::fs::remove_file(&from_path).await;
                    }
                    Err(err) => return Err(err),
                    Ok(_) => {}
                }
                Ok(())
            }
            Some(JumboBytesMutInner::Ram { bytes, .. }) => {
                let mut file = File::create(&to_path).await?;
                file.write_all(&bytes.get_ref()).await?;
                file.flush().await?;
                Ok(())
            }
            None => Err(reuse_after_drop_error()),
            _ => unreachable!("complete_transition_if_needed should have worked"),
        }
    }

    async fn into_read_only_inner(
        path: PathBuf,
        mut file: File,
        file_size: u64,
    ) -> Result<JumboBytesInner> {
        file.flush().await?;
        drop(file);
        let path: Arc<Path> = path.into();
        let file = File::open(&path).await?;
        Ok(JumboBytesInner::File {
            path,
            file,
            file_size,
            cursor_pos: 0,
        })
    }

    /// Turns the tempfile into a read-only one. The resulting item will be clonable.
    /// The cursor is also reset to the 0 position.
    pub async fn into_read_only(mut self) -> JumboBytes {
        if let Err(err) = self.complete_transition_if_needed().await {
            return JumboBytes {
                inner: Some(JumboBytesInner::Error(err)),
            };
        };
        match self.inner.take() {
            Some(JumboBytesMutInner::File {
                path,
                file,
                cursor_pos: _,
                file_size,
            }) => JumboBytes {
                inner: Some(
                    match Self::into_read_only_inner(path, file, file_size).await {
                        Ok(inner) => inner,
                        Err(err) => JumboBytesInner::Error(err),
                    },
                ),
            },
            Some(JumboBytesMutInner::Ram { bytes }) => JumboBytes {
                inner: Some(JumboBytesInner::Ram {
                    cursor_pos: bytes.position() as usize,
                    bytes: bytes.into_inner().into(),
                }),
            },
            None => JumboBytes {
                inner: Some(JumboBytesInner::Error(reuse_after_drop_error())),
            },
            _ => unreachable!("complete_transition_if_needed should have worked"),
        }
    }

    // If false, return pending, if true, then the transition succeeded, and the originally requested task should be
    // worked on.
    fn handle_poll_transition(&mut self, cx: &mut AsyncContext<'_>) -> Result<bool> {
        let (mut future, original_cursor_pos, cursor_pos, file_size) = match self.inner.take() {
            Some(JumboBytesMutInner::Transitioning {
                future,
                original_cursor_pos,
                cursor_pos,
                file_size,
            }) => (future, original_cursor_pos, cursor_pos, file_size),
            inner => {
                self.inner = inner;
                return Ok(true);
            }
        };
        match future.poll_unpin(cx) {
            Poll::Ready(Ok((path, mut file))) => {
                if let Some(cursor_pos) = cursor_pos {
                    // Someone tried to do a seek while we were transitioning
                    // guess we better start that seek now.
                    future = async move {
                        file.seek(SeekFrom::Start(cursor_pos as u64))
                            .await
                            .map(|_| (path, file))
                    }
                    .boxed();
                    self.inner = Some(JumboBytesMutInner::Transitioning {
                        future,
                        original_cursor_pos: cursor_pos,
                        cursor_pos: None,
                        file_size,
                    });
                    self.handle_poll_transition(cx)
                } else {
                    self.inner = Some(JumboBytesMutInner::File {
                        path,
                        file,
                        file_size,
                        cursor_pos: original_cursor_pos as u64,
                    });
                    Ok(true)
                }
            }
            Poll::Ready(Err(err)) => Err(err),
            Poll::Pending => Ok(false),
        }
    }
}
impl From<Vec<u8>> for JumboBytesMut {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            inner: Some(JumboBytesMutInner::Ram {
                bytes: Cursor::new(bytes),
            }),
            max_size: u64::MAX,
            max_ram_size: SPILLOVER_SIZE.load(Ordering::Relaxed),
            directory: TEMPFILE_DIR.read().clone(),
        }
    }
}
impl From<String> for JumboBytesMut {
    fn from(data: String) -> Self {
        Vec::<u8>::from(data).into()
    }
}
impl AsyncRead for JumboBytesMut {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let self_mut = self.get_mut();
        if !self_mut.handle_poll_transition(cx)? {
            return Poll::Pending;
        }
        match &mut self_mut.inner {
            Some(JumboBytesMutInner::Ram { bytes }) => Pin::new(bytes).poll_read(cx, buf),
            Some(JumboBytesMutInner::File { file, .. }) => Pin::new(file).poll_read(cx, buf),
            Some(JumboBytesMutInner::Transitioning { .. }) => unreachable!(),
            None => Poll::Ready(Err(reuse_after_drop_error())),
        }
    }
}
impl futures::AsyncRead for JumboBytesMut {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let mut tokio_buf = ReadBuf::new(buf);
        AsyncRead::poll_read(self, cx, &mut tokio_buf).map_ok(|_| tokio_buf.filled().len())
    }
}

impl AsyncSeek for JumboBytesMut {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> Result<()> {
        match &mut self.get_mut().inner {
            Some(JumboBytesMutInner::Ram { bytes, .. }) => Pin::new(bytes).start_seek(position),
            Some(JumboBytesMutInner::Transitioning {
                file_size,
                cursor_pos,
                original_cursor_pos,
                ..
            }) => {
                match position {
                    SeekFrom::Start(index) => {
                        let index: usize = index.try_into().map_err(|_| bad_seek_error())?;
                        *cursor_pos = Some(index);
                    }
                    SeekFrom::End(from_end) => {
                        let new_cursor_pos = file_size
                            .checked_add_signed(from_end)
                            .ok_or_else(bad_seek_error)?;
                        *cursor_pos =
                            Some(new_cursor_pos.try_into().map_err(|_| bad_seek_error())?);
                    }
                    SeekFrom::Current(from_current) => {
                        let from_current: isize =
                            from_current.try_into().map_err(|_| bad_seek_error())?;
                        *cursor_pos = Some(
                            cursor_pos
                                .unwrap_or(*original_cursor_pos)
                                .checked_add_signed(from_current)
                                .ok_or_else(bad_seek_error)?,
                        );
                    }
                }
                Ok(())
            }
            Some(JumboBytesMutInner::File { file, .. }) => Pin::new(file).start_seek(position),
            None => Err(reuse_after_drop_error()),
        }
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
    ) -> Poll<std::io::Result<u64>> {
        let self_mut = self.get_mut();
        if !self_mut.handle_poll_transition(cx)? {
            return Poll::Pending;
        }
        match &mut self_mut.inner {
            Some(JumboBytesMutInner::Ram { bytes, .. }) => Pin::new(bytes).poll_complete(cx),
            Some(JumboBytesMutInner::File {
                file, cursor_pos, ..
            }) => Pin::new(file).poll_complete(cx).map_ok(|new_cur_pos| {
                *cursor_pos = new_cur_pos;
                new_cur_pos
            }),
            Some(JumboBytesMutInner::Transitioning { .. }) => unreachable!(),
            None => Poll::Ready(Err(reuse_after_drop_error())),
        }
    }
}
impl futures::AsyncSeek for JumboBytesMut {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        if let Err(err) = AsyncSeek::start_seek(self.as_mut(), pos) {
            return Poll::Ready(Err(err));
        }
        AsyncSeek::poll_complete(self, cx)
    }
}

impl AsyncWrite for JumboBytesMut {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        let self_mut = self.get_mut();
        if !self_mut.handle_poll_transition(cx)? {
            return Poll::Pending;
        }
        if self_mut
            .len()
            .checked_add(buf.len() as u64)
            .filter(|new_size| *new_size <= self_mut.max_size)
            .is_none()
        {
            return Poll::Ready(Result::Err(IoError::new(
                IoErrorKind::FileTooLarge,
                format!(
                    "buffer exceeded maximum size of {} bytes",
                    self_mut.max_size
                ),
            )));
        }
        match &mut self_mut.inner {
            Some(JumboBytesMutInner::Ram { bytes }) => {
                if buf.len() + bytes.get_mut().len() > self_mut.max_ram_size {
                    let cursor_pos = bytes.position();
                    let new_path = new_tempfile_path(&self_mut.directory);
                    let bytes = std::mem::take(bytes.get_mut());
                    let file_size = bytes.len();
                    let future: BoxFuture<'static, Result<(PathBuf, File)>> = async move {
                        let mut file = File::create_new(&new_path).await?;
                        file.write_all(&bytes).await?;
                        file.seek(SeekFrom::Start(cursor_pos as u64)).await?;
                        Ok((new_path, file))
                    }
                    .boxed();
                    self_mut.inner = Some(JumboBytesMutInner::Transitioning {
                        future,
                        original_cursor_pos: cursor_pos as usize,
                        cursor_pos: None,
                        file_size: file_size as u64,
                    });
                    if self_mut.handle_poll_transition(cx)? {
                        Pin::new(self_mut).poll_write(cx, buf)
                    } else {
                        Poll::Pending
                    }
                } else {
                    Pin::new(bytes).poll_write(cx, buf)
                }
            }
            Some(JumboBytesMutInner::File {
                file,
                path: _,
                cursor_pos,
                file_size,
            }) => match Pin::new(file).poll_write(cx, buf)? {
                Poll::Pending => Poll::Pending,
                Poll::Ready(bytes_written) => {
                    *cursor_pos += bytes_written as u64;
                    *file_size = *file_size.max(cursor_pos);
                    Poll::Ready(Ok(bytes_written))
                }
            },
            Some(JumboBytesMutInner::Transitioning { .. }) => unreachable!(),
            None => Poll::Ready(Err(reuse_after_drop_error())),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut AsyncContext<'_>) -> Poll<Result<()>> {
        let self_mut = self.get_mut();
        if !self_mut.handle_poll_transition(cx)? {
            return Poll::Pending;
        }
        match &mut self_mut.inner {
            Some(JumboBytesMutInner::Ram { .. }) => Poll::Ready(Ok(())),
            Some(JumboBytesMutInner::File { file, .. }) => Pin::new(file).poll_flush(cx),
            Some(JumboBytesMutInner::Transitioning { .. }) => unreachable!(),
            None => Poll::Ready(Err(reuse_after_drop_error())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut AsyncContext<'_>) -> Poll<Result<()>> {
        let self_mut = self.get_mut();
        if !self_mut.handle_poll_transition(cx)? {
            return Poll::Pending;
        }
        match &mut self_mut.inner {
            Some(JumboBytesMutInner::Ram { .. }) => Poll::Ready(Ok(())),
            Some(JumboBytesMutInner::File { file, .. }) => Pin::new(file).poll_shutdown(cx),
            Some(JumboBytesMutInner::Transitioning { .. }) => unreachable!(),
            None => Poll::Ready(Err(reuse_after_drop_error())),
        }
    }
}

impl Drop for JumboBytesMut {
    fn drop(&mut self) {
        match self.inner.take() {
            Some(JumboBytesMutInner::File { path, file, .. }) => {
                let file = file.try_into_std().expect(
                    "if we're being dropped, then there must be no pending async operations",
                );
                // We expect the cleanup thread to be infalliable and always exist, so we don't need to handle errors.
                // The cleanup thread is also what allows dropping to be non-blocking
                let _ = TEMPFILE_CLEANER.send((file, path.into()));
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
enum JumboBytesInner {
    Ram {
        bytes: Bytes,
        cursor_pos: usize,
    },
    File {
        path: Arc<Path>,
        file: File,
        file_size: u64,
        // the cursor_pos is here because clones need to seek from the same position, and keeping track of it
        // externally seems to be the only way to get the value syncronosuly
        cursor_pos: u64,
    },
    Error(IoError),
}
impl Clone for JumboBytesInner {
    fn clone(&self) -> Self {
        match self {
            Self::Ram { bytes, cursor_pos } => Self::Ram {
                bytes: bytes.clone(),
                cursor_pos: *cursor_pos,
            },
            Self::File {
                path,
                file_size,
                cursor_pos,
                ..
            } => {
                let path = path.clone();
                let file_size = *file_size;
                let cursor_pos = *cursor_pos;
                // Unfortunately we need to open the file syncronosuly. The only reason why we even have synchronous
                // clones of async-readable streams is because azure wants it.
                let mut file = match StdFile::open(&path) {
                    Ok(new_file) => new_file,
                    Err(err) => return Self::Error(err),
                };
                // The entire reason why we keep track of the cursor position
                if let Err(err) = file.seek(SeekFrom::Start(cursor_pos)) {
                    return Self::Error(err);
                };

                Self::File {
                    path,
                    file: File::from_std(file),
                    file_size,
                    cursor_pos,
                }
            }
            Self::Error(err) => Self::Error(IoError::new(err.kind(), err.to_string())),
        }
    }
}

/// The sharable and cloneable counterpart to [JumboBytesMut]. All clones are pointers to the same underlying data.
/// This was originally created because azure _just needs_ cloneable and indivudally seekable streams.
#[derive(Debug, Clone)]
pub struct JumboBytes {
    inner: Option<JumboBytesInner>,
}
impl JumboBytes {
    /// Returns the inner [Bytes] but only if we `self` does not represent a temporary file.
    /// If this returns `Some`, then `Self` becomes empty
    pub fn take_bytes(&mut self) -> Option<Bytes> {
        match self.inner.as_mut() {
            Some(JumboBytesInner::Ram {
                bytes, cursor_pos, ..
            }) => {
                *cursor_pos = 0;
                Some(std::mem::take(bytes))
            }
            _ => None,
        }
    }

    /// Allows you to see the inner data but only if `self` does not represent a temporary file.
    pub fn as_slice(&self) -> Option<&[u8]> {
        match self.inner.as_ref() {
            Some(JumboBytesInner::Ram { bytes, .. }) => Some(&bytes),
            _ => None,
        }
    }

    #[inline]
    /// The size of the underlying data.
    pub fn len(&self) -> usize {
        match self.inner.as_ref() {
            Some(JumboBytesInner::File { file_size, .. }) => *file_size as usize,
            Some(JumboBytesInner::Ram { bytes, .. }) => bytes.len(),
            _ => 0,
        }
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AsyncRead for JumboBytes {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        match &mut self.get_mut().inner {
            Some(JumboBytesInner::Ram {
                bytes, cursor_pos, ..
            }) => {
                let bytes_slice = if *cursor_pos > bytes.len() {
                    // seek might have been called to a wild location
                    &[]
                } else {
                    &bytes[(*cursor_pos)..]
                };
                let bytes_to_pass = buf.remaining();
                if bytes_slice.len() > bytes_to_pass {
                    buf.put_slice(&bytes_slice[0..bytes_to_pass]);
                    *cursor_pos += bytes_to_pass;
                } else {
                    buf.put_slice(bytes_slice);
                    *cursor_pos = bytes.len();
                }
                Poll::Ready(Ok(()))
            }
            Some(JumboBytesInner::File {
                file, cursor_pos, ..
            }) => {
                let pre_remaining = buf.remaining() as u64;
                let result = Pin::new(file).poll_read(cx, buf);
                let post_remaining = buf.remaining() as u64;
                *cursor_pos += pre_remaining - post_remaining;
                result
            }
            Some(JumboBytesInner::Error(err)) => {
                Poll::Ready(Err(IoError::new(err.kind(), err.to_string())))
            }
            None => Poll::Ready(Err(reuse_after_drop_error())),
        }
    }
}
impl futures::AsyncRead for JumboBytes {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let mut tokio_buf = ReadBuf::new(buf);
        match AsyncRead::poll_read(self, cx, &mut tokio_buf) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(tokio_buf.filled().len())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl AsyncSeek for JumboBytes {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> Result<()> {
        match &mut self.get_mut().inner {
            Some(JumboBytesInner::Ram {
                bytes, cursor_pos, ..
            }) => {
                match position {
                    SeekFrom::Start(index) => {
                        let index: usize = index.try_into().map_err(|_| bad_seek_error())?;
                        *cursor_pos = index;
                    }
                    SeekFrom::End(from_end) => {
                        let from_end: isize = from_end.try_into().map_err(|_| bad_seek_error())?;
                        *cursor_pos = bytes
                            .len()
                            .checked_add_signed(from_end)
                            .ok_or_else(bad_seek_error)?;
                    }
                    SeekFrom::Current(from_current) => {
                        let from_current: isize =
                            from_current.try_into().map_err(|_| bad_seek_error())?;
                        *cursor_pos = cursor_pos
                            .checked_add_signed(from_current)
                            .ok_or_else(bad_seek_error)?;
                    }
                }
                Ok(())
            }
            Some(JumboBytesInner::File { file, .. }) => Pin::new(file).start_seek(position),
            Some(JumboBytesInner::Error(err)) => Err(IoError::new(err.kind(), err.to_string())),
            None => Err(reuse_after_drop_error()),
        }
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
    ) -> Poll<std::io::Result<u64>> {
        match &mut self.get_mut().inner {
            Some(JumboBytesInner::Ram { cursor_pos, .. }) => Poll::Ready(Ok(*cursor_pos as u64)),
            Some(JumboBytesInner::File {
                file, cursor_pos, ..
            }) => Pin::new(file).poll_complete(cx).map_ok(|new_cursor_pos| {
                *cursor_pos = new_cursor_pos;
                new_cursor_pos
            }),
            Some(JumboBytesInner::Error(err)) => {
                Poll::Ready(Err(IoError::new(err.kind(), err.to_string())))
            }
            None => Poll::Ready(Err(reuse_after_drop_error())),
        }
    }
}
impl futures::AsyncSeek for JumboBytes {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut AsyncContext<'_>,
        pos: SeekFrom,
    ) -> Poll<Result<u64>> {
        if let Err(err) = AsyncSeek::start_seek(self.as_mut(), pos) {
            return Poll::Ready(Err(err));
        }
        AsyncSeek::poll_complete(self, cx)
    }
}

impl Drop for JumboBytes {
    fn drop(&mut self) {
        match self.inner.take() {
            Some(JumboBytesInner::File { path, file, .. }) => {
                // Only clean up the file if this is the last JumboBytes of the same file remaining
                if Arc::strong_count(&path) <= 1 {
                    let file = file.try_into_std().expect(
                        "if we're being dropped, then there must be no pending async operations",
                    );
                    // We expect the cleanup thread to be infalliable and always exist, so we don't need to handle errors.
                    // The cleanup thread is also what allows dropping to be non-blocking
                    let _ = TEMPFILE_CLEANER.send((file, path));
                }
            }
            _ => {}
        }
    }
}

enum JumboBytesMutSyncInner {
    Ram {
        bytes: Cursor<Vec<u8>>,
    },
    File {
        path: PathBuf,
        file: StdFile,
        file_size: u64,
    },
}
impl std::fmt::Debug for JumboBytesMutSyncInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JumboBytesMutSyncInner::Ram { .. } => f.write_str("JumboBytesMutSyncInner::Ram { .. }"),
            JumboBytesMutSyncInner::File { path, .. } => f.write_fmt(format_args!(
                "JumboBytesMutSyncInner::File {{ path: {path:?}, .. }}"
            )),
        }
    }
}

/// Blocking counterpart to [JumboBytesMut]
#[derive(Debug)]
pub struct JumboBytesMutSync {
    /// The maximum size that the underlying data can be. If the internal data size already exceeds this value, or if a
    /// write to `self` would result in the internal data size exceeding this value, then all future writes will fail
    /// with [IoErrorKind::FileTooLarge].
    pub max_size: u64,
    inner: Option<JumboBytesMutSyncInner>,
    max_ram_size: usize,
    directory: Arc<Path>,
}
impl JumboBytesMutSync {
    /// Creates a new [JumboBytesMutSync] where the underlying data cannot be larger than `max_size`. If a write attempts
    /// this, it will fail.
    pub fn new(max_size: u64) -> Self {
        Self {
            inner: Some(JumboBytesMutSyncInner::Ram {
                bytes: Cursor::new(Vec::new()),
            }),
            max_size,
            max_ram_size: SPILLOVER_SIZE.load(Ordering::Relaxed),
            directory: TEMPFILE_DIR.read().clone(),
        }
    }

    /// Returns the inner `Vec<u8>` but only if `self` does not represent a temporary file. If this returns `Some`,
    /// then this JumboBytesMutSync becomes empty
    pub fn take_vec(&mut self) -> Option<Vec<u8>> {
        match self.inner.as_mut() {
            Some(JumboBytesMutSyncInner::Ram { bytes }) => {
                bytes.set_position(0);
                Some(std::mem::take(bytes.get_mut()))
            }
            _ => None,
        }
    }

    /// Allows you to see the inner data but only if `self` does not represent a temporary file.
    pub fn as_slice(&self) -> Option<&[u8]> {
        match self.inner.as_ref() {
            Some(JumboBytesMutSyncInner::Ram { bytes, .. }) => Some(&bytes.get_ref()),
            _ => None,
        }
    }

    /// Coerces the underlying data into a `Vec<u8>`. Returns`Err` if the underlying data is larger than `max_size`.
    pub fn into_vec(mut self, max_size: u64) -> Result<Vec<u8>> {
        match self.inner.take() {
            Some(JumboBytesMutSyncInner::Ram { bytes, .. }) => Ok(bytes.into_inner()),
            Some(JumboBytesMutSyncInner::File {
                path,
                mut file,
                file_size,
            }) => {
                let mut resulting_bytes = Vec::<u8>::with_capacity(1024);
                if file_size > max_size {
                    return Err(IoError::new(
                        IoErrorKind::FileTooLarge,
                        "file exceeds maximum buffer size",
                    ));
                }
                file.rewind()?;
                std::io::copy(&mut file, &mut resulting_bytes)?;
                let _ = TEMPFILE_CLEANER.send((file, path.into()));
                Ok(resulting_bytes)
            }
            None => Err(reuse_after_drop_error()),
        }
    }

    #[inline]
    /// Get the size of the underlying data
    pub fn len(&self) -> u64 {
        match self.inner.as_ref() {
            Some(JumboBytesMutSyncInner::File { file_size, .. }) => *file_size,
            Some(JumboBytesMutSyncInner::Ram { bytes, .. }) => bytes.get_ref().len() as u64,
            None => 0,
        }
    }

    #[inline]
    /// `self.len() == 0`
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl Read for JumboBytesMutSync {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self.inner.as_mut() {
            Some(JumboBytesMutSyncInner::Ram { bytes }) => bytes.read(buf),
            Some(JumboBytesMutSyncInner::File { file, .. }) => file.read(buf),
            None => Err(reuse_after_drop_error()),
        }
    }
}
impl Seek for JumboBytesMutSync {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        match self.inner.as_mut() {
            Some(JumboBytesMutSyncInner::Ram { bytes }) => Seek::seek(bytes, pos),
            Some(JumboBytesMutSyncInner::File { file, .. }) => file.seek(pos),
            None => Err(reuse_after_drop_error()),
        }
    }
}
impl Write for JumboBytesMutSync {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match self.inner.as_mut() {
            Some(JumboBytesMutSyncInner::Ram { bytes }) => {
                if buf.len() + bytes.get_mut().len() > self.max_ram_size {
                    let cursor_pos = bytes.position();
                    let new_path = new_tempfile_path(&self.directory);
                    let bytes = std::mem::take(bytes.get_mut());
                    let file_size = bytes.len() as u64;

                    let mut file = StdFile::create_new(&new_path)?;
                    file.write_all(&bytes)?;
                    file.seek(SeekFrom::Start(cursor_pos))?;
                    let buf_bytes_written = file.write(buf)?;
                    self.inner = Some(JumboBytesMutSyncInner::File {
                        path: new_path,
                        file,
                        file_size,
                    });
                    Ok(buf_bytes_written)
                } else {
                    Write::write(bytes, buf)
                }
            }
            Some(JumboBytesMutSyncInner::File { file, .. }) => file.write(buf),
            None => Err(reuse_after_drop_error()),
        }
    }

    fn flush(&mut self) -> Result<()> {
        match self.inner.as_mut() {
            Some(JumboBytesMutSyncInner::Ram { bytes }) => Write::flush(bytes),
            Some(JumboBytesMutSyncInner::File { file, .. }) => file.flush(),
            None => Err(reuse_after_drop_error()),
        }
    }
}
impl From<Vec<u8>> for JumboBytesMutSync {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            inner: Some(JumboBytesMutSyncInner::Ram {
                bytes: Cursor::new(bytes),
            }),
            max_size: u64::MAX,
            max_ram_size: SPILLOVER_SIZE.load(Ordering::Relaxed),
            directory: TEMPFILE_DIR.read().clone(),
        }
    }
}
impl From<String> for JumboBytesMutSync {
    fn from(data: String) -> Self {
        Vec::<u8>::from(data).into()
    }
}
impl From<JumboBytesMut> for JumboBytesMutSync {
    fn from(mut value: JumboBytesMut) -> Self {
        Self {
            max_size: value.max_size,
            inner: match value.inner.take() {
                Some(JumboBytesMutInner::Ram { bytes }) => {
                    Some(JumboBytesMutSyncInner::Ram { bytes })
                }
                Some(JumboBytesMutInner::File {
                    path,
                    file,
                    cursor_pos,
                    file_size,
                }) => {
                    // This unwraps may seem scary, but JumboBytesMutSync is only used during backups, which is a very
                    // sequential operation. Barring catastrophic disk failure, these should be harmless.
                    let mut file = file.try_into_std().unwrap();
                    file.seek(SeekFrom::Start(cursor_pos)).unwrap();
                    Some(JumboBytesMutSyncInner::File {
                        path,
                        file,
                        file_size,
                    })
                }
                None => None,
                _ => unimplemented!(),
            },
            max_ram_size: value.max_ram_size,
            directory: value.directory.clone(),
        }
    }
}
impl From<JumboBytesMutSync> for JumboBytesMut {
    fn from(mut value: JumboBytesMutSync) -> Self {
        Self {
            max_size: value.max_size,
            inner: match value.inner.take() {
                Some(JumboBytesMutSyncInner::Ram { bytes }) => {
                    Some(JumboBytesMutInner::Ram { bytes })
                }
                Some(JumboBytesMutSyncInner::File {
                    path,
                    mut file,
                    file_size,
                }) => {
                    // Not moving the cursor shouldn't produce an error
                    let cursor_pos = file.seek(SeekFrom::Current(0)).unwrap();
                    Some(JumboBytesMutInner::File {
                        path,
                        file: File::from_std(file),
                        file_size,
                        cursor_pos,
                    })
                }
                None => None,
            },
            max_ram_size: value.max_ram_size,
            directory: value.directory.clone(),
        }
    }
}

impl Drop for JumboBytesMutSync {
    fn drop(&mut self) {
        match self.inner.take() {
            Some(JumboBytesMutSyncInner::File { path, file, .. }) => {
                // We expect the cleanup thread to be infalliable and always exist, so we don't need to handle errors.
                // The cleanup thread is also what allows dropping to be non-blocking
                let _ = TEMPFILE_CLEANER.send((file, path.into()));
            }
            _ => {}
        }
    }
}

fn bad_seek_error() -> IoError {
    // inspired by Rust's stdlib
    IoError::new(
        IoErrorKind::InvalidInput,
        "invalid seek to a negative or overflowing position",
    )
}

fn reuse_after_drop_error() -> IoError {
    // This error shouldn't happen at all, though Rust's nature compells us to consider the posiibility
    IoError::new(
        IoErrorKind::BrokenPipe,
        "this item was apparently already dropped",
    )
}

#[cfg(feature = "azure_core")]
#[async_trait::async_trait]
impl azure_core::SeekableStream for JumboBytes {
    async fn reset(&mut self) -> azure_core::Result<()> {
        self.rewind().await?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.len()
    }
}
