use std::sync::Arc;

use crate::backend::traits::{ObjectWriter, StorageBackend};
use crate::core::buffer::BlockCache;
use crate::core::config::PyroIOConfig;
use crate::error::{PyroError, Result};

/// File open mode — binary read or binary write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Read,
    Write,
}

/// Owns cursor, buffers, and mode enforcement.
/// Delegates I/O to a [`StorageBackend`].
pub struct PyroIO {
    backend: Arc<dyn StorageBackend>,
    mode: OpenMode,
    cursor: u64,
    /// Cached file size from metadata; None until first access.
    size: Option<u64>,
    closed: bool,
    cache: BlockCache,
    writer: Option<Box<dyn ObjectWriter>>,
}

impl PyroIO {
    /// Create a new PyroIO in the given mode.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        mode: OpenMode,
        config: PyroIOConfig,
    ) -> Self {
        Self {
            backend,
            mode,
            cursor: 0,
            size: None,
            closed: false,
            cache: BlockCache::new(&config.read_config),
            writer: None,
        }
    }

    /// Read up to `size` bytes. If `size` is negative, read all remaining bytes.
    pub fn read(&mut self, size: i64) -> Result<Vec<u8>> {
        if self.mode != OpenMode::Read {
            return Err(PyroError::NotSupported);
        }
        if self.closed {
            return Err(PyroError::Closed);
        }

        if size == 0 {
            return Ok(Vec::new());
        }

        // read(-1) means "read remaining"
        if size < 0 {
            return self.read_remaining();
        }

        let size = size as usize;
        let mut out = vec![0u8; size];
        let filled = self.cache.read(self.cursor, &mut out, self.backend.as_ref())?;
        self.cursor += filled as u64;
        out.truncate(filled);
        Ok(out)
    }

    /// Read directly into a caller-provided buffer.
    pub fn read_into(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.mode != OpenMode::Read {
            return Err(PyroError::NotSupported);
        }
        if self.closed {
            return Err(PyroError::Closed);
        }

        let filled = self.cache.read(self.cursor, buf, self.backend.as_ref())?;
        self.cursor += filled as u64;
        Ok(filled)
    }

    /// Read all remaining bytes from cursor to EOF.
    fn read_remaining(&mut self) -> Result<Vec<u8>> {
        let file_size = self.get_size()?;
        if self.cursor >= file_size {
            return Ok(Vec::new());
        }
        let remaining = file_size - self.cursor;
        let data = self.backend.download(self.cursor, remaining)?;
        self.cursor += data.len() as u64;
        Ok(data)
    }

    /// Write data. Returns the number of bytes written (always == data.len()).
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.mode != OpenMode::Write {
            return Err(PyroError::NotSupported);
        }
        if self.closed {
            return Err(PyroError::Closed);
        }

        // Lazy-init writer on first write.
        if self.writer.is_none() {
            self.writer = Some(self.backend.create_writer()?);
        }

        self.writer.as_mut().unwrap().write(data)?;
        self.cursor += data.len() as u64;
        Ok(data.len())
    }

    /// Seek to a new position. In write mode, only `seek(0, SEEK_CUR)` is
    /// allowed (equivalent to `tell()`).
    pub fn seek(&mut self, whence: std::io::SeekFrom) -> Result<u64> {
        if self.closed {
            return Err(PyroError::Closed);
        }

        // In write mode, only allow SeekFrom::Current(0) as a tell() equivalent.
        if self.mode == OpenMode::Write {
            if matches!(whence, std::io::SeekFrom::Current(0)) {
                return Ok(self.cursor);
            }
            return Err(PyroError::NotSupported);
        }

        let new_pos = match whence {
            std::io::SeekFrom::Start(n) => Some(n),
            std::io::SeekFrom::Current(n) => {
                let pos = self.cursor as i64 + n as i64;
                if pos >= 0 { Some(pos as u64) } else { None }
            }
            std::io::SeekFrom::End(n) => {
                let size = self.get_size()? as i64;
                let pos = size + n as i64;
                if pos >= 0 { Some(pos as u64) } else { None }
            }
        };

        match new_pos {
            Some(p) => {
                self.cursor = p;
                Ok(p)
            }
            None => Err(PyroError::InvalidArgument(
                "negative seek position".into(),
            )),
        }
    }

    /// Return the current cursor position.
    pub fn tell(&self) -> u64 {
        self.cursor
    }

    /// Flush buffered write data toward the backend.
    pub fn flush(&mut self) -> Result<()> {
        if self.closed {
            return Err(PyroError::Closed);
        }
        if let Some(ref mut w) = self.writer {
            w.flush()?;
        }
        Ok(())
    }

    /// Close the file. Finalizes writes if in write mode.
    pub fn close(&mut self) -> Result<()> {
        self.shutdown(|w| w.close())
    }

    /// Abort the file. Discards any pending writes.
    pub fn abort(&mut self) -> Result<()> {
        self.shutdown(|w| w.abort())
    }

    fn shutdown(&mut self, finalize: impl FnOnce(&mut Box<dyn ObjectWriter>) -> Result<()>) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if let Some(ref mut w) = self.writer {
            finalize(w)?;
        }
        self.closed = true;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn mode(&self) -> OpenMode {
        self.mode
    }

    pub fn backend_name(&self) -> &str {
        self.backend.name()
    }

    /// Get the file size, fetching from metadata if not yet cached.
    fn get_size(&mut self) -> Result<u64> {
        if let Some(s) = self.size {
            return Ok(s);
        }
        let meta = self.backend.metadata()?;
        let size = meta.content_length.ok_or_else(|| {
            PyroError::NotSupported
        })?;
        self.size = Some(size);
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalBackend;

    fn write_test_file(path: &std::path::Path, data: &[u8]) {
        std::fs::write(path, data).unwrap();
    }

    #[test]
    fn read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        {
            let backend = Arc::new(LocalBackend::new(&path));
            let mut f = PyroIO::new(backend, OpenMode::Write, PyroIOConfig::default());
            assert_eq!(f.write(b"hello pyrofile").unwrap(), 14);
            f.close().unwrap();
        }
        {
            let backend = Arc::new(LocalBackend::new(&path));
            let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());
            assert_eq!(f.read(-1).unwrap(), b"hello pyrofile");
        }
    }

    #[test]
    fn read_with_exact_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"abcdefghij");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());
        assert_eq!(f.read(5).unwrap(), b"abcde");
        assert_eq!(f.read(5).unwrap(), b"fghij");
        assert!(f.read(5).unwrap().is_empty());
    }

    #[test]
    fn read_zero_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"data");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());
        assert!(f.read(0).unwrap().is_empty());
        assert_eq!(f.tell(), 0);
    }

    #[test]
    fn seek_set_cur_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"abcdefghij");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());

        f.seek(std::io::SeekFrom::Start(5)).unwrap();
        assert_eq!(f.tell(), 5);
        assert_eq!(f.read(3).unwrap(), b"fgh");

        f.seek(std::io::SeekFrom::Current(-3)).unwrap();
        assert_eq!(f.tell(), 5);

        f.seek(std::io::SeekFrom::End(-2)).unwrap();
        assert_eq!(f.read(2).unwrap(), b"ij");
    }

    #[test]
    fn seek_negative_position_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"abcde");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());
        assert!(matches!(
            f.seek(std::io::SeekFrom::Current(-1)),
            Err(PyroError::InvalidArgument(_))
        ));
    }

    #[test]
    fn seek_blocked_in_write_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Write, PyroIOConfig::default());
        assert!(matches!(
            f.seek(std::io::SeekFrom::Start(0)),
            Err(PyroError::NotSupported)
        ));
    }

    #[test]
    fn seek_current_zero_allowed_in_write_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Write, PyroIOConfig::default());
        f.write(b"hello").unwrap();
        assert_eq!(f.seek(std::io::SeekFrom::Current(0)).unwrap(), 5);
    }

    #[test]
    fn tell_tracks_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Write, PyroIOConfig::default());
        assert_eq!(f.tell(), 0);
        f.write(b"hello").unwrap();
        assert_eq!(f.tell(), 5);
        f.write(b" world").unwrap();
        assert_eq!(f.tell(), 11);
    }

    #[test]
    fn write_in_read_mode_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"data");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());
        assert!(matches!(f.write(b"nope"), Err(PyroError::NotSupported)));
    }

    #[test]
    fn read_in_write_mode_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Write, PyroIOConfig::default());
        assert!(matches!(f.read(10), Err(PyroError::NotSupported)));
    }

    #[test]
    fn operations_after_close_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"data");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());
        f.close().unwrap();

        assert!(f.read(1).is_err());
        assert!(f.seek(std::io::SeekFrom::Start(0)).is_err());
        assert!(f.flush().is_err());
    }

    #[test]
    fn close_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"data");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, PyroIOConfig::default());
        f.close().unwrap();
        f.close().unwrap();
    }

    #[test]
    fn abort_discards_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Write, PyroIOConfig::default());
        f.write(b"hello").unwrap();
        f.abort().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn read_with_small_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        write_test_file(&path, b"abcdefghijklmnopqrstuvwxyz");

        let config = PyroIOConfig {
            read_config: crate::core::config::ReadConfig {
                block_size: 4,  // tiny blocks to exercise multi-block reads
                max_blocks: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        let backend = Arc::new(LocalBackend::new(&path));
        let mut f = PyroIO::new(backend, OpenMode::Read, config);
        let result = f.read(-1).unwrap();
        assert_eq!(result, b"abcdefghijklmnopqrstuvwxyz");
    }
}
