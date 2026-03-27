use std::fs;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use crate::backend::traits::{ObjectMeta, ObjectWriter, StorageBackend};
use crate::error::{PyroError, Result};

/// Local filesystem backend.
pub struct LocalBackend {
    path: PathBuf,
    // Lazily opened on first read. Lock-free after initialization.
    read_handle: std::sync::OnceLock<fs::File>,
}

impl LocalBackend {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            read_handle: std::sync::OnceLock::new(),
        }
    }

    fn get_read_handle(&self) -> Result<&fs::File> {
        // OnceLock::get_or_init is stable but can't propagate errors.
        // We try to open the file; if it fails, we don't cache and return error.
        if let Some(f) = self.read_handle.get() {
            return Ok(f);
        }
        let file = fs::File::open(&self.path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                PyroError::NotFound(self.path.display().to_string())
            } else {
                PyroError::Io(e)
            }
        })?;
        // Race: another thread might have initialized in the meantime.
        // set() returns Err(file) if already set — that's fine, we just use get().
        let _ = self.read_handle.set(file);
        Ok(self.read_handle.get().unwrap())
    }
}

impl StorageBackend for LocalBackend {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let file = self.get_read_handle()?;
        let n = file.read_at(buf, offset)?;
        Ok(n)
    }

    fn metadata(&self) -> Result<ObjectMeta> {
        let meta = fs::metadata(&self.path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                PyroError::NotFound(self.path.display().to_string())
            } else {
                PyroError::Io(e)
            }
        })?;
        Ok(ObjectMeta {
            content_length: Some(meta.len()),
            content_type: None,
        })
    }

    fn create_writer(&self) -> Result<Box<dyn ObjectWriter>> {
        let file = fs::File::create(&self.path)?;
        Ok(Box::new(LocalWriter {
            file,
            path: self.path.clone(),
            closed: false,
        }))
    }

    fn name(&self) -> &str {
        self.path.to_str().unwrap_or("<non-utf8 path>")
    }
}

/// Writer for the local filesystem backend.
pub struct LocalWriter {
    file: fs::File,
    path: PathBuf,
    closed: bool,
}

impl ObjectWriter for LocalWriter {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.closed {
            return Err(PyroError::Closed);
        }
        self.file.write_all(data)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.closed {
            return Err(PyroError::Closed);
        }
        self.file.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.file.sync_all()?;
        self.closed = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<()> {
        self.closed = true;
        let _ = fs::remove_file(&self.path);
        Ok(())
    }
}

impl Drop for LocalWriter {
    fn drop(&mut self) {
        if !self.closed {
            // Consistent with SmartWriter: drop without close aborts.
            let _ = self.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::traits::StorageBackend;

    #[test]
    fn read_at_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"hello world").unwrap();

        let backend = LocalBackend::new(&path);
        let mut buf = [0u8; 5];
        let n = backend.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn read_at_with_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"hello world").unwrap();

        let backend = LocalBackend::new(&path);
        let mut buf = [0u8; 5];
        let n = backend.read_at(6, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn read_at_empty_buffer_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"hello").unwrap();

        let backend = LocalBackend::new(&path);
        let n = backend.read_at(0, &mut []).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn read_at_past_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"hi").unwrap();

        let backend = LocalBackend::new(&path);
        let mut buf = [0u8; 10];
        let n = backend.read_at(100, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn read_at_reuses_file_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abcdefghij").unwrap();

        let backend = LocalBackend::new(&path);
        let mut buf = [0u8; 3];
        backend.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"abc");
        backend.read_at(7, &mut buf).unwrap();
        assert_eq!(&buf, b"hij");
        // OnceLock should have been initialized once.
        assert!(backend.read_handle.get().is_some());
    }

    #[test]
    fn metadata_returns_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"twelve bytes").unwrap();

        let backend = LocalBackend::new(&path);
        let meta = backend.metadata().unwrap();
        assert_eq!(meta.content_length, Some(12));
    }

    #[test]
    fn metadata_not_found() {
        let backend = LocalBackend::new("/nonexistent/path/file.bin");
        assert!(matches!(backend.metadata(), Err(PyroError::NotFound(_))));
    }

    #[test]
    fn name_returns_path() {
        let backend = LocalBackend::new("/tmp/test.bin");
        assert_eq!(backend.name(), "/tmp/test.bin");
    }

    #[test]
    fn writer_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = LocalBackend::new(&path);
        let mut w = backend.create_writer().unwrap();
        w.write(b"hello ").unwrap();
        w.write(b"world").unwrap();
        w.close().unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    }

    #[test]
    fn writer_abort_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = LocalBackend::new(&path);
        let mut w = backend.create_writer().unwrap();
        w.write(b"data").unwrap();
        w.abort().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn writer_drop_without_close_aborts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");

        let backend = LocalBackend::new(&path);
        {
            let mut w = backend.create_writer().unwrap();
            w.write(b"data").unwrap();
        }
        assert!(!path.exists());
    }
}
