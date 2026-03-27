use std::sync::Arc;
use std::thread;

use crate::backend::traits::StorageBackend;
use crate::error::{PyroError, Result};

/// A completed prefetch result.
struct FetchResult {
    data: Vec<u8>,
    file_offset: u64,
    valid: usize,
}

/// Read-ahead buffer with background prefetching.
///
/// Maintains a current buffer and optionally prefetches the next sequential
/// chunk in a background thread. The prefetch thread shares the backend via
/// Arc and communicates results back through a channel.
pub(crate) struct PrefetchReader {
    backend: Arc<dyn StorageBackend>,
    buf_size: usize,

    // Current buffer
    data: Vec<u8>,
    file_offset: u64,
    valid: usize,

    // Prefetch state
    pending: Option<std::sync::mpsc::Receiver<Result<FetchResult>>>,
    pending_offset: u64,
}

impl PrefetchReader {
    pub fn new(backend: Arc<dyn StorageBackend>, buf_size: usize) -> Self {
        Self {
            backend,
            buf_size,
            data: vec![0u8; buf_size],
            file_offset: 0,
            valid: 0,
            pending: None,
            pending_offset: 0,
        }
    }

    /// Check if the cursor is within the current buffer.
    pub fn hit(&self, cursor: u64) -> bool {
        self.valid > 0
            && cursor >= self.file_offset
            && cursor < self.file_offset + self.valid as u64
    }

    /// Read from the current buffer into `dest`.
    /// Returns bytes copied. Does not advance any cursor.
    pub fn read_into(&self, cursor: u64, dest: &mut [u8]) -> usize {
        if !self.hit(cursor) {
            return 0;
        }
        let buf_offset = (cursor - self.file_offset) as usize;
        let available = self.valid - buf_offset;
        let n = dest.len().min(available);
        dest[..n].copy_from_slice(&self.data[buf_offset..buf_offset + n]);
        n
    }

    /// Fill the buffer for the given offset. If a matching prefetch is ready,
    /// use it. Otherwise, do a synchronous read.
    pub fn fill(&mut self, file_offset: u64) -> Result<()> {
        // Check if prefetch matches
        if let Some(rx) = self.pending.take() {
            if self.pending_offset == file_offset {
                match rx.recv() {
                    Ok(Ok(result)) => {
                        self.data.resize(result.data.len(), 0);
                        self.data[..result.valid].copy_from_slice(&result.data[..result.valid]);
                        self.file_offset = result.file_offset;
                        self.valid = result.valid;
                        self.start_prefetch(file_offset + self.valid as u64);
                        return Ok(());
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {} // channel closed, fall through to sync
                }
            }
            // Prefetch was for wrong offset — discard it
        }

        // Synchronous fill
        let n = self.backend.read_at(file_offset, &mut self.data)?;
        self.file_offset = file_offset;
        self.valid = n;

        // Start prefetching the next chunk
        self.start_prefetch(file_offset + n as u64);

        Ok(())
    }

    /// Kick off a background prefetch for the given offset.
    fn start_prefetch(&mut self, offset: u64) {
        let backend = Arc::clone(&self.backend);
        let buf_size = self.buf_size;
        let (tx, rx) = std::sync::mpsc::channel();

        thread::spawn(move || {
            let mut data = vec![0u8; buf_size];
            let result = backend.read_at(offset, &mut data).map(|n| FetchResult {
                data,
                file_offset: offset,
                valid: n,
            });
            let _ = tx.send(result);
        });

        self.pending = Some(rx);
        self.pending_offset = offset;
    }

    /// Cancel any in-flight prefetch (e.g., after a seek).
    pub fn cancel_prefetch(&mut self) {
        self.pending = None;
    }

    pub fn capacity(&self) -> usize {
        self.buf_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalBackend;

    #[test]
    fn basic_fill_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abcdefghijklmnopqrstuvwxyz").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PrefetchReader::new(backend, 10);

        reader.fill(0).unwrap();
        assert!(reader.hit(0));
        assert!(reader.hit(9));
        assert!(!reader.hit(10));

        let mut dest = [0u8; 5];
        let n = reader.read_into(0, &mut dest);
        assert_eq!(n, 5);
        assert_eq!(&dest, b"abcde");
    }

    #[test]
    fn prefetch_hits_on_sequential_access() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abcdefghijklmnopqrstuvwxyz").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PrefetchReader::new(backend, 10);

        // First fill at 0 — also starts prefetch at 10
        reader.fill(0).unwrap();

        // Give prefetch thread time to complete
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Second fill at 10 should use prefetched data
        reader.fill(10).unwrap();
        assert!(reader.hit(10));

        let mut dest = [0u8; 5];
        let n = reader.read_into(10, &mut dest);
        assert_eq!(n, 5);
        assert_eq!(&dest, b"klmno");
    }

    #[test]
    fn seek_invalidates_prefetch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abcdefghijklmnopqrstuvwxyz").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PrefetchReader::new(backend, 10);

        reader.fill(0).unwrap();
        reader.cancel_prefetch();

        // Fill at non-sequential offset — no prefetch to use
        reader.fill(20).unwrap();
        assert!(reader.hit(20));

        let mut dest = [0u8; 5];
        let n = reader.read_into(20, &mut dest);
        assert_eq!(n, 5);
        assert_eq!(&dest, b"uvwxy");
    }

    #[test]
    fn empty_buffer_has_no_hits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"data").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let reader = PrefetchReader::new(backend, 1024);
        assert!(!reader.hit(0));
    }

    #[test]
    fn read_into_miss_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"data").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let reader = PrefetchReader::new(backend, 1024);
        let mut dest = [0u8; 5];
        assert_eq!(reader.read_into(0, &mut dest), 0);
    }
}
