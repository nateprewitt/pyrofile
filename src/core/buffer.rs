use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;

use crate::backend::traits::StorageBackend;
use crate::error::{PyroError, Result};

/// A completed prefetch chunk.
struct Chunk {
    file_offset: u64,
    data: Vec<u8>,
}

/// Pipelined reader for read-ahead fetches.
///
pub(crate) struct PipelinedReader {
    backend: Arc<dyn StorageBackend>,
    chunk_size: usize,
    max_in_flight: usize,

    current: Option<Chunk>,
    current_pos: usize,

    ready: VecDeque<Chunk>,

    in_flight: VecDeque<std::sync::mpsc::Receiver<Result<Chunk>>>,

    next_offset: u64,

    eof: bool,
}

impl PipelinedReader {
    const INITIAL_CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
    const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16 MB
    const DEFAULT_MAX_IN_FLIGHT: usize = 8;

    pub fn new(backend: Arc<dyn StorageBackend>, _buf_size: usize) -> Self {
        Self {
            backend,
            chunk_size: Self::INITIAL_CHUNK_SIZE,
            max_in_flight: Self::DEFAULT_MAX_IN_FLIGHT,
            current: None,
            current_pos: 0,
            ready: VecDeque::new(),
            in_flight: VecDeque::new(),
            next_offset: 0,
            eof: false,
        }
    }

    pub fn read_into(&mut self, cursor: u64, dest: &mut [u8]) -> Result<usize> {
        if dest.is_empty() {
            return Ok(0);
        }

        // If cursor doesn't match where we are, reset
        if !self.cursor_matches(cursor) {
            self.reset(cursor);
        }

        self.ensure_pipeline_full();

        let mut filled = 0;
        while filled < dest.len() {
            if let Some(ref chunk) = self.current {
                let available = chunk.data.len() - self.current_pos;
                if available > 0 {
                    let n = (dest.len() - filled).min(available);
                    dest[filled..filled + n]
                        .copy_from_slice(&chunk.data[self.current_pos..self.current_pos + n]);
                    self.current_pos += n;
                    filled += n;

                    if self.current_pos >= chunk.data.len() {
                        self.grow_chunk_size();
                        self.current = None;
                        self.current_pos = 0;
                    }
                    continue;
                }
            }

            if let Some(chunk) = self.ready.pop_front() {
                self.current = Some(chunk);
                self.current_pos = 0;
                continue;
            }

            if let Some(rx) = self.in_flight.pop_front() {
                match rx.recv() {
                    Ok(Ok(chunk)) => {
                        if chunk.data.is_empty() {
                            self.eof = true;
                            break;
                        }
                        self.current = Some(chunk);
                        self.current_pos = 0;
                        self.ensure_pipeline_full();
                        continue;
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(_) => break, // channel closed
                }
            }
            break;
        }

        Ok(filled)
    }

    /// Check if the cursor matches where we expect to be reading.
    fn cursor_matches(&self, cursor: u64) -> bool {
        if let Some(ref chunk) = self.current {
            let expected = chunk.file_offset + self.current_pos as u64;
            return cursor == expected;
        }
        if let Some(ref chunk) = self.ready.front() {
            return cursor == chunk.file_offset;
        }
        if self.in_flight.is_empty() && self.ready.is_empty() && self.current.is_none() {
            return true; // empty state, will reset anyway
        }
        cursor == self.next_offset && self.in_flight.is_empty()
    }

    /// Reset the pipeline for a new cursor position (e.g., after seek).
    pub fn reset(&mut self, offset: u64) {
        self.current = None;
        self.current_pos = 0;
        self.ready.clear();
        self.in_flight.clear();
        self.next_offset = offset;
        self.eof = false;
        self.chunk_size = Self::INITIAL_CHUNK_SIZE;
    }

    fn ensure_pipeline_full(&mut self) {
        if self.eof {
            return;
        }
        while self.in_flight.len() + self.ready.len() < self.max_in_flight {
            let offset = self.next_offset;
            let size = self.chunk_size;
            let backend = Arc::clone(&self.backend);
            let (tx, rx) = std::sync::mpsc::channel();

            thread::spawn(move || {
                let mut buf = vec![0u8; size];
                let result = backend.read_at(offset, &mut buf).map(|n| {
                    buf.truncate(n);
                    Chunk {
                        file_offset: offset,
                        data: buf,
                    }
                });
                let _ = tx.send(result);
            });

            self.in_flight.push_back(rx);
            self.next_offset += size as u64;
        }
    }

    fn grow_chunk_size(&mut self) {
        if self.chunk_size < Self::MAX_CHUNK_SIZE {
            self.chunk_size = (self.chunk_size * 2).min(Self::MAX_CHUNK_SIZE);
        }
    }

    pub fn capacity(&self) -> usize {
        self.chunk_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalBackend;

    #[test]
    fn sequential_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let data = b"abcdefghijklmnopqrstuvwxyz";
        std::fs::write(&path, data).unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PipelinedReader::new(backend, 10);
        reader.chunk_size = 10; // small chunks for testing

        let mut out = [0u8; 26];
        let n = reader.read_into(0, &mut out).unwrap();
        assert_eq!(n, 26);
        assert_eq!(&out, data);
    }

    #[test]
    fn small_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abcdefghijklmnopqrstuvwxyz").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PipelinedReader::new(backend, 10);
        reader.chunk_size = 10;

        let mut buf = [0u8; 5];
        let n = reader.read_into(0, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"abcde");

        let n = reader.read_into(5, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"fghij");
    }

    #[test]
    fn read_after_reset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abcdefghijklmnopqrstuvwxyz").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PipelinedReader::new(backend, 10);
        reader.chunk_size = 10;

        let mut buf = [0u8; 5];
        reader.read_into(0, &mut buf).unwrap();
        assert_eq!(&buf, b"abcde");

        reader.reset(20);
        let n = reader.read_into(20, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"uvwxy");
    }

    #[test]
    fn empty_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"data").unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PipelinedReader::new(backend, 1024);

        let mut buf = [0u8; 0];
        let n = reader.read_into(0, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn chunk_size_grows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let data = vec![0xABu8; 10 * 1024 * 1024];
        std::fs::write(&path, &data).unwrap();

        let backend = Arc::new(LocalBackend::new(&path));
        let mut reader = PipelinedReader::new(backend, 1024);
        assert_eq!(reader.chunk_size, PipelinedReader::INITIAL_CHUNK_SIZE);

        let mut out = vec![0u8; 5 * 1024 * 1024];
        reader.read_into(0, &mut out).unwrap();

        assert!(reader.chunk_size > PipelinedReader::INITIAL_CHUNK_SIZE);
    }
}
