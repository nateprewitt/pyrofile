use crate::backend::traits::StorageBackend;
use crate::core::config::ReadConfig;
use crate::error::Result;

/// A single cached block of file data.
struct Block {
    /// Block-aligned file offset where this block starts.
    start: u64,
    /// Actual bytes read (may be less than block_size at EOF).
    data: Vec<u8>,
    /// Access counter for LRU eviction.
    last_access: u64,
}

/// Block-aligned LRU read cache.
pub(crate) struct BlockCache {
    blocks: Vec<Block>,
    block_size: usize,
    max_blocks: usize,
    access_counter: u64,
}

impl BlockCache {
    pub fn new(config: &ReadConfig) -> Self {
        Self {
            blocks: Vec::with_capacity(config.max_blocks),
            block_size: config.block_size,
            max_blocks: config.max_blocks,
            access_counter: 0,
        }
    }

    /// Read from the cache into `dest`, starting at the given file `cursor`.
    /// Fetches blocks from the backend on cache miss. Returns the number
    /// of bytes copied.
    pub fn read(
        &mut self,
        cursor: u64,
        dest: &mut [u8],
        backend: &dyn StorageBackend,
    ) -> Result<usize> {
        if dest.is_empty() {
            return Ok(0);
        }

        let mut filled = 0;
        let mut pos = cursor;

        while filled < dest.len() {
            let block_start = self.align(pos);
            let block = self.get_or_fetch(block_start, backend)?;

            let offset_in_block = (pos - block.start) as usize;
            let available = block.data.len().saturating_sub(offset_in_block);
            if available == 0 {
                break; // EOF
            }

            let to_copy = (dest.len() - filled).min(available);
            dest[filled..filled + to_copy]
                .copy_from_slice(&block.data[offset_in_block..offset_in_block + to_copy]);
            filled += to_copy;
            pos += to_copy as u64;
        }

        Ok(filled)
    }

    /// Look up or fetch the block starting at `block_start`.
    fn get_or_fetch(
        &mut self,
        block_start: u64,
        backend: &dyn StorageBackend,
    ) -> Result<&Block> {
        self.access_counter += 1;
        let ac = self.access_counter;

        if let Some(idx) = self.blocks.iter().position(|b| b.start == block_start) {
            self.blocks[idx].last_access = ac;
            return Ok(&self.blocks[idx]);
        }

        let mut buf = vec![0u8; self.block_size];
        let n = backend.read_at(block_start, &mut buf)?;
        buf.truncate(n);

        let block = Block {
            start: block_start,
            data: buf,
            last_access: ac,
        };

        let idx = if self.blocks.len() < self.max_blocks {
            self.blocks.push(block);
            self.blocks.len() - 1
        } else {
            let lru_idx = self
                .blocks
                .iter()
                .enumerate()
                .min_by_key(|(_, b)| b.last_access)
                .map(|(i, _)| i)
                .unwrap();
            self.blocks[lru_idx] = block;
            lru_idx
        };

        Ok(&self.blocks[idx])
    }

    /// Align a file offset down to the nearest block boundary.
    fn align(&self, offset: u64) -> u64 {
        (offset / self.block_size as u64) * self.block_size as u64
    }

    /// Returns the configured block size.
    pub fn block_size(&self) -> usize {
        self.block_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::local::LocalBackend;
    use std::sync::Arc;

    fn test_config(block_size: usize, max_blocks: usize) -> ReadConfig {
        ReadConfig {
            block_size,
            max_blocks,
        }
    }

    #[test]
    fn sequential_read_within_one_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abcdefghij").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(1024, 4));

        let mut buf = [0u8; 5];
        let n = cache.read(0, &mut buf, &backend).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"abcde");

        let n = cache.read(5, &mut buf, &backend).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"fghij");
    }

    #[test]
    fn read_spanning_two_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"aabbccddee").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(4, 4));

        let mut buf = [0u8; 6];
        let n = cache.read(2, &mut buf, &backend).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, b"bbccdd");
    }

    #[test]
    fn read_at_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abc").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(1024, 4));

        let mut buf = [0u8; 10];
        let n = cache.read(0, &mut buf, &backend).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..3], b"abc");
    }

    #[test]
    fn read_past_eof_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abc").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(1024, 4));

        let mut buf = [0u8; 5];
        let n = cache.read(100, &mut buf, &backend).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn empty_read_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abc").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(1024, 4));

        let n = cache.read(0, &mut [], &backend).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn seek_does_not_invalidate_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"AAAAbbbb").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(4, 4));

        let mut buf = [0u8; 4];
        cache.read(0, &mut buf, &backend).unwrap();
        assert_eq!(&buf, b"AAAA");

        cache.read(4, &mut buf, &backend).unwrap();
        assert_eq!(&buf, b"bbbb");

        cache.read(0, &mut buf, &backend).unwrap();
        assert_eq!(&buf, b"AAAA");

        assert_eq!(cache.blocks.len(), 2);
    }

    #[test]
    fn lru_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"AAAAbbbbCCCC").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(4, 2));

        let mut buf = [0u8; 4];

        cache.read(0, &mut buf, &backend).unwrap();
        assert_eq!(&buf, b"AAAA");
        cache.read(4, &mut buf, &backend).unwrap();
        assert_eq!(&buf, b"bbbb");
        assert_eq!(cache.blocks.len(), 2);

        cache.read(0, &mut buf, &backend).unwrap();

        cache.read(8, &mut buf, &backend).unwrap();
        assert_eq!(&buf, b"CCCC");
        assert_eq!(cache.blocks.len(), 2);

        assert!(cache.blocks.iter().any(|b| b.start == 0));
        assert!(!cache.blocks.iter().any(|b| b.start == 4));
    }

    #[test]
    fn block_alignment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"0123456789abcdef").unwrap();

        let backend = LocalBackend::new(&path);
        let mut cache = BlockCache::new(&test_config(8, 4));

        let mut buf = [0u8; 2];
        cache.read(3, &mut buf, &backend).unwrap();
        assert_eq!(&buf, b"34");

        assert_eq!(cache.blocks[0].start, 0);
        assert_eq!(cache.blocks[0].data.len(), 8);
    }
}
