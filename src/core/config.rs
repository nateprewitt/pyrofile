/// Configuration for [`PyroIO`](super::file::PyroIO).
#[derive(Debug, Clone)]
pub struct PyroIOConfig {
    /// Read cache configuration.
    pub read_config: ReadConfig,

    /// Write configuration passed through to SmartWriter.
    pub write_config: WriteConfig,
}

/// Read configuration for the block cache and parallel downloads.
#[derive(Debug, Clone)]
pub struct ReadConfig {
    /// Cache block size in bytes. Default: 16 MB.
    pub block_size: usize,

    /// Maximum cached blocks. Default: 4 (64 MB total).
    pub max_blocks: usize,

    /// Chunk size per HTTP request in parallel downloads. Default: 16 MB.
    pub parallel_chunk_size: usize,

    /// Maximum concurrent download threads. Default: 32.
    pub max_read_concurrency: usize,
}

/// Configuration for write operations (passed to SmartWriter).
#[derive(Debug, Clone)]
pub struct WriteConfig {
    /// Part size for multipart uploads in bytes. Default: 8 MB.
    pub part_size: usize,

    /// Maximum data size for a single PUT upload in bytes. Above this,
    /// multipart is used. Should reflect the provider's single-object PUT
    /// limit (e.g., 5 GB for most cloud providers). Default: 5 GB.
    pub put_max: u64,
}

impl Default for PyroIOConfig {
    fn default() -> Self {
        Self {
            read_config: ReadConfig::default(),
            write_config: WriteConfig::default(),
        }
    }
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            block_size: 16 * 1024 * 1024,         // 16 MB
            max_blocks: 4,                         // 64 MB total
            parallel_chunk_size: 16 * 1024 * 1024, // 16 MB
            max_read_concurrency: 32,
        }
    }
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            part_size: 8 * 1024 * 1024,              // 8 MB
            put_max: 5 * 1024 * 1024 * 1024_u64,      // 5 GB
        }
    }
}
