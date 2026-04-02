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
    /// Size of each cache block in bytes. Reads are aligned to this boundary.
    /// Default: 8 MB.
    pub block_size: usize,

    /// Maximum number of blocks to keep in the LRU cache.
    /// Total memory usage is up to `block_size * max_blocks`.
    /// Default: 4
    pub max_blocks: usize,
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
            block_size: 8 * 1024 * 1024, // 8 MB
            max_blocks: 4,               // 32 MB total
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
