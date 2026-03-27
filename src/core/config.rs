/// Configuration for [`PyroIO`](super::file::PyroIO).
#[derive(Debug, Clone)]
pub struct PyroIOConfig {
    /// Size of the read-ahead buffer in bytes. Default: 16 MB.
    pub read_buffer_size: usize,

    /// Write configuration passed through to SmartWriter.
    pub write_config: WriteConfig,
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
            read_buffer_size: 16 * 1024 * 1024, // 16 MB
            write_config: WriteConfig::default(),
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
