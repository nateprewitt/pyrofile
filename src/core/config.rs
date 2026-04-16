/// Parse a size value from an environment variable.
/// Accepts suffixed values: KB, MB, GB.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| parse_size(&v))
        .unwrap_or(default)
}

fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim();
    let (num, multiplier) = if let Some(n) = s.strip_suffix("GB").or_else(|| s.strip_suffix("gb")) {
        (n.trim(), 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("MB").or_else(|| s.strip_suffix("mb")) {
        (n.trim(), 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("KB").or_else(|| s.strip_suffix("kb")) {
        (n.trim(), 1024)
    } else {
        (s, 1)
    };
    num.parse::<usize>().ok().map(|n| n * multiplier)
}

/// Configuration for [`PyroIO`](super::file::PyroIO).
#[derive(Debug, Clone)]
pub struct PyroIOConfig {
    /// Read cache configuration.
    pub read_config: ReadConfig,

    /// Write configuration passed through to SmartWriter.
    pub write_config: WriteConfig,
}

/// Read configuration for the block cache and parallel downloads.
///
/// Override via environment variables:
/// - `PYROFILE_CACHE_BLOCK_SIZE`: cache block size
/// - `PYROFILE_CACHE_BLOCKS`: max cached blocks
/// - `PYROFILE_READ_CHUNK_SIZE`: chunk size per parallel download request
/// - `PYROFILE_READ_CONCURRENCY`: max concurrent download workers
#[derive(Debug, Clone)]
pub struct ReadConfig {
    /// Cache block size in bytes. Default: 16 MB.
    pub block_size: usize,

    /// Maximum cached blocks. Default: 4 (64 MB total).
    pub max_blocks: usize,

    /// Chunk size per HTTP request in parallel downloads. Default: 16 MB.
    pub parallel_chunk_size: usize,

    /// Maximum concurrent download workers. Default: 32.
    pub max_read_concurrency: usize,
}

/// Configuration for write operations.
///
/// Override via environment variables:
/// - `PYROFILE_WRITE_BLOCK_SIZE`: block size for multipart uploads
/// - `PYROFILE_WRITE_CONCURRENCY`: max concurrent upload tasks
#[derive(Debug, Clone)]
pub struct WriteConfig {
    /// Part size for multipart uploads in bytes. Default: 16 MB.
    pub part_size: usize,

    /// Maximum concurrent upload tasks. Default: 64.
    pub max_concurrent_uploads: usize,

    /// Maximum data size for a single PUT upload in bytes. Default: 5 GB.
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
            block_size: env_usize("PYROFILE_CACHE_BLOCK_SIZE", 16 * 1024 * 1024),
            max_blocks: env_usize("PYROFILE_CACHE_BLOCKS", 4),
            parallel_chunk_size: env_usize("PYROFILE_READ_CHUNK_SIZE", 16 * 1024 * 1024),
            max_read_concurrency: env_usize("PYROFILE_READ_CONCURRENCY", 32),
        }
    }
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            part_size: env_usize("PYROFILE_WRITE_BLOCK_SIZE", 16 * 1024 * 1024),
            max_concurrent_uploads: env_usize("PYROFILE_WRITE_CONCURRENCY", 64),
            put_max: 5 * 1024 * 1024 * 1024_u64,
        }
    }
}
