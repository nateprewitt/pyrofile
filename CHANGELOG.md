# Changelog

## [Unreleased]

- Nothing yet.

## [0.3.1] - 2026-06-30

### Performance

- Reads no longer issue a HEAD request ahead of download. The object size is harvested
  from the first ranged GET's `Content-Range` header.
- Credentials and tokio runtime are now cached process-wide to prevent resolution
  thrash when opening many small files.

### Feature

- Azure credential resolution now supports managed identity and environment
  based credentials.

### Changed

- Credentials, HTTP clients, and tokio runtimes are now regenerated after process
  forking to prevent concurrency issues.

## [0.3.0] - 2026-06-25

### Performance

- Azure integration download strategy changed to ranged reads for higher throughput.
- Azure integration now stages blocks with bounded concurrency and backpressure.

### Feature

- (Provisional) Added provisional environment-variable configuration for read
  and write tuning. Values accept ``KB``, ``MB``, and ``GB`` suffixes:
  - `PYROFILE_READ_CHUNK_SIZE`: chunk size per parallel download request (default 16 MB)
  - `PYROFILE_READ_CONCURRENCY`: max concurrent download workers (default 32)
  - `PYROFILE_CACHE_BLOCK_SIZE`: read cache block size (default 16 MB)
  - `PYROFILE_CACHE_BLOCKS`: max cached blocks (default 4)
  - `PYROFILE_WRITE_BLOCK_SIZE`: block size for multipart uploads (default 16 MB)
  - `PYROFILE_WRITE_CONCURRENCY`: max concurrent upload tasks (default 64)

### Changed

- Increased the default write block size from 8 MB to 16 MB.

### Bug Fix

- `read(size)` can no longer return more bytes than remain in the file. Reads at or
  past the end of the file now return an empty `bytes` object instead of remaining
  zero-padded data.

## [0.2.1] - 2026-04-03

### Performance

- Optimized parallelization and write strategy for Azure backend.

## [0.2.0] - 2026-04-02

### Performance

- Added read caching to avoid redundant fetches for seek-heavy workloads
  like ``torch.load``.
- Large downloads are now parallelized after exceeding cache threshold.

### Feature

 - Added `readinto()` support.

## [0.1.0] - 2026-03-26

### Feature

- Added initial Rust Backend for File-like interfaces
- Added initial interfaces for "pluggable" backends
- Added initial Python bindings for pyrofile package
- Added initial backends for Local and Azure access
