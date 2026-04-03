# Changelog

## [Unreleased]

- Nothing yet.

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
