use crate::error::{PyroError, Result};

/// Metadata about a storage object.
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    /// Total size in bytes, if known.
    pub content_length: Option<u64>,
    /// MIME type, if known.
    pub content_type: Option<String>,
}

/// Describes a completed upload part (for multipart uploads).
#[derive(Debug, Clone)]
pub struct CompletedPart {
    pub part_number: u32,
    /// Opaque identifier returned by the storage service (e.g., ETag).
    pub etag: String,
}

/// Core storage backend trait.
///
/// Reads are positional and stateless. Writes go through a separate
/// [`ObjectWriter`] obtained via [`create_writer`].
pub trait StorageBackend: Send + Sync {
    /// Positional read: read up to `buf.len()` bytes starting at `offset`.
    /// Returns the number of bytes actually read (0 at EOF).
    /// If `buf` is empty, returns `Ok(0)` without performing I/O.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// Return object metadata. Backends may cache this after the first call.
    fn metadata(&self) -> Result<ObjectMeta>;

    /// Create a new writer for this storage location.
    /// The returned writer owns all write state (buffers, upload IDs, etc.).
    fn create_writer(&self) -> Result<Box<dyn ObjectWriter>>;

    /// Return a display name for this storage location (path, URI, etc.).
    fn name(&self) -> &str;

    /// Bulk download `length` bytes starting at `offset`.
    fn download(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; length as usize];
        let n = self.read_at(offset, &mut buf)?;
        buf.truncate(n);
        Ok(buf)
    }

    /// Read multiple ranges concurrently, writing directly into the
    /// provided buffer pointers.
    ///
    /// # Safety
    /// Each (ptr, len) must point to a valid, writable region that does not
    /// overlap another range and remains live for the duration of this call.
    fn read_ranges(&self, ranges: &[(u64, *mut u8, usize)]) -> Result<Vec<usize>> {
        let mut results = Vec::with_capacity(ranges.len());
        for &(offset, ptr, len) in ranges {
            let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
            results.push(self.read_at(offset, buf)?);
        }
        Ok(results)
    }
}

/// Write interface for storage backends.
///
/// All writes are all-or-nothing. Implementations must call `abort()` on
/// drop if `close()` was not called.
pub trait ObjectWriter: Send {
    /// Write all of `data`. On error, the caller should abort.
    fn write(&mut self, data: &[u8]) -> Result<()>;

    /// Flush any internally buffered data toward the backend.
    fn flush(&mut self) -> Result<()>;

    /// Finalize: flush remaining data, complete upload, close handle.
    fn close(&mut self) -> Result<()>;

    /// Abort the write, cleaning up any partial state.
    fn abort(&mut self) -> Result<()>;
}

/// Thin SDK wrapper trait for cloud backends.
///
/// Implement these five methods and pass the implementor to [`SmartWriter`]
/// to get multipart/oneshot upload logic.
pub trait UploadPrimitives: Send + Sync {
    /// Upload the entire object in a single request.
    fn put_object(&self, data: &[u8]) -> Result<()>;

    /// Initiate a multipart upload. Returns an opaque upload ID.
    fn create_multipart_upload(&self) -> Result<String>;

    /// Upload one part of a multipart upload.
    fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
    ) -> Result<CompletedPart>;

    /// Finalize a multipart upload, assembling all parts.
    fn complete_multipart_upload(
        &self,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<()>;

    /// Abort a multipart upload, cleaning up remote state.
    fn abort_multipart_upload(&self, upload_id: &str) -> Result<()>;
}
