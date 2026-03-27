/// Read-ahead buffer backed by a sliding window.
pub(crate) struct ReadBuffer {
    /// Buffer storage, allocated to `capacity` once.
    data: Vec<u8>,
    /// The file offset at which `data[0]` starts.
    file_offset: u64,
    /// Number of valid bytes in `data` (may be less than capacity at EOF).
    valid: usize,
}

impl ReadBuffer {
    /// Create a new read buffer with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            file_offset: 0,
            valid: 0,
        }
    }

    /// Check if the given file position is within the buffered range.
    /// Returns the offset into `data` if hit, None if miss.
    pub fn hit(&self, cursor: u64) -> Option<usize> {
        if self.valid == 0 {
            return None;
        }
        if cursor >= self.file_offset && cursor < self.file_offset + self.valid as u64 {
            Some((cursor - self.file_offset) as usize)
        } else {
            None
        }
    }

    /// Read from the buffer into `dest`, starting at the given file `cursor`.
    /// Returns the number of bytes copied and does NOT advance any cursor.
    pub fn read_into(&self, cursor: u64, dest: &mut [u8]) -> usize {
        let Some(buf_offset) = self.hit(cursor) else {
            return 0;
        };
        let available = self.valid - buf_offset;
        let to_copy = dest.len().min(available);
        dest[..to_copy].copy_from_slice(&self.data[buf_offset..buf_offset + to_copy]);
        to_copy
    }

    /// Fill the buffer by reading from the given backend at `file_offset`.
    pub fn fill_from_backend(
        &mut self,
        file_offset: u64,
        backend: &dyn crate::backend::traits::StorageBackend,
    ) -> crate::error::Result<()> {
        let n = backend.read_at(file_offset, &mut self.data)?;
        self.file_offset = file_offset;
        self.valid = n;
        Ok(())
    }

    /// Returns the buffer capacity.
    pub fn capacity(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_buffer(data: &[u8], file_offset: u64) -> ReadBuffer {
        let mut buf = ReadBuffer::new(data.len().max(64));
        buf.data[..data.len()].copy_from_slice(data);
        buf.file_offset = file_offset;
        buf.valid = data.len();
        buf
    }

    #[test]
    fn new_buffer_has_no_hits() {
        let buf = ReadBuffer::new(1024);
        assert_eq!(buf.hit(0), None);
        assert_eq!(buf.hit(100), None);
    }

    #[test]
    fn hit_returns_offset_within_range() {
        let buf = make_buffer(b"hello", 10);
        assert_eq!(buf.hit(10), Some(0));
        assert_eq!(buf.hit(12), Some(2));
        assert_eq!(buf.hit(14), Some(4));
    }

    #[test]
    fn hit_misses_outside_range() {
        let buf = make_buffer(b"hello", 10);
        assert_eq!(buf.hit(9), None);
        assert_eq!(buf.hit(15), None);
        assert_eq!(buf.hit(100), None);
    }

    #[test]
    fn read_into_copies_correct_bytes() {
        let buf = make_buffer(b"abcde", 0);
        let mut dest = [0u8; 3];
        let n = buf.read_into(1, &mut dest);
        assert_eq!(n, 3);
        assert_eq!(&dest, b"bcd");
    }

    #[test]
    fn read_into_at_end_of_buffer() {
        let buf = make_buffer(b"abcde", 0);
        let mut dest = [0u8; 10];
        let n = buf.read_into(3, &mut dest);
        assert_eq!(n, 2); // only "de" available
        assert_eq!(&dest[..2], b"de");
    }

    #[test]
    fn read_into_miss_returns_zero() {
        let buf = make_buffer(b"abcde", 10);
        let mut dest = [0u8; 5];
        let n = buf.read_into(0, &mut dest);
        assert_eq!(n, 0);
    }

    #[test]
    fn capacity_matches_construction() {
        let buf = ReadBuffer::new(4096);
        assert_eq!(buf.capacity(), 4096);
    }
}
