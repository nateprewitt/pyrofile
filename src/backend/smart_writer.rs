use crate::backend::traits::{ObjectWriter, UploadPrimitives};
use crate::core::config::WriteConfig;
use crate::error::{PyroError, Result};

enum UploadState {
    /// Haven't committed to a strategy yet — still buffering.
    Buffering,
    /// Multipart upload in progress.
    Multipart {
        upload_id: String,
        next_part_number: u32,
    },
}

/// Wraps an [`UploadPrimitives`] implementor to handle buffering,
/// oneshot vs. multipart decisions, and part tracking.
pub struct SmartWriter<U: UploadPrimitives> {
    primitives: U,
    config: WriteConfig,
    buffer: Vec<u8>,
    state: UploadState,
    completed_parts: Vec<crate::backend::traits::CompletedPart>,
    closed: bool,
}

impl<U: UploadPrimitives> SmartWriter<U> {
    pub fn new(primitives: U, config: WriteConfig) -> Self {
        Self {
            primitives,
            config,
            buffer: Vec::new(),
            state: UploadState::Buffering,
            completed_parts: Vec::new(),
            closed: false,
        }
    }

    /// Ensure we are in the Multipart state, initiating the upload if needed.
    fn ensure_multipart(&mut self) -> Result<()> {
        if matches!(self.state, UploadState::Buffering) {
            let upload_id = self.primitives.create_multipart_upload()?;
            self.state = UploadState::Multipart {
                upload_id,
                next_part_number: 1,
            };
        }
        Ok(())
    }

    /// Upload `data` as one part of the multipart upload.
    /// Caller must ensure we are in Multipart state.
    fn upload_part_data(&mut self, data: &[u8]) -> Result<()> {
        let UploadState::Multipart {
            upload_id,
            next_part_number,
        } = &mut self.state
        else {
            return Err(PyroError::Backend(
                "upload_part_data called in Buffering state".into(),
            ));
        };
        let part =
            self.primitives
                .upload_part(upload_id, *next_part_number, data)?;
        self.completed_parts.push(part);
        *next_part_number += 1;
        Ok(())
    }

    /// Drain the buffer into part-sized chunks, upload each, and complete
    /// the multipart upload.
    fn flush_buffer_as_parts(&mut self) -> Result<()> {
        self.ensure_multipart()?;
        while self.buffer.len() >= self.config.part_size {
            let remainder = self.buffer.split_off(self.config.part_size);
            let part_data = std::mem::replace(&mut self.buffer, remainder);
            self.upload_part_data(&part_data)?;
        }
        if !self.buffer.is_empty() {
            let data = std::mem::take(&mut self.buffer);
            self.upload_part_data(&data)?;
        }
        self.complete_upload()
    }

    /// Finalize the multipart upload. Consumes the completed parts list.
    /// No-op if parts have already been consumed (guard against double-complete).
    fn complete_upload(&mut self) -> Result<()> {
        if self.completed_parts.is_empty() {
            return Ok(());
        }
        if let UploadState::Multipart { upload_id, .. } = &self.state {
            let parts = std::mem::take(&mut self.completed_parts);
            self.primitives
                .complete_multipart_upload(upload_id, parts)?;
        }
        Ok(())
    }
}

impl<U: UploadPrimitives> ObjectWriter for SmartWriter<U> {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.closed {
            return Err(PyroError::Closed);
        }
        self.buffer.extend_from_slice(data);

        while self.buffer.len() >= self.config.part_size {
            let remainder = self.buffer.split_off(self.config.part_size);
            let part_data = std::mem::replace(&mut self.buffer, remainder);
            self.ensure_multipart()?;
            self.upload_part_data(&part_data)?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // Flushing mid-stream is a no-op for cloud backends.
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        // Flush remaining parts and complete. On failure, Drop aborts.
        match &self.state {
            UploadState::Buffering => {
                if self.buffer.len() as u64 <= self.config.put_max {
                    self.primitives.put_object(&self.buffer)?;
                } else {
                    self.flush_buffer_as_parts()?;
                }
            }
            UploadState::Multipart { .. } => {
                if !self.buffer.is_empty() {
                    let data = std::mem::take(&mut self.buffer);
                    self.upload_part_data(&data)?;
                }
                self.complete_upload()?;
            }
        }

        self.closed = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<()> {
        self.closed = true;
        if let UploadState::Multipart { upload_id, .. } = &self.state {
            self.primitives.abort_multipart_upload(upload_id)?;
        }
        self.buffer.clear();
        Ok(())
    }
}

impl<U: UploadPrimitives> Drop for SmartWriter<U> {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::traits::*;
    use std::sync::Mutex;

    struct MockPrimitives {
        log: Mutex<Vec<String>>,
        fail_on: Mutex<Option<String>>,
    }

    impl MockPrimitives {
        fn new() -> Self {
            Self {
                log: Mutex::new(Vec::new()),
                fail_on: Mutex::new(None),
            }
        }

        fn with_failure(method: &str) -> Self {
            Self {
                log: Mutex::new(Vec::new()),
                fail_on: Mutex::new(Some(method.to_string())),
            }
        }

        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }

        fn check_fail(&self, method: &str) -> Result<()> {
            if self.fail_on.lock().unwrap().as_deref() == Some(method) {
                return Err(PyroError::Backend(format!("{method} failed")));
            }
            Ok(())
        }
    }

    impl UploadPrimitives for MockPrimitives {
        fn put_object(&self, data: &[u8]) -> Result<()> {
            self.check_fail("put_object")?;
            self.log.lock().unwrap().push(format!("put_object({})", data.len()));
            Ok(())
        }

        fn create_multipart_upload(&self) -> Result<String> {
            self.check_fail("create_multipart")?;
            self.log.lock().unwrap().push("create_multipart".into());
            Ok("upload-123".into())
        }

        fn upload_part(&self, _id: &str, num: u32, data: &[u8]) -> Result<CompletedPart> {
            self.check_fail("upload_part")?;
            self.log.lock().unwrap().push(format!("upload_part({num}, {})", data.len()));
            Ok(CompletedPart { part_number: num, etag: format!("etag-{num}") })
        }

        fn complete_multipart_upload(&self, _id: &str, parts: Vec<CompletedPart>) -> Result<()> {
            self.check_fail("complete_multipart")?;
            self.log.lock().unwrap().push(format!("complete_multipart({})", parts.len()));
            Ok(())
        }

        fn abort_multipart_upload(&self, _id: &str) -> Result<()> {
            self.check_fail("abort_multipart")?;
            self.log.lock().unwrap().push("abort_multipart".into());
            Ok(())
        }
    }

    fn small_config() -> WriteConfig {
        WriteConfig { part_size: 10, max_concurrent_uploads: 64, put_max: 25 }
    }

    #[test]
    fn small_write_uses_put_object() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.write(b"hello").unwrap();
        w.close().unwrap();
        assert_eq!(w.primitives.log(), vec!["put_object(5)"]);
    }

    #[test]
    fn write_at_put_max_uses_put_object() {
        // part_size larger than data so nothing flushes during write().
        let config = WriteConfig { part_size: 100, max_concurrent_uploads: 64, put_max: 25 };
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, config);
        w.write(&vec![0u8; 25]).unwrap();
        w.close().unwrap();
        assert_eq!(w.primitives.log(), vec!["put_object(25)"]);
    }

    #[test]
    fn write_exceeding_part_size_triggers_multipart() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.write(&vec![0u8; 15]).unwrap();
        w.close().unwrap();
        assert_eq!(w.primitives.log(), vec![
            "create_multipart",
            "upload_part(1, 10)",
            "upload_part(2, 5)",
            "complete_multipart(2)",
        ]);
    }

    #[test]
    fn buffered_data_exceeding_put_max_uses_multipart_on_close() {
        let config = WriteConfig { part_size: 20, max_concurrent_uploads: 64, put_max: 10 };
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, config);
        w.write(&vec![0u8; 15]).unwrap();
        w.close().unwrap();
        assert_eq!(w.primitives.log(), vec![
            "create_multipart",
            "upload_part(1, 15)",
            "complete_multipart(1)",
        ]);
    }

    #[test]
    fn multiple_writes_accumulate_parts() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        for _ in 0..3 {
            w.write(&vec![0u8; 10]).unwrap();
        }
        w.close().unwrap();
        assert_eq!(w.primitives.log(), vec![
            "create_multipart",
            "upload_part(1, 10)",
            "upload_part(2, 10)",
            "upload_part(3, 10)",
            "complete_multipart(3)",
        ]);
    }

    #[test]
    fn close_is_idempotent() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.write(b"hello").unwrap();
        w.close().unwrap();
        w.close().unwrap();
        assert_eq!(w.primitives.log(), vec!["put_object(5)"]);
    }

    #[test]
    fn write_after_close_errors() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.close().unwrap();
        assert!(matches!(w.write(b"hello"), Err(PyroError::Closed)));
    }

    #[test]
    fn abort_cleans_up_multipart() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.write(&vec![0u8; 15]).unwrap();
        w.abort().unwrap();
        assert!(w.primitives.log().contains(&"abort_multipart".to_string()));
    }

    #[test]
    fn abort_in_buffering_state_skips_abort_call() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.write(b"hello").unwrap();
        w.abort().unwrap();
        assert!(!w.primitives.log().iter().any(|s| s.contains("abort")));
    }

    #[test]
    fn empty_close_puts_empty_object() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.close().unwrap();
        assert_eq!(w.primitives.log(), vec!["put_object(0)"]);
    }

    #[test]
    fn complete_upload_called_exactly_once() {
        let prims = MockPrimitives::new();
        let mut w = SmartWriter::new(prims, small_config());
        w.write(&vec![0u8; 15]).unwrap();
        w.close().unwrap();
        let count = w.primitives.log().iter()
            .filter(|s| s.starts_with("complete_multipart"))
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn failed_close_leaves_unclosed_for_drop_abort() {
        let prims = MockPrimitives::with_failure("put_object");
        let mut w = SmartWriter::new(prims, small_config());
        w.write(b"hello").unwrap();
        assert!(w.close().is_err());
        assert!(!w.closed);
    }

    #[test]
    fn failed_multipart_close_leaves_unclosed() {
        let prims = MockPrimitives::with_failure("complete_multipart");
        let mut w = SmartWriter::new(prims, small_config());
        w.write(&vec![0u8; 15]).unwrap();
        assert!(w.close().is_err());
        assert!(!w.closed);
    }
}
