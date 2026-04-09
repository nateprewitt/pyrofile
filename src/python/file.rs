use std::sync::{Arc, Mutex, MutexGuard};

use pyo3::prelude::*;
use pyo3::buffer::PyBuffer;
use pyo3::types::PyBytes;

use crate::backend::local::LocalBackend;
use crate::backend::traits::StorageBackend;
use crate::core::config::PyroIOConfig;
use crate::core::file::{OpenMode, PyroIO};
use crate::error::PyroError;

/// File-like object for accessing local and cloud storage.
#[pyclass(name = "PyroFile")]
pub struct PyPyroFile {
    inner: Mutex<PyroIO>,
    mode_str: String,
}

impl PyPyroFile {
    pub fn from_backend(
        backend: Arc<dyn StorageBackend>,
        mode: OpenMode,
        config: PyroIOConfig,
    ) -> Self {
        let mode_str = match mode {
            OpenMode::Read => "rb".to_string(),
            OpenMode::Write => "wb".to_string(),
        };
        Self {
            inner: Mutex::new(PyroIO::new(backend, mode, config)),
            mode_str,
        }
    }

    fn lock_inner(&self) -> crate::error::Result<MutexGuard<'_, PyroIO>> {
        self.inner
            .lock()
            .map_err(|e| PyroError::Backend(format!("lock poisoned: {e}")))
    }

    /// Resolve a path/URI to the appropriate storage backend.
    ///
    /// TODO: stub for testing right now. It needs more thought out
    /// design and backend handling.
    fn resolve_backend(path: &str) -> PyResult<Arc<dyn StorageBackend>> {
        if let Some(rest) = path.strip_prefix("az://") {
            #[cfg(feature = "azure")]
            {
                let blob_url = az_uri_to_blob_url(rest)?;
                let backend = crate::backend::azure::AzureBackend::new(&blob_url)
                    .map_err(|e| -> pyo3::PyErr { e.into() })?;
                Ok(Arc::new(backend) as Arc<dyn StorageBackend>)
            }
            #[cfg(not(feature = "azure"))]
            {
                let _ = rest;
                Err(PyroError::NotSupported.into())
            }
        } else if path.starts_with("http://") || path.starts_with("https://") {
            // Treat HTTPS as azure for now.
            #[cfg(feature = "azure")]
            {
                let backend = crate::backend::azure::AzureBackend::new(path)
                    .map_err(|e| -> pyo3::PyErr { e.into() })?;
                Ok(Arc::new(backend) as Arc<dyn StorageBackend>)
            }
            #[cfg(not(feature = "azure"))]
            {
                Err(PyroError::NotSupported.into())
            }
        } else {
            Ok(Arc::new(LocalBackend::new(path)) as Arc<dyn StorageBackend>)
        }
    }
}

/// Convert `az://` paths to HTTPS URI
#[cfg(feature = "azure")]
fn az_uri_to_blob_url(rest: &str) -> PyResult<String> {
    let parts: Vec<&str> = rest.splitn(3, '/').collect();
    if parts.len() < 3 {
        return Err(PyroError::InvalidArgument(
            "az:// URI must be az://account/container/blob".into(),
        )
        .into());
    }
    let account = parts[0];
    let container = parts[1];
    let blob = parts[2];
    Ok(format!(
        "https://{account}.blob.core.windows.net/{container}/{blob}"
    ))
}

#[pymethods]
impl PyPyroFile {
    #[new]
    #[pyo3(signature = (path, mode="r"))]
    fn py_new(path: &str, mode: &str) -> PyResult<Self> {
        let open_mode = match mode {
            "r" | "rb" => OpenMode::Read,
            "w" | "wb" => OpenMode::Write,
            _ => {
                return Err(PyroError::InvalidArgument(format!(
                    "unsupported mode: {mode:?}"
                ))
                .into())
            }
        };

        let backend = Self::resolve_backend(path)?;
        Ok(Self::from_backend(backend, open_mode, PyroIOConfig::default()))
    }

    #[pyo3(signature = (size=None))]
    fn read<'py>(&self, py: Python<'py>, size: Option<i64>) -> PyResult<Bound<'py, PyBytes>> {
        let size = size.unwrap_or(-1);
        let bytes = py.allow_threads(|| self.lock_inner()?.read(size))?;
        Ok(PyBytes::new(py, &bytes))
    }

    fn readinto(&self, py: Python<'_>, buffer: PyBuffer<u8>) -> PyResult<usize> {
        if buffer.readonly() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "readinto requires a writable buffer",
            ));
        }
        if !buffer.is_c_contiguous() {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "readinto requires a contiguous buffer",
            ));
        }

        let ptr = buffer.buf_ptr() as *mut u8;
        let len = buffer.len_bytes();

        if len == 0 {
            return Ok(0);
        }

        // SAFETY: We verified the buffer is contiguous,
        // so buf_ptr() is valid for len_bytes() bytes.
        let dest = unsafe { std::slice::from_raw_parts_mut(ptr, len) };

        py.allow_threads(|| self.lock_inner()?.read_into(dest))
            .map_err(|e: PyroError| e.into())
    }

    fn write(&self, py: Python<'_>, data: PyBuffer<u8>) -> PyResult<usize> {
        if !data.is_c_contiguous() {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "write requires a contiguous buffer",
            ));
        }

        let ptr = data.buf_ptr() as *const u8;
        let len = data.len_bytes();

        if len == 0 {
            return Ok(0);
        }

        // SAFETY: We verified the buffer is contiguous,
        // so buf_ptr() is valid for len_bytes() bytes.
        let src = unsafe { std::slice::from_raw_parts(ptr, len) };

        let start = std::time::Instant::now();
        let result = py.allow_threads(|| self.lock_inner()?.write(src))
            .map_err(|e: PyroError| e.into());
        let elapsed = start.elapsed();
        if elapsed.as_millis() > 100 || len > 1_000_000 {
            eprintln!("[py_write] size={len} elapsed={elapsed:?}");
        }
        result
    }

    #[pyo3(signature = (offset, whence=0))]
    fn seek(&self, py: Python<'_>, offset: i64, whence: i32) -> PyResult<u64> {
        let seek_from = match whence {
            0 => {
                if offset < 0 {
                    return Err(PyroError::InvalidArgument("negative seek position".into()).into());
                }
                std::io::SeekFrom::Start(offset as u64)
            }
            1 => std::io::SeekFrom::Current(offset),
            2 => std::io::SeekFrom::End(offset),
            _ => {
                return Err(
                    PyroError::InvalidArgument(format!("invalid whence value: {whence}")).into(),
                );
            }
        };
        py.allow_threads(|| self.lock_inner()?.seek(seek_from))
            .map_err(|e: PyroError| e.into())
    }

    fn tell(&self) -> PyResult<u64> {
        Ok(self.lock_inner()?.tell())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.lock_inner()?.flush())
            .map_err(|e: PyroError| e.into())
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.lock_inner()?.close())
            .map_err(|e: PyroError| e.into())
    }

    fn readable(&self) -> PyResult<bool> {
        Ok(self.lock_inner()?.mode() == OpenMode::Read)
    }

    fn writable(&self) -> PyResult<bool> {
        Ok(self.lock_inner()?.mode() == OpenMode::Write)
    }

    fn seekable(&self) -> PyResult<bool> {
        Ok(self.lock_inner()?.mode() == OpenMode::Read)
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self.lock_inner()?.is_closed())
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.lock_inner()?.backend_name().to_string())
    }

    #[getter]
    fn mode(&self) -> &str {
        &self.mode_str
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if exc_type.is_some() {
            // Abort on exception to avoid committing partial data.
            // Swallow abort errors to preserve the original exception.
            let _ = py.allow_threads(|| self.lock_inner()?.abort());
        } else {
            self.close(py)?;
        }
        Ok(false)
    }

    // TODO: We don't need these yet. Come back and implement them later.

    #[pyo3(signature = (_size=-1))]
    fn readline(&self, _size: i64) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err("readline"))
    }

    #[pyo3(signature = (_hint=-1))]
    fn readlines(&self, _hint: i64) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err("readlines"))
    }

    fn writelines(&self, _lines: &Bound<'_, PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "writelines",
        ))
    }

    fn fileno(&self) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err("fileno"))
    }

    #[pyo3(signature = (_size=None))]
    fn truncate(&self, _size: Option<u64>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err("truncate"))
    }

    fn isatty(&self) -> bool {
        false
    }

    fn __iter__(&self) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err("__iter__"))
    }

    fn __next__(&self) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err("__next__"))
    }
}
