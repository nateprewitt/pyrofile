use pyo3::exceptions::{PyFileExistsError, PyFileNotFoundError, PyOSError, PyPermissionError, PyValueError};
use pyo3::PyErr;

pyo3::import_exception!(io, UnsupportedOperation);

pub type Result<T> = std::result::Result<T, PyroError>;

#[derive(Debug, thiserror::Error)]
pub enum PyroError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("operation not supported by this backend")]
    NotSupported,

    #[error("I/O operation on closed file")]
    Closed,

    #[error("not found: {0}")]
    NotFound(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl From<PyroError> for PyErr {
    fn from(err: PyroError) -> PyErr {
        match err {
            PyroError::Io(e) => PyOSError::new_err(e.to_string()),
            PyroError::NotSupported => {
                UnsupportedOperation::new_err("operation not supported by this backend")
            }
            PyroError::Closed => {
                PyValueError::new_err("I/O operation on closed file")
            }
            PyroError::NotFound(msg) => PyFileNotFoundError::new_err(msg),
            PyroError::PermissionDenied(msg) => PyPermissionError::new_err(msg),
            PyroError::AlreadyExists(msg) => PyFileExistsError::new_err(msg),
            PyroError::Backend(msg) => PyOSError::new_err(msg),
            PyroError::InvalidArgument(msg) => PyValueError::new_err(msg),
        }
    }
}
