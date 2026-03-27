mod backend;
mod core;
mod error;
mod python;

use pyo3::prelude::*;

/// pyrofile: File-like object for accessing local and cloud storage.
#[pymodule]
fn _pyrofile(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<python::PyPyroFile>()?;
    Ok(())
}
