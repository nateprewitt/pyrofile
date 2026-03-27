"""File-like object for accessing local and cloud storage."""

import io

from pyrofile._pyrofile import PyroFile

io.RawIOBase.register(PyroFile)

__all__ = ["PyroFile"]
