# pyrofile

File-like object for accessing local and cloud storage.
Written in Rust, with the goal of supporting most modern cloud storage platforms.

## Install

```bash
pip install pyrofile
```

## Usage

```python
from pyrofile import PyroFile

# Local filesystem
with PyroFile("/tmp/model.pt", "w") as f:
    f.write(data)

with PyroFile("/tmp/model.pt", "r") as f:
    data = f.read()
```

## Backends

| Backend | URI scheme | Status |
|---------|-----------|--------|
| Local filesystem | `/path/to/file` | Stable |
| Azure Blob Storage | `az://account/container/blob` | Planned |
| Amazon S3 | `s3://bucket/key` | Planned |
| Google Cloud Storage | `gs://bucket/object` | Planned |

## Development

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install maturin pytest
maturin develop --release
cargo test --lib
pytest tests/python/test_local.py
```
