# pyrofile

File-like object for accessing local and cloud storage.
Built in Rust with growing support for major cloud storage platforms.

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

# Azure Blob Storage
with PyroFile("az://account/container/model.pt", "w") as f:
    f.write(data)
```

Works with `torch.save` and `torch.load`:

```python
import torch
from pyrofile import PyroFile

with PyroFile("az://account/container/checkpoint.pt", "w") as f:
    torch.save(model.state_dict(), f)

with PyroFile("az://account/container/checkpoint.pt", "r") as f:
    state = torch.load(f, weights_only=True)
```

## Backends

| Backend | URI scheme | Status |
|---------|-----------|--------|
| Local filesystem | `/path/to/file` | Stable |
| Azure Blob Storage | `az://account/container/blob` | Preview |
| Amazon S3 | `s3://bucket/key` | Planned |
| Google Cloud Storage | `gs://bucket/object` | Planned |

## Development

```bash
python -m venv .venv && source .venv/bin/activate
python -m pip install maturin pytest
maturin develop --release --features azure
cargo test --lib
python -m pytest tests/python/test_local.py
python -m pytest tests/python/test_azure_integration.py
```
