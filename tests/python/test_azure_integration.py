"""Integration tests for PyroFile Azure backend against Azurite.

These tests require Azurite to be running locally. They are designed to run
in GHA via the CI workflow, but can also be run locally:

    npx azurite-blob --silent --blobHost 127.0.0.1 --blobPort 10000 &

    # Create the container
    python -c "
    from azure.storage.blob import BlobServiceClient
    cs = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1'
    BlobServiceClient.from_connection_string(cs).create_container('testcontainer')
    "

    export AZURITE_BLOB_URL=http://127.0.0.1:10000/devstoreaccount1/testcontainer
    export AZURITE_SAS_TOKEN="sv=2024-11-04&ss=b&srt=sco&sp=rwdlacitfx&se=2030-01-01T00:00:00Z&st=2020-01-01T00:00:00Z&spr=http&sig=test"
    pytest tests/python/test_azure_integration.py -v
"""
import os
import pytest

from pyrofile import PyroFile

BLOB_URL = os.environ.get("AZURITE_BLOB_URL")
SAS_TOKEN = os.environ.get("AZURITE_SAS_TOKEN")

pytestmark = pytest.mark.skipif(
    not BLOB_URL or not SAS_TOKEN,
    reason="AZURITE_BLOB_URL and AZURITE_SAS_TOKEN not set",
)


def blob_url(name):
    return f"{BLOB_URL}/{name}?{SAS_TOKEN}"


def az_uri(name):
    """Build an az:// URI that resolves to the Azurite blob URL.

    Since az:// resolves to https://<account>.blob.core.windows.net/...,
    but Azurite runs on localhost, we bypass the az:// scheme and pass
    the full HTTP URL directly to PyroFile for these tests.
    """
    return blob_url(name)


class TestAzureWriteRead:
    def test_small_write_and_read(self):
        url = blob_url("test_small.bin")
        data = b"hello from pyrofile azure backend"

        with PyroFile(url, "w") as f:
            f.write(data)

        with PyroFile(url, "r") as f:
            result = f.read()
            assert result == data

    def test_medium_write_and_read(self):
        url = blob_url("test_medium.bin")
        data = os.urandom(5 * 1024 * 1024)  # 5 MB

        with PyroFile(url, "w") as f:
            f.write(data)

        with PyroFile(url, "r") as f:
            result = f.read()
            assert result == data

    def test_multiple_writes(self):
        url = blob_url("test_multi_write.bin")
        chunks = [os.urandom(1024) for _ in range(100)]

        with PyroFile(url, "w") as f:
            for chunk in chunks:
                f.write(chunk)

        expected = b"".join(chunks)
        with PyroFile(url, "r") as f:
            assert f.read() == expected

    def test_large_write_triggers_multipart(self):
        """Write > 32MB to trigger block staging."""
        url = blob_url("test_large.bin")
        data = os.urandom(40 * 1024 * 1024)  # 40 MB

        with PyroFile(url, "w") as f:
            f.write(data)

        with PyroFile(url, "r") as f:
            result = f.read()
            assert result == data


class TestAzureSeek:
    def test_seek_and_read(self):
        url = blob_url("test_seek.bin")
        data = b"abcdefghijklmnopqrstuvwxyz"

        with PyroFile(url, "w") as f:
            f.write(data)

        with PyroFile(url, "r") as f:
            f.seek(10)
            assert f.read(5) == b"klmno"

    def test_seek_end(self):
        url = blob_url("test_seek_end.bin")
        data = b"abcdefghijklmnopqrstuvwxyz"

        with PyroFile(url, "w") as f:
            f.write(data)

        with PyroFile(url, "r") as f:
            f.seek(-5, 2)  # SEEK_END
            assert f.read() == b"vwxyz"


class TestAzureProperties:
    def test_tell_during_write(self):
        url = blob_url("test_tell.bin")
        with PyroFile(url, "w") as f:
            assert f.tell() == 0
            f.write(b"hello")
            assert f.tell() == 5

    def test_name(self):
        url = blob_url("test_name.bin")
        with PyroFile(url, "w") as f:
            assert "test_name.bin" in f.name

    def test_mode(self):
        url = blob_url("test_mode_r.bin")
        # Write first so read doesn't fail on missing blob
        with PyroFile(url, "w") as f:
            f.write(b"data")
        with PyroFile(url, "r") as f:
            assert f.mode == "rb"
        with PyroFile(url, "w") as f:
            assert f.mode == "wb"


class TestAzureContextManager:
    def test_abort_on_exception(self):
        url = blob_url("test_abort.bin")
        with pytest.raises(ValueError):
            with PyroFile(url, "w") as f:
                f.write(b"should be discarded")
                raise ValueError("intentional error")
        # The blob may or may not exist (uncommitted blocks expire).
        # The key assertion is that close() was not called (no commit).
