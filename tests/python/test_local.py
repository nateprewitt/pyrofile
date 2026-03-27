"""Integration tests for PyroFile with the local filesystem backend."""
import io
import os
import tempfile
import pytest

from pyrofile import PyroFile


class TestReadWrite:
    def test_write_and_read_back(self, tmp_path):
        path = str(tmp_path / "test.bin")
        data = b"hello pyrofile"

        with PyroFile(path, "w") as f:
            n = f.write(data)
            assert n == len(data)

        with PyroFile(path, "r") as f:
            result = f.read()
            assert result == data

    def test_write_multiple_chunks(self, tmp_path):
        path = str(tmp_path / "test.bin")
        chunks = [os.urandom(1024) for _ in range(100)]

        with PyroFile(path, "w") as f:
            for chunk in chunks:
                f.write(chunk)

        expected = b"".join(chunks)
        with PyroFile(path, "r") as f:
            assert f.read() == expected

    def test_read_with_size(self, tmp_path):
        path = str(tmp_path / "test.bin")
        data = b"abcdefghij"

        with open(path, "wb") as f:
            f.write(data)

        with PyroFile(path, "r") as f:
            assert f.read(5) == b"abcde"
            assert f.read(5) == b"fghij"
            assert f.read(5) == b""

    def test_read_zero_returns_empty(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            assert f.read(0) == b""
            assert f.tell() == 0

    def test_large_file_roundtrip(self, tmp_path):
        path = str(tmp_path / "large.bin")
        # 10 MB of random data
        data = os.urandom(10 * 1024 * 1024)

        with PyroFile(path, "w") as f:
            f.write(data)

        with PyroFile(path, "r") as f:
            result = f.read()
            assert result == data


class TestSeekTell:
    def test_seek_set(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"abcdefghij")

        with PyroFile(path, "r") as f:
            f.seek(5)
            assert f.read(3) == b"fgh"

    def test_seek_cur(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"abcdefghij")

        with PyroFile(path, "r") as f:
            f.read(3)
            f.seek(2, 1)  # SEEK_CUR
            assert f.read(3) == b"fgh"

    def test_seek_end(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"abcdefghij")

        with PyroFile(path, "r") as f:
            f.seek(-3, 2)  # SEEK_END
            assert f.read() == b"hij"

    def test_tell_in_write_mode(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with PyroFile(path, "w") as f:
            assert f.tell() == 0
            f.write(b"hello")
            assert f.tell() == 5

    def test_seek_blocked_in_write_mode(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with PyroFile(path, "w") as f:
            with pytest.raises(IOError):
                f.seek(0)

    def test_seek_current_zero_in_write_mode(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with PyroFile(path, "w") as f:
            f.write(b"hello")
            assert f.seek(0, 1) == 5  # seek(0, SEEK_CUR) is tell()


class TestModeEnforcement:
    def test_write_in_read_mode_raises(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            with pytest.raises(IOError):
                f.write(b"nope")

    def test_read_in_write_mode_raises(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with PyroFile(path, "w") as f:
            with pytest.raises(IOError):
                f.read(10)

    def test_readable_writable_seekable(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            assert f.readable() is True
            assert f.writable() is False
            assert f.seekable() is True

        with PyroFile(path, "w") as f:
            assert f.readable() is False
            assert f.writable() is True
            assert f.seekable() is False


class TestProperties:
    def test_closed_property(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        f = PyroFile(path, "r")
        assert f.closed is False
        f.close()
        assert f.closed is True

    def test_name_property(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            assert f.name == path

    def test_mode_property(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            assert f.mode == "rb"
        with PyroFile(path, "w") as f:
            assert f.mode == "wb"


class TestContextManager:
    def test_close_on_clean_exit(self, tmp_path):
        path = str(tmp_path / "test.bin")
        f = PyroFile(path, "w")
        with f:
            f.write(b"hello")
        assert f.closed is True
        assert os.path.exists(path)
        assert open(path, "rb").read() == b"hello"

    def test_abort_on_exception(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with pytest.raises(ValueError):
            with PyroFile(path, "w") as f:
                f.write(b"hello")
                raise ValueError("something went wrong")
        # File should be aborted (deleted)
        assert not os.path.exists(path)


class TestIOBaseRegistration:
    def test_isinstance_iobase(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            assert isinstance(f, io.IOBase)
            assert isinstance(f, io.RawIOBase)


class TestNotImplemented:
    def test_readline_raises(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            with pytest.raises(NotImplementedError):
                f.readline()

    def test_fileno_raises(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            with pytest.raises(NotImplementedError):
                f.fileno()

    def test_isatty_returns_false(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        with PyroFile(path, "r") as f:
            assert f.isatty() is False


class TestOperationsAfterClose:
    def test_read_after_close(self, tmp_path):
        path = str(tmp_path / "test.bin")
        with open(path, "wb") as f:
            f.write(b"data")

        f = PyroFile(path, "r")
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.read()

    def test_write_after_close(self, tmp_path):
        path = str(tmp_path / "test.bin")
        f = PyroFile(path, "w")
        f.close()
        with pytest.raises(ValueError, match="closed"):
            f.write(b"data")
