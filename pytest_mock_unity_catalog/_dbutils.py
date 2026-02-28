import builtins
import shutil
from collections import namedtuple
from pathlib import Path

import pytest

from pytest_mock_unity_catalog._common import IS_DATABRICKS, _remap_volume_path

FileInfo = namedtuple("FileInfo", ["path", "name", "size", "modificationTime"])


class _MockFsUtil:
    """Local implementation of the ``dbutils.fs`` API.

    All path arguments that start with ``/Volumes/`` are transparently
    remapped to the same local temp directory used by ``mock_volume``.
    Non-volume paths are passed through unchanged.
    """

    def __init__(self, base: Path) -> None:
        self._base = base

    def _remap(self, path: str) -> Path:
        return Path(_remap_volume_path(path, self._base))

    def ls(self, path: str) -> list:
        """List the contents of a directory. Returns a list of ``FileInfo``."""
        local = self._remap(path)
        entries = []
        for entry in local.iterdir():
            stat = entry.stat()
            # Match Databricks convention: directories end with '/'
            display_path = path.rstrip("/") + "/" + entry.name
            if entry.is_dir():
                display_path += "/"
                size = 0
            else:
                size = stat.st_size
            entries.append(
                FileInfo(
                    path=display_path,
                    name=entry.name + ("/" if entry.is_dir() else ""),
                    size=size,
                    modificationTime=int(stat.st_mtime * 1000),
                )
            )
        return entries

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        """Write a UTF-8 string to a file, creating parent directories as needed."""
        local = self._remap(path)
        if local.exists() and not overwrite:
            raise FileExistsError(
                f"File already exists: {path}. Use overwrite=True to replace it."
            )
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(contents, encoding="utf-8")
        return True

    def head(self, path: str, max_bytes: int = 65536) -> str:
        """Return the first ``max_bytes`` bytes of a file decoded as UTF-8."""
        local = self._remap(path)
        with open(local, "rb") as f:
            return f.read(max_bytes).decode("utf-8")

    def mkdirs(self, path: str) -> bool:
        """Create a directory and all missing parents."""
        self._remap(path).mkdir(parents=True, exist_ok=True)
        return True

    def rm(self, path: str, recurse: bool = False) -> bool:
        """Remove a file or directory."""
        local = self._remap(path)
        if local.is_dir():
            if not recurse:
                raise IsADirectoryError(
                    f"{path} is a directory. Pass recurse=True to remove it."
                )
            shutil.rmtree(local)
        else:
            local.unlink()
        return True

    def cp(self, from_path: str, to_path: str, recurse: bool = False) -> bool:
        """Copy a file or directory."""
        src = self._remap(from_path)
        dst = self._remap(to_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)
        return True

    def mv(self, from_path: str, to_path: str, recurse: bool = False) -> bool:
        """Move a file or directory."""
        src = self._remap(from_path)
        dst = self._remap(to_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), dst)
        return True


class _MockDbUtils:
    def __init__(self, base: Path) -> None:
        self.fs = _MockFsUtil(base)


@pytest.fixture
def mock_dbutils(mock_volume):
    """Inject a ``dbutils``-compatible object into the test environment.

    The mock is set as ``builtins.dbutils`` for the duration of the test, so
    code under test can reference ``dbutils`` as a bare name — exactly as it
    appears in Databricks notebooks — without any import or fixture argument.

    All ``dbutils.fs.*`` calls that target ``/Volumes/...`` paths are redirected
    to the same local temp directory as ``mock_volume``.

    Supported methods: ``ls``, ``put``, ``head``, ``mkdirs``, ``rm``, ``cp``, ``mv``.

    On Databricks the real ``DBUtils`` instance is injected instead, so the same
    tests run against the live Unity Catalog volume without modification.

    Example — code under test uses ``dbutils`` directly::

        # production code (no imports needed)
        def list_files(path):
            return dbutils.fs.ls(path)

        # test
        def test_list(mock_dbutils):
            dbutils.fs.put("/Volumes/c/s/v/f.txt", "x", overwrite=True)
            assert any(e.name == "f.txt" for e in list_files("/Volumes/c/s/v"))
    """
    if IS_DATABRICKS:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        _mock = DBUtils(SparkSession.getActiveSession())
    else:
        _mock = _MockDbUtils(mock_volume)

    builtins.dbutils = _mock
    try:
        yield _mock
    finally:
        try:
            del builtins.dbutils
        except AttributeError:
            pass
