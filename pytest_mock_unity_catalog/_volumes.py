import builtins
import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from pytest_mock_unity_catalog._common import (
    _VOLUMES_PREFIX,
    IS_DATABRICKS,
    _remap_volume_path,
)


@pytest.fixture(scope="session")
def volume_base_path(tmp_path_factory) -> Path:
    """Session-scoped root directory standing in for ``/Volumes``."""
    return tmp_path_factory.mktemp("volumes")


@pytest.fixture
def mock_volume(tmp_path_factory):
    """Redirect ``/Volumes/...`` filesystem access to a local temp directory.

    Intercepted access patterns:

    - ``open("/Volumes/...", ...)``              built-in ``open``
    - ``open(Path("/Volumes/..."), ...)``        built-in ``open`` via PathLike
    - ``Path("/Volumes/...").read_text()``       pathlib via ``__fspath__``
    - ``Path("/Volumes/...").write_text(...)``   pathlib via ``__fspath__``
    - ``Path("/Volumes/...").exists()`` etc.     pathlib via ``__fspath__``
    - ``pd.read_csv("/Volumes/...", ...)``       pandas delegates to ``open()``
    - ``pd.DataFrame.to_csv("/Volumes/...", ...)`` pandas delegates to ``open()``

    Binary/columnar readers that bypass Python's ``open()`` — e.g.
    ``pandas.read_parquet`` via pyarrow — are **not** intercepted.

    On Databricks the fixture is a no-op; ``/Volumes`` paths reach the real
    Unity Catalog volume.

    The fixture yields the local base ``Path`` so tests can seed files::

        def test_read_csv(mock_volume):
            f = mock_volume / "cat" / "schema" / "vol" / "data.csv"
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text("id,value\\n1,a\\n")

            df = pd.read_csv("/Volumes/cat/schema/vol/data.csv")
            assert len(df) == 1
    """
    if IS_DATABRICKS:
        yield Path(_VOLUMES_PREFIX)
        return

    _base = tmp_path_factory.mktemp("volumes")
    _orig_open = builtins.open
    _orig_fspath = Path.__fspath__

    def _patched_open(file, *args, **kwargs):
        try:
            s = os.fspath(file)
            if isinstance(s, str):
                remapped = _remap_volume_path(s, _base)
                if remapped != s:
                    mode = args[0] if args else kwargs.get("mode", "r")
                    if any(c in mode for c in "wax"):
                        os.makedirs(os.path.dirname(remapped), exist_ok=True)
                    file = remapped
        except TypeError:
            pass
        return _orig_open(file, *args, **kwargs)

    def _patched_fspath(self):
        original = _orig_fspath(self)
        remapped = _remap_volume_path(original, _base)
        if remapped != original:
            os.makedirs(os.path.dirname(remapped), exist_ok=True)
        return remapped

    with (
        patch("builtins.open", _patched_open),
        patch("pathlib.Path.__fspath__", _patched_fspath),
    ):
        yield _base

    shutil.rmtree(_base, ignore_errors=True)
