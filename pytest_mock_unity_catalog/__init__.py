import builtins
import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

IS_DATABRICKS = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

_VOLUMES_PREFIX = "/Volumes/"


def _remap_volume_path(path: str, base: Path) -> str:
    """Remap a ``/Volumes/...`` string to the local temp volume root."""
    if path.startswith(_VOLUMES_PREFIX):
        return str(base / path[len(_VOLUMES_PREFIX) :])
    return path


def _table_name_to_path(base_path: Path, table_name: str) -> Path:
    """Map Unity Catalog table name to local directory path."""
    return base_path / table_name.replace(".", os.sep)


@pytest.fixture(scope="session")
def local_table_base_path(tmp_path_factory) -> Path:
    """Base directory for local table storage (replacing Unity Catalog)."""
    return tmp_path_factory.mktemp("local_tables")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    if IS_DATABRICKS:
        return SparkSession.getActiveSession()
    return (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.compress", "false")
        .config("spark.executor.cores", 1)
        .config("spark.executor.instances", 1)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config(
            "spark.jars.packages",
            f"io.delta:delta-spark_{os.getenv('SPARK_VERSION', '4.1_2.13:4.1.0')}",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .appName("pytest spark session")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def volume_base_path(tmp_path_factory) -> Path:
    """Session-scoped root directory standing in for ``/Volumes``."""
    return tmp_path_factory.mktemp("volumes")


@pytest.fixture
def mock_volume(tmp_path_factory):
    """Redirect ``/Volumes/...`` filesystem access to a local temp directory.

    Intercepted access patterns:

    - ``open("/Volumes/...")``                  built-in ``open``
    - ``open(Path("/Volumes/..."))``            built-in ``open`` via PathLike
    - ``Path("/Volumes/...").read_text()``      pathlib via ``__fspath__``
    - ``Path("/Volumes/...").write_text(...)``  pathlib via ``__fspath__``
    - ``Path("/Volumes/...").exists()`` etc.    pathlib via ``__fspath__``
    - ``pd.read_csv("/Volumes/...")``           pandas delegates to ``open()``
    - ``pd.DataFrame.to_csv("/Volumes/...")``   pandas delegates to ``open()``

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


@pytest.fixture
def mock_read_table(spark, local_table_base_path):
    """Mock spark.read.table and spark.table to read from local Delta path derived from table name."""

    if IS_DATABRICKS:
        yield
        return

    def _reader_table(self, tableName, *args, **kwargs):
        path = _table_name_to_path(local_table_base_path, tableName)
        return self.format("delta").load(str(path))

    def _session_table(self, tableName, *_args, **_kwargs):
        path = _table_name_to_path(local_table_base_path, tableName)
        return self.read.format("delta").load(str(path))

    with (
        patch("pyspark.sql.DataFrameReader.table", _reader_table),
        patch("pyspark.sql.SparkSession.table", _session_table),
    ):
        yield


@pytest.fixture
def mock_save_as_table(local_table_base_path):
    """Mock DataFrame.write.saveAsTable to write to local Delta path derived from table name."""
    written_paths: list[Path] = []
    if IS_DATABRICKS:
        written_paths.append(local_table_base_path)
        yield
        for path in written_paths:
            spark.sql(f"DROP TABLE IF EXISTS {local_table_base_path};")
    else:

        def _save_as_table(self, name, mode=None, *args, **kwargs):
            path = _table_name_to_path(local_table_base_path, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            written_paths.append(path)
            writer = self.format("delta")
            if mode is not None:
                writer = writer.mode(mode)
            return writer.save(str(path))

        with patch("pyspark.sql.DataFrameWriter.saveAsTable", _save_as_table):
            yield

        for path in written_paths:
            if path.exists():
                shutil.rmtree(str(path))
