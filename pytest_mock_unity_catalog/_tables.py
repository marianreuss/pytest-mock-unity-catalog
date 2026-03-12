import re
import shutil
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest

from pytest_mock_unity_catalog._common import IS_DATABRICKS, _table_name_to_path


@pytest.fixture(scope="session")
def local_table_base_path(tmp_path_factory) -> Path:
    """Base directory for local table storage (replacing Unity Catalog)."""
    return tmp_path_factory.mktemp("local_tables")


def _mock_save_as_table_databricks(spark):
    from pyspark.sql import DataFrameWriter

    written_names: list[str] = []

    def _make_tracker(orig):
        def _track(self, name, *args, **kwargs):
            written_names.append(name)
            return orig(self, name, *args, **kwargs)

        return _track

    # Patch both the classic and Spark Connect DataFrameWriter (DBR 13+).
    active_patches = [
        patch(
            "pyspark.sql.DataFrameWriter.saveAsTable",
            _make_tracker(DataFrameWriter.saveAsTable),
        )
    ]
    try:
        from pyspark.sql.connect.readwriter import DataFrameWriter as ConnectDFW

        active_patches.append(
            patch(
                "pyspark.sql.connect.readwriter.DataFrameWriter.saveAsTable",
                _make_tracker(ConnectDFW.saveAsTable),
            )
        )
    except ImportError:
        pass

    try:
        with ExitStack() as stack:
            for p in active_patches:
                stack.enter_context(p)
            yield
    finally:
        for name in written_names:
            spark.sql(f"DROP TABLE IF EXISTS {name}")


def _mock_save_as_table_local(local_table_base_path):
    written_paths: list[Path] = []

    def _save_as_table(self, name, mode=None, *_):
        path = _table_name_to_path(local_table_base_path, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        written_paths.append(path)
        writer = self.format("delta")
        if mode is not None:
            writer = writer.mode(mode)
        return writer.save(str(path))

    try:
        with patch("pyspark.sql.DataFrameWriter.saveAsTable", _save_as_table):
            yield
    finally:
        for path in written_paths:
            if path.exists():
                shutil.rmtree(str(path))


@pytest.fixture
def mock_save_as_table(spark, local_table_base_path):
    """Mock DataFrame.write.saveAsTable to write to local Delta path."""
    impl = (
        _mock_save_as_table_databricks(spark)
        if IS_DATABRICKS
        else _mock_save_as_table_local(local_table_base_path)
    )
    yield from impl


@pytest.fixture
def mock_read_table(spark, local_table_base_path):
    """Mock spark.read.table and spark.table to read from local Delta path."""
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


_THREE_PART_RE = re.compile(
    r"`[^`]+`\s*\.\s*`[^`]+`\s*\.\s*`[^`]+`"  # backtick-quoted
    r"|[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*",  # plain identifiers
)
_DML_RE = re.compile(r"^\s*(?:MERGE\s+INTO|DELETE\s+FROM|UPDATE)\s+", re.IGNORECASE)


def _rewrite_sql_for_local(sql: str, base_path: Path) -> str:
    """Rewrite three-part Unity Catalog names in DML SQL to delta.`/local/path` references."""
    if not _DML_RE.match(sql):
        return sql

    def _replace(m: re.Match) -> str:
        raw = m.group(0)
        parts = [p.strip() for p in re.split(r"[\s`.]+", raw) if p.strip()]
        if len(parts) != 3:
            return raw
        name = ".".join(parts)
        path = _table_name_to_path(base_path, name)
        return f"delta.`{path}`" if path.exists() else raw

    return _THREE_PART_RE.sub(_replace, sql)


def _mock_delta_table_local(spark, local_table_base_path: Path):
    from pyspark.sql import SparkSession

    original_sql = SparkSession.sql

    def _patched_sql(self, sqlQuery, *args, **kwargs):
        return original_sql(
            self,
            _rewrite_sql_for_local(sqlQuery, local_table_base_path),
            *args,
            **kwargs,
        )

    active_patches = [patch("pyspark.sql.SparkSession.sql", _patched_sql)]

    try:
        from delta.tables import DeltaTable

        def _for_name(cls, sparkSession, tableOrViewName):
            path = _table_name_to_path(local_table_base_path, tableOrViewName)
            if path.exists():
                return DeltaTable.forPath(sparkSession, str(path))
            raise RuntimeError(
                f"mock_delta_table: no local table found for '{tableOrViewName}' "
                f"(expected path: {path}). Write the table first with mock_save_as_table."
            )

        active_patches.append(
            patch.object(DeltaTable, "forName", classmethod(_for_name))
        )
    except ImportError:
        pass  # delta Python package not installed; SQL rewriting still applies

    with ExitStack() as stack:
        for p in active_patches:
            stack.enter_context(p)
        yield


@pytest.fixture
def mock_delta_table(spark, local_table_base_path):
    """Mock DeltaTable.forName and spark.sql DML to operate on local Delta paths.

    Use alongside mock_save_as_table when production code performs merge, delete,
    or update operations on Unity Catalog tables.

    Supported:
      - DeltaTable.forName(spark, "cat.schema.tbl").merge(...).execute()
      - DeltaTable.forName(spark, "cat.schema.tbl").delete(condition)
      - DeltaTable.forName(spark, "cat.schema.tbl").update(condition, {...})
      - spark.sql("MERGE INTO cat.schema.tbl USING ...")
      - spark.sql("DELETE FROM cat.schema.tbl WHERE ...")
      - spark.sql("UPDATE cat.schema.tbl SET ...")

    On Databricks: no-op (real Delta APIs operate against Unity Catalog directly).
    """
    if IS_DATABRICKS:
        yield
        return
    yield from _mock_delta_table_local(spark, local_table_base_path)
