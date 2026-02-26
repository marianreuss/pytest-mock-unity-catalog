import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession


def _table_name_to_path(base_path: Path, table_name: str) -> Path:
    """Map Unity Catalog table name to local directory path."""
    return base_path / table_name.replace(".", os.sep)


@pytest.fixture(scope="session")
def local_table_base_path(tmp_path_factory) -> Path:
    """Base directory for local table storage (replacing Unity Catalog)."""
    return tmp_path_factory.mktemp("local_tables")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
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
            f"io.delta:delta-spark_{os.getenv('SPARK_VERSION', '2.12:3.2.1')}",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .appName("pytest spark session")
        .getOrCreate()
    )


@pytest.fixture
def mock_read_table(spark, local_table_base_path):
    """Mock spark.read.table to read from local Delta path derived from table name."""

    def _table(self, tableName, *args, **kwargs):
        path = _table_name_to_path(local_table_base_path, tableName)
        return self.format("delta").load(str(path))

    with patch("pyspark.sql.DataFrameReader.table", _table):
        yield


@pytest.fixture
def mock_save_as_table(local_table_base_path):
    """Mock DataFrame.write.saveAsTable to write to local Delta path derived from table name."""
    written_paths: list[Path] = []

    def _save_as_table(self, name, *args, **kwargs):
        path = _table_name_to_path(local_table_base_path, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        written_paths.append(path)
        return self.format("delta").mode("overwrite").save(str(path))

    with patch("pyspark.sql.DataFrameWriter.saveAsTable", _save_as_table):
        yield

    for path in written_paths:
        if path.exists():
            shutil.rmtree(path)
