# pytest-mock-unity-catalog

Pytest plugin that provides PySpark fixtures for testing code that reads and writes Unity Catalog tables — without a live Databricks cluster. Table operations are redirected to a local Delta directory so tests run fully offline.

## Installation

```bash
pip install pytest-mock-unity-catalog
```

Pytest discovers the plugin automatically via its entry point. No imports or `conftest.py` changes are needed in the consuming project.

## Fixtures

### `spark`

A session-scoped `SparkSession` configured for local testing with Delta Lake enabled.

```python
def test_something(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "value"])
    assert df.count() == 1
```

By default uses `delta-spark_2.12:3.2.1`. Override via the `SPARK_VERSION` environment variable if your PySpark uses a different Scala version:

```bash
SPARK_VERSION=2.13:3.2.1 pytest
```

### `mock_save_as_table`

Patches `DataFrame.write.saveAsTable` to write a Delta table to a local temp directory instead of Unity Catalog. The Unity Catalog-style three-part name (`catalog.schema.table`) is mapped to a directory path.

```python
def test_write(spark, mock_save_as_table):
    df = spark.createDataFrame([(1, "a")], ["id", "value"])
    df.write.saveAsTable("my_catalog.my_schema.my_table")  # writes locally
```

### `mock_read_table`

Patches `spark.read.table` to read from the same local Delta path that `mock_save_as_table` writes to. Use both fixtures together to round-trip through a table.

```python
def test_read(spark, mock_read_table):
    df = spark.read.table("my_catalog.my_schema.my_table")
    assert df.count() == 2
```

### `local_table_base_path`

The `Path` to the session-scoped temp directory used as the root for all table storage. Useful for asserting on the filesystem directly or for sharing the path in custom fixtures.

```python
def test_path(local_table_base_path):
    assert (local_table_base_path / "my_catalog" / "my_schema" / "my_table").exists()
```

## Example: full round-trip

```python
def test_round_trip(spark, mock_save_as_table, mock_read_table):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df.write.saveAsTable("my_catalog.my_schema.my_table")

    result = spark.read.table("my_catalog.my_schema.my_table")
    assert result.count() == 2
```

## How it works

`mock_save_as_table` and `mock_read_table` patch PySpark's `DataFrameWriter.saveAsTable` and `DataFrameReader.table` for the duration of the test. The Unity Catalog table name is converted to a filesystem path by replacing `.` separators with `/`:

```
my_catalog.my_schema.my_table  →  <tmp>/my_catalog/my_schema/my_table
```

The temp directory is managed by pytest (`tmp_path_factory`) and lives under the OS temp space (e.g. `/var/folders/.../pytest-of-<user>/pytest-<N>/`). Pytest retains the last three runs before pruning.
