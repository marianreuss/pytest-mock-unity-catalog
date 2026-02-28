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

By default uses `delta-spark_4.1_2.13:4.1.0` (PySpark 4.1, Scala 2.13). Override via the `SPARK_VERSION` environment variable for other versions:

```bash
# PySpark 3.5 / Scala 2.12
SPARK_VERSION=2.12:3.2.1 pytest

# PySpark 4.0 / Scala 2.13
SPARK_VERSION=4.0_2.13:4.0.0 pytest
```


### `mock_save_as_table`

Patches `DataFrame.write.saveAsTable` to write a Delta table to a local temp directory instead of Unity Catalog. The Unity Catalog-style three-part name (`catalog.schema.table`) is mapped to a directory path.

```python
def test_write(spark, mock_save_as_table):
    df = spark.createDataFrame([(1, "a")], ["id", "value"])
    df.write.saveAsTable("my_catalog.my_schema.my_table")  # writes locally
```

### `mock_read_table`

Patches both `spark.read.table` and `spark.table` to read from the same local Delta path that `mock_save_as_table` writes to. Use both fixtures together to round-trip through a table.

```python
def test_read(spark, mock_read_table):
    df = spark.read.table("my_catalog.my_schema.my_table")
    assert df.count() == 2

    df2 = spark.table("my_catalog.my_schema.my_table")
    assert df2.count() == 2
```

### `local_table_base_path`

The `Path` to the session-scoped temp directory used as the root for all table storage. Useful for asserting on the filesystem directly or for sharing the path in custom fixtures.

```python
def test_path(local_table_base_path):
    assert (local_table_base_path / "my_catalog" / "my_schema" / "my_table").exists()
```

### `mock_volume`

Redirects all `/Volumes/...` filesystem access to a local temp directory for the duration of the test. The fixture yields the local base `Path` so tests can seed files before exercising the code under test.

Intercepted access patterns:

| Pattern | Mechanism |
|---|---|
| `open("/Volumes/...")` | patches `builtins.open` |
| `open(Path("/Volumes/..."))` | patches `builtins.open` via PathLike |
| `Path("/Volumes/...").read_text()` | patches `Path.__fspath__` |
| `Path("/Volumes/...").write_text(...)` | patches `Path.__fspath__` |
| `Path("/Volumes/...").exists()` / `.stat()` / `.mkdir()` | patches `Path.__fspath__` |
| `pd.read_csv("/Volumes/...")` | pandas delegates to `open()` |
| `pd.DataFrame.to_csv("/Volumes/...")` | pandas delegates to `open()` |

> **Limitation:** binary/columnar readers that bypass Python's `open()` — e.g. `pandas.read_parquet` backed by pyarrow — are not intercepted.

Parent directories under the temp root are created automatically, so no explicit `mkdir` is needed before writing.

```python
def test_read_volume(mock_volume):
    # Seed a file at the equivalent of /Volumes/cat/schema/vol/data.csv
    seed = mock_volume / "cat" / "schema" / "vol" / "data.csv"
    seed.parent.mkdir(parents=True, exist_ok=True)
    seed.write_text("id,value\n1,a\n2,b\n")

    # Code under test uses the real /Volumes path — it is transparently redirected
    import pandas as pd
    df = pd.read_csv("/Volumes/cat/schema/vol/data.csv")
    assert len(df) == 2
```

Works with `pathlib.Path` too:

```python
def test_write_volume(mock_volume):
    from pathlib import Path

    Path("/Volumes/cat/schema/vol/out.txt").write_text("hello")

    result = Path("/Volumes/cat/schema/vol/out.txt").read_text()
    assert result == "hello"
```

### `volume_base_path`

The session-scoped `Path` used as the root for all volume storage. Injected automatically into `mock_volume`; only needed directly when building custom fixtures on top of the volume base.

### `mock_dbutils`

Injects a `dbutils`-compatible object into `builtins` for the duration of the test, so code under test can reference `dbutils` as a bare name — exactly as it does inside a Databricks notebook — without any import or fixture argument.

All `dbutils.fs.*` calls that target `/Volumes/...` paths are redirected to the same local temp directory as `mock_volume`, so both `open()` and `dbutils.fs.*` access the same files.

```python
# Production code — no imports, bare dbutils reference
def list_files(path):
    return dbutils.fs.ls(path)

# Test — just request the fixture; dbutils is available globally
def test_list(mock_dbutils):
    dbutils.fs.put("/Volumes/cat/schema/vol/data.txt", "hello", overwrite=True)
    assert any(e.name == "data.txt" for e in list_files("/Volumes/cat/schema/vol"))
```

The fixture also yields the mock object, so tests can reference it via the parameter name when that reads more clearly.

Supported `dbutils.fs` methods:

| Method | Signature |
|---|---|
| `ls` | `ls(path) → list[FileInfo]` |
| `put` | `put(path, contents, overwrite=False) → bool` |
| `head` | `head(path, max_bytes=65536) → str` |
| `mkdirs` | `mkdirs(path) → bool` |
| `rm` | `rm(path, recurse=False) → bool` |
| `cp` | `cp(from_path, to_path, recurse=False) → bool` |
| `mv` | `mv(from_path, to_path, recurse=False) → bool` |

`ls` returns a list of `FileInfo(path, name, size, modificationTime)` namedtuples that match the Databricks shape. Directory entries have a trailing `/` in `name` and `size=0`.

Files seeded via `mock_volume` (or via `open()`) are immediately visible to `dbutils.fs`, and vice versa:

```python
def test_cross_access(mock_volume, mock_dbutils):
    # Write via pathlib, read via dbutils
    (mock_volume / "cat" / "schema" / "vol").mkdir(parents=True, exist_ok=True)
    (mock_volume / "cat" / "schema" / "vol" / "file.txt").write_text("shared")
    assert dbutils.fs.head("/Volumes/cat/schema/vol/file.txt") == "shared"

    # Write via dbutils, read via open()
    dbutils.fs.put("/Volumes/cat/schema/vol/out.txt", "also shared", overwrite=True)
    with open("/Volumes/cat/schema/vol/out.txt") as f:
        assert f.read() == "also shared"
```

On Databricks the real `DBUtils(spark)` instance is injected instead, so the same tests run against the live Unity Catalog volume without modification.

## Example: full round-trip

```python
def test_round_trip(spark, mock_save_as_table, mock_read_table):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df.write.saveAsTable("my_catalog.my_schema.my_table")

    result = spark.read.table("my_catalog.my_schema.my_table")
    assert result.count() == 2
```

## Databricks / on-cluster usage

When tests run inside a Databricks notebook or job (i.e. `DATABRICKS_RUNTIME_VERSION` is set), the plugin detects this automatically:

- **`spark`** returns the active `SparkSession` instead of creating a local one.
- **`mock_read_table`** is a no-op — `spark.read.table` hits Unity Catalog as normal.
- **`mock_save_as_table`** is a no-op — `df.write.saveAsTable` writes to Unity Catalog as normal. The table is dropped with `DROP TABLE IF EXISTS` in teardown.
- **`mock_volume`** is a no-op — `/Volumes/...` paths reach the real Unity Catalog volume.

No code changes are needed; the same tests run locally (mocked) and on Databricks (real).

## How it works

`mock_save_as_table` and `mock_read_table` patch PySpark's `DataFrameWriter.saveAsTable` and `DataFrameReader.table` for the duration of the test. The Unity Catalog table name is converted to a filesystem path by replacing `.` separators with `/`:

```
my_catalog.my_schema.my_table  →  <tmp>/my_catalog/my_schema/my_table
```

The temp directory is managed by pytest (`tmp_path_factory`) and lives under the OS temp space (e.g. `/var/folders/.../pytest-of-<user>/pytest-<N>/`). Pytest retains the last three runs before pruning.
