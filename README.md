# pytest-mock-unity-catalog

[![PyPI](https://img.shields.io/pypi/v/pytest-mock-unity-catalog)](https://pypi.org/project/pytest-mock-unity-catalog/)
[![Tests](https://github.com/marianreuss/pytest-mock-unity-catalog/actions/workflows/run_build.yml/badge.svg)](https://github.com/marianreuss/pytest-mock-unity-catalog/actions/workflows/run_build.yml)

Pytest plugin that provides PySpark fixtures for testing code that reads and writes Unity Catalog tables — without a live Databricks cluster. Table operations are redirected to a local Delta directory so tests run fully offline.

## Installation

For local development with PySpark install as follows:

```bash
pip install "pytest-mock-unity-catalog[spark]"
```

Running on databricks is automatically detected and Unity Catalog is used without any changes. On Databricks, make sure to install without the `spark` dependency.

```bash
pip install pytest-mock-unity-catalog
```

Pytest discovers the plugin automatically via its entry point. No imports or `conftest.py` changes are needed in the consuming project.

## Fixtures

<details>
<summary><code>spark</code></summary>

A session-scoped `SparkSession` configured for local testing with Delta Lake enabled.

```python
def test_something(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "value"])
    assert df.count() == 1
```

By default uses `delta-spark_4.1_2.13:4.1.0` (PySpark 4.1, Scala 2.13). Override via the `DELTA_ARTIFACT_SUFFIX` environment variable for other versions:

```bash
# PySpark 3.5 / Scala 2.12
DELTA_ARTIFACT_SUFFIX=2.12:3.2.1 pytest

# PySpark 4.0 / Scala 2.13
DELTA_ARTIFACT_SUFFIX=4.0_2.13:4.0.0 pytest
```

</details>

<details>
<summary><code>mock_save_as_table</code></summary>

Patches `DataFrame.write.saveAsTable` to write a Delta table to a local temp directory instead of Unity Catalog. The Unity Catalog-style three-part name (`catalog.schema.table`) is mapped to a directory path.

```python
def test_write(spark, mock_save_as_table):
    df = spark.createDataFrame([(1, "a")], ["id", "value"])
    df.write.saveAsTable("my_catalog.my_schema.my_table")  # writes locally
```

</details>

<details>
<summary><code>mock_read_table</code></summary>

Patches both `spark.read.table` and `spark.table` to read from the same local Delta path that `mock_save_as_table` writes to. Use both fixtures together to round-trip through a table.

```python
def test_read(spark, mock_read_table):
    df = spark.read.table("my_catalog.my_schema.my_table")
    assert df.count() == 2

    df2 = spark.table("my_catalog.my_schema.my_table")
    assert df2.count() == 2
```

</details>

<details>
<summary><code>mock_delta_table</code></summary>

Patches `DeltaTable.forName` and `spark.sql` DML statements to redirect Unity
Catalog three-part names (`catalog.schema.table`) to the same local Delta paths
used by `mock_save_as_table`. Use alongside `mock_save_as_table` when the code
under test performs merge, delete, or update operations.

| Pattern | Mechanism |
|---|---|
| `DeltaTable.forName(spark, "cat.schema.tbl").merge(...).execute()` | `DeltaTable.forName` → `DeltaTable.forPath` |
| `DeltaTable.forName(spark, "cat.schema.tbl").delete(condition)` | `DeltaTable.forName` → `DeltaTable.forPath` |
| `DeltaTable.forName(spark, "cat.schema.tbl").update(condition, {...})` | `DeltaTable.forName` → `DeltaTable.forPath` |
| `spark.sql("MERGE INTO cat.schema.tbl USING ...")` | rewrites name to `delta.\`/local/path\`` |
| `spark.sql("DELETE FROM cat.schema.tbl WHERE ...")` | rewrites name to `delta.\`/local/path\`` |
| `spark.sql("UPDATE cat.schema.tbl SET ...")` | rewrites name to `delta.\`/local/path\`` |

> **Note:** SQL rewriting only applies to `MERGE INTO`, `DELETE FROM`, and `UPDATE`
> statements where the target matches a table already written locally. `SELECT` and
> other SQL are passed through unmodified.

```python
from delta.tables import DeltaTable

def test_merge(spark, mock_read_table, mock_save_as_table, mock_delta_table):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df.write.saveAsTable("my_catalog.my_schema.my_table")

    updates = spark.createDataFrame([(1, "updated"), (3, "new")], ["id", "value"])
    (
        DeltaTable.forName(spark, "my_catalog.my_schema.my_table")
        .alias("t")
        .merge(updates.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    result = spark.read.table("my_catalog.my_schema.my_table")
    assert result.count() == 3
```

On Databricks the fixture is a no-op; `DeltaTable.forName` and `spark.sql` reach
the real Unity Catalog directly.

</details>

<details>
<summary><code>mock_volume</code></summary>

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

</details>

<details>
<summary><code>mock_dbutils</code></summary>

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

</details>
