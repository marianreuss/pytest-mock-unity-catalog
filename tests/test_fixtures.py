from pathlib import Path

import pytest
from pyspark.errors import AnalysisException


@pytest.fixture
def table_name1():
    return "my_catalog.schema.mytable"


@pytest.fixture
def table_name2():
    return "my_catalog.schema.mytable2"


@pytest.fixture
def df(spark):
    return spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])


def test_mock_read_and_save_as_table(
    spark, df, table_name1, table_name2, mock_read_table, mock_save_as_table
):
    """Write to a Unity Catalog-style table name, then read it back from local path."""
    df.write.saveAsTable(table_name1)
    df.write.saveAsTable(table_name2)

    read_df = spark.read.table(table_name1)
    assert read_df.count() == 2

    read_df2 = spark.table(table_name2)
    assert read_df2.count() == 2


def test_table_deleted(
    spark, df, table_name1, table_name2, mock_read_table, mock_save_as_table
):
    """Tables should be deleted after first test."""
    with pytest.raises(AnalysisException):
        spark.read.table(table_name1)
    with pytest.raises(AnalysisException):
        spark.table(table_name2)


def test_mock_volume_open(mock_volume):
    """open() with a /Volumes/ string path is redirected to the temp dir."""
    seed = mock_volume / "cat" / "schema" / "vol" / "data.txt"
    seed.parent.mkdir(parents=True, exist_ok=True)
    seed.write_text("hello")

    with open("/Volumes/cat/schema/vol/data.txt") as f:
        assert f.read() == "hello"


def test_mock_volume_open_write(mock_volume):
    """open() in write mode auto-creates parent dirs and writes to the temp dir."""
    with open("/Volumes/cat/schema/vol/out.txt", "w") as f:
        f.write("world")

    result = (mock_volume / "cat" / "schema" / "vol" / "out.txt").read_text()
    assert result == "world"


def test_mock_volume_pathlib(mock_volume):
    """Path('/Volumes/...') operations are redirected via __fspath__."""
    seed = mock_volume / "cat" / "schema" / "vol" / "file.txt"
    seed.parent.mkdir(parents=True, exist_ok=True)
    seed.write_text("via pathlib")

    assert Path("/Volumes/cat/schema/vol/file.txt").read_text() == "via pathlib"
    assert Path("/Volumes/cat/schema/vol/file.txt").exists()
    assert not Path("/Volumes/cat/schema/vol/missing.txt").exists()


def test_mock_volume_pathlib_write(mock_volume):
    """Path('/Volumes/...').write_text() is redirected to the temp dir."""
    Path("/Volumes/cat/schema/vol/out.txt").write_text("written")

    assert (mock_volume / "cat" / "schema" / "vol" / "out.txt").read_text() == "written"


# ---------------------------------------------------------------------------
# mock_dbutils tests
# ---------------------------------------------------------------------------


def test_mock_dbutils_put_and_head(mock_dbutils):
    """dbutils.fs.put writes a file; dbutils.fs.head reads it back."""
    mock_dbutils.fs.put("/Volumes/c/s/v/file.txt", "hello", overwrite=True)
    assert mock_dbutils.fs.head("/Volumes/c/s/v/file.txt") == "hello"


def test_mock_dbutils_head_max_bytes(mock_dbutils):
    """dbutils.fs.head respects the max_bytes limit."""
    mock_dbutils.fs.put("/Volumes/c/s/v/big.txt", "abcdefghij", overwrite=True)
    assert mock_dbutils.fs.head("/Volumes/c/s/v/big.txt", max_bytes=3) == "abc"


def test_mock_dbutils_ls(mock_dbutils):
    """dbutils.fs.ls returns FileInfo entries for all items in the directory."""
    mock_dbutils.fs.mkdirs("/Volumes/c/s/v")
    mock_dbutils.fs.put("/Volumes/c/s/v/a.txt", "x", overwrite=True)
    mock_dbutils.fs.put("/Volumes/c/s/v/b.txt", "y", overwrite=True)

    entries = mock_dbutils.fs.ls("/Volumes/c/s/v")
    names = {e.name for e in entries}
    assert names == {"a.txt", "b.txt"}
    assert all(e.size > 0 for e in entries)


def test_mock_dbutils_ls_directory_entry(mock_dbutils):
    """dbutils.fs.ls marks subdirectory entries with a trailing slash."""
    mock_dbutils.fs.mkdirs("/Volumes/c/s/v/subdir")
    entries = mock_dbutils.fs.ls("/Volumes/c/s/v")
    dir_entry = next(e for e in entries if "subdir" in e.name)
    assert dir_entry.name.endswith("/")
    assert dir_entry.size == 0


def test_mock_dbutils_rm_file(mock_dbutils):
    """dbutils.fs.rm removes a single file."""
    mock_dbutils.fs.put("/Volumes/c/s/v/del.txt", "gone", overwrite=True)
    mock_dbutils.fs.rm("/Volumes/c/s/v/del.txt")
    with pytest.raises(Exception):
        mock_dbutils.fs.head("/Volumes/c/s/v/del.txt")


def test_mock_dbutils_rm_recurse(mock_dbutils):
    """dbutils.fs.rm with recurse=True removes a directory tree."""
    mock_dbutils.fs.put("/Volumes/c/s/v/dir/f.txt", "data", overwrite=True)
    mock_dbutils.fs.rm("/Volumes/c/s/v/dir", recurse=True)
    with pytest.raises(Exception):
        mock_dbutils.fs.ls("/Volumes/c/s/v/dir")


def test_mock_dbutils_cp(mock_dbutils):
    """dbutils.fs.cp copies a file to a new location."""
    mock_dbutils.fs.put("/Volumes/c/s/v/src.txt", "copy me", overwrite=True)
    mock_dbutils.fs.cp("/Volumes/c/s/v/src.txt", "/Volumes/c/s/v/dst.txt")
    assert mock_dbutils.fs.head("/Volumes/c/s/v/src.txt") == "copy me"
    assert mock_dbutils.fs.head("/Volumes/c/s/v/dst.txt") == "copy me"


def test_mock_dbutils_mv(mock_dbutils):
    """dbutils.fs.mv moves a file, making the original unavailable."""
    mock_dbutils.fs.put("/Volumes/c/s/v/old.txt", "move me", overwrite=True)
    mock_dbutils.fs.mv("/Volumes/c/s/v/old.txt", "/Volumes/c/s/v/new.txt")
    assert mock_dbutils.fs.head("/Volumes/c/s/v/new.txt") == "move me"
    with pytest.raises(Exception):
        mock_dbutils.fs.head("/Volumes/c/s/v/old.txt")


def test_mock_dbutils_and_open_share_same_dir(mock_volume, mock_dbutils):
    """Files seeded via mock_volume are readable via dbutils.fs, and vice versa."""
    # Seed via mock_volume (pathlib), read via dbutils.fs
    local = mock_volume / "c" / "s" / "v"
    local.mkdir(parents=True, exist_ok=True)
    (local / "data.txt").write_text("shared")
    assert mock_dbutils.fs.head("/Volumes/c/s/v/data.txt") == "shared"

    # Seed via dbutils.fs.put, read via open()
    mock_dbutils.fs.put("/Volumes/c/s/v/written.txt", "also shared", overwrite=True)
    with open("/Volumes/c/s/v/written.txt") as f:
        assert f.read() == "also shared"
