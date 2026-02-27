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
