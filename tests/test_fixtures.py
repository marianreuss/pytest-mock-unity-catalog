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
