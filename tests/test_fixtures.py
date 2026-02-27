import pytest


@pytest.fixture
def table_name1():
    return "my_catalog.schema.mytable"


@pytest.fixture
def table_name2():
    return "my_catalog.schema.mytable2"


def test_mock_read_and_save_as_table(
    spark, table_name1, table_name2, mock_read_table, mock_save_as_table
):
    """Write to a Unity Catalog-style table name, then read it back from local path."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df.write.saveAsTable(table_name1)
    df.write.saveAsTable(table_name2)

    read_df = spark.read.table(table_name1)
    assert read_df.count() == 2

    read_df2 = spark.table(df.write.saveAsTable(table_name2))
    assert read_df2.count() == 2


def test_table_deleted(
    spark, table_name1, table_name2, mock_read_table, mock_save_as_table
):
    """Tables should be deleted after first test."""
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df.write.saveAsTable("my_catalog.schema.mytable")
    df.write.saveAsTable("my_catalog.schema.mytable2")

    spark.read.table(table_name1)
