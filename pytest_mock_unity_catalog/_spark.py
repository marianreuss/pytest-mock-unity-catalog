import os

import pytest

from pytest_mock_unity_catalog._common import IS_DATABRICKS


@pytest.fixture(scope="session")
def spark():
    SparkSession = pytest.importorskip(
        "pyspark.sql",
        reason="pyspark not installed; install pytest-mock-unity-catalog[spark]",
    ).SparkSession
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
            "io.delta:delta-spark_"
            + os.getenv("DELTA_ARTIFACT_SUFFIX", "4.1_2.13:4.1.0"),
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .appName("pytest spark session")
        .getOrCreate()
    )
