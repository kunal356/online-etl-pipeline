import os
from pyspark.sql import SparkSession


def is_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark_session() -> SparkSession:
    builder = SparkSession.builder.appName("Retail ETL Pipeline")
    if not is_databricks():
        builder = builder.master("local[*]")
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark
