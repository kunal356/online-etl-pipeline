import os
import logging
from pyspark.sql import SparkSession

_SPARK = None


def is_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark_session() -> SparkSession:
    global _SPARK
    if _SPARK is None:
        builder = SparkSession.builder.appName("Retail ETL Pipeline")
        if not is_databricks():
            builder = builder.master("local[*]")
        _SPARK = builder \
            .getOrCreate()
        _SPARK.sparkContext.setLogLevel("ERROR")
    return _SPARK


def stop_spark_session():
    global _SPARK
    if _SPARK is not None:
        _SPARK.stop()
        _SPARK = None


def get_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(name)
