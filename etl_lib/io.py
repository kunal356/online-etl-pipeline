from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def read_csv_from_adls(path, spark) -> DataFrame:
    return spark.read.option("header", "true").csv(path)


def write_to_adls(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)
