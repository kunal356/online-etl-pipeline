from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing import Optional, Dict


def read_from_adls(
        path: str,
        spark: SparkSession,
        file_format: str = "csv",
        options: Optional[Dict[str, str]] = None
) -> DataFrame:
    reader = spark.read.format(file_format)

    if options:
        for key, value in options.items():
            reader = reader.option(key=key, value=value)
    return reader.load(path=path)


def write_to_adls(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)
