from pyspark.sql.functions import *
from pyspark.sql import DataFrame


def remove_invalid_columns(df: DataFrame, col_name: str) -> DataFrame:
    return df.filter((col(col_name) > 0))


def capitalize_first_letter(df: DataFrame, col_name: str) -> DataFrame:
    return df \
        .withColumn(
            col_name,
            expr(
                f"concat(upper(substring(trim({col_name}), 1, 1)), lower(substring(trim({col_name}), 2)))")
        )


def cap_every_first_letter(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(col_name, initcap(col(col_name)))


def calc_total_price(df: DataFrame, decimals: int = 2):
    return df.withColumn("TotalPrice", round(col("Quantity") * col("Price"), decimals))


def extract_date(df: DataFrame) -> DataFrame:
    return df.withColumn("InvoiceYear", year("InvoiceDate")) \
        .withColumn("InvoiceMonth", month("InvoiceDate")) \
        .withColumn("InvoiceDay", dayofmonth("InvoiceDate")) \
        .withColumn("InvoiceTime", date_format("InvoiceDate", "HH:mm:ss"))


def isReturn(df: DataFrame) -> DataFrame:
    return df.withColumn("IsReturn", when(col("Quantity") < 0, True).otherwise(False)) \



def isUKCustomer(df: DataFrame) -> DataFrame:
    return df.withColumn("IsUKCustomer", when(col("Country") == "United Kingdom", True).otherwise(False))
