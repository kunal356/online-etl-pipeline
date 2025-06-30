from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType
from etl_lib.io import *
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:%(name)s: %(message)s')

logger = logging.getLogger(__name__)


def rename_column(df, curr_col_name, new_col_name) -> DataFrame:
    return df.withColumnRenamed(curr_col_name, new_col_name)


def clean_and_cast_columns(df: DataFrame, raise_on_parse_failure: bool = False):
    # Check for required columns
    required_columns = {"Quantity", "Price", "InvoiceDate"}
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        error_msg = f"Missing requried columns: {', '.join(missing_cols)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Different Date Formats
    date_formats = [
        "yyyy-MM-dd HH:mm:ss.SSSSSS",   # With milliseconds
        "dd-MM-yyyy HH:mm:ss.SSSSSS",  # With milliseconds
        "dd/MM/yyyy HH:mm:ss.SSSSSS",  # With milliseconds
        "yyyy-MM-dd HH:mm:ss",        # Without milliseconds
        "yyyy/MM/dd HH:mm:ss",
        "MM-dd-yyyy HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss",
        "dd-MM-yyyy h:mm:ss a",       # For "01-12-2010 8:26:00 AM" (AM/PM)
        "yyyy-MM-dd",                 # Date only
        "MM-dd-yyyy",                 # Date only
    ]

    logger.info("Attempting to parse Invoice Date Column using multiple formats")
    timestamp_exprs = [to_timestamp(
        col("InvoiceDate"), fmt) for fmt in date_formats]

    parsed_timestamp = coalesce(*timestamp_exprs)
    # Add None if date parsing failed
    if raise_on_parse_failure:
        parsed_timestamp = when(
            parsed_timestamp.isNotNull(), parsed_timestamp
        ).otherwise(
            None
        )
    # Force Fail using the following code:
    # if raise_on_parse_failure:
    #     null_count = df_transformed.filter(col("InvoiceDate").isNull()).count()
    # if null_count > 0:
    #     error_msg = f"{null_count} rows could not parse InvoiceDate."
    #     logger.error(error_msg)
    #     raise ValueError(error_msg)

    df_transformed = (
        df.withColumn("Quantity", col("Quantity").cast(IntegerType()))
        .withColumn("Price", col("Price").cast(DoubleType()))
        .withColumn(
            "InvoiceDate", parsed_timestamp
        )

    )
    return df_transformed


def remove_invalid_columns(df: DataFrame, col_name: str) -> DataFrame:
    return df.filter((col(col_name) > 0))


def capitalize_first_letter(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(
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


def is_return(df: DataFrame) -> DataFrame:
    return df.withColumn("IsReturn", when(col("Quantity") < 0, True).otherwise(False)) \



def is_UK_customer(df: DataFrame) -> DataFrame:
    return df.withColumn("IsUKCustomer", when(col("Country") == "United Kingdom", True).otherwise(False))


def revenue_by_month(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    df = (
        df.groupBy("InvoiceYear", "InvoiceMonth")
        .agg(sum("TotalPrice").alias("TotalRevenue"))
        .orderBy("InvoiceYear", "InvoiceMonth")
    )
    write_to_adls(df, path=path, mode=mode)


def top_n_products(df: DataFrame, n: int, path: str, mode: str = "overwrite") -> None:
    df = (
        df.groupBy("Description")
        .agg(sum("Quantity").alias("TotalSold"))
        .orderBy("TotalSold", ascending=False)
        .limit(n)
    )
    write_to_adls(df, path=path, mode=mode)


def sales_by_country(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    df = (
        df.groupBy("Country")
        .agg(sum("TotalPrice").alias("Revenue"))
        .orderBy("Revenue", ascending=False)
    )

    write_to_adls(df, path=path, mode=mode)


def revenue_per_customer(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    df = (
        df.groupBy("CustomerID")
        .agg(sum("TotalPrice").alias("CustomerRevenue"))
        .orderBy("CustomerRevenue", ascending=False)
    )
    write_to_adls(df, path=path, mode=mode)
