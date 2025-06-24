from etl_lib.transformations import *
from pyspark.sql import DataFrame


def clean_df(df: DataFrame) -> DataFrame:
    df = df.dropna().dropDuplicates()
    df = clean_and_cast_columns(df)
    df = remove_invalid_columns(df=df, col_name="Price")
    df = remove_invalid_columns(df=df, col_name="Quantity")
    df = cap_every_first_letter(df=df, col_name="Country")
    df = capitalize_first_letter(df=df, col_name="Description")
    df = rename_column(df=df, curr_col_name="Customer ID",
                       new_col_name="CustomerID")
    return df


def add_features(df: DataFrame) -> DataFrame:
    df = calc_total_price(df)
    df = is_return(df)
    df = is_UK_customer(df)
    df = extract_date(df)
    return df


def comupte_gold_transformation(df: DataFrame, base_path: str, n: int = 10):
    tasks = [("revenue_by_month", revenue_by_month),
             ("top_n_products", lambda df,
              path: top_n_products(df=df, n=n, path=path)),
             ("sales_by_country", sales_by_country),
             ("revenue_per_customer", revenue_per_customer)
             ]

    for folder, func in tasks:
        path = f"{base_path}/{folder}"
        func(df=df, path=path)
