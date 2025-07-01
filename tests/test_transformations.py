from base_test import BaseTestClass
from etl_lib.transformations import *
from pyspark.sql.functions import col
from utils.env_utils import get_spark_session, get_logger


logger = get_logger(__name__)


class TestTransformation(BaseTestClass):

    def test_capitalize_first_letter(self):
        logger.info("\nTesting: capitalize_first_letter")
        df = self.spark.createDataFrame([(" hello world ",)], ["desc"])
        result_df = capitalize_first_letter(df, "desc")
        self.assertEqual(result_df.collect()[0]["desc"], "Hello world")
        logger.info("Test 1: Passed")

    def test_cap_every_first_letter(self):
        logger.info("Testing: cap_every_first letter")
        df = self.spark.createDataFrame([("hello world",)], ["desc"])
        result = cap_every_first_letter(df, "desc").collect()[0]["desc"]
        self.assertEqual(result, "Hello World")
        logger.info("Test 2: Passed")

    def test_calc_total_price(self):
        logger.info("Testing: calc_total_price")
        df = self.spark.createDataFrame([(2, 3.456)], ["Quantity", "Price"])
        result = calc_total_price(df).collect()[0]["TotalPrice"]
        self.assertEqual(result, 6.91)
        logger.info("Test 3: Passed")

    def test_extract_date(self):
        logger.info("Testing: extract_date")
        df = self.spark.createDataFrame(
            [("2024-06-11 13:45:00",)], ["InvoiceDate"])
        df = df.withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))
        result = extract_date(df).collect()[0]
        self.assertEqual(result["InvoiceYear"], 2024)
        self.assertEqual(result["InvoiceMonth"], 6)
        self.assertEqual(result["InvoiceDay"], 11)
        self.assertEqual(result["InvoiceTime"], "13:45:00")
        logger.info("Test 4: Passed")

    def test_remove_invalid_columns(self):
        logger.info("Testing: Remove Invalid Columns")
        df = self.spark.createDataFrame(
            [(1,), (-2,), (100,), (0,), (1300,)], ["Price"])
        result_df = remove_invalid_columns(df, "Price")
        result = [row["Price"] for row in result_df.collect()]
        self.assertEqual(result, [1, 100, 1300])
        logger.info("Test 5: Passed")

    def test_isReturn(self):
        logger.info("Testing: IsReturn")
        df = self.spark.createDataFrame(
            [(0,), (100,), (-1,), (10,), (1,)], ["Quantity"])
        result_df = is_return(df)
        result = [row["IsReturn"] for row in result_df.collect()]
        self.assertEqual(result, [False, False, True, False, False])
        logger.info("Test 6 Passed")

    def test_isUKCustomer(self):
        logger.info("Testing: isUKCustomer")
        df = self.spark.createDataFrame(
            [("United Kingdom",), ("United States",), ("Germany",), ("Australia",), ("United Kingdom",)], ["Country"])
        result_df = is_UK_customer(df)
        result = [row["IsUKCustomer"] for row in result_df.collect()]

        self.assertEqual(result, [True, False, False, False, True])
        logger.info("Test 7 Passed")
