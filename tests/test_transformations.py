import logging
import unittest
from pyspark.sql import SparkSession
from etl_lib.transformations import *
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger(__name__)


class TestTransformation(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession\
            .builder\
            .master("local[*]")\
            .appName("TestTransformation")\
            .getOrCreate()
        logger.info("\nSpark Session Started")

    def test_capitalize_first_letter(self):
        logger.info("\nTesting: capitalize_first_letter")
        df = self.spark.createDataFrame([(" hello world ",)], ["desc"])
        result_df = capitalize_first_letter(df, "desc")
        try:
            self.assertEqual(result_df.collect()[0]["desc"], "Hello world")
            logger.info("Test 1: Passed")
        except AssertionError as e:
            logger.error(f" Test 1 Failed with error: {e}")

    def test_cap_every_first_letter(self):
        logger.info("Testing: cap_every_first letter")
        df = self.spark.createDataFrame([("hello world",)], ["desc"])
        result = cap_every_first_letter(df, "desc").collect()[0]["desc"]
        try:
            self.assertEqual(result, "Hello World")
            logger.info("Test 2: Passed")
        except AssertionError as e:
            logger.error(f"Test 2 Failed with error: {e}")

    def test_calc_total_price(self):
        logger.info("Testing: calc_total_price")
        df = self.spark.createDataFrame([(2, 3.456)], ["Quantity", "Price"])
        result = calc_total_price(df).collect()[0]["TotalPrice"]
        try:
            self.assertEqual(result, 6.91)
            logger.info("Test 3: Passed")
        except AssertionError as e:
            logger.error(f"Test 3 Failed with error: {e}")

    def test_extract_date(self):
        logger.info("Testing: extract_date")
        df = self.spark.createDataFrame(
            [("2024-06-11 13:45:00",)], ["InvoiceDate"])
        df = df.withColumn("InvoiceDate", col("InvoiceDate").cast("timestamp"))
        result = extract_date(df).collect()[0]
        try:
            self.assertEqual(result["InvoiceYear"], 2024)
            self.assertEqual(result["InvoiceMonth"], 6)
            self.assertEqual(result["InvoiceDay"], 11)
            self.assertEqual(result["InvoiceTime"], "13:45:00")
            logger.info("Test 4: Passed")
        except AssertionError as e:
            logger.error(f"Test 4 Failed with error: {e}")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        logger.info("Spark session stopped")
