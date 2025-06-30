import unittest
from unittest.mock import patch
import logging
from pyspark.sql import DataFrame
from utils.env_utils import get_spark_session
from etl_lib.io import read_from_adls
from etl_lib.steps import clean_df
import datetime


logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:%(name)s: %(message)s')

logger = logging.getLogger(__name__)


class TestSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session()
        logger.info("Spark Session Started")

    def test_clean_df(self) -> DataFrame:
        logger.info("**********Testing: Clean df**************")
        data = [(101, "A1", "Red Apple", 10, 1.75,
                 "2025-02-22 08:26:00", 101, "UK",)]
        columns = ["Invoice", "StockCode", "Description", "Quantity",
                   "Price", "InvoiceDate", "CustomerID", "Country"]
        df = self.spark.createDataFrame(data, columns)
        result = clean_df(df=df).collect()[0]
        expected = {
            "Invoice": 101,
            "StockCode": "A1",
            "Description": "Red apple",
            "Quantity": 10,
            "Price": 1.75,
            "InvoiceDate": datetime.datetime(2025, 2, 22, 8, 26),
            "CustomerID": 101,
            "Country": "Uk"
        }
        try:
            self.assertEqual(result.asDict(), expected)
            logger.info("Clean Dataframe test passed successfully")
        except AssertionError as e:
            logger.error(f"Clean Dataframe test failed with error: {e}")
        return df

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        logger.info(f"Spark session in stopped.")
