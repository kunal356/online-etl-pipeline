import datetime
from base_test import BaseTestClass
from utils.env_utils import get_logger
from etl_lib.steps import clean_df, add_features

logger = get_logger(__name__)


class TestSteps(BaseTestClass):
    def test_clean_df(self):
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
        self.assertEqual(result.asDict(), expected)
        logger.info("Clean Dataframe test passed successfully")

    def test_add_featrues(self):
        logger.info("**********Testing: Add features**************")
        data = [(101, "A1", "Red Apple", 10, 1.75,
                 "2025-02-22 08:26:00", 101, "UK",)]
        columns = ["Invoice", "StockCode", "Description", "Quantity",
                   "Price", "InvoiceDate", "CustomerID", "Country"]
        df = self.spark.createDataFrame(data, columns)
        result = add_features(df=df).collect()[0]
        expected = {
            "Invoice": 101,
            "StockCode": "A1",
            "Description": "Red Apple",
            "Quantity": 10,
            "Price": 1.75,
            "InvoiceDate": "2025-02-22 08:26:00",
            'InvoiceDay': 22,
            "InvoiceMonth": 2,
            "InvoiceTime": "08:26:00",
            "InvoiceYear": 2025,
            "CustomerID": 101,
            "Country": "UK",
            "TotalPrice": 17.5,
            "IsReturn": False,
            "IsUKCustomer": True
        }

        self.assertEqual(result.asDict(), expected)
        logger.info("Add features test passed successfully")
