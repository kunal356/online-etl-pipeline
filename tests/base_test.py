import unittest
from utils.env_utils import get_spark_session, stop_spark_session, get_logger

logger = get_logger(__name__)


class BaseTestClass(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark_session()
        logger.info("Spark Session started for tests.")

    @classmethod
    def tearDownClass(cls):
        stop_spark_session()
        logger.info("Spark session stopped")
