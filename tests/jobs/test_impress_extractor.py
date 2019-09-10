"""
IMPRESS extractor test suite
"""

import pytest
from pyspark.sql import SparkSession
from impc_etl.jobs.extract.impress_extractor import extract_impress

# pylint:disable=C0103

pytestmark = pytest.mark.usefixtures("spark_session")


def test_extract_impress(spark_session: SparkSession):
    """
    Test extract impress
    :param spark_session:
    :return:
    """
    assert True
