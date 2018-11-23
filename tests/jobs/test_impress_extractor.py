"""
IMPRESS extractor test suite
"""

import pytest
from pyspark.sql import SparkSession
from impc_etl.jobs.extraction.impress_extractor import extract_impress
# pylint:disable=C0103

pytestmark = pytest.mark.usefixtures("spark_session")


def test_extract_impress(spark_session: SparkSession):
    """
    Test extract impress
    :param spark_session:
    :return:
    """
    impress_api_url = 'http://sandbox.mousephenotype.org/impress/'
    impress_pipeline_type = 'pipeline'
    impress_df = extract_impress(spark_session, impress_api_url, impress_pipeline_type)
    impress_df.printSchema()
