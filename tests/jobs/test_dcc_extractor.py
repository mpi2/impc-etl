"""
IMPRESS extractor test suite
"""
from impc_etl.jobs.extract.dcc_extractor import *
import pytest
from impc_etl.shared.exceptions import *


def test_extract_dcc_xml_files(spark_session: SparkSession):
    """
    If the file_type is not supported should raise an UnsupportedFileTypeException
    If the provided path is empty return None

    """
    with pytest.raises(UnsupportedFileTypeError):
        extract_dcc_xml_files(spark_session, 'SOME XML PATH', 'NON EXISTENT FILE TYPE')
