"""
Utils package
"""
from collections import OrderedDict
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType


def extract_tsv(spark_session: SparkSession,
                file_path: str,
                schema: StructType = None) -> DataFrame:
    """

    :param spark_session:
    :param file_path:
    :param schema:
    :return:
    """
    return spark_session.read.csv(file_path,
                                  header=True,
                                  schema=schema,
                                  mode='DROPMALFORMED',
                                  sep='\t').load(file_path)


def convert_to_row(dictionary: dict) -> Row:
    """

    :param d:
    :return:
    """
    return Row(**OrderedDict(sorted(dictionary.items())))
