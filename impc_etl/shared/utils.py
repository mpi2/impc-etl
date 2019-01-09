"""
Utils package
"""
from collections import OrderedDict
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType
from pyspark.sql.functions import col


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
                                  sep='\t')


def convert_to_row(dictionary: dict) -> Row:
    """

    :param dictionary:
    :return:
    """
    return Row(**OrderedDict(sorted(dictionary.items())))


def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(col(prefix + elem.name).alias(elem.name))
    return result
