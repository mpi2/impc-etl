"""
Utils package
"""
from collections import OrderedDict
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType, SparkContext
from pyspark.sql.functions import col
import re
from datetime import datetime


EPOCH = datetime.utcfromtimestamp(0)


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


def file_exists(sc: SparkContext, path: str) -> bool:
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))


def pheno_dcc_derivator(sc: SparkContext, column):
    from pyspark.sql.column import Column, _to_java_column, _to_seq
    jc = sc._jvm.org.mousephenotype.dcc.derived.parameters.SparkDerivator
    return Column(jc(_to_seq(sc, [column], _to_java_column)))


def extract_parameters_from_derivation(derivation: str):
    return list({match[0] for match in re.findall(r"\'(([A-Z]|\d|\_)*)\'", derivation)})


def unix_time_millis(dt):
    return (dt - EPOCH).total_seconds() * 1000.0
