"""
Utils package
"""
from typing import List, Dict
from collections import OrderedDict
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark import SparkContext
import re
from datetime import datetime
from impc_etl.config import Constants
from pyspark.sql.utils import AnalysisException


EPOCH = datetime.utcfromtimestamp(0)


def extract_tsv(
    spark_session: SparkSession, file_path: str, schema: StructType = None, header=True
) -> DataFrame:
    """

    :param header:
    :param spark_session:
    :param file_path:
    :param schema:
    :return:
    """
    return spark_session.read.csv(
        file_path, header=header, schema=schema, mode="DROPMALFORMED", sep="\t"
    )


def convert_to_dataframe(spark: SparkSession, dict_list: List[Dict]) -> DataFrame:
    return spark.createDataFrame([Row(**x) for x in dict_list], samplingRatio=1.0)


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
    return list({match[0] for match in re.findall(r"\'(([A-Z]|\d|_)*)\'", derivation)})


def unix_time_millis(dt):
    return (dt - EPOCH).total_seconds() * 1000.0


def truncate_specimen_id(specimen_id: str) -> str:
    specimen_id = str(specimen_id)
    if str(specimen_id).endswith("_MRC_Harwell"):
        return specimen_id[: specimen_id.rfind("_MRC_Harwell")]
    else:
        return specimen_id[: specimen_id.rfind("_")]


def truncate_colony_id(colony_id: str) -> str:
    if colony_id in Constants.EUROPHENOME_VALID_COLONIES or colony_id is None:
        return colony_id
    else:
        return colony_id[: colony_id.rfind("_")].strip()


def map_centre_id(centre_id: str):
    return Constants.CENTRE_ID_MAP[centre_id.lower()] if centre_id is not None else None


def map_project_id(centre_id: str):
    return (
        Constants.PROJECT_ID_MAP[centre_id.lower()] if centre_id is not None else None
    )


def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False
