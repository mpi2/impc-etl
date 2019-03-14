"""
DCC loader module
    extract_experiment_files
    extract_specimen_files
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit, input_file_name, udf
from impc_etl import logger


def extract_dcc_xml_files(spark_session: SparkSession, xml_path: str, file_type: str) -> DataFrame:
    path = f"{xml_path}*/*{file_type}*"

    logger.info(f"loading DCC datasource from path '{path}'")

    experiment_df = spark_session.read.format("com.databricks.spark.xml") \
        .options(rowTag='centre', samplingRatio=1, nullValue='', mode='FAILFAST') \
        .load(path)

    experiment_df = experiment_df.withColumn('_sourceFile', lit(input_file_name()))
    data_source_extract = udf(lambda x: x.split('/')[-2], StringType())
    experiment_df = experiment_df.withColumn('_dataSource', data_source_extract('_sourceFile'))
    logger.info(f"finished load of DCC datasource from path '{path}'")
    return experiment_df
