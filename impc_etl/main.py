"""
IMPC ETL Pipeline
"""
import os
import sys
if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('impc_etl.zip'):
    sys.path.insert(0, './impc_etl.zip')
else:
    sys.path.insert(0, '.')

# pylint:disable=E0401,C0412
# try:
#     from pyspark import SparkConf
#     from pyspark.sql import SparkSession
# except ModuleNotFoundError:
import findspark
findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, asc


# pylint:disable=C0413
from impc_etl.jobs.extraction.impress_extractor import extract_impress
from impc_etl.jobs.load.prisma_graphql import load


def impc_pipeline(spark_context):
    """
    :param spark_context:
    :return:
    """
    return spark_context


def main():
    """
    :return:
    """
    conf = SparkConf().setAll([('spark.jars.packages', 'com.databricks:spark-xml_2.11:0.4.1')])
    spark = SparkSession.builder.appName("IMPC_ETL").config(conf=conf).getOrCreate()
    if os.path.exists('../tests/data/impress_parquet'):
        impress_df = spark.read.parquet('../tests/data/impress_parquet')
    else:
        impress_api_url = 'http://sandbox.mousephenotype.org/impress/'
        impress_pipeline_type = 'pipeline'
        impress_df = extract_impress(spark, impress_api_url, impress_pipeline_type)
    load(None, None, None, None, impress_df)


if __name__ == "__main__":
    main()
