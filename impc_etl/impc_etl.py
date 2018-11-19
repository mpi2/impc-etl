"""
IMPC ETL Pipeline
"""
import os
import sys
if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

if os.path.exists('shared.zip'):
    sys.path.insert(0, 'shared.zip')
else:
    sys.path.insert(0, './shared')

# pylint:disable=E0401
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark

from pyspark import SparkConf
from pyspark.sql import SparkSession


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
    conf = SparkConf().setAll([('spark.jars.packages', ['com.databricks:spark-xml_2.11:0.4.1'])])
    spark = SparkSession.builder.appName("IMPC_ETL_TEST").config(conf=conf).getOrCreate()
    experiment_file = "./impc_data/*experiment*"
    experiments_df = spark.read.format("com.databricks.spark.xml") \
        .options(rowTag="centre").load(experiment_file, )
    experiments_df.show()
    experiments_df.printSchema()


if __name__ == "__main__":
    main()
