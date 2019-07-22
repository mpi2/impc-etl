"""
IMPC ETL Pipeline
"""
import argparse
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

from impc_etl import config
from impc_etl import logger
from impc_etl.jobs.extract import dcc_extractor as dcc_e
from impc_etl.jobs.extract import imits_extractor as imits_e
from impc_etl.jobs.extract import impress_extractor as impress_e
from impc_etl.jobs.normalize import dcc_transformations as dcc_t
from impc_etl.shared.utils import file_exists
from impc_etl.jobs.load import stats_pipeline_loader as stats_loader

try:
    import findspark

    findspark.init()
    from pyspark import SparkConf
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import lower, col, udf, lit, explode
    from pyspark.sql.types import IntegerType, StringType
except ValueError:
    from pyspark import SparkConf
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import lower, col, udf, lit, explode
    from pyspark.sql.types import IntegerType, StringType


def main():
    """
    http://sandbox.mousephenotype.org/impress
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-d, --dcc', dest='dcc_path', nargs=1, help="DCC XML files path")
    parser.add_argument('-e, --env', dest='exec_env', nargs=1, help="Execution environment")
    conf = SparkConf().setAll([
        ('spark.jars.packages', config.SparkConfig.SPARK_JAR_PACKAGES),
        ('spark.sql.sources.partitionColumnTypeInference.enabled', 'false'),
        ('spark.driver.memory', '5g')
    ])
    spark = SparkSession.builder.appName("IMPC_ETL").config(conf=conf).getOrCreate()
    args = parser.parse_args()
    dcc_path = '../tests/data/' if args.dcc_path is None else args.dcc_path[0]
    exec_env = 'prod' if args.exec_env is None else args.exec_env[0]
    base_path = '' if exec_env is 'prod' else '../tests/'

    logger.info('IMITS data load started')
    imits_df = imits_e.extract_phenotyping_colonies(spark,
                                                    base_path + 'data/imits/imits-report.tsv')
    logger.info('IMITS data load finished')

    logger.info('Specimens data load started')
    if file_exists(spark.sparkContext, base_path + 'out/dcc_xml_spe_df.parquet'):
        logger.info('Found parquet file for specimen data')
        dcc_xml_spe_df = spark.read.parquet(base_path + 'out/dcc_xml_spe_df.parquet')
    else:
        logger.info('Not found parquet file for specimen data, loading from XML')
        dcc_xml_spe_df = dcc_e.extract_dcc_xml_files(spark, dcc_path, 'specimen')
        dcc_xml_spe_df.write.mode('overwrite').parquet(base_path + 'out/dcc_xml_spe_df.parquet')
    logger.info('Specimens data load finished')

    logger.info('Specimens cleaning started')
    mice_df = dcc_t.get_mice(dcc_xml_spe_df)
    mice_df = dcc_t.process_specimens(mice_df, imits_df)
    mice_df = dcc_t.add_mouse_life_stage_acc(mice_df)
    embryo_df = dcc_t.get_embryo(dcc_xml_spe_df)
    embryo_df = dcc_t.process_specimens(embryo_df, imits_df)
    embryo_df = dcc_t.add_embryo_life_stage_acc(embryo_df)
    logger.info('Specimens cleaning finished')

    logger.info('Experiments data load started')
    if file_exists(spark.sparkContext, base_path + 'out/dcc_xml_exp_df.parquet'):
        logger.info('Found parquet file for specimens data')
        dcc_xml_exp_df = spark.read.parquet(base_path + 'out/dcc_xml_exp_df.parquet')
    else:
        logger.info('Not found parquet file for experiments data, loading from XML')
        dcc_xml_exp_df = dcc_e.extract_dcc_xml_files(spark, dcc_path, 'experiment')
        dcc_xml_exp_df.write.mode('overwrite').parquet(base_path + 'out/dcc_xml_exp_df.parquet')
    logger.info('Experiments data load finished')

    logger.info('Impress data load started')
    if file_exists(spark.sparkContext, base_path + 'out/impress_df.parquet'):
        logger.info('Found parquet file for impress data')
        impress_df = spark.read.parquet(base_path + 'out/impress_df.parquet')
    else:
        logger.info('Not found parquet file for experiments data, loading from API')
        impress_api_url = 'http://api.mousephenotype.org/impress/'
        impress_pipeline_type = 'pipeline'
        impress_df = impress_e.extract_impress(spark, impress_api_url, impress_pipeline_type)
        impress_df.write.mode('overwrite').parquet(base_path + 'out/impress_df.parquet')
    logger.info('Impress data load finished')

    logger.info('Experiments cleaning started')
    experiment_df = dcc_t.get_experiments(dcc_xml_exp_df)
    experiment_df = dcc_t.process_experiments(experiment_df, mice_df, embryo_df, impress_df)

    logger.info('Experiments cleaning finished')

    if file_exists(spark.sparkContext, base_path + 'out/dcc_xml_exp_df_derivation.parquet'):
        logger.info('Found parquet file for derived parameters data')
        experiment_df = spark.read.parquet(base_path + 'out/dcc_xml_exp_df_derivation.parquet')
    else:
        logger.info('Not found parquet file for experiments data, calculating derived parameters')
        experiment_df = dcc_t.get_derived_parameters(spark, experiment_df, impress_df)
        experiment_df = experiment_df.withColumnRenamed('Phenotyping Consortium',
                                                        '_phenotypingConsortium')
        experiment_df.write.mode('overwrite').parquet(
            base_path + 'out/dcc_xml_exp_df_derivation.parquet')

    # line_experiments_df = dcc_t.get_lines(dcc_xml_exp_df)
    # line_experiments_df = dcc_t.process_lines(line_experiments_df)

    # allele2_df = imits_e.extract_alleles(spark, base_path + 'data/imits/allele2Entries.tsv')

    # logger.info('SPECIMEN LEVEL EXPERIMENTS SCHEMA')
    # experiment_df.printSchema()
    # logger.info('LINE LEVEL EXPERIMENTS SCHEMA')
    # line_experiments_df.printSchema()
    # logger.info('MOUSE SCHEMA')
    # mice_df.printSchema()
    # logger.info('EMBRYO SCHEMA')
    # embryo_df.printSchema()
    # logger.info('IMPRESS SCHEMA')
    # impress_df.printSchema()
    # logger.info('IMITS SCHEMA')
    # imits_df.printSchema()
    # logger.info('ALLELE2 SCHEMA')
    # allele2_df.printSchema()

    #stats_loader.load(experiment_df, line_experiments_df, mice_df,
                      #embryo_df, imits_df, impress_df, allele2_df)

    return


if __name__ == "__main__":
    main()
