"""
IMPC ETL Pipeline
"""
import argparse
import os
import sys

from impc_etl import config
from impc_etl import logger

try:
    import findspark

    findspark.init()
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lower, col, udf, lit, explode
    from pyspark.sql.types import IntegerType
except ValueError:
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lower, col, udf, lit, explode
    from pyspark.sql.types import IntegerType

from impc_etl.jobs.extraction import dcc_extractor as dcc_e
from impc_etl.jobs.extraction import imits_extractor as imits_e
from impc_etl.jobs.extraction import impress_extractor as impress_e
from impc_etl.jobs.transformation import dcc_transformations as dcc_t
from impc_etl.shared.utils import file_exists

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('impc_etl.zip'):
    sys.path.insert(0, './impc_etl.zip')
else:
    sys.path.insert(0, '.')


def main():
    """
    http://sandbox.mousephenotype.org/impress
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-d, --dcc', dest='dcc_path', nargs=1, help="DCC XML files path")
    conf = SparkConf().setAll([
        ('spark.jars.packages', config.SparkConfig.SPARK_JAR_PACKAGES),
        ('spark.sql.sources.partitionColumnTypeInference.enabled', 'false')
    ])
    spark = SparkSession.builder.appName("IMPC_ETL").config(conf=conf).getOrCreate()
    args = parser.parse_args()
    imits_df = imits_e.extract_phenotyping_colonies(spark, 'data/imits-report.tsv')
    dcc_path = None if args.dcc_path is None else args.dcc_path[0]

    logger.info('Specimens data load started')
    if file_exists(spark.sparkContext, 'out/dcc_xml_spe_df.parquet'):
        logger.info('Found parquet file for specimen data')
        dcc_xml_spe_df = spark.read.parquet('out/dcc_xml_spe_df.parquet')
    else:
        logger.info('Not found parquet file for specimen data, loading from XML')
        dcc_xml_spe_df = dcc_e.extract_dcc_xml_files(spark, dcc_path, 'specimen')
        dcc_xml_spe_df.write.mode('overwrite').parquet('out/dcc_xml_spe_df.parquet')
    logger.info('Specimens data load finished')

    logger.info('Specimens cleaning started')
    mice_df = dcc_t.get_mice(dcc_xml_spe_df)
    mice_df = dcc_t.process_specimens(mice_df, imits_df)
    embryo_df = dcc_t.get_embryo(dcc_xml_spe_df)
    embryo_df = dcc_t.process_specimens(embryo_df, imits_df)
    logger.info('Specimens cleaning finished')

    logger.info('Experiments data load started')
    if file_exists(spark.sparkContext, 'out/dcc_xml_exp_df.parquet'):
        logger.info('Found parquet file for specimens data')
        dcc_xml_exp_df = spark.read.parquet('out/dcc_xml_exp_df.parquet')
    else:
        logger.info('Not found parquet file for experiments data, loading from XML')
        dcc_xml_exp_df = dcc_e.extract_dcc_xml_files(spark, dcc_path, 'experiment')
        dcc_xml_exp_df.write.mode('overwrite').parquet('out/dcc_xml_exp_df.parquet')
    logger.info('Experiments data load finished')



    logger.info('Impress data load started')
    if file_exists(spark.sparkContext, 'out/impress_df.parquet'):
        logger.info('Found parquet file for impress data')
        impress_df = spark.read.parquet('out/impress_df.parquet')
    else:
        logger.info('Not found parquet file for experiments data, loading from API')
        impress_api_url = 'http://api.mousephenotype.org/impress/'
        impress_pipeline_type = 'pipeline'
        impress_df = impress_e.extract_impress(spark, impress_api_url, impress_pipeline_type)
        impress_df.write.mode('overwrite').parquet('out/impress_df.parquet')
    logger.info('Impress data load finished')

    logger.info('Experiments cleaning started')
    experiment_df = dcc_t.get_experiments(dcc_xml_exp_df)
    experiment_df = dcc_t.process_experiments(experiment_df, mice_df, embryo_df, impress_df)
    logger.info('Experiments cleaning finished')

    # if file_exists(spark.sparkContext, 'out/dcc_xml_exp_df_derivation.parquet') and False:
    #     logger.info('Found parquet file for derived parameters data')
    #     experiment_df = spark.read.parquet('out/dcc_xml_exp_df_derivation.parquet')
    # else:
    #     logger.info('Not found parquet file for experiments data, calculating derived parameters')
    #     experiment_df = dcc_t.get_derived_parameters(spark, experiment_df, impress_df)
    #     experiment_df.write.mode('overwrite').parquet('out/dcc_xml_exp_df_derivation.parquet')
    #
    #
    # logger.info('Derived experiments')
    # experiment_df.withColumn('result', explode('results')).where(experiment_df['_dataSource'] == 'impc').groupby(
    #     experiment_df['_procedureID']).count().sort('_procedureID').show(100)
    experiment_df.where(col('_experimentID') == '3647_7102').show(1, False)

    # logger.info('SHOW SCHEMAS')
    #
    # mice_df.printSchema()
    # embryo_df.printSchema()
    # logger.info('END SHOW SCHEMAS')
    # logger.info('DROP NULL COLONY')
    #

    # experiment_df.where((col('Phenotyping Consortium').isNull()) & (col('_isBaseline') != True)).drop('seriesParameter').drop('procedureMetadata').drop('simpleParameter').show(100, False)

    # experiment_df.where(experiment_df['_experimentID'].isin(
    #     ['6336_32732', '6336_32730', '6336_32733', '6336_32726', '6336_32727', '6336_32729',
    #      '6336_32734', '6416_36309', '6416_36310', '6416_36312', '6416_36305', '6416_36306',
    #      '6433_38197', '6433_38198', '6993_46319', '8505_72300', '8505_72301', '8505_72296',
    #      '8505_73673', '8505_73674', '8505_72297', '8505_72298', '8505_72299', '8615_74515',
    #      '8615_74516', '8615_74517', '8615_74511', '8615_74512', '8810_77979', '8858_79473',
    #      '8858_79474', '9641_86165', '9641_86166', '9641_86167', '9849_89996', '9849_89997',
    #      '16915_1420', '16915_1421', '16915_1422', '16915_1423', '16915_1424', '16915_1425',
    #      '16915_1426', '16915_1427', '16915_1428', '16915_1429', '16915_1430', '16915_1431',
    #      '16915_1432', '16915_1433', '16915_1434', '16915_1435', '16915_1436', '16915_1437',
    #      '16915_1438', '16915_1439'])).drop('procedureMetadata').show(100, False)
    # logger.info('SHOW GROUP BY')
    # experiment_df.where(experiment_df['_dataSource'] == 'impc').groupby(
    #     experiment_df['_procedureID']).count().sort('_procedureID').show(100)
    # experiment_df.where(experiment_df['_dataSource'] == 'EuroPhenome').groupby(
    #     experiment_df['_procedureID']).count().sort('_procedureID').show(100)
    # experiment_df.where(experiment_df['_dataSource'] == 'MGP').groupby(
    #     experiment_df['_procedureID']).count().sort('_procedureID').show(100)
    # experiment_df.where(experiment_df['_dataSource'] == '3i').groupby(
    #     experiment_df['_procedureID']).count().sort('_procedureID').show(100)
    return


def show_info(experiment_df, mice_df, embryo_df):
    logger.info('SHOW SCHEMAS')

    mice_df.printSchema()
    embryo_df.printSchema()
    logger.info('END SHOW SCHEMAS')
    logger.info('DROP NULL COLONY')

    logger.info('SHOW GROUP BY')

    experiment_df.where(experiment_df['_dataSource'] == 'impc').groupby(
        experiment_df['procedure._procedureID']).count().show(100)
    experiment_df.where(experiment_df['_dataSource'] == 'EuroPhenome').groupby(
        experiment_df['procedure._procedureID']).count().show(100)
    experiment_df.where(experiment_df['_dataSource'] == 'MGP').groupby(
        experiment_df['procedure._procedureID']).count().show(100)
    experiment_df.where(experiment_df['_dataSource'] == '3i').groupby(
        experiment_df['procedure._procedureID']).count().show(100)
    return


if __name__ == "__main__":
    main()
