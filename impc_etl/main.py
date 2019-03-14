"""
IMPC ETL Pipeline
"""
import os
import sys
import argparse
from impc_etl import config
from impc_etl import logger

try:
    import findspark

    findspark.init()
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
except ValueError:
    from pyspark import SparkConf
    from pyspark.sql import SparkSession

from impc_etl.jobs.extraction import dcc_extractor as dcc_e
from impc_etl.jobs.transformation import dcc_transformations as dcc_t

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
    dcc_path = None if args.dcc_path is None else args.dcc_path[0]
    dcc_xml_df = dcc_e.extract_dcc_xml_files(spark, dcc_path, 'experiment')
    dcc_xml_df.printSchema()
    dcc_specimen_df = dcc_e.extract_dcc_xml_files(spark, dcc_path, 'specimen')
    experiment_df = dcc_t.get_experiments(dcc_xml_df)
    experiment_df = experiment_df\
        .transform(dcc_t.drop_skipped_procedures)\
        .transform(dcc_t.standarize_europhenome_experiments)\
        .transform(dcc_t.drop_skipped_experiments)\
        .transform(dcc_t.standarize_3i_experiments)\
        .transform(dcc_t.drop_null_centre_id)\
        .transform(dcc_t.drop_null_data_source)\
        .transform(dcc_t.drop_null_date_of_experiment)\
        .transform(dcc_t.drop_null_pipeline)\
        .transform(dcc_t.drop_null_project)\
        .transform(dcc_t.drop_null_specimen_id)
    mice_df = dcc_t.get_mice(dcc_specimen_df)
    embryo_df = dcc_t.get_embryo(dcc_specimen_df)
    mice_df.printSchema()
    embryo_df.printSchema()
    # experiment_df = dcc_t.drop_null_colony_id(experiment_df, mice_df, embryo_df)
    #
    # experiment_df.show()
    # experiment_df.where(experiment_df['_dataSource'] == 'impc').groupby(
    #     experiment_df['procedure._procedureID']).count().show(100)
    # experiment_df.where(experiment_df['_dataSource'] == 'EuroPhenome').groupby(
    #     experiment_df['procedure._procedureID']).count().show(100)
    # experiment_df.where(experiment_df['_dataSource'] == 'MGP').groupby(
    #     experiment_df['procedure._procedureID']).count().show(100)
    # experiment_df.where(experiment_df['_dataSource'] == '3i').groupby(
    #     experiment_df['procedure._procedureID']).count().show(100)

    # dcc_specimen_df = dcc_extractor.extract_dcc_files(spark, dcc_path, 'specimen')
    # mice_df = dcc_transformations.get_mice(dcc_specimen_df)
    # mice_df.show()
    return


if __name__ == "__main__":
    main()
