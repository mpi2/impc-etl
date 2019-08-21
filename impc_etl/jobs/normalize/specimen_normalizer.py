import sys
from pyspark.sql import SparkSession
from impc_etl.shared.transformations.specimens import *


def normalize_specimens(spark_session: SparkSession, specimen_parquet_path: str,
                        colonies_parquet_path: str, entity_type: str) -> DataFrame:
    """
    DCC specimen normalizer

    :param entity_type:
    :param colonies_parquet_path:  path to a parquet file with colonies clean data
    :param SparkSession spark_session: PySpark session object
    :param str specimen_parquet_path: path to a parquet file with specimen clean data
    :return: a normalized specimen parquet file
    :rtype: DataFrame
    """
    specimen_df = spark_session.read.parquet(specimen_parquet_path)
    colonies_df = spark_session.read.parquet(colonies_parquet_path)
    specimen_df = specimen_df.alias('specimen')
    colonies_df = colonies_df.alias('colony')

    specimen_df = specimen_df.join(colonies_df,
                                   (specimen_df['_colonyID'] == colonies_df[
                                       'colony_name']) & (
                                           lower(specimen_df['_centreID']) ==
                                           lower(colonies_df['phenotyping_centre'])),
                                   'left_outer')
    specimen_df = specimen_df \
        .transform(generate_allelic_composition) \
        .transform(override_europhenome_datasource) \
        .transform(override_3i_specimen_project)

    specimen_df = specimen_df.select('specimen.*',
                                     'allelic_composition',
                                     'colony.phenotyping_consortium')
    if entity_type == 'embryo':
        specimen_df = specimen_df.transform(add_embryo_life_stage_acc)
    if entity_type == 'mouse':
        specimen_df = specimen_df.transform(add_mouse_life_stage_acc)
    return specimen_df


def main(argv):
    specimen_parquet_path = argv[1]
    colonies_parquet_path = argv[2]
    entity_type = argv[3]
    output_path = argv[4]
    spark = SparkSession.builder.getOrCreate()
    specimen_normalized_df = normalize_specimens(spark, specimen_parquet_path,
                                                 colonies_parquet_path,
                                                 entity_type)
    specimen_normalized_df.write.mode('overwrite').parquet(output_path)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
