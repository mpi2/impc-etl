import sys
from pyspark.sql import SparkSession
from impc_etl.jobs.normalize.dcc_transformations.specimens import *


def normalize_specimens(spark_session: SparkSession, specimen_parquet_path: str,
                        colonies_parquet_path: str) -> DataFrame:
    """
    DCC specimen normalizer

    :param colonies_parquet_path:  path to a parquet file with colonies clean data
    :param SparkSession spark_session: PySpark session object
    :param str specimen_parquet_path: path to a parquet file with specimen clean data
    :return: a normalized specimen parquet file
    :rtype: DataFrame
    """
    specimen_df = spark_session.read.parquet(specimen_parquet_path)
    colonies_df = spark_session.read.parquet(colonies_parquet_path)
    colonies_df = colonies_df.transform(map_colonies_df_ids)

    specimen_df = specimen_df.alias("specimen")
    colonies_df = colonies_df.alias("colony")

    specimen_df = specimen_df.join(colonies_df,
                                       (specimen_df['_colonyID'] == colonies_df[
                                           'colony_name']) & (
                                               lower(specimen_df['_centreID']) ==
                                               lower(colonies_df['phenotyping_centre'])),
                                       'left_outer')
    specimen_df = specimen_df \
        .transform(override_europhenome_datasource) \
        .transform(override_3i_specimen_project)

    specimen_df = specimen_df.select('specimen.*', 'colony.phenotyping_consortium')
    return specimen_df


def map_colonies_df_ids(colonies_df: DataFrame) -> DataFrame:
    colonies_df = colonies_df.withColumn('phenotyping_centre',
                                         udf(map_centre_ids, StringType())(
                                             'phenotyping_centre'))
    colonies_df = colonies_df.withColumn('production_centre',
                                         udf(map_centre_ids, StringType())(
                                             'production_centre'))
    colonies_df = colonies_df.withColumn('phenotyping_consortium',
                                         udf(map_project_ids, StringType())(
                                             'phenotyping_consortium'))
    colonies_df = colonies_df.withColumn('production_consortium',
                                         udf(map_project_ids, StringType())(
                                             'production_consortium'))
    return colonies_df


def main(argv):
    specimen_parquet_path = argv[1]
    colonies_parquet_path = argv[2]
    output_path = argv[3]
    spark = SparkSession.builder.getOrCreate()
    specimen_normalized_df = normalize_specimens(spark, specimen_parquet_path, colonies_parquet_path)
    specimen_normalized_df.write.mode('overwrite').parquet(output_path)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
