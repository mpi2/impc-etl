import sys
from pyspark.sql import SparkSession
from impc_etl.jobs.normalize.dcc_transformations.specimens import *


def clean_specimens(spark_session: SparkSession, specimen_parquet_path: str) -> DataFrame:
    """
    DCC specimen cleaner

    :param SparkSession spark_session: PySpark session object
    :param str specimen_parquet_path: path to a parquet file with specimen raw data
    :return: a clean specimen parquet file
    :rtype: DataFrame
    """
    specimen_df = spark_session.read.parquet(specimen_parquet_path)
    specimen_df = specimen_df.transform(map_centre_id) \
        .transform(map_project_id) \
        .transform(map_production_centre_id) \
        .transform(map_phenotyping_centre_id) \
        .transform(standarize_europhenome_specimen_ids) \
        .transform(standarize_europhenome_colony_ids) \
        .transform(override_3i_specimen_data)
    return specimen_df


def main(argv):
    input_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    specimen_clean_df = clean_specimens(spark, input_path)
    specimen_clean_df.write.mode('overwrite').parquet(output_path)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
