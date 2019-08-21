import sys
from pyspark.sql import SparkSession
from impc_etl.shared.transformations.colonies import *


def clean_colonies(spark_session: SparkSession, colonies_parquet_path: str) -> DataFrame:
    """
    DCC colonies cleaner

    :param SparkSession spark_session: PySpark session object
    :param str colonies_parquet_path: path to a parquet file with specimen raw data
    :return: a clean specimen parquet file
    :rtype: DataFrame
    """
    colonies_df = spark_session.read.parquet(colonies_parquet_path)
    colonies_df = colonies_df\
        .transform(map_colonies_df_ids)\
        .transform(generate_genetic_background)
    return colonies_df


def main(argv):
    input_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    specimen_clean_df = clean_colonies(spark, input_path)
    specimen_clean_df.write.mode('overwrite').parquet(output_path)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
