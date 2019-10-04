import sys
from pyspark.sql import SparkSession
from impc_etl.shared.transformations.colonies import *


def clean_colonies(colonies_df: DataFrame) -> DataFrame:
    """
    DCC colonies cleaner

    :param europhenome_colonies_df:
    :param colonies_df: Spark DataFrame
    :param str colonies_df: colonies DataFrame with the raw colonies data
    :return: a clean specimen parquet file
    :rtype: DataFrame
    """
    colonies_df = colonies_df.transform(map_colonies_df_ids)
    colonies_df = map_strain_names(colonies_df)
    colonies_df = colonies_df.transform(generate_genetic_background)
    return colonies_df


def main(argv):
    imits_colonies_parquet_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    colonies_df = spark.read.parquet(imits_colonies_parquet_path)
    specimen_clean_df = clean_colonies(colonies_df)
    specimen_clean_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
