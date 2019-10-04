import sys
from pyspark.sql import SparkSession, DataFrame
from impc_etl.shared.transformations.specimens import *


def clean_specimens(specimen_df: DataFrame) -> DataFrame:
    """
    DCC specimen cleaner
    :return: a clean specimen parquet file
    :rtype: DataFrame
    """
    specimen_df = (
        specimen_df.transform(map_centre_id)
        .transform(map_project_id)
        .transform(map_production_centre_id)
        .transform(map_phenotyping_centre_id)
        .transform(standarize_europhenome_specimen_ids)
        .transform(standarize_europhenome_colony_ids)
        .transform(standarize_strain_ids)
        .transform(override_3i_specimen_data)
    )
    return specimen_df


def main(argv):
    input_path = argv[1]
    output_path = argv[2]
    spark_session = SparkSession.builder.getOrCreate()
    specimen_df = spark_session.read.parquet(input_path)
    specimen_clean_df = clean_specimens(specimen_df)
    specimen_clean_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
