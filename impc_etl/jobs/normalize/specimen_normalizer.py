import sys
from pyspark.sql import SparkSession
from impc_etl.shared.transformations.specimens import *


def normalize_specimens(
    specimen_df: DataFrame, colonies_df: DataFrame, entity_type: str
) -> DataFrame:
    """
    DCC specimen normalizer

    :param colonies_df:
    :param specimen_df:
    :param entity_type:
    :return: a normalized specimen parquet file
    :rtype: DataFrame
    """
    specimen_df = specimen_df.alias("specimen")
    colonies_df = colonies_df.alias("colony")

    specimen_df = specimen_df.join(
        colonies_df,
        (specimen_df["_colonyID"] == colonies_df["colony_name"]),
        "left_outer",
    )

    specimen_df = (
        specimen_df.transform(generate_allelic_composition)
        .transform(override_europhenome_datasource)
        .transform(override_3i_specimen_project)
    )

    specimen_df = specimen_df.select(
        "specimen.*", "allelicComposition", "colony.phenotyping_consortium"
    )
    if entity_type == "embryo":
        specimen_df = specimen_df.transform(add_embryo_life_stage_acc)
    if entity_type == "mouse":
        specimen_df = specimen_df.transform(add_mouse_life_stage_acc)
    return specimen_df


def main(argv):
    specimen_parquet_path = argv[1]
    colonies_parquet_path = argv[2]
    entity_type = argv[3]
    output_path = argv[4]
    spark = SparkSession.builder.getOrCreate()
    specimen_df = spark.read.parquet(specimen_parquet_path)
    colonies_df = spark.read.parquet(colonies_parquet_path)

    specimen_normalized_df = normalize_specimens(specimen_df, colonies_df, entity_type)
    specimen_normalized_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
