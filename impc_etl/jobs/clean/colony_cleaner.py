import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, lit, concat
from pyspark.sql.types import StringType
from impc_etl.shared import utils
from impc_etl.config.constants import Constants


def main(argv):
    imits_colonies_parquet_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    colonies_df = spark.read.parquet(imits_colonies_parquet_path)
    specimen_clean_df = clean_colonies(colonies_df)
    specimen_clean_df.write.mode("overwrite").parquet(output_path)


def clean_colonies(colonies_df: DataFrame) -> DataFrame:
    """
    DCC colonies cleaner

    :param colonies_df: Spark DataFrame
    :param str colonies_df: colonies DataFrame with the raw colonies data
    :return: a clean specimen parquet file
    :rtype: DataFrame
    """
    colonies_df = colonies_df.transform(map_colonies_df_ids)
    # colonies_df = map_strain_names(colonies_df)
    colonies_df = colonies_df.transform(generate_genetic_background)
    return colonies_df


def map_colonies_df_ids(colonies_df: DataFrame) -> DataFrame:
    colonies_df = colonies_df.withColumn(
        "phenotyping_centre",
        udf(utils.map_centre_id, StringType())("phenotyping_centre"),
    )
    colonies_df = colonies_df.withColumn(
        "production_centre", udf(utils.map_centre_id, StringType())("production_centre")
    )
    colonies_df = colonies_df.withColumn(
        "phenotyping_consortium",
        udf(utils.map_project_id, StringType())("phenotyping_consortium"),
    )
    colonies_df = colonies_df.withColumn(
        "production_consortium",
        udf(utils.map_project_id, StringType())("production_consortium"),
    )
    return colonies_df


def map_strain_names(colonies_df: DataFrame) -> DataFrame:
    map_strain_name_udf = udf(map_strain_name, StringType())
    colonies_df = colonies_df.withColumn(
        "colony_background_strain", map_strain_name_udf("colony_background_strain")
    )
    return colonies_df


def generate_genetic_background(colonies_df: DataFrame) -> DataFrame:
    colonies_df = colonies_df.withColumn(
        "genetic_background", concat(lit("involves: "), col("colony_background_strain"))
    )
    return colonies_df


def map_strain_name(strain_name: str) -> str:
    if strain_name is None:
        return None

    if "_" in strain_name:
        intermediate_backgrounds = strain_name.split("_")
    elif ";" in strain_name:
        intermediate_backgrounds = strain_name.split(";")
    elif strain_name == "Balb/c.129S2":
        intermediate_backgrounds = "BALB/c;129S2/SvPas".split(";")
    elif strain_name in ["B6N.129S2.B6J", "B6J.129S2.B6N", "B6N.B6J.129S2"]:
        intermediate_backgrounds = "C57BL/6N;129S2/SvPas;C57BL/6J".split(";")
    elif strain_name == "B6J.B6N":
        intermediate_backgrounds = "C57BL/6J;C57BL/6N".split(";")
    else:
        intermediate_backgrounds = [strain_name]
    intermediate_backgrounds = [
        Constants.BACKGROUND_STRAIN_MAPPER[strain]
        if strain in Constants.BACKGROUND_STRAIN_MAPPER.keys()
        else strain
        for strain in intermediate_backgrounds
    ]

    return ";".join(intermediate_backgrounds)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
