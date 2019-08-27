from impc_etl.shared.transformations.commons import *
from pyspark.sql.functions import concat, col


def map_strain_names(colonies_df: DataFrame) -> DataFrame:
    map_strain_name_udf = udf(_map_strain_name, StringType())
    colonies_df = colonies_df.withColumn(
        "colony_background_strain", map_strain_name_udf("colony_background_strain")
    )
    return colonies_df


def generate_genetic_background(colonies_df: DataFrame) -> DataFrame:
    colonies_df = colonies_df.withColumn(
        "genetic_background", concat(lit("involves: "), col("colony_background_strain"))
    )
    return colonies_df


def map_colonies_df_ids(colonies_df: DataFrame) -> DataFrame:
    colonies_df = colonies_df.withColumn(
        "phenotyping_centre", udf(map_centre_ids, StringType())("phenotyping_centre")
    )
    colonies_df = colonies_df.withColumn(
        "production_centre", udf(map_centre_ids, StringType())("production_centre")
    )
    colonies_df = colonies_df.withColumn(
        "phenotyping_consortium",
        udf(map_project_ids, StringType())("phenotyping_consortium"),
    )
    colonies_df = colonies_df.withColumn(
        "production_consortium",
        udf(map_project_ids, StringType())("production_consortium"),
    )
    return colonies_df


def _map_strain_name(strain_name: str) -> str:
    if strain_name is None:
        return None

    intermediate_backgrounds = []

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

    return " * ".join(intermediate_backgrounds)
