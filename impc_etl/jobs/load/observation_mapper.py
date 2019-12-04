import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    concat,
    col,
    when,
    lit,
    explode,
    regexp_extract,
    lower,
    regexp_replace,
)
from pyspark.sql.types import IntegerType


def map_columns():
    return


def add_impress_info():
    return


def add_observation_type():
    return


def format_columns():
    return


def map_experiments_to_observations(
    experiment_df: DataFrame,
    mouse_df: DataFrame,
    allele_df: DataFrame,
    colony_df: DataFrame,
    pipeline_df: DataFrame,
    strain_df: DataFrame,
):
    experiment_df = experiment_df.alias("experiment")
    colony_df = colony_df.alias("colony")
    mouse_df = mouse_df.alias("specimen")
    allele_df = allele_df.alias("allele")
    strain_df = strain_df.alias("strain")

    mouse_df = mouse_df.join(
        colony_df,
        mouse_df["specimen._colonyID"] == colony_df["colony.colony_name"],
        "left_outer",
    )
    mice_experiments_df: DataFrame = experiment_df.join(
        mouse_df,
        (experiment_df["experiment._centreID"] == mouse_df["specimen._centreID"])
        & (experiment_df["experiment.specimenID"] == mouse_df["specimen._specimenID"]),
        "left_outer",
    )
    mice_experiments_df = mice_experiments_df.join(
        allele_df,
        mice_experiments_df["colony.allele_symbol"] == allele_df["allele.alleleSymbol"],
        "left_outer",
    )

    mice_experiments_df_exp = mice_experiments_df.where(
        (lower(col("specimen._colonyID")) != "baseline")
        & (col("specimen._isBaseline") != True)
    ).join(
        strain_df,
        col("colony.colony_background_strain") == col("strain.strainName"),
        "left_outer",
    )

    mice_experiments_df_baseline = mice_experiments_df.where(
        (lower(col("specimen._colonyID")) == "baseline")
        | (col("specimen._isBaseline") == True)
    ).join(
        strain_df,
        when(
            concat(lit("MGI:"), col("specimen._strainID")) == col("strain.mgiStrainID"),
            concat(lit("MGI:"), col("specimen._strainID")) == col("strain.mgiStrainID"),
        ).otherwise(col("specimen._strainID") == col("strain.strainName")),
        "left_outer",
    )

    mice_experiments_df = mice_experiments_df_baseline.unionAll(mice_experiments_df_exp)

    mice_experiments_df = mice_experiments_df.drop(
        "ontologyParameter",
        "procedureMetadata",
        "seriesMediaParameter",
        "seriesParameter",
    )
    return


def main(argv):
    experiment_parquet_path = argv[1]
    mouse_parquet_path = argv[2]
    allele_parquet_path = argv[3]
    colony_parquet_path = argv[4]
    pipeline_parquet_path = argv[5]
    strain_parquet_path = argv[6]
    output_path = argv[7]
    spark = SparkSession.builder.getOrCreate()
    experiment_df = spark.read.parquet(experiment_parquet_path)
    mouse_df = spark.read.parquet(mouse_parquet_path)
    allele_df = spark.read.parquet(allele_parquet_path)
    colony_df = spark.read.parquet(colony_parquet_path)
    pipeline_df = spark.read.parquet(pipeline_parquet_path)
    strain_df = spark.read.parquet(strain_parquet_path)

    observations_df = map(
        experiment_df, mouse_df, allele_df, colony_df, pipeline_df, strain_df
    )
    observations_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
