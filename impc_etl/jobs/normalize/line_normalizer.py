import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType
from impc_etl.jobs.normalize.experiment_normalizer import (
    re_map_europhenome_experiments,
    generate_metadata_group,
    generate_metadata,
    get_derived_parameters,
)
from impc_etl.jobs.normalize.specimen_normalizer import _generate_allelic_composition


def main(argv):
    line_parquet_path = argv[1]
    colony_parquet_path = argv[2]
    pipeline_parquet_path = argv[3]
    output_path = argv[4]
    spark = SparkSession.builder.getOrCreate()
    line_normalized_df = normalize_lines(
        spark, line_parquet_path, colony_parquet_path, pipeline_parquet_path
    )
    line_normalized_df.write.mode("overwrite").parquet(output_path)


def normalize_lines(
    spark_session: SparkSession,
    line_parquet_path,
    colony_parquet_path,
    pipeline_parquet_path,
):
    line_df = spark_session.read.parquet(line_parquet_path)
    colony_df = spark_session.read.parquet(colony_parquet_path)
    pipeline_df = spark_session.read.parquet(pipeline_parquet_path)

    line_colony_df = line_df.join(
        colony_df, (line_df["_colonyID"] == colony_df["colony_name"])
    )

    line_colony_df = line_colony_df.withColumn(
        "_project", line_colony_df["phenotyping_consortium"]
    )
    line_colony_df = re_map_europhenome_experiments(line_colony_df)
    line_colony_df = generate_metadata_group(line_colony_df, pipeline_df, type="line")
    line_colony_df = generate_metadata(line_colony_df, pipeline_df, type="line")
    line_colony_df = get_derived_parameters(
        spark_session, line_colony_df, pipeline_df, type="line"
    )
    line_colony_df = generate_allelic_composition(line_colony_df)

    line_columns = [
        col_name
        for col_name in line_df.columns
        if col_name not in ["_dataSource", "_project"]
    ] + ["allelicComposition", "metadata", "metadataGroup", "_project", "_dataSource"]
    line_df = line_colony_df.select(line_columns)
    return line_df


def generate_allelic_composition(line_df: DataFrame) -> DataFrame:
    generate_allelic_composition_udf = udf(_generate_allelic_composition, StringType())
    line_df = line_df.withColumn(
        "allelicComposition",
        generate_allelic_composition_udf(
            lit("homozygous"), "allele_symbol", "marker_symbol", lit(False), "_colonyID"
        ),
    )
    return line_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
