from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
import sys

from pyspark.sql.functions import (
    explode_outer,
    col,
    lit,
    split,
    collect_set,
    when,
    udf,
    expr,
)
from pyspark.sql.types import StructType, StructField, StringType, Row

from impc_etl.config import Constants
from impc_etl.shared.utils import convert_to_row

ONTOLOGY_STATS_MAP = {
    "mp_term_name": "term",
    "top_level_mp_term_id": "top_level_ids",
    "top_level_mp_term_name": "top_level_terms",
    "intermediate_mp_term_id": "intermediate_ids",
    "intermediate_mp_term_name": "intermediate_terms",
}

PIPELINE_STATS_MAP = {
    "mp_term_id_options": "mp_id",
    "mp_term_name_options": "mp_term",
    "parameter_stable_key": "parameter_stable_key",
    "pipeline_stable_key": "pipeline_stable_key",
    "procedure_stable_id": "procedure_stable_id",
    "procedure_stable_key": "procedure_stable_key",
}


OBSERVATIONS_STATS_MAP = {
    "genetic_background": "genetic_background",
    "production_center": "production_center",
    "project_name": "project_name",
    "project_fullname": "project_name",
    "strain_name": "strain_name",
    "life_stage_name": "life_stage_name",
    "resource_name": "datasource_name",
    "resource_fullname": "datasource_name",
    "life_stage_acc": "life_stage_acc",
}


ALLELE_STATS_MAP = {"allele_name": "allele_name"}

STATS_RESULTS_COLUMNS = [
    "additional_information",
    "allele_accession_id",
    "allele_name",
    "allele_symbol",
    "batch_significant",
    "both_mutant_count",
    "both_mutant_diversity_in_response",
    "both_mutant_mean",
    "both_mutant_sd",
    "both_mutant_unique_n",
    "classification_tag",
    "colony_id",
    "data_type",
    "effect_size",
    "female_control_count",
    "female_control_diversity_in_response",
    "female_control_mean",
    "female_control_sd",
    "female_control_unique_n",
    "female_ko_effect_p_value",
    "female_ko_effect_stderr_estimate",
    "female_ko_parameter_estimate",
    "female_mutant_count",
    "female_mutant_diversity_in_response",
    "female_mutant_mean",
    "female_mutant_sd",
    "female_mutant_unique_n",
    "female_percentage_change",
    "female_pvalue_low_normal_vs_high",
    "female_pvalue_low_vs_normal_high",
    "genotype_effect_p_value",
    "genotype_effect_parameter_estimate",
    "genotype_effect_size_low_normal_vs_high",
    "genotype_effect_size_low_vs_normal_high",
    "genotype_effect_stderr_estimate",
    "genotype_pvalue_low_normal_vs_high",
    "genotype_pvalue_low_vs_normal_high",
    "weight_effect_p_value",
    "weight_effect_stderr_estimate",
    "weight_effect_parameter_estimate",
    "group_1_genotype",
    "group_1_residuals_normality_test",
    "group_2_genotype",
    "group_2_residuals_normality_test",
    "interaction_effect_p_value",
    "interaction_significant",
    "intercept_estimate",
    "intercept_estimate_stderr_estimate",
    "male_control_count",
    "male_control_diversity_in_response",
    "male_control_mean",
    "male_control_sd",
    "male_control_unique_n",
    "male_ko_effect_p_value",
    "male_ko_effect_stderr_estimate",
    "male_ko_parameter_estimate",
    "male_mutant_count",
    "male_mutant_diversity_in_response",
    "male_mutant_mean",
    "male_mutant_sd",
    "male_mutant_unique_n",
    "male_percentage_change",
    "male_pvalue_low_normal_vs_high",
    "male_pvalue_low_vs_normal_high",
    "marker_accession_id",
    "marker_symbol",
    "metadata_group",
    "percentage_change",
    "no_data_control_count",
    "no_data_control_diversity_in_response",
    "no_data_control_unique_n",
    "no_data_mutant_count",
    "no_data_mutant_diversity_in_response",
    "no_data_mutant_unique_n",
    "p_value",
    "parameter_name",
    "parameter_stable_id",
    "phenotype_sex",
    "phenotyping_center",
    "pipeline_name",
    "pipeline_stable_id",
    "procedure_group",
    "procedure_name",
    "procedure_stable_id",
    "procedure_stable_key",
    "sex_effect_p_value",
    "sex_effect_parameter_estimate",
    "sex_effect_stderr_estimate",
    "statistical_method",
    "status",
    "strain_accession_id",
    "variance_significant",
    "zygosity",
    "mp_term_id",
    "mp_term_name",
    "mp_term_event",
    "mp_term_sex",
    "top_level_mp_term_id",
    "top_level_mp_term_name",
    "intermediate_mp_term_id",
    "intermediate_mp_term_name",
    "mp_term_id_options",
    "mp_term_name_options",
    "parameter_stable_key",
    "pipeline_stable_key",
    "genetic_background",
    "production_center",
    "project_name",
    "project_fullname",
    "resource_name",
    "resource_fullname",
    "strain_name",
    "life_stage_name",
    "life_stage_acc",
    "sex",
    "significant",
    "full_mp_term",
]


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: Open stats parquet file
                    [2]: Observations parquet
                    [3]: Ontology parquet
                    [4]: Ontology metadata parquet
                    [5]: Pipeline core parquet
                    [6]: Allele parquet
                    [7]: Output Path
    """
    open_stats_parquet_path = argv[1]
    observations_parquet_path = argv[2]
    ontology_parquet_path = argv[3]
    ontology_metadata_parquet_path = argv[4]
    pipeline_core_parquet_path = argv[5]
    allele_parquet_path = argv[6]
    output_path = argv[7]
    spark = SparkSession.builder.getOrCreate()
    open_stats_df = spark.read.parquet(open_stats_parquet_path)
    ontology_df = spark.read.parquet(ontology_parquet_path)
    ontology_df = ontology_df.alias("ontology")
    allele_df = spark.read.parquet(allele_parquet_path)
    ontology_metadata_df = spark.read.parquet(ontology_metadata_parquet_path)
    pipeline_core_df = spark.read.parquet(pipeline_core_parquet_path)
    observations_df = spark.read.parquet(observations_parquet_path)
    stats_observations_join = [
        "procedure_group",
        "parameter_stable_id",
        "phenotyping_center",
        "pipeline_stable_id",
        "colony_id",
        "metadata_group",
        "zygosity",
    ]
    observations_metadata_df = observations_df.select(
        stats_observations_join + list(set(OBSERVATIONS_STATS_MAP.values()))
    ).dropDuplicates()
    open_stats_df = open_stats_df.withColumn(
        "collapsed_mp_term",
        when(
            expr(
                "exists(mp_term.sex, sex -> sex = 'male') AND exists(mp_term.sex, sex -> sex = 'female')"
            ),
            expr("filter(mp_term, mp -> mp.sex NOT IN ('male', 'female'))"),
        ).otherwise(col("mp_term")),
    )
    open_stats_df = open_stats_df.withColumn(
        "collapsed_mp_term", expr("collapsed_mp_term[0]")
    )

    open_stats_df = open_stats_df.withColumn(
        "mp_term_id", col("collapsed_mp_term.term_id")
    )
    open_stats_df = open_stats_df.withColumn(
        "mp_term_event", col("collapsed_mp_term.event")
    )
    open_stats_df = open_stats_df.withColumn(
        "mp_term_sex", col("collapsed_mp_term.sex")
    )
    open_stats_df = open_stats_df.withColumnRenamed("mp_term", "full_mp_term")

    open_stats_df = open_stats_df.withColumnRenamed(
        "procedure_stable_id", "procedure_stable_id_str"
    )
    open_stats_df = open_stats_df.withColumn(
        "procedure_stable_id", split(col("procedure_stable_id_str"), "~")
    )
    open_stats_df = open_stats_df.alias("stats")

    for col_name in STATS_RESULTS_COLUMNS:
        if col_name not in open_stats_df.columns:
            open_stats_df = open_stats_df.withColumn(col_name, lit(None))
    open_stats_df = open_stats_df.join(
        ontology_df, col("mp_term_id") == col("id"), "left_outer"
    )

    for column_name, ontology_column in ONTOLOGY_STATS_MAP.items():
        open_stats_df = open_stats_df.withColumn(f"{column_name}", col(ontology_column))
    pipeline_core_join = ["parameter_stable_id", "pipeline_stable_id", "procedure_name"]
    pipeline_core_df = (
        pipeline_core_df.select(
            [
                col_name
                for col_name in pipeline_core_df.columns
                if col_name in pipeline_core_join
                or col_name in PIPELINE_STATS_MAP.values()
            ]
        )
        .groupBy(
            [
                "parameter_stable_key",
                "pipeline_stable_key",
                "parameter_stable_id",
                "pipeline_stable_id",
                "procedure_name",
                "mp_id",
                "mp_term",
            ]
        )
        .agg(
            collect_set("procedure_stable_id").alias("procedure_stable_id"),
            collect_set("procedure_stable_key").alias("procedure_stable_key"),
        )
        .dropDuplicates()
        .alias("impress")
    )
    for col_name in pipeline_core_df.columns:
        if col_name not in pipeline_core_join:
            pipeline_core_df = pipeline_core_df.withColumnRenamed(
                col_name, f"impress_{col_name}"
            )
    open_stats_df = open_stats_df.join(
        pipeline_core_df, pipeline_core_join, "left_outer"
    )

    for column_name, impress_column in PIPELINE_STATS_MAP.items():
        open_stats_df = open_stats_df.withColumn(
            column_name, col(f"impress_{impress_column}")
        )
        open_stats_df = open_stats_df.drop(f"impress_{impress_column}")

    for col_name in observations_metadata_df.columns:
        if col_name not in stats_observations_join:
            observations_metadata_df = observations_metadata_df.withColumnRenamed(
                col_name, f"observations_{col_name}"
            )

    open_stats_df = open_stats_df.join(
        observations_metadata_df, stats_observations_join
    )

    for column_name, observations_column in OBSERVATIONS_STATS_MAP.items():
        open_stats_df = open_stats_df.withColumn(
            column_name, col(f"observations_{observations_column}")
        )

    for col_name in allele_df.columns:
        if col_name != "allele_symbol":
            allele_df = allele_df.withColumnRenamed(col_name, f"allele_{col_name}")

    open_stats_df = open_stats_df.join(allele_df, "allele_symbol")

    for column_name, allele_column in ALLELE_STATS_MAP.items():
        open_stats_df = open_stats_df.withColumn(
            column_name, col(f"allele_{allele_column}")
        )
        open_stats_df = open_stats_df.drop(f"allele_{allele_column}")
    open_stats_df = open_stats_df.withColumn(
        "sex",
        when(col("mp_term_sex") == "not_considered", lit("both")).otherwise(
            col("mp_term_sex")
        ),
    )
    open_stats_df = open_stats_df.withColumn(
        "significant", col("mp_term_id").isNotNull()
    )
    open_stats_df.select(*STATS_RESULTS_COLUMNS).distinct().write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
