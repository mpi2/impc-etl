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
    struct,
    lower,
    regexp_replace,
    size,
    array,
    regexp_extract,
    count,
    flatten,
    array_distinct,
    first,
    monotonically_increasing_id,
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
    "procedure_stable_id": "procedure_stable_id",
    "pipeline_stable_key": "pipeline_stable_key",
    "procedure_stable_key": "procedure_stable_key",
}

THREEI_STATS_MAP = {
    "colony_id": "Colony.Prefixes",
    "parameter_name": "Parameter.Name",
    "marker_symbol": "Gene",
    "procedure_name": "Procedure.Name",
    "procedure_stable_id": "Procedure.Id",
    "parameter_stable_id": "Parameter.Id",
    "classification_tag": "Call.Type",
    "mp_id": "Annotation.Calls",
    "allele_name": "Construct",
    "zygosity": "Genotype",
    "combine_sex_call": "Combine.Gender.Call",
    "sex": "Gender",
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
    "experiment_sex": "sex",
}


ALLELE_STATS_MAP = {"allele_name": "allele_name"}

STATS_RESULTS_COLUMNS = [
    "doc_id",
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
    "female_effect_size_low_normal_vs_high",
    "female_effect_size_low_vs_normal_high",
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
    "male_effect_size_low_normal_vs_high",
    "male_effect_size_low_vs_normal_high",
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
    "male_effect_size",
    "female_effect_size",
]


##TODO missing strain name and genetic background


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: Open stats parquet file
                    [2]: Observations parquet
                    [3]: Ontology parquet
                    [4]: Threei stats results file
                    [5]: Pipeline core parquet
                    [6]: Allele parquet
                    [7]: Output Path
    """
    open_stats_parquet_path = argv[1]
    observations_parquet_path = argv[2]
    ontology_parquet_path = argv[3]
    pipeline_core_parquet_path = argv[4]
    allele_parquet_path = argv[5]
    threei_parquet_path = argv[6]
    output_path = argv[7]
    spark = SparkSession.builder.getOrCreate()
    open_stats_df = spark.read.parquet(open_stats_parquet_path)
    ontology_df = spark.read.parquet(ontology_parquet_path)
    allele_df = spark.read.parquet(allele_parquet_path)
    pipeline_core_df = spark.read.parquet(pipeline_core_parquet_path)
    observations_df = spark.read.parquet(observations_parquet_path)
    threei_df = spark.read.csv(threei_parquet_path, header=True)
    threei_df = standardize_threei_schema(threei_df)
    stats_observations_join = [
        "procedure_group",
        "procedure_name",
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
    observations_metadata_df = observations_metadata_df.groupBy(
        *[
            col_name
            for col_name in observations_metadata_df.columns
            if col_name != "sex"
        ]
    ).agg(collect_set("sex").alias("sex"))

    aggregation_expresion = []

    for col_name in list(set(OBSERVATIONS_STATS_MAP.values())):
        if col_name not in ["datasource_name", "production_center"]:
            if col_name == "sex":
                aggregation_expresion.append(
                    array_distinct(flatten(collect_set(col_name))).alias(col_name)
                )
            elif col_name in ["strain_name", "genetic_background"]:
                aggregation_expresion.append(first(col(col_name)).alias(col_name))
            else:
                aggregation_expresion.append(collect_set(col_name).alias(col_name))

    observations_metadata_df = observations_metadata_df.groupBy(
        stats_observations_join + ["datasource_name", "production_center"]
    ).agg(*aggregation_expresion)
    open_stats_df = map_to_stats(
        open_stats_df,
        observations_metadata_df,
        stats_observations_join,
        OBSERVATIONS_STATS_MAP,
        "observations",
    )
    open_stats_df = open_stats_df.withColumn(
        "pipeline_stable_id",
        when(col("procedure_stable_id") == "ESLIM_022_001", lit("ESLIM_001")).otherwise(
            col("pipeline_stable_id")
        ),
    )
    open_stats_df = open_stats_df.withColumn(
        "procedure_stable_id",
        when(
            col("procedure_stable_id").contains("~"),
            split(col("procedure_stable_id"), "~"),
        ).otherwise(array(col("procedure_stable_id"))),
    )
    open_stats_df = open_stats_df.alias("stats")

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

    open_stats_df = open_stats_df.join(
        threei_df,
        [
            "resource_name",
            "colony_id",
            "marker_symbol",
            "procedure_stable_id",
            "parameter_stable_id",
            "zygosity",
        ],
        "left_outer",
    )

    open_stats_df = open_stats_df.withColumn(
        "collapsed_mp_term",
        when(
            col("threei_collapsed_mp_term").isNotNull(), col("threei_collapsed_mp_term")
        ).otherwise(col("collapsed_mp_term")),
    )

    open_stats_df = open_stats_df.drop("threei_collapsed_mp_term")

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
    open_stats_df = open_stats_df.withColumn(
        "full_mp_term",
        when(col("full_mp_term").isNull(), array(col("collapsed_mp_term"))).otherwise(
            col("full_mp_term")
        ),
    )

    for col_name in STATS_RESULTS_COLUMNS:
        if col_name not in open_stats_df.columns:
            open_stats_df = open_stats_df.withColumn(col_name, lit(None))

    ontology_df = ontology_df.withColumnRenamed("id", "mp_term_id")
    open_stats_df = map_to_stats(
        open_stats_df, ontology_df, ["mp_term_id"], ONTOLOGY_STATS_MAP, "ontology"
    )

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
                "parameter_stable_id",
                "pipeline_stable_id",
                "procedure_name",
                "pipeline_stable_key",
            ]
        )
        .agg(
            *[
                array_distinct(flatten(collect_set(col_name))).alias(col_name)
                if col_name in ["mp_id", "mp_term"]
                else collect_set(col_name).alias(col_name)
                for col_name in list(set(PIPELINE_STATS_MAP.values()))
                if col_name != "pipeline_stable_key"
            ]
        )
        .dropDuplicates()
    )
    open_stats_df = map_to_stats(
        open_stats_df,
        pipeline_core_df,
        pipeline_core_join,
        PIPELINE_STATS_MAP,
        "impress",
    )

    allele_df = allele_df.select(
        ["allele_symbol"] + list(ALLELE_STATS_MAP.values())
    ).dropDuplicates()

    open_stats_df = map_to_stats(
        open_stats_df, allele_df, ["allele_symbol"], ALLELE_STATS_MAP, "allele"
    )

    open_stats_df = open_stats_df.withColumn("sex", col("mp_term_sex"))
    open_stats_df = open_stats_df.withColumn(
        "phenotype_sex",
        when(col("phenotype_sex").isNull(), lit(None))
        .when(
            col("phenotype_sex").contains("Both sexes included"),
            array(lit("male"), lit("female")),
        )
        .otherwise(
            array(
                lower(
                    regexp_extract(
                        col("phenotype_sex"),
                        r"Only one sex included in the analysis; (.*)\[.*\]",
                        1,
                    )
                )
            )
        ),
    )
    open_stats_df = open_stats_df.withColumn(
        "phenotype_sex",
        when(
            col("phenotype_sex").isNull() & col("mp_term_sex").isNotNull(),
            when(
                col("mp_term_sex") == "not_considered",
                array(lit("male"), lit("female")),
            ).otherwise(array(col("mp_term_sex"))),
        ).otherwise(col("phenotype_sex")),
    )
    open_stats_df = open_stats_df.withColumn(
        "significant",
        when(col("mp_term_id").isNotNull(), lit(True)).otherwise(lit(False)),
    )
    open_stats_df = open_stats_df.withColumn(
        "doc_id", monotonically_increasing_id().astype(StringType())
    )
    open_stats_df.select(*STATS_RESULTS_COLUMNS).distinct().write.parquet(output_path)


def map_to_stats(
    open_stats_df, metadata_df, join_columns, source_stats_map, source_name
):
    for col_name in metadata_df.columns:
        if col_name not in join_columns:
            metadata_df = metadata_df.withColumnRenamed(
                col_name, f"{source_name}_{col_name}"
            )

    open_stats_df = open_stats_df.join(metadata_df, join_columns, "left_outer")
    for column_name, source_column in source_stats_map.items():
        open_stats_df = open_stats_df.withColumn(
            column_name, col(f"{source_name}_{source_column}")
        )
    for source_column in source_stats_map.values():
        open_stats_df = open_stats_df.drop(f"{source_name}_{source_column}")
    return open_stats_df


def standardize_threei_schema(threei_df: DataFrame):
    for col_name, threei_column in THREEI_STATS_MAP.items():
        threei_df = threei_df.withColumnRenamed(threei_column, col_name)
    threei_df = threei_df.withColumn("resource_name", lit("3i"))
    threei_df = threei_df.withColumn(
        "procedure_stable_id", array(col("procedure_stable_id"))
    )
    threei_df = threei_df.withColumn(
        "sex",
        when(col("sex") == "both", lit("not_considered")).otherwise(lower(col("sex"))),
    )
    threei_df = threei_df.withColumn(
        "zygosity",
        when(col("zygosity") == "Hom", lit("homozygote"))
        .when(col("zygosity") == "Hemi", lit("hemizygote"))
        .otherwise(lit("heterozygote")),
    )
    threei_df = threei_df.withColumn("term_id", regexp_replace("mp_id", "\[", ""))

    threei_df = threei_df.withColumn(
        "threei_collapsed_mp_term",
        when(
            (col("mp_id") != "NA") & (col("mp_id").isNotNull()),
            struct(
                lit(None).cast(StringType()).alias("event"),
                "sex",
                col("term_id").alias("term_id"),
            ),
        ).otherwise(lit(None)),
    )
    threei_df = threei_df.withColumn(
        "threei_p_value",
        when(col("classification_tag") == "Significant", lit(0.0)).otherwise(lit(1.0)),
    )
    threei_df = threei_df.withColumn(
        "threei_genotype_effect_p_value",
        when(col("classification_tag") == "Significant", lit(0.0)).otherwise(lit(1.0)),
    )
    threei_df = threei_df.withColumn(
        "threei_genotype_effect_parameter_estimate",
        when(col("classification_tag") == "Significant", lit(1.0)).otherwise(lit(0.0)),
    )
    threei_df = threei_df.withColumn(
        "threei_significant",
        when(col("classification_tag") == "Significant", lit(True)).otherwise(
            lit(False)
        ),
    )
    threei_df = threei_df.withColumn(
        "threei_status",
        when(
            col("classification_tag").isin(["Significant", "Not Significant"]),
            lit("Successful"),
        ).otherwise(lit("NotProcessed")),
    )
    threei_df = threei_df.withColumn(
        "threei_statistical_method", lit("Supplied as data")
    )
    threei_df = threei_df.drop(
        "sex",
        "term_id",
        "mp_id",
        "parameter_name",
        "procedure_name",
        "combine_sex_call",
        "samples",
        "allele_name",
        "classification_tag",
    )
    return threei_df


def map_three_i(open_stats_df, threei_df):
    open_stats_df = open_stats_df.withColumn(
        "genotype_effect_parameter_estimate",
        when(
            col("threei_genotype_effect_parameter_estimate").isNotNull(),
            col("threei_genotype_effect_parameter_estimate"),
        ).otherwise(col("genotype_effect_parameter_estimate")),
    )
    open_stats_df = open_stats_df.drop("threei_genotype_effect_parameter_estimate")

    open_stats_df = open_stats_df.withColumn(
        "significant",
        when(
            col("threei_significant").isNotNull(), col("threei_significant")
        ).otherwise(col("significant")),
    )
    open_stats_df = open_stats_df.drop("threei_significant")

    open_stats_df = open_stats_df.withColumn(
        "status",
        when(col("threei_status").isNotNull(), col("threei_status")).otherwise(
            col("status")
        ),
    )
    open_stats_df = open_stats_df.drop("threei_status")

    open_stats_df = open_stats_df.withColumn(
        "statistical_method",
        when(
            col("threei_statistical_method").isNotNull(),
            col("threei_statistical_method"),
        ).otherwise(col("statistical_method")),
    )
    open_stats_df = open_stats_df.drop("threei_statistical_method")

    open_stats_df = open_stats_df.withColumn(
        "p_value",
        when(col("threei_p_value").isNotNull(), col("threei_p_value")).otherwise(
            col("p_value")
        ),
    )
    open_stats_df = open_stats_df.drop("threei_p_value")

    open_stats_df = open_stats_df.withColumn(
        "genotype_effect_p_value",
        when(
            col("threei_genotype_effect_p_value").isNotNull(),
            col("threei_genotype_effect_p_value"),
        ).otherwise(col("genotype_effect_p_value")),
    )
    open_stats_df = open_stats_df.drop("threei_genotype_effect_p_value")
    return open_stats_df


def stop_and_count(df):
    print(df.count())
    raise ValueError


if __name__ == "__main__":
    sys.exit(main(sys.argv))
