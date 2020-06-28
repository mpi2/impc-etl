from pyspark.sql import DataFrame, SparkSession
import sys

from pyspark.sql.functions import col, explode_outer, when, lit, least

ONTOLOGY_STATS_MAP = {
    "mp_term_name": "term",
    "top_level_mp_term_id": "top_level_ids",
    "top_level_mp_term_name": "top_level_terms",
    "intermediate_mp_term_id": "intermediate_ids",
    "intermediate_mp_term_name": "intermediate_terms",
}

GENOTYPE_PHENOTYPE_COLUMNS = [
    "marker_symbol",
    "marker_accession_id",
    "colony_id",
    "allele_name",
    "allele_symbol",
    "allele_accession_id",
    "strain_name",
    "strain_accession_id",
    "phenotyping_center",
    "project_name",
    "resource_name",
    "zygosity",
    "pipeline_name",
    "pipeline_stable_id",
    "pipeline_stable_key",
    "procedure_name",
    "procedure_stable_id",
    "procedure_stable_key",
    "parameter_name",
    "parameter_stable_id",
    "parameter_stable_key",
    "percentage_change",
    "effect_size",
    "life_stage_acc",
    "life_stage_name",
]

STATS_RESULTS_COLUMNS = [
    "full_mp_term",
    "statistical_method",
    "genotype_effect_p_value",
    "female_pvalue_low_vs_normal_high",
    "female_pvalue_low_normal_vs_high",
    "female_effect_size_low_vs_normal_high",
    "female_effect_size_low_normal_vs_high",
    "male_pvalue_low_vs_normal_high",
    "male_pvalue_low_normal_vs_high",
    "male_effect_size_low_vs_normal_high",
    "male_effect_size_low_normal_vs_high",
    "female_ko_effect_p_value",
    "male_ko_effect_p_value",
    "male_percentage_change",
    "female_percentage_change",
    "male_effect_size",
    "female_effect_size",
]


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    stats_results_parquet_path = argv[1]
    ontology_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    stats_results_df = spark.read.parquet(stats_results_parquet_path)
    ontology_df = spark.read.parquet(ontology_parquet_path)

    genotype_phenotype_df = stats_results_df.where(col("significant")).select(
        GENOTYPE_PHENOTYPE_COLUMNS + STATS_RESULTS_COLUMNS
    )
    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "mp_term", explode_outer("full_mp_term")
    )
    genotype_phenotype_df = genotype_phenotype_df.withColumn("sex", col("mp_term.sex"))
    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "mp_term_id", col("mp_term.term_id")
    )

    genotype_phenotype_df = genotype_phenotype_df.join(
        ontology_df, col("mp_term_id") == col("id"), "left_outer"
    )

    for column_name, ontology_column in ONTOLOGY_STATS_MAP.items():
        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            f"{column_name}", col(ontology_column)
        )

    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "p_value",
        when(
            ~col("statistical_method").contains("Reference Range Plus"),
            when(col("sex") == "male", col("male_ko_effect_p_value"))
            .when(col("sex") == "female", col("female_ko_effect_p_value"))
            .otherwise(col("genotype_effect_p_value")),
        ).otherwise(
            when(
                col("sex") == "male",
                least(
                    col("male_pvalue_low_vs_normal_high"),
                    col("male_pvalue_low_normal_vs_high"),
                ),
            )
            .when(
                col("sex") == "female",
                least(
                    col("female_pvalue_low_vs_normal_high"),
                    col("female_pvalue_low_normal_vs_high"),
                ),
            )
            .otherwise(col("genotype_effect_p_value"))
        ),
    )

    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "effect_size",
        when(
            ~col("statistical_method").contains("Reference Range Plus"),
            when(col("sex") == "male", col("male_effect_size"))
            .when(col("sex") == "female", col("female_effect_size"))
            .otherwise(col("effect_size")),
        ).otherwise(
            when(
                col("sex") == "male",
                when(
                    col("male_effect_size_low_vs_normal_high")
                    <= col("male_effect_size_low_normal_vs_high"),
                    col("genotype_effect_size_low_vs_normal_high"),
                ).otherwise(col("genotype_effect_size_low_normal_vs_high")),
            )
            .when(
                col("sex") == "female",
                when(
                    col("female_effect_size_low_vs_normal_high")
                    <= col("female_effect_size_low_normal_vs_high"),
                    col("genotype_effect_size_low_vs_normal_high"),
                ).otherwise(col("genotype_effect_size_low_normal_vs_high")),
            )
            .otherwise(col("effect_size"))
        ),
    )

    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "percentage_change",
        when(col("sex") == "male", col("male_percentage_change"))
        .when(col("sex") == "female", col("female_percentage_change"))
        .otherwise(col("percentage_change")),
    )

    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "assertion_type_id",
        when(
            col("statistical_method").isin(["Manual", "Supplied as data"]),
            lit("ECO:0000218"),
        ).otherwise(lit("ECO:0000203")),
    )

    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "assertion_type",
        when(
            col("statistical_method").isin(["Manual", "Supplied as data"]),
            lit("manual"),
        ).otherwise(lit("automatic")),
    )

    genotype_phenotype_df = genotype_phenotype_df.select(
        GENOTYPE_PHENOTYPE_COLUMNS
        + list(ONTOLOGY_STATS_MAP.keys())
        + ["assertion_type_id", "assertion_type"]
    )
    genotype_phenotype_df.distinct().write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
