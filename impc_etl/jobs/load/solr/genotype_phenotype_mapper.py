from pyspark.sql import DataFrame, SparkSession
import sys

from pyspark.sql.functions import col, explode_outer, when, lit, least

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
    "statistical_method",
    "life_stage_acc",
    "life_stage_name",
    "full_mp_term",
    "genotype_effect_p_value",
    "female_pvalue_low_vs_normal_high",
    "female_pvalue_low_normal_vs_high",
    "male_pvalue_low_vs_normal_high",
    "male_pvalue_low_normal_vs_high",
    "female_ko_effect_p_value",
    "male_ko_effect_p_value",
    "statistical_method",
    "male_percentage_change",
    "female_percentage_change",
]


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    stats_results_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    stats_results_df = spark.read.parquet(stats_results_parquet_path)

    genotype_phenotype_df = stats_results_df.where(col("significant")).select(
        GENOTYPE_PHENOTYPE_COLUMNS
    )
    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "mp_term", explode_outer("full_mp_term")
    )
    genotype_phenotype_df = genotype_phenotype_df.withColumn("sex", col("mp_term.sex"))
    genotype_phenotype_df = genotype_phenotype_df.withColumn(
        "mp_term_id", col("mp_term.term_id")
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
        "percentage_change",
        when(col("sex") == "male", col("male_percentage_change"))
        .when(col("sex") == "female", col("female_percentage_change"))
        .otherwise(lit(None)),
    )
    genotype_phenotype_df.where(col("female_percentage_change").isNotNull()).show(
        vertical=True, truncate=False
    )
    raise ValueError


if __name__ == "__main__":
    sys.exit(main(sys.argv))
