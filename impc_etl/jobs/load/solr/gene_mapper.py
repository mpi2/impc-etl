"""
SOLR module
   Generates the required Solr cores
"""
import base64
import gzip
import json
import sys

import requests
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import count, col, collect_set
from pyspark.sql.types import StringType, DoubleType

from impc_etl.config.constants import Constants

GENE_CORE_COLUMNS = [
    "mgi_accession_id",
    "marker_symbol",
    "human_gene_symbol",
    "marker_name",
    "marker_synonym",
    "marker_type",
    "es_cell_production_status",
    "null_allele_production_status",
    "conditional_allele_production_status",
    "crispr_allele_production_status",
    "mouse_production_status",
    "phenotype_status",
    "assignment_status",
    "phenotyping_data_available",
    "allele_mgi_accession_id",
    "allele_name",
    "is_umass_gene",
    "is_idg_gene",
    "embryo_data_available",
    "embryo_analysis_view_url",
    "embryo_analysis_view_name",
    "embryo_modalities",
    "chr_name",
    "seq_region_id",
    "seq_region_start",
    "seq_region_end",
    "chr_strand",
    "ensembl_gene_id",
    "ccds_id",
    "ncbi_id",
    "latest_production_centre",
    "latest_phenotyping_centre",
]

IMITS_GENE_COLUMNS = [
    "marker_symbol",
    "allele_design_project",
    "marker_mgi_accession_id",
    "latest_es_cell_status",
    "latest_mouse_status",
    "latest_phenotype_status",
    "latest_project_status",
    "latest_production_centre",
    "latest_phenotyping_centre",
    "latest_phenotype_started",
    "latest_phenotype_complete",
    "latest_project_status_legacy",
]


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    imits_gene_parquet_path = argv[1]
    imits_allele_parquet_path = argv[2]
    mgi_homologene_report_parquet_path = argv[3]
    mgi_mrk_list_report_parquet_path = argv[4]
    embryo_data_json_path = argv[5]
    observations_parquet_path = argv[6]
    stats_results_parquet_path = argv[7]
    ontology_metadata_parquet_path = argv[8]
    gene_production_status_path = argv[9]
    output_path = argv[10]

    spark = SparkSession.builder.getOrCreate()
    imits_gene_df = spark.read.parquet(imits_gene_parquet_path).select(
        IMITS_GENE_COLUMNS
    )
    imits_allele_df = spark.read.parquet(imits_allele_parquet_path)
    mgi_homologene_df = spark.read.parquet(mgi_homologene_report_parquet_path)
    mgi_mrk_list_df = spark.read.parquet(mgi_mrk_list_report_parquet_path)
    mgi_mrk_list_df = mgi_mrk_list_df.select(
        [
            "mgi_accession_id",
            "chr",
            "genome_coordinate_start",
            "genome_coordinate_end",
            "strand",
        ]
    )
    embryo_data_df = spark.read.json(embryo_data_json_path, mode="FAILFAST")
    observations_df = spark.read.parquet(observations_parquet_path)
    phenotyping_data_availability_df = observations_df.groupBy("gene_accession_id").agg(
        count("*").alias("data_points")
    )
    phenotyping_data_availability_df = phenotyping_data_availability_df.withColumn(
        "phenotyping_data_available", col("data_points") > 0
    )
    phenotyping_data_availability_df = phenotyping_data_availability_df.drop(
        "data_points"
    )
    phenotyping_data_availability_df = (
        phenotyping_data_availability_df.withColumnRenamed(
            "gene_accession_id", "mgi_accession_id"
        )
    )
    stats_results_df = spark.read.parquet(stats_results_parquet_path)
    ontology_metadata_df = spark.read.parquet(ontology_metadata_parquet_path)
    ontology_metadata_df = ontology_metadata_df.select(
        functions.col("curie").alias("phenotype_term_id"),
        functions.col("name").alias("phenotype_term_name"),
    ).distinct()

    gene_production_status_df = spark.read.parquet(gene_production_status_path)

    stats_results_df = stats_results_df.withColumnRenamed(
        "marker_accession_id", "gene_accession_id"
    )
    stats_results_df = stats_results_df.withColumnRenamed(
        "marker_symbol", "gene_symbol"
    )
    stats_results_df = stats_results_df.withColumn(
        "procedure_stable_id", functions.explode("procedure_stable_id")
    )
    stats_results_df = stats_results_df.withColumn(
        "procedure_name", functions.explode("procedure_name")
    )
    stats_results_df = stats_results_df.withColumn(
        "life_stage_name", functions.explode("life_stage_name")
    )
    stats_results_df = stats_results_df.withColumn(
        "top_level_mp_term_name",
        functions.when(
            (functions.size("top_level_mp_term_name") == 0)
            | functions.col("top_level_mp_term_name").isNull(),
            functions.array("mp_term_name"),
        ).otherwise(functions.col("top_level_mp_term_name")),
    )

    significant_mp_term = _get_significance_fields_by_gene(stats_results_df)
    mgi_datasets_df = _get_datasets_by_gene(
        stats_results_df, observations_df, ontology_metadata_df
    )

    gene_allele_info_df = imits_allele_df.select(
        [
            col_name
            for col_name in imits_allele_df.columns
            if col_name not in IMITS_GENE_COLUMNS
            or col_name == "marker_mgi_accession_id"
        ]
    )

    gene_allele_info_df = gene_allele_info_df.groupBy("marker_mgi_accession_id").agg(
        *[
            collect_set(col_name).alias(col_name)
            for col_name in gene_allele_info_df.columns
            if col_name != "marker_mgi_accession_id"
        ]
    )
    gene_df = imits_gene_df.where(
        functions.col("latest_project_status").isNotNull()
        & functions.col("feature_type").isNotNull()
        & (functions.col("allele_design_project") == "IMPC")
    )
    gene_df.printSchema()
    gene_df.show()
    raise TypeError
    gene_df = gene_df.join(gene_allele_info_df, "marker_mgi_accession_id", "left_outer")
    gene_df = gene_df.withColumn(
        "is_umass_gene", functions.col("marker_symbol").isin(Constants.UMASS_GENES)
    )
    gene_df = gene_df.withColumn(
        "is_idg_gene", functions.col("mgi_accession_id").isin(Constants.IDG_GENES)
    )

    embryo_data_df = embryo_data_df.withColumn(
        "colonies", functions.explode("colonies")
    )
    embryo_data_df = embryo_data_df.select("colonies.*")
    embryo_data_df = embryo_data_df.withColumn(
        "embryo_analysis_view_name",
        functions.when(
            functions.col("analysis_view_url").isNotNull(),
            functions.lit("volumetric analysis"),
        ).otherwise(functions.lit(None).cast(StringType())),
    )
    embryo_data_df = embryo_data_df.withColumnRenamed(
        "analysis_view_url", "embryo_analysis_view_url"
    )
    embryo_data_df = embryo_data_df.withColumn(
        "embryo_modalities", functions.col("procedures_parameters.modality")
    )
    embryo_data_df = embryo_data_df.alias("embryo")
    gene_df = gene_df.join(
        embryo_data_df,
        functions.col("mgi_accession_id") == functions.col("mgi"),
        "left_outer",
    )
    gene_df = gene_df.withColumn(
        "embryo_data_available", functions.col("embryo.mgi").isNotNull()
    )

    gene_df = gene_df.join(mgi_mrk_list_df, "mgi_accession_id", "left_outer")
    gene_df = gene_df.withColumn("seq_region_id", functions.col("chr"))
    gene_df = gene_df.withColumnRenamed("chr", "chr_name")
    gene_df = gene_df.withColumnRenamed("genome_coordinate_start", "seq_region_start")
    gene_df = gene_df.withColumnRenamed("genome_coordinate_end", "seq_region_end")
    gene_df = gene_df.withColumnRenamed("strand", "chr_strand")

    mgi_homologene_df = mgi_homologene_df.select(
        ["mgi_accession_id", "entrezgene_id", "ensembl_gene_id", "ccds_ids"]
    )
    gene_df = gene_df.join(mgi_homologene_df, "mgi_accession_id", "left_outer")
    gene_df = gene_df.withColumn(
        "ensembl_gene_id", functions.split(functions.col("ensembl_gene_id"), r"\|")
    )
    gene_df = gene_df.withColumn(
        "ccds_id", functions.split(functions.col("ccds_ids"), ",")
    )
    gene_df = gene_df.withColumn("ncbi_id", functions.col("entrezgene_id"))

    gene_df = gene_df.withColumn(
        "marker_synonym", functions.split(functions.col("marker_synonym"), r"\|")
    )
    gene_df = gene_df.join(gene_production_status_df, "mgi_accession_id", "left_outer")
    gene_df = gene_df.join(
        phenotyping_data_availability_df, "mgi_accession_id", "left_outer"
    )
    gene_df = gene_df.join(mgi_datasets_df, "mgi_accession_id", "left_outer")
    gene_df = gene_df.join(significant_mp_term, "mgi_accession_id", "left_outer")
    gene_df.distinct().write.parquet(output_path)


def get_embryo_data(spark: SparkSession):
    r = requests.get("")
    df = spark.createDataFrame([json.loads(line) for line in r.iter_lines()])
    return df


def _compress_and_encode(json_text):
    if json_text is None:
        return None
    else:
        return str(base64.b64encode(gzip.compress(bytes(json_text, "utf-8"))), "utf-8")


def _get_datasets_by_gene(stats_results_df, observations_df, ontology_metadata_df):
    significance_cols = [
        "female_ko_effect_p_value",
        "male_ko_effect_p_value",
        "genotype_effect_p_value",
        "male_pvalue_low_vs_normal_high",
        "male_pvalue_low_normal_vs_high",
        "female_pvalue_low_vs_normal_high",
        "female_pvalue_low_normal_vs_high",
        "genotype_pvalue_low_normal_vs_high",
        "genotype_pvalue_low_vs_normal_high",
        "male_ko_effect_p_value",
        "female_ko_effect_p_value",
        "p_value",
        "effect_size",
        "male_effect_size",
        "female_effect_size",
        "male_effect_size_low_vs_normal_high",
        "male_effect_size_low_normal_vs_high",
        "genotype_effect_size_low_vs_normal_high",
        "genotype_effect_size_low_normal_vs_high",
        "female_effect_size_low_vs_normal_high",
        "female_effect_size_low_normal_vs_high",
        "significant",
        "full_mp_term",
        "metadata_group",
        "male_mutant_count",
        "female_mutant_count",
        "statistical_method",
        "mp_term_id",
        "sex",
    ]

    data_set_cols = [
        "allele_accession_id",
        "allele_symbol",
        "gene_symbol",
        "gene_accession_id",
        "parameter_stable_id",
        "parameter_name",
        "procedure_stable_id",
        "procedure_name",
        "pipeline_name",
        "pipeline_stable_id",
        "zygosity",
        "phenotyping_center",
        "life_stage_name",
    ]

    stats_results_df = stats_results_df.select(*(data_set_cols + significance_cols))
    stats_results_df = stats_results_df.withColumn(
        "selected_p_value",
        functions.when(
            functions.col("statistical_method").isin(["Manual", "Supplied as data"]),
            functions.col("p_value"),
        )
        .when(
            functions.col("statistical_method").contains("Reference Range Plus"),
            functions.when(
                functions.col("sex") == "male",
                functions.least(
                    functions.col("male_pvalue_low_vs_normal_high"),
                    functions.col("male_pvalue_low_normal_vs_high"),
                ),
            )
            .when(
                functions.col("sex") == "female",
                functions.least(
                    functions.col("female_pvalue_low_vs_normal_high"),
                    functions.col("female_pvalue_low_normal_vs_high"),
                ),
            )
            .otherwise(
                functions.least(
                    functions.col("genotype_pvalue_low_normal_vs_high"),
                    functions.col("genotype_pvalue_low_vs_normal_high"),
                )
            ),
        )
        .otherwise(
            functions.when(
                functions.col("sex") == "male", functions.col("male_ko_effect_p_value")
            )
            .when(
                functions.col("sex") == "female",
                functions.col("female_ko_effect_p_value"),
            )
            .otherwise(
                functions.when(
                    functions.col("statistical_method").contains(
                        "Fisher Exact Test framework"
                    ),
                    functions.col("p_value"),
                ).otherwise(functions.col("genotype_effect_p_value"))
            )
        ),
    )
    stats_results_df = stats_results_df.withColumn(
        "selected_p_value", functions.col("selected_p_value").cast(DoubleType())
    )
    stats_results_df = stats_results_df.withColumn(
        "selected_effect_size",
        functions.when(
            functions.col("statistical_method").isin(["Manual", "Supplied as data"]),
            functions.lit(1.0),
        )
        .when(
            ~functions.col("statistical_method").contains("Reference Range Plus"),
            functions.when(
                functions.col("sex") == "male", functions.col("male_effect_size")
            )
            .when(functions.col("sex") == "female", functions.col("female_effect_size"))
            .otherwise(functions.col("effect_size")),
        )
        .otherwise(
            functions.when(
                functions.col("sex") == "male",
                functions.when(
                    functions.col("male_effect_size_low_vs_normal_high")
                    <= functions.col("male_effect_size_low_normal_vs_high"),
                    functions.col("genotype_effect_size_low_vs_normal_high"),
                ).otherwise(functions.col("genotype_effect_size_low_normal_vs_high")),
            )
            .when(
                functions.col("sex") == "female",
                functions.when(
                    functions.col("female_effect_size_low_vs_normal_high")
                    <= functions.col("female_effect_size_low_normal_vs_high"),
                    functions.col("genotype_effect_size_low_vs_normal_high"),
                ).otherwise(functions.col("genotype_effect_size_low_normal_vs_high")),
            )
            .otherwise(functions.col("effect_size"))
        ),
    )
    stats_results_df = stats_results_df.withColumn(
        "selected_phenotype_term", functions.col("mp_term_id")
    )
    observations_df = observations_df.select(*data_set_cols).distinct()
    datasets_df = observations_df.join(stats_results_df, data_set_cols, "left_outer")
    datasets_df = datasets_df.groupBy(data_set_cols).agg(
        functions.collect_set(
            functions.struct(
                *[
                    "selected_p_value",
                    "selected_effect_size",
                    "selected_phenotype_term",
                    "metadata_group",
                    "male_mutant_count",
                    "female_mutant_count",
                    "significant",
                ]
            )
        ).alias("stats_data")
    )
    datasets_df = datasets_df.withColumn(
        "successful_stats_data",
        functions.expr("filter(stats_data, stat -> stat.selected_p_value IS NOT NULL)"),
    )
    datasets_df = datasets_df.withColumn(
        "stats_data",
        functions.when(
            functions.size("successful_stats_data") > 0,
            functions.sort_array("successful_stats_data").getItem(0),
        ).otherwise(functions.sort_array("stats_data").getItem(0)),
    )
    datasets_df = datasets_df.select(*data_set_cols, "stats_data.*")
    datasets_df = datasets_df.withColumnRenamed("selected_p_value", "p_value")
    datasets_df = datasets_df.withColumnRenamed("selected_effect_size", "effect_size")
    datasets_df = datasets_df.withColumnRenamed(
        "selected_phenotype_term", "phenotype_term_id"
    )
    datasets_df = datasets_df.join(
        ontology_metadata_df, "phenotype_term_id", "left_outer"
    )
    datasets_df = datasets_df.withColumn(
        "significance",
        functions.when(
            functions.col("significant") == True, functions.lit("Significant")
        )
        .when(functions.col("p_value").isNotNull(), functions.lit("Not significant"))
        .otherwise(functions.lit("N/A")),
    )
    mgi_datasets_df = datasets_df.groupBy("gene_accession_id").agg(
        functions.collect_set(
            functions.struct(
                *(
                    data_set_cols
                    + [
                        "significance",
                        "p_value",
                        "effect_size",
                        "metadata_group",
                        "male_mutant_count",
                        "female_mutant_count",
                        "phenotype_term_id",
                        "phenotype_term_name",
                    ]
                )
            )
        ).alias("datasets_raw_data")
    )

    mgi_datasets_df = mgi_datasets_df.withColumnRenamed(
        "gene_accession_id", "mgi_accession_id"
    )

    to_json_udf = functions.udf(
        lambda row: None
        if row is None
        else json.dumps(
            [{key: value for key, value in item.asDict().items()} for item in row]
        ),
        StringType(),
    )
    mgi_datasets_df = mgi_datasets_df.withColumn(
        "datasets_raw_data", to_json_udf("datasets_raw_data")
    )
    compress_and_encode = functions.udf(_compress_and_encode, StringType())
    mgi_datasets_df = mgi_datasets_df.withColumn(
        "datasets_raw_data", compress_and_encode("datasets_raw_data")
    )
    return mgi_datasets_df


def _get_significance_fields_by_gene(stats_results_df):
    significant_mp_term = stats_results_df.select(
        "gene_accession_id", "top_level_mp_term_name", "significant"
    )

    significant_mp_term = significant_mp_term.withColumn(
        "top_level_mp_term_name", functions.explode("top_level_mp_term_name")
    )

    significant_mp_term = significant_mp_term.groupBy("gene_accession_id").agg(
        functions.collect_set(
            functions.when(
                functions.col("significant") == True,
                functions.col("top_level_mp_term_name"),
            ).otherwise(functions.lit(None))
        ).alias("significant_top_level_mp_terms"),
        functions.collect_set(
            functions.when(
                functions.col("significant") == False,
                functions.col("top_level_mp_term_name"),
            ).otherwise(functions.lit(None))
        ).alias("not_significant_top_level_mp_terms"),
    )
    significant_mp_term = significant_mp_term.withColumn(
        "not_significant_top_level_mp_terms",
        functions.array_except(
            "not_significant_top_level_mp_terms", "significant_top_level_mp_terms"
        ),
    )
    significant_mp_term = significant_mp_term.withColumnRenamed(
        "gene_accession_id", "mgi_accession_id"
    )
    return significant_mp_term


if __name__ == "__main__":
    sys.exit(main(sys.argv))
