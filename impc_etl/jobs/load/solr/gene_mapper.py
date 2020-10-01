"""
SOLR module
   Generates the required Solr cores
"""
import base64
import gzip

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    collect_set,
    col,
    when,
    count,
    explode,
    lit,
    split,
    min,
    struct,
    expr,
    sort_array,
    udf,
    size,
)
import requests
import json
import sys

from pyspark.sql.types import StringType, DoubleType

from impc_etl.config import Constants

GENE_CORE_COLUMNS = [
    "mgi_accession_id",
    "marker_symbol",
    "human_gene_symbol",
    "marker_name",
    "marker_synonym",
    "marker_type",
    "latest_es_cell_status",
    "latest_mouse_status",
    "latest_phenotype_status",
    "latest_project_status",
    "latest_phenotyping_centre",
    "latest_production_centre",
    "allele_mgi_accession_id",
    "allele_name",
    "es_cell_status",
    "mouse_status",
    "phenotype_status",
    "production_centre",
    "phenotyping_centre",
    "latest_phenotype_started",
    "latest_phenotype_complete",
    "imits_phenotype_started",
    "imits_phenotype_complete",
    "imits_phenotype_status",
    "imits_es_cell_status",
    "imits_mouse_status",
    "status",
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

CELL_MOUSE_STATUS_MAP = {
    "Chimeras obtained": "Assigned for Mouse Production and Phenotyping",
    "Cre Excision Complete": "Mice Produced",
    "Cre Excision Started": "Mice Produced",
    "ES Cell Production in Progress": "Assigned for ES Cell Production",
    "ES Cell Targeting Confirmed": "ES Cells Produced",
    "Genotype confirmed": "Mice Produced",
    "Micro-injection in progress": "Assigned for Mouse Production and Phenotyping",
    "No ES Cell Production": "Not Assigned for ES Cell Production",
    "Phenotype Attempt Registered": "Mice Produced",
    "Rederivation Complete": "Mice Produced",
    "Rederivation Started": "Mice Produced",
}

IMITS_GENE_MAP = {
    "mgi_accession_id": "marker_mgi_accession_id",
    "imits_phenotype_started": "latest_phenotype_started",
    "imits_phenotype_complete": "latest_phenotype_complete",
    "imits_phenotype_status": "latest_project_status",
    "imits_es_cell_status": "latest_es_cell_status",
    "imits_mouse_status": "latest_mouse_status",
    "status": "latest_project_status_legacy",
}


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
    output_path = argv[9]

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
    stats_results_df = spark.read.parquet(stats_results_parquet_path)
    ontology_metadata_df = spark.read.parquet(ontology_metadata_parquet_path)
    ontology_metadata_df = ontology_metadata_df.select(
        col("curie").alias("phenotype_term_id"),
        col("name").alias("phenotype_term_name"),
    ).distinct()

    stats_results_df = stats_results_df.withColumnRenamed(
        "marker_accession_id", "gene_accession_id"
    )
    stats_results_df = stats_results_df.withColumnRenamed(
        "marker_symbol", "gene_symbol"
    )
    stats_results_df = stats_results_df.withColumn(
        "procedure_stable_id", explode("procedure_stable_id")
    )
    stats_results_df = stats_results_df.withColumn(
        "procedure_name", explode("procedure_name")
    )
    stats_results_df = stats_results_df.withColumn(
        "life_stage_name", explode("life_stage_name")
    )

    significant_mp_term = stats_results_df.select(
        "gene_accession_id", "top_level_mp_term_name", "significant"
    )

    significant_mp_term = significant_mp_term.withColumn(
        "top_level_mp_term_name", explode("top_level_mp_term_name")
    )

    significant_mp_term = significant_mp_term.groupBy("gene_accession_id").agg(
        collect_set(
            when(col("significant"), col("top_level_mp_term_name")).otherwise(lit(None))
        ).alias("significant_top_level_mp_terms"),
        collect_set(
            when(~col("significant"), col("top_level_mp_term_name")).otherwise(
                lit(None)
            )
        ).alias("not_significant_top_level_mp_terms"),
    )
    significant_mp_term.where(col("gene_accession_id") == "MGI:1929293").show(
        vertical=True, truncate=False
    )
    raise ValueError
    significance_cols = [
        "female_ko_effect_p_value",
        "male_ko_effect_p_value",
        "genotype_effect_p_value",
        "p_value",
        "effect_size",
        "male_effect_size",
        "female_effect_size",
        "significant",
        "full_mp_term",
        "metadata_group",
        "male_mutant_count",
        "female_mutant_count",
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
        when(
            (col("female_ko_effect_p_value") < col("male_ko_effect_p_value"))
            & (col("female_ko_effect_p_value") < col("genotype_effect_p_value")),
            col("female_ko_effect_p_value"),
        )
        .when(
            (col("male_ko_effect_p_value") < col("female_ko_effect_p_value"))
            & (col("male_ko_effect_p_value") < col("genotype_effect_p_value")),
            col("male_ko_effect_p_value"),
        )
        .when(
            col("genotype_effect_p_value").isNotNull(), col("genotype_effect_p_value")
        )
        .otherwise(col("p_value")),
    )
    stats_results_df = stats_results_df.withColumn(
        "selected_p_value", col("selected_p_value").cast(DoubleType())
    )
    stats_results_df = stats_results_df.withColumn(
        "selected_effect_size",
        when(
            (col("female_ko_effect_p_value") < col("male_ko_effect_p_value"))
            & (col("female_ko_effect_p_value") < col("genotype_effect_p_value")),
            col("female_effect_size"),
        )
        .when(
            (col("male_ko_effect_p_value") < col("female_ko_effect_p_value"))
            & (col("male_ko_effect_p_value") < col("genotype_effect_p_value")),
            col("male_effect_size"),
        )
        .when(col("genotype_effect_p_value").isNotNull(), col("effect_size"))
        .otherwise(col("effect_size")),
    )
    stats_results_df = stats_results_df.withColumn(
        "selected_phenotype_term",
        when(
            (col("female_ko_effect_p_value") < col("male_ko_effect_p_value"))
            & (col("female_ko_effect_p_value") < col("genotype_effect_p_value")),
            expr(
                "transform(filter(full_mp_term, mp -> mp.sex = 'female'), mp -> mp.term_id)"
            ).getItem(0),
        )
        .when(
            (col("male_ko_effect_p_value") < col("female_ko_effect_p_value"))
            & (col("male_ko_effect_p_value") < col("genotype_effect_p_value")),
            expr(
                "transform(filter(full_mp_term, mp -> mp.sex = 'male'), mp -> mp.term_id)"
            ).getItem(0),
        )
        .otherwise(
            expr(
                "transform(filter(full_mp_term, mp -> mp.sex NOT IN ('female', 'male')), mp -> mp.term_id)"
            ).getItem(0)
        ),
    )
    observations_df = observations_df.select(*data_set_cols).distinct()
    datasets_df = observations_df.join(stats_results_df, data_set_cols, "left_outer")
    datasets_df = datasets_df.groupBy(data_set_cols).agg(
        collect_set(
            struct(
                *[
                    "selected_p_value",
                    "selected_effect_size",
                    "selected_phenotype_term",
                    "metadata_group",
                    "male_mutant_count",
                    "female_mutant_count",
                ]
            )
        ).alias("stats_data")
    )
    datasets_df = datasets_df.withColumn(
        "successful_stats_data",
        expr("filter(stats_data, stat -> stat.selected_p_value IS NOT NULL)"),
    )
    datasets_df = datasets_df.withColumn(
        "stats_data",
        when(
            size("successful_stats_data") > 0,
            sort_array("successful_stats_data").getItem(0),
        ).otherwise(sort_array("stats_data").getItem(0)),
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
        when(col("phenotype_term_id").isNotNull(), lit("Significant"))
        .when(col("p_value").isNotNull(), lit("Not significant"))
        .otherwise(lit("N/A")),
    )
    mgi_datasets_df = datasets_df.groupBy("gene_accession_id").agg(
        collect_set(
            struct(
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

    to_json_udf = udf(
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
    compress_and_encode = udf(_compress_and_encode, StringType())
    mgi_datasets_df = mgi_datasets_df.withColumn(
        "datasets_raw_data", compress_and_encode("datasets_raw_data")
    )

    grouped_columns = [
        "allele_mgi_accession_id",
        "allele_name",
        "phenotype_status",
        "es_cell_status",
        "mouse_status",
        "production_centre",
        "phenotyping_centre",
        "latest_production_centre",
        "latest_phenotyping_centre",
    ]
    gene_df = imits_allele_df.select(
        [
            col_name
            for col_name in imits_allele_df.columns
            if col_name not in IMITS_GENE_COLUMNS
            or col_name == "marker_mgi_accession_id"
        ]
    )
    gene_df = imits_gene_df.where(
        col("latest_project_status").isNotNull()
        & col("feature_type").isNotNull()
        & (col("allele_design_project") == "IMPC")
    ).join(gene_df, "marker_mgi_accession_id", "left_outer")

    status_map_df_json = spark.sparkContext.parallelize(
        [
            {"old_status": key, "new_status": value}
            for key, value in CELL_MOUSE_STATUS_MAP.items()
        ]
    )
    status_map_df = spark.read.json(status_map_df_json)
    for status_col in [
        "mouse_status",
        "es_cell_status",
        "latest_mouse_status",
        "latest_es_cell_status",
    ]:
        gene_df = map_status(gene_df, status_map_df, status_col)
    for gene_core_field, imits_col_name in IMITS_GENE_MAP.items():
        gene_df = gene_df.withColumn(gene_core_field, col(imits_col_name))

    gene_df = gene_df.withColumn(
        "is_umass_gene", col("marker_symbol").isin(Constants.UMASS_GENES)
    )
    gene_df = gene_df.withColumn(
        "is_idg_gene", col("mgi_accession_id").isin(Constants.IDG_GENES)
    )

    embryo_data_df = embryo_data_df.withColumn("colonies", explode("colonies"))
    embryo_data_df = embryo_data_df.select("colonies.*")
    embryo_data_df = embryo_data_df.withColumn(
        "embryo_analysis_view_name",
        when(
            col("analysis_view_url").isNotNull(), lit("volumetric analysis")
        ).otherwise(lit(None).cast(StringType())),
    )
    embryo_data_df = embryo_data_df.withColumnRenamed(
        "analysis_view_url", "embryo_analysis_view_url"
    )
    embryo_data_df = embryo_data_df.withColumn(
        "embryo_modalities", col("procedures_parameters.modality")
    )
    embryo_data_df = embryo_data_df.alias("embryo")
    gene_df = gene_df.join(
        embryo_data_df, col("mgi_accession_id") == col("mgi"), "left_outer"
    )
    gene_df = gene_df.withColumn("embryo_data_available", col("embryo.mgi").isNotNull())

    gene_df = gene_df.join(mgi_mrk_list_df, "mgi_accession_id", "left_outer")
    gene_df = gene_df.withColumn("seq_region_id", col("chr"))
    gene_df = gene_df.withColumnRenamed("chr", "chr_name")
    gene_df = gene_df.withColumnRenamed("genome_coordinate_start", "seq_region_start")
    gene_df = gene_df.withColumnRenamed("genome_coordinate_end", "seq_region_end")
    gene_df = gene_df.withColumnRenamed("strand", "chr_strand")

    mgi_homologene_df = mgi_homologene_df.select(
        ["mgi_accession_id", "entrezgene_id", "ensembl_gene_id", "ccds_ids"]
    )
    gene_df = gene_df.join(mgi_homologene_df, "mgi_accession_id", "left_outer")
    gene_df = gene_df.withColumn(
        "ensembl_gene_id", split(col("ensembl_gene_id"), r"\|")
    )
    gene_df = gene_df.withColumn("ccds_id", split(col("ccds_ids"), ","))
    gene_df = gene_df.withColumn("ncbi_id", col("entrezgene_id"))

    gene_df = gene_df.withColumn("marker_synonym", split(col("marker_synonym"), r"\|"))

    gene_df = gene_df.select(*GENE_CORE_COLUMNS)
    gene_df = gene_df.groupBy(
        [col_name for col_name in gene_df.columns if col_name not in grouped_columns]
    ).agg(*[collect_set(col_name).alias(col_name) for col_name in grouped_columns])
    gene_df = gene_df.join(mgi_datasets_df, "mgi_accession_id", "left_outer")
    gene_df.distinct().write.parquet(output_path)


def map_status(gene_df, status_map_df, status_col):
    gene_df = gene_df.join(
        status_map_df, col(status_col) == col("old_status"), "left_outer"
    )
    gene_df = gene_df.withColumn(
        status_col,
        when(col("new_status").isNotNull(), col("new_status")).otherwise(
            col(status_col)
        ),
    )
    gene_df = gene_df.drop("old_status", "new_status")
    return gene_df


def get_embryo_data(spark: SparkSession):
    r = requests.get("")
    df = spark.createDataFrame([json.loads(line) for line in r.iter_lines()])
    return df


def _compress_and_encode(json_text):
    if json_text is None:
        return None
    else:
        return str(base64.b64encode(gzip.compress(bytes(json_text, "utf-8"))), "utf-8")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
