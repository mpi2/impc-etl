"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import collect_set, col, when, count, explode, lit, split
import requests
import json
import sys

from pyspark.sql.types import StringType

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
    output_path = argv[6]

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


if __name__ == "__main__":
    sys.exit(main(sys.argv))
