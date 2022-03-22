"""
SOLR module
   Generates the required Solr cores
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    concat_ws,
    monotonically_increasing_id,
)
from pyspark.sql.types import StringType

from impc_etl.jobs.extract import (
    MGIGenePhenoReportExtractor,
    MGIPhenotypicAlleleExtractor,
)
from impc_etl.jobs.extract.ontology_hierarchy_extractor import (
    OntologyTermHierarchyExtractor,
)
from impc_etl.workflow.config import ImpcConfig

ONTOLOGY_STATS_MAP = {
    "mp_term_name": "term",
    "top_level_mp_term_id": "top_level_ids",
    "top_level_mp_term_name": "top_level_terms",
    "intermediate_mp_term_id": "intermediate_ids",
    "intermediate_mp_term_name": "intermediate_terms",
}


class MGIPhenotypeCoreLoader(PySparkTask):
    name = "IMPC_MGI_Phenotype_Loader"

    mgi_allele_input_path = luigi.Parameter()
    mgi_gene_pheno_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            MGIGenePhenoReportExtractor(),
            MGIPhenotypicAlleleExtractor(),
            OntologyTermHierarchyExtractor(),
        ]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}mgi_phenotype_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Solr Core loader
        :param list argv: the list elements should be:
                        [1]: source IMPC parquet file
                        [2]: Output Path
        """
        mgi_phenotype_parquet_path = args[0]
        mgi_allele_parquet_path = args[1]
        ontology_parquet_path = args[2]
        output_path = args[3]

        spark = SparkSession.builder.getOrCreate()
        mgi_phenotype_df = spark.read.parquet(mgi_phenotype_parquet_path)
        mgi_allele_df = spark.read.parquet(mgi_allele_parquet_path)
        ontology_df = spark.read.parquet(ontology_parquet_path)

        mgi_phenotype_df = mgi_phenotype_df.join(
            ontology_df, col("mammalianPhenotypeID") == col("id"), "left_outer"
        )

        for column_name, ontology_column in ONTOLOGY_STATS_MAP.items():
            mgi_phenotype_df = mgi_phenotype_df.withColumn(
                f"{column_name}", col(ontology_column)
            )

        mgi_phenotype_df = mgi_phenotype_df.withColumn("assertion_type", lit("manual"))
        mgi_phenotype_df = mgi_phenotype_df.withColumn(
            "assertion_type_id", lit("ECO:0000218")
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumn(
            "life_stage_acc", lit("EFO:0002948")
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumn(
            "life_stage_name", lit("postnatal")
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumn("ontology_db_id", lit(5))

        mgi_phenotype_df = mgi_phenotype_df.withColumnRenamed(
            "mammalianPhenotypeID", "mp_term_id"
        )
        mgi_phenotype_df = mgi_phenotype_df.join(
            mgi_allele_df,
            ["mgiAlleleID", "mgiMarkerAccessionID", "alleleSymbol"],
            "left_outer",
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumnRenamed(
            "mgiAlleleID", "allele_accession_id"
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumnRenamed(
            "alleleSymbol", "allele_symbol"
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumnRenamed(
            "alleleName", "allele_name"
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumnRenamed(
            "markerSymbol", "marker_symbol"
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumnRenamed(
            "mgiMarkerAccessionID", "marker_accession_id"
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumn(
            "external_id",
            concat_ws("-", "pubMedID", "marker_accession_id", "mp_term_id"),
        )
        mgi_phenotype_df = mgi_phenotype_df.withColumn(
            "doc_id", monotonically_increasing_id().astype(StringType())
        )
        mgi_phenotype_df.write.parquet(output_path)
