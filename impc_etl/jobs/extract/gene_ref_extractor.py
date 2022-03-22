"""
    Allele reference database extractor module.
        This module groups together tasks to extract allele data from the GenTar Reference DataBase.
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

from impc_etl.workflow.config import ImpcConfig

gene_ref_cols = [
    "ensembl_chromosome",
    "ensembl_gene_acc_id",
    "ensembl_start",
    "ensembl_stop",
    "ensembl_strand",
    "entrez_gene_acc_id",
    "genome_build",
    "mgi_gene_acc_id",
    "name",
    "mgi_cm",
    "mgi_chromosome",
    "mgi_start",
    "mgi_stop",
    "mgi_strand",
    "ncbi_chromosome",
    "ncbi_start",
    "ncbi_stop",
    "ncbi_strand",
    "symbol",
    "type",
    "subtype",
]


class ExtractGeneRef(PySparkTask):
    """
    PySparkTask task to extract allele reference data from the GenTar reference database.
    """

    #: Name of the Spark task
    name = "IMPC_Extract_Gene_Ref"

    #: Reference DB connection JDBC string
    ref_db_jdbc_connection_str = luigi.Parameter()

    #: Reference DB user
    ref_database_user = luigi.Parameter()

    #: Reference DB password
    ref_database_password = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr16.0/parquet/gene_ref_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}gene_ref_parquet")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.ref_db_jdbc_connection_str,
            self.ref_database_user,
            self.ref_database_password,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        DCC Extractor job runner
        :param list argv: the list elements should be:
        """
        jdbc_connection_str = args[0]
        ref_database_user = args[1]
        ref_database_password = args[2]
        output_path = args[3]

        db_properties = {
            "user": ref_database_user,
            "password": ref_database_password,
            "driver": "org.postgresql.Driver",
        }

        spark = SparkSession(sc)

        sql_query = """
        SELECT mouse_gene.*, synonym FROM public.mouse_gene LEFT JOIN public.mouse_gene_synonym_relation ON mouse_gene.id = mouse_gene_synonym_relation.mouse_gene_id LEFT JOIN public.mouse_gene_synonym ON mouse_gene_synonym_relation.mouse_gene_synonym_id = mouse_gene_synonym.id;
        """

        mouse_gene_df = spark.read.jdbc(
            jdbc_connection_str,
            table=f"(SELECT CAST(id AS BIGINT) AS numericId, * FROM ({sql_query})) AS mouse_gene_df",
            properties=db_properties,
            numPartitions=10,
            column="numericId",
            lowerBound=0,
            upperBound=100000,
        )
        mouse_gene_df = mouse_gene_df.groupBy(
            [col_name for col_name in mouse_gene_df.columns if col_name != "synonym"]
        ).agg(collect_set("synonym").alias("synonyms"))
        mouse_gene_df.select(*gene_ref_cols).write.parquet(output_path)
