"""
    Allele reference database extractor module.
        This module groups together tasks to extract allele data from the GenTar Reference DataBase.
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from impc_etl.workflow.config import ImpcConfig

allele_ref_cols = [
    "allele_mgi_accession_id",
    "allele_symbol",
    "allele_name",
    "allele_description",
    "allele_image",
    "gene_mgi_accession_id",
    "gene_symbol",
]


class ExtractAlleleRef(PySparkTask):
    """
    PySparkTask task to extract allele reference data from the GenTar reference database.
    """

    #: Name of the Spark task
    name = "IMPC_Extract_Allele_Ref"

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
        (e.g. impc/dr16.0/parquet/allele_ref_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}allele_ref_parquet")

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

        mouse_allele_df = self._get_table_df(
            spark, db_properties, jdbc_connection_str, "mouse_allele"
        )
        mgi_phenotypic_allele_df = self._get_table_df(
            spark, db_properties, jdbc_connection_str, "mgi_phenotypic_allele"
        )

        mgi_allele_df = self._get_table_df(
            spark, db_properties, jdbc_connection_str, "mgi_allele"
        )

        mgi_phenotypic_allele_df = mgi_phenotypic_allele_df.select(
            *[
                col_name
                for col_name in mgi_phenotypic_allele_df.columns
                if col_name not in mouse_allele_df.columns
            ]
        )

        mouse_allele_df = mouse_allele_df.join(
            mgi_phenotypic_allele_df, col("id") == col("mouse_allele_id"), "left_outer"
        )

        mgi_allele_df = mgi_allele_df.select(
            *[
                col_name
                for col_name in mgi_allele_df.columns
                if col_name not in mouse_allele_df.columns
            ]
        )

        mouse_allele_df = mouse_allele_df.join(
            mgi_allele_df, col("id") == col("mouse_allele_id"), "left_outer"
        )
        mouse_allele_df.write.parquet(output_path)

    def _get_table_df(
        self, spark, db_properties, jdbc_connection_str, table_name
    ) -> DataFrame:
        return spark.read.jdbc(
            jdbc_connection_str,
            table=f"(SELECT CAST(id AS BIGINT) AS numericId, * FROM {table_name}) AS mouse_allele_df",
            properties=db_properties,
            numPartitions=10,
            column="numericId",
            lowerBound=0,
            upperBound=100000,
        )
