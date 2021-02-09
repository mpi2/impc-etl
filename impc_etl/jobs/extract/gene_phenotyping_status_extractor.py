import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit

from impc_etl.workflow.config import ImpcConfig


class GenePhenotypingStatusExtractor(PySparkTask):
    name = "IMPC_Gene_Statuses_Extractor"

    imits_gene_status_path = luigi.Parameter()
    gentar_gene_status_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}gene_status_parquet")

    def main(self, sc, *args):
        spark = SparkSession(sc)
        imits_gene_status_df = spark.read.csv(
            self.imits_gene_status_path, header=True, sep="\t"
        )
        gentar_gene_status_df = spark.read.csv(
            self.gentar_gene_status_path, header=True, sep="\t"
        )
        # Renaming imits TSV columns to match the gene core
        for col_name in imits_gene_status_df.columns:
            new_col_name = (
                "imits_" + col_name.replace("gene_", "")
                if "mgi_accession_id" not in col_name
                and "marker_symbol" not in col_name
                else col_name.replace("gene_", "")
            )
            imits_gene_status_df = imits_gene_status_df.withColumnRenamed(
                col_name, new_col_name
            )
        # Renaming gentar TSV columns to match both imits and the gene core
        gentar_column_map = {
            "Gene Symbol": "marker_symbol",
            "MGI ID": "mgi_accession_id",
            "Assignment Status": "assignment_status",
            "ES Null Production Status": "null_allele_production_status",
            "ES Conditional Production Status": "conditional_allele_production_status",
            "Crispr Production Status": "crispr_allele_production_status",
        }
        for col_name in gentar_gene_status_df.columns:
            new_col_name = (
                "gentar_" + col_name
                if "mgi_accession_id" not in col_name
                and "marker_symbol" not in col_name
                else col_name
            )
            gentar_gene_status_df = gentar_gene_status_df.withColumnRenamed(
                col_name, new_col_name
            )
        gene_status_df = imits_gene_status_df.join(
            gentar_gene_status_df,
            ["mgi_accession_id", "marker_symbol"],
            "full",
        )

        gene_statuses_cols = [
            "mgi_accession_id",
            "marker_symbol",
            "assignment_status",
            "null_allele_production_status",
            "conditional_allele_production_status",
            "crispr_allele_production_status",
            "es_cell_production_status",
            "mouse_production_status",
            "phenotyping_status",
        ]

        gene_status_df = self._resolve_assigment_status(gene_status_df)

        gene_status_df = gene_status_df.withColumn(
            "null_allele_production_status",
            when(
                col("gentar_null_allele_production_status").isNull(),
                col("imits_null_allele_production_status"),
            ).otherwise(col("gentar_null_allele_production_status")),
        )

        gene_status_df = gene_status_df.withColumn(
            "conditional_allele_production_status",
            when(
                col("gentar_conditional_allele_production_status").isNull(),
                col("imits_conditional_allele_production_status"),
            ).otherwise(col("gentar_conditional_allele_production_status")),
        )

        gene_status_df = gene_status_df.withColumn(
            "crispr_allele_production_status",
            col("gentar_conditional_allele_production_status"),
        )
        gene_status_df = gene_status_df.withColumn(
            "project_status",
            col("gentar_conditional_allele_production_status"),
        )

        ALLELE_MOUSE_PROD_STATUS_MAP = {
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

        status_map_df_json = spark.sparkContext.parallelize(
            [
                {"allele_production_status": key, "mice_production_status": value}
                for key, value in ALLELE_MOUSE_PROD_STATUS_MAP.items()
            ]
        )
        status_map_df = spark.read.json(status_map_df_json)

        for status_col in [
            "null_allele_production_status",
            "conditional_allele_production_status",
        ]:
            gene_status_df = gene_status_df.withColumn(status_col)

        gene_status_df.printSchema()
        gene_status_df.select(gene_statuses_cols).show(truncate=False)

    def _resolve_assigment_status(self, gene_status_df):
        gene_status_df = gene_status_df.withColumn(
            "assignment_status",
            when(
                col("imits_assignment_status").isNull()
                & col("gentar_assignment_status").isNotNull(),
                col("gentar_assignment_status"),
            )
            .when(
                col("imits_assignment_status").isNotNull()
                & col("gentar_assignment_status").isNull(),
                col("imits_assignment_status"),
            )
            .when(
                col("gentar_assignment_status").startswith("Assigned"),
                col("gentar_assignment_status"),
            )
            .when(
                col("gentar_assignment_status").startswith("Inspect")
                & col("imits_assignment_status").startswith("Assigned"),
                col("imits_assignment_status"),
            )
            .when(
                col("gentar_assignment_status").startswith("Inspect")
                & ~col("imits_assignment_~status").startswith("Assigned"),
                lit("data_issue"),
            )
            .when(
                (
                    col("gentar_assignment_status").startswith("Inactive")
                    | col("gentar_assignment_status").startswith("Abandoned")
                )
                & col("imits_assignment_status").startswith("Assigned"),
                col("imits_assignment_status"),
            )
            .when(
                (
                    col("gentar_assignment_status").startswith("Inactive")
                    | col("gentar_assignment_status").startswith("Abandoned")
                )
                & (
                    col("imits_assignment_status").startswith("Inactive")
                    | col("imits_assignment_status").startswith("Withdrawn")
                ),
                col("gentar_assignment_status"),
            )
            .otherwise(lit("data_issue")),
        )
        return gene_status_df
