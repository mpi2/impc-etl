import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, lower, udf
from pyspark.sql.types import StringType, IntegerType

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
                "gentar_" + gentar_column_map[col_name]
                if "mgi_accession_id" not in gentar_column_map[col_name]
                else gentar_column_map[col_name]
            )
            gentar_gene_status_df = gentar_gene_status_df.withColumnRenamed(
                col_name, new_col_name
            )
        gene_status_df = imits_gene_status_df.join(
            gentar_gene_status_df,
            "mgi_accession_id",
            "full",
        )

        gene_statuses_cols = [
            "mgi_accession_id",
            "assignment_status",
            "null_allele_production_status",
            "conditional_allele_production_status",
            "crispr_allele_production_status",
            "es_cell_production_status",
            "mouse_production_status",
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
            col("gentar_crispr_allele_production_status"),
        )

        allele_mouse_prod_status_map = {
            "Chimeras obtained": "Assigned for Mouse Production and Phenotyping",
            "Micro-injection in progress": "Assigned for Mouse Production and Phenotyping",
            "Cre Excision Complete": "Mice Produced",
            "Cre Excision Started": "Mice Produced",
            "Genotype confirmed": "Mice Produced",
            "Phenotype Attempt Registered": "Mice Produced",
            "Rederivation Complete": "Mice Produced",
            "Rederivation Started": "Mice Produced",
        }

        gene_status_df = self.map_status(
            spark,
            gene_status_df,
            allele_mouse_prod_status_map,
            "mouse_production_status",
        )

        allele_es_cells_prod_status_map = {
            "No ES Cell Production": "Not Assigned for ES Cell Production",
            "ES Cell Production in Progress": "Assigned for ES Cell Production",
            "ES Cell Targeting Confirmed": "ES Cells Produced",
        }

        gene_status_df = self.map_status(
            spark,
            gene_status_df,
            allele_es_cells_prod_status_map,
            "es_cell_production_status",
        )
        gene_status_df.select(gene_statuses_cols).distinct().write.parquet(
            self.output().path
        )

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
                & ~col("imits_assignment_status").startswith("Assigned"),
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
            .otherwise(lit(None)),
        )
        return gene_status_df

    def map_status(self, spark, gene_status_df, status_map_dict, target_status_col):
        status_map_df_json = spark.sparkContext.parallelize(
            [
                {
                    "src_production_status": key,
                    "dst_production_status": value,
                }
                for key, value in status_map_dict.items()
            ]
        )
        status_map_df = spark.read.json(status_map_df_json)
        gene_status_df = gene_status_df.withColumn(
            target_status_col, lit(None).astype(StringType())
        )
        get_status_hierarchy_udf = udf(
            lambda x: list(status_map_dict.values()).index(x) if x is not None else 0,
            IntegerType(),
        )

        for status_col in [
            "null_allele_production_status",
            "conditional_allele_production_status",
            "crispr_allele_production_status",
        ]:
            gene_status_df = gene_status_df.join(
                status_map_df,
                lower(col(status_col)) == lower(col("src_production_status")),
                "left_outer",
            )
            gene_status_df = gene_status_df.withColumn(
                target_status_col,
                when(
                    col(target_status_col).isNull()
                    & col("dst_production_status").isNotNull(),
                    col("dst_production_status"),
                )
                .when(
                    get_status_hierarchy_udf("dst_production_status")
                    > get_status_hierarchy_udf(target_status_col),
                    col("dst_production_status"),
                )
                .otherwise(col(target_status_col)),
            )
            gene_status_df = gene_status_df.drop(
                "src_production_status", "dst_production_status"
            )
        return gene_status_df
