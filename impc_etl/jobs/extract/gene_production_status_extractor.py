import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    col,
    lit,
    lower,
    udf,
    collect_set,
    array_contains,
)
from pyspark.sql.types import StringType, IntegerType

from impc_etl.workflow.config import ImpcConfig
from impc_etl.workflow.extraction import ProductExtractor


class GeneProductionStatusExtractor(PySparkTask):
    name = "IMPC_Gene_Production_Status_Extractor"

    imits_gene_status_path = luigi.Parameter()
    gentar_gene_status_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}gene_status_parquet")

    def requires(self):
        return [ProductExtractor()]

    def app_options(self):
        return [
            self.input()[0].path,
            self.imits_gene_status_path,
            self.gentar_gene_status_path,
            self.output().path,
        ]

    def main(self, sc, *args):
        spark = SparkSession(sc)
        product_parquet_path = args[0]
        imits_gene_status_path = args[1]
        gentar_gene_status_path = args[2]
        output_path = args[3]

        imits_gene_status_df = spark.read.csv(
            imits_gene_status_path, header=True, sep="\t"
        )
        gentar_gene_status_df = spark.read.csv(
            gentar_gene_status_path, header=True, sep="\t"
        )

        product_df = spark.read.parquet(product_parquet_path)
        product_df = product_df.select("mgi_accession_id", "type").distinct()
        has_products = product_df.groupBy("mgi_accession_id").agg(
            collect_set("type").alias("product_types")
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
            "Early Adult Phenotyping Status": "phenotyping_status",
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
            col("gentar_crispr_allele_production_status"),
        )

        mice_production_status_cols = [
            "null_allele_production_status",
            "conditional_allele_production_status",
            "crispr_allele_production_status",
        ]

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

        gene_status_df = self.collapse_production_status(
            spark,
            gene_status_df,
            allele_mouse_prod_status_map,
            mice_production_status_cols,
            "mouse_production_status",
        )

        assignment_status_es_cells_prod_status_map = {
            "Aborted - ES Cell QC Failed": "Not Assigned for ES Cell Production",
            "Assigned - ES Cell QC In Progress": "Assigned for ES Cell Production",
            "Assigned - ES Cell QC Complete": "ES Cells Produced",
        }

        gene_status_df = self.collapse_production_status(
            spark,
            gene_status_df,
            assignment_status_es_cells_prod_status_map,
            ["imits_assignment_status"],
            "es_cell_production_status",
        )

        allele_es_cells_prod_status_map = {
            "Micro-injection in progress": "Assigned for ES Cell Production",
            "Chimeras obtained": "ES Cells Produced",
            "Genotype confirmed": "ES Cells Produced",
            "Micro-injection aborted": "Not Assigned for ES Cell Production",
        }

        map_allele_es_cells_udf = udf(
            lambda x: allele_es_cells_prod_status_map[x]
            if x in allele_es_cells_prod_status_map
            else None,
            StringType(),
        )

        gene_status_df = gene_status_df.withColumn(
            "es_cell_production_status",
            when(
                col("imits_assignment_status") == "Assigned",
                when(
                    col("conditional_allele_production_status").isNotNull(),
                    map_allele_es_cells_udf("conditional_allele_production_status"),
                ).otherwise(lit("Assigned - ES Cell QC In Progress")),
            ).otherwise(col("es_cell_production_status")),
        )
        gene_status_df = gene_status_df.join(
            has_products, "mgi_accession_id", "left_outer"
        )
        gene_status_df = gene_status_df.withColumn(
            "es_cell_production_status",
            when(
                (
                    col("es_cell_production_status").isNull()
                    | (col("es_cell_production_status") != "ES Cells Produced")
                )
                & (array_contains("product_types", "es_cell")),
                lit("ES Cells Produced"),
            ).otherwise(col("es_cell_production_status")),
        )
        imits_gene_prod_status_map = {
            "Aborted - ES Cell QC Failed": "Selected for production",
            "Assigned - ES Cell QC Complete": "Selected for production",
            "Assigned - ES Cell QC In Progress": "Selected for production",
            "Assigned": "Selected for production",
            "Conflict": "Selected for production",
            "Inspect - Conflict": "Selected for production",
            "Inspect - GLT Mouse": "Selected for production",
            "Inspect - MI Attempt": "Selected for production",
            "Interest": "Selected for production",
            "Chimeras obtained": "Started",
            "Chimeras/Founder obtained": "Started",
            "Cre Excision Started": "Started",
            "Founder obtained": "Started",
            "Micro-injection aborted": "Started",
            "Micro-injection in progress": "Started",
            "Cre Excision Complete": "Genotype confirmed mice",
            "Genotype confirmed": "Genotype confirmed mice",
            "Inactive": "Withdrawn",
            "Withdrawn": "Withdrawn",
        }

        for status_col in gene_statuses_cols:
            if status_col != "mgi_accession_id":
                gene_status_df = self.map_status(
                    spark, gene_status_df, imits_gene_prod_status_map, status_col
                )

        gentar_gene_prod_status_map = {
            "Attempt In Progress": "Started",
            "Embryos Obtained": "Started",
            "Founder Obtained": "Started",
            "Genotype In Progress": "Started",
            "Genotype Not Confirmed": "Started",
            "Plan Created": "Selected for production",
            "Genotype Confirmed": "Genotype Confirmed Mice",
            "Abandoned": "Withdrawn",
            "Attempt Aborted": "Withdrawn",
            "Colony Aborted": "Withdrawn",
            "Genotype Extinct": "Withdrawn",
            "Inactive": "Withdrawn",
            "Plan Abandoned": "Withdrawn",
        }

        for status_col in gene_statuses_cols:
            if status_col not in ["mgi_accession_id", "phenotyping_status"]:
                gene_status_df = self.map_status(
                    spark, gene_status_df, gentar_gene_prod_status_map, status_col
                )
        phenotyping_status_map = {
            "Phenotype Production Aborted": "NULL",
            "Phenotype Attempt Registered": "Phenotype attempt registered",
            "Phenotyping Registered": "Phenotype attempt registered",
            "Rederivation Complete": "Phenotyping started",
            "Phenotyping Started": "Phenotyping started",
            "Phenotyping All Data Processed": "Phenotyping started",
            "Phenotyping Complete": "Phenotyping data available",
        }

        get_status_hierarchy_udf = udf(
            lambda x: list(phenotyping_status_map.values()).index(
                phenotyping_status_map[x]
            )
            if x is not None
            else 0,
            IntegerType(),
        )
        gene_status_df = gene_status_df.withColumn(
            "phenotyping_status",
            when(
                col("imits_phenotyping_status").isNull()
                & col("gentar_phenotyping_status").isNotNull(),
                col("gentar_phenotyping_status"),
            )
            .when(
                col("imits_phenotyping_status").isNotNull()
                & col("gentar_phenotyping_status").isNull(),
                col("gentar_phenotyping_status"),
            )
            .when(
                col("imits_phenotyping_status").isNotNull()
                & col("gentar_phenotyping_status").isNotNull(),
                when(
                    get_status_hierarchy_udf("imits_phenotyping_status")
                    > get_status_hierarchy_udf("gentar_phenotyping_status"),
                    col("imits_phenotyping_status"),
                ).otherwise(col("gentar_phenotyping_status")),
            )
            .otherwise(lit(None)),
        )
        gene_status_df = self.map_status(
            spark, gene_status_df, phenotyping_status_map, "phenotyping_status"
        )
        gene_status_df.select(gene_statuses_cols).distinct().write.parquet(output_path)

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

    def _create_status_map(self, spark, status_map_dict):
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
        return status_map_df

    def map_status(self, spark, gene_status_df, status_map_dict, status_col_to_map):
        status_map_df = self._create_status_map(spark, status_map_dict)
        gene_status_df = gene_status_df.join(
            status_map_df,
            lower(col(status_col_to_map)) == lower(col("src_production_status")),
            "left_outer",
        )
        gene_status_df = gene_status_df.withColumn(
            status_col_to_map,
            when(
                (
                    col("dst_production_status").isNotNull()
                    & (col("dst_production_status") != "NULL")
                ),
                col("dst_production_status"),
            )
            .when(col("dst_production_status") == "NULL", lit(None))
            .otherwise(col(status_col_to_map)),
        )
        gene_status_df = gene_status_df.drop(
            "src_production_status", "dst_production_status"
        )
        return gene_status_df

    def collapse_production_status(
        self, spark, gene_status_df, status_map_dict, src_status_list, target_status_col
    ):
        status_map_df = self._create_status_map(spark, status_map_dict)

        gene_status_df = gene_status_df.withColumn(
            target_status_col, lit(None).astype(StringType())
        )
        get_status_hierarchy_udf = udf(
            lambda x: list(status_map_dict.values()).index(x) if x is not None else 0,
            IntegerType(),
        )

        for status_col in src_status_list:
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
