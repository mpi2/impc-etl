"""
    Gene production status report extractor.
"""
from typing import Dict, List

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
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

from impc_etl.jobs.extract.product_report_extractor import ProductReportExtractor
from impc_etl.workflow import SmallPySparkTask
from impc_etl.workflow.config import ImpcConfig


class GeneProductionStatusExtractor(SmallPySparkTask):
    """
    PySpark Task class to extract gene status information from tracking systems reports.
    This also maps the different status in the reports to the ones we use on the website.
    It takes as an input the Gene Interest report from gentar (https://www.gentar.org/tracker-api/api/reports/gene_interest)
    And the Products report from Gentar ().
    The output is a Parquet file containing a full report of the different Statuses available for a Gene.
    """

    #: Name of the Spark task
    name: str = "IMPC_Gene_Production_Status_Extractor"

    #: Path in the filesystem (local or HDFS) to the GenTar gene status report
    gentar_gene_status_path: luigi.Parameter = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task (e.g. impc/dr15.2/parquet/gene_status_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}gene_status_parquet")

    def requires(self):
        """
        Returns the list of dependencies for the PySpark task.
        """
        return [ProductReportExtractor()]

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            # Output of the only dependency
            self.input()[0].path,
            self.gentar_gene_status_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark: SparkSession = SparkSession(sc)
        product_parquet_path = args[0]
        gentar_gene_status_path = args[1]
        output_path = args[2]

        gentar_gene_status_df = spark.read.csv(
            gentar_gene_status_path, header=True, sep="\t"
        )

        product_df = spark.read.parquet(product_parquet_path)
        product_df = product_df.select("mgi_accession_id", "type").distinct()
        has_products = product_df.groupBy("mgi_accession_id").agg(
            collect_set("type").alias("product_types")
        )

        # Renaming gentar TSV columns to match the gene core
        gentar_column_map = {
            "Gene Symbol": "marker_symbol",
            "MGI ID": "mgi_accession_id",
            "Assignment Status": "assignment_status",
            "ES Null Production Status": "null_allele_production_status",
            "ES Null Production Work Unit": "null_allele_production_centre",
            "ES Conditional Production Status": "conditional_allele_production_status",
            "ES Conditional Production Work Unit": "conditional_allele_production_centre",
            "Crispr Production Status": "crispr_allele_production_status",
            "Crispr Production Work Unit": "crispr_allele_production_centre",
            "Crispr Conditional Production Status": "crispr_conditional_allele_production_status",
            "Crispr Conditional Production Work Unit": "crispr_conditional_allele_production_centre",
            "Early Adult Phenotyping Status": "phenotyping_status",
        }
        for col_name in gentar_gene_status_df.columns:
            new_col_name = (
                gentar_column_map[col_name]
                if "mgi_accession_id" not in gentar_column_map[col_name]
                else gentar_column_map[col_name]
            )
            gentar_gene_status_df = gentar_gene_status_df.withColumnRenamed(
                col_name, new_col_name
            )
        gene_status_df = gentar_gene_status_df

        # Resolve assigment statuses
        gene_statuses_cols = [
            "mgi_accession_id",
            "assignment_status",
            "null_allele_production_status",
            "conditional_allele_production_status",
            "crispr_allele_production_status",
            "crispr_conditional_allele_production_status",
            "conditional_allele_production_status",
            "es_cell_production_status",
            "mouse_production_status",
            "phenotyping_status",
        ]

        mice_production_status_cols = [
            "null_allele_production_status",
            "conditional_allele_production_status",
            "crispr_allele_production_status",
            "crispr_conditional_allele_production_centre",
        ]

        allele_mouse_prod_status_map = {
            "Chimeras obtained": "Assigned for Mouse Production and Phenotyping",
            "Micro-injection in progress": "Assigned for Mouse Production and Phenotyping",
            "Mouse Allele Modification Genotype Confirmed": "Mice Produced",
            "Rederivation Complete": "Mice Produced",
            "Rederivation Started": "Mice Produced",
            "Cre Excision Started": "Started",
            "Cre Excision Complete": "Started",
            "Genotype confirmed": "Mice Produced",
            "Phenotype Attempt Registered": "Mice Produced",
        }

        # Collapse mouse production status
        gene_status_df = collapse_production_status(
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

        # Collapse ES Cells production status
        gene_status_df = collapse_production_status(
            spark,
            gene_status_df,
            assignment_status_es_cells_prod_status_map,
            ["null_allele_production_status", "conditional_allele_production_status"],
            "es_cell_production_status",
        )

        # Map ES Cell production status
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
            map_allele_es_cells_udf("conditional_allele_production_status"),
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
        gene_prod_status_map = {
            "Aborted - ES Cell QC Failed": "Selected for production",
            "Assigned - ES Cell QC Complete": "Selected for production",
            "Assigned - ES Cell QC In Progress": "Selected for production",
            "Assigned": "Selected for production",
            "Conflict": "Selected for production",
            "Inspect - Conflict": "Selected for production",
            "Inspect - GLT Mouse": "Selected for production",
            "Inspect - MI Attempt": "Selected for production",
            "Interest": "Selected for production",
            # Production statuses
            "Chimeras obtained": "Started",
            "Chimeras/Founder Obtained": "Started",
            "Mouse Allele Modification Genotype Confirmed": "Genotype confirmed mice",
            # Changed
            "Cre Excision Started": "Started",
            "Founder obtained": "Started",
            "Micro-injection aborted": "Started",
            "Micro-injection in progress": "Started",
            # Changed
            "Cre Excision Complete": "Started",
            "Genotype confirmed": "Genotype confirmed mice",
            "Inactive": "Withdrawn",
            "Withdrawn": "Withdrawn",
        }

        for status_col in gene_statuses_cols:
            if status_col not in ["mgi_accession_id", "phenotyping_status"]:
                gene_status_df = map_status(
                    spark, gene_status_df, gene_prod_status_map, status_col
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
                gene_status_df = map_status(
                    spark, gene_status_df, gentar_gene_prod_status_map, status_col
                )
        phenotyping_status_map = {
            "Phenotype Production Aborted": "NULL",
            "Phenotype Attempt Registered": "Phenotype attempt registered",
            "Phenotyping Registered": "Phenotype attempt registered",
            "Rederivation Complete": "Phenotyping started",
            "Rederivation Started": "Phenotyping started",
            "Phenotyping Started": "Phenotyping started",
            "Phenotyping All Data Processed": "Phenotyping started",
            "Phenotyping Complete": "Phenotyping data available",
            "Phenotyping Finished": "Phenotyping finished",
        }

        get_status_hierarchy_udf = udf(
            lambda x: list(phenotyping_status_map.values()).index(
                phenotyping_status_map[x]
            )
            if x is not None
            else 0,
            IntegerType(),
        )
        gene_status_df = map_status(
            spark, gene_status_df, phenotyping_status_map, "phenotyping_status"
        )
        gene_status_df.select(gene_statuses_cols).distinct().write.parquet(output_path)


def _create_status_map(spark: SparkSession, status_map_dict: Dict) -> DataFrame:
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


def map_status(
    spark: SparkSession,
    gene_status_df: DataFrame,
    status_map_dict: Dict,
    status_col_to_map: str,
) -> DataFrame:
    status_map_df = _create_status_map(spark, status_map_dict)
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
    spark: SparkSession,
    gene_status_df: DataFrame,
    status_map_dict: Dict,
    src_status_list: List[str],
    target_status_col: str,
) -> DataFrame:
    status_map_df = _create_status_map(spark, status_map_dict)

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
