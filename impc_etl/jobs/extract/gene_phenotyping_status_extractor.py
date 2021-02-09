import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession

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
        imits_gene_status_df = spark.read.csv(self.imits_gene_status_path, header=True)
        gentar_gene_status_df = spark.read.csv(
            self.gentar_gene_status_path, header=True
        )
        # Renaming gentar TSV columns to match both imits and the gene core
        gentar_column_map = {
            "Gene Symbol": "gene_marker_symbol",
            "MGI ID": "gene_mgi_accession_id",
            "Assignment Status": "gene_assignment_status",
            "ES Null Production Status": "null_allele_production_status",
            "ES Conditional Production Status": "conditional_allele_production_status",
            "Crispr Production Status": "crispr_allele_production_status",
        }
        for col_name in gentar_gene_status_df.columns:
            gentar_gene_status_df = gentar_gene_status_df.withColumnRenamed(
                col_name, gentar_column_map[col_name]
            )

        gene_status_df = imits_gene_status_df.join(
            gentar_gene_status_df,
            ["gene_mgi_accession_id", "gene_marker_symbol"],
            "full",
        )
        gene_status_df.printSchema()
        gene_status_df.select(gentar_gene_status_df.columns).show(truncate=False)
