import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql.functions import col, collect_set, explode
from pyspark.sql.session import SparkSession

from impc_etl.jobs.extract import OntologyMetadataExtractor
from impc_etl.workflow.config import ImpcConfig
from impc_etl.workflow.load import (
    GeneCoreLoader,
    ExperimentToObservationMapper,
    StatsResultsCoreLoader,
)


class BatchQueryLoader(PySparkTask):
    name = "IMPC_Batch_Query_Loader"
    orthologe_parquet_path = luigi.Parameter()
    ontology_metadata_parquet = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}batch_query_parquet")

    def requires(self):
        return [
            ExperimentToObservationMapper(),
            GeneCoreLoader(),
            StatsResultsCoreLoader(),
            OntologyMetadataExtractor(),
        ]

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.orthologe_parquet_path,
            self.output().path,
        ]

    def main(self, sc, *args):
        spark = SparkSession(sc)
        observations_parquet_path = args[0]
        gene_parquet_path = args[1]
        stats_parquet_path = args[2]
        ontology_metadata_parquet_path = args[3]
        orthologe_parquet_path = args[4]
        output_path = args[5]

        stats_df = spark.read.parquet(stats_parquet_path)
        observations_df = spark.read.parquet(observations_parquet_path)
        gene_df = spark.read.parquet(gene_parquet_path)
        ontology_df = spark.read.parquet(ontology_metadata_parquet_path)
        orthologe_df = spark.read.parquet(orthologe_parquet_path)

        group_by_cols = [
            "gene_symbol",
            "gene_accession_id",
            "allele_symbol",
            "allele_accession_id",
            "life_stage_name",
            "zygosity",
            "strain_name",
            "strain_accession_id",
        ]

        grouped_stats_cols = [
            "mp_term_id",
            "top_level_mp_term_id",
        ]

        stats_df = stats_df.withColumnRenamed("marker_symbol", "gene_symbol")
        stats_df = stats_df.withColumnRenamed(
            "marker_accession_id", "gene_accession_id"
        )
        stats_df = stats_df.withColumn("life_stage_name", explode("life_stage_name"))

        batch_query_df = (
            stats_df.where(col("significant"))
            .groupBy(*group_by_cols)
            .agg(
                *[
                    collect_set(col_name).alias(col_name)
                    for col_name in grouped_stats_cols
                ]
            )
        )

        grouped_obs_cols = [
            "procedure_stable_id",
            "procedure_name",
            "parameter_stable_id",
            "parameter_name",
        ]

        experiment_data = observations_df.groupBy(*group_by_cols).agg(
            *[collect_set(col_name).alias(col_name) for col_name in grouped_obs_cols]
        )

        batch_query_df = batch_query_df.join(
            experiment_data, group_by_cols, "left_outer"
        )

        gene_df = gene_df.select(
            col("mgi_accession_id").alias("gene_accession_id"),
            "ensembl_gene_id",
            "assignment_status",
            "conditional_allele_production_status",
            "es_cell_production_status",
            "mouse_production_status",
            "phenotype_status",
        )

        batch_query_df = batch_query_df.join(gene_df, "gene_accession_id", "left_outer")

        grouped_orth_cols = ["hg_hgnc_acc_id", "hg_symbol"]

        orthologe_df = orthologe_df.withColumnRenamed(
            "mg_mgi_gene_acc_id", "gene_accession_id"
        )
        orthologe_df = orthologe_df.where(
            (col("o_is_max_human_to_mouse") == "max")
            & (col("o_is_max_mouse_to_human") == "max")
            & (col("mmf_category_for_threshold") == "one-to-one")
            & (col("hmf_category_for_threshold") == "one-to-one")
            & (col("o_support_count") >= 5)
        ).select("gene_accession_id", "hg_hgnc_acc_id", "hg_symbol")
        # Remove the ones that have more than one orthologue mmf_category_for_threshold=one-to-one hmf_category_for_threshold=one-to-one
        orthologe_df = orthologe_df.groupBy("gene_accession_id").agg(
            *[collect_set(col_name).alias(col_name) for col_name in grouped_orth_cols]
        )

        batch_query_df = batch_query_df.join(
            orthologe_df, "gene_accession_id", "left_outer"
        )

        batch_query_df.write.parquet(output_path)
