from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import collect_set, struct, lit, col, arrays_zip

from impc_etl.jobs.extract import ProductReportExtractor
from impc_etl.jobs.load.observation_mapper import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.gene_mapper import GeneLoader
from impc_etl.jobs.load.solr.genotype_phenotype_mapper import GenotypePhenotypeLoader
from impc_etl.jobs.load.solr.impc_images_mapper import ImpcImagesLoader
from impc_etl.workflow.config import ImpcConfig

EXCLUDE_PRODUCT_COLUMNS = [
    "marker_mgi_accession_id",
    "marker_type",
    "marker_name",
    "marker_synonym",
    "allele_mgi_accession_id",
    "allele_symbol",
    "allele_has_issue",
    "allele_synonym",
    "associated_product_colony_name",
    "associated_products_vector_names",
    "loa_assays",
]


class ImpcGeneBundleMapper(PySparkTask):
    name = "IMPC_Bundle_Mapper"
    embryo_data_json_path = luigi.Parameter()
    mongodb_database = luigi.Parameter()
    output_path = luigi.Parameter()
    packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    mongodb_connection_uri = luigi.Parameter()
    mongodb_genes_collection = luigi.Parameter()
    mongodb_replica_set = luigi.Parameter()

    @property
    def packages(self):
        return super().packages + super(PySparkTask, self).packages

    def requires(self):
        return [
            ExperimentToObservationMapper(),
            GenotypePhenotypeLoader(),
            ImpcImagesLoader(),
            ProductReportExtractor(),
            GeneLoader(raw_data_in_output="bundled", compress_data_sets=False),
        ]

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}gene_bundle_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.input()[4].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        observations_parquet_path = args[0]
        genotype_phenotype_parquet_path = args[1]
        impc_images_parquet_path = args[2]
        product_parquet_path = args[3]
        gene_core_parquet_path = args[4]
        output_path = args[5]
        spark = SparkSession(sc)

        observations_df = spark.read.parquet(observations_parquet_path)
        genotype_phenotype_df = spark.read.parquet(genotype_phenotype_parquet_path)
        impc_images_df = spark.read.parquet(impc_images_parquet_path)
        product_df = spark.read.parquet(product_parquet_path)
        gene_df: DataFrame = spark.read.parquet(gene_core_parquet_path)
        gene_df = gene_df.withColumnRenamed(
            "datasets_raw_data", "gene_statistical_results"
        )
        gene_df = gene_df.withColumn(
            "_class", lit("org.mousephenotype.api.models.GeneBundle")
        )

        impc_images_df = impc_images_df.withColumnRenamed(
            "gene_accession_id", "mgi_accession_id"
        )

        images_by_gene_df = impc_images_df.groupBy("mgi_accession_id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in impc_images_df.columns
                        if col_name != "mgi_accession_id"
                    ]
                )
            ).alias("gene_images")
        )
        gene_df = gene_df.join(images_by_gene_df, "mgi_accession_id", "left_outer")

        products_by_gene = product_df.groupBy("mgi_accession_id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in product_df.columns
                        if col_name
                        not in ["mgi_accession_id"] + EXCLUDE_PRODUCT_COLUMNS
                    ]
                )
            ).alias("gene_products")
        )
        gene_df = gene_df.join(products_by_gene, "mgi_accession_id", "left_outer")

        parameters_by_gene = observations_df.select(
            "gene_accession_id",
            "pipeline_stable_id",
            "pipeline_name",
            "procedure_stable_id",
            "procedure_name",
            "parameter_stable_id",
            "parameter_name",
        ).distinct()

        parameters_by_gene = parameters_by_gene.groupBy("gene_accession_id").agg(
            collect_set(
                struct(
                    "pipeline_stable_id",
                    "pipeline_name",
                    "procedure_stable_id",
                    "procedure_name",
                    "parameter_stable_id",
                    "parameter_name",
                )
            ).alias("tested_parameters")
        )
        parameters_by_gene = parameters_by_gene.withColumnRenamed(
            "gene_accession_id", "mgi_accession_id"
        )
        gene_df = gene_df.join(parameters_by_gene, "mgi_accession_id", "left_outer")

        gene_df = gene_df.withColumn("_id", col("mgi_accession_id"))
        gene_df.write.format("mongo").mode("append").option(
            "spark.mongodb.output.uri",
            f"{self.mongodb_connection_uri}/admin?replicaSet={self.mongodb_replica_set}",
        ).option("database", str(self.mongodb_database)).option(
            "collection", str(self.mongodb_genes_collection)
        ).save()

        genotype_phenotype_df = genotype_phenotype_df.withColumnRenamed(
            "marker_accession_id", "mgi_accession_id"
        )
        gp_by_gene_df = genotype_phenotype_df.groupBy("mgi_accession_id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in genotype_phenotype_df.columns
                        if col_name != "mgi_accession_id"
                    ]
                )
            ).alias("gene_phenotype_associations")
        )
        gene_bundle_df = gene_df.join(gp_by_gene_df, "mgi_accession_id", "left_outer")

        ## Create gene search index
        gp_mp_term_structured = genotype_phenotype_df.withColumn(
            "significant_mp_term",
            struct(
                "mp_term_id",
                "mp_term_name",
                arrays_zip(
                    col("intermediate_mp_term_id").alias("mp_term_id"),
                    col("intermediate_mp_term_name").alias("mp_term_name"),
                ).alias("intermediate_ancestors"),
                arrays_zip(
                    col("top_level_mp_term_id").alias("mp_term_id"),
                    col("top_level_mp_term_name").alias("mp_term_name"),
                ).alias("top_level_ancestors"),
            ).alias("significant_mp_term"),
        )
        gp_mp_term_structured = gp_mp_term_structured.select(
            "mgi_accession_id", "significant_mp_term"
        )
        gp_mp_term_structured_gene_df = gp_mp_term_structured.groupBy(
            "mgi_accession_id"
        ).agg(collect_set("significant_mp_term").alias("significant_mp_terms"))

        gene_search_df = gene_df.join(
            gp_mp_term_structured_gene_df, "mgi_accession_id", "left_outer"
        )
        gene_search_df = gene_search_df.select(
            col("mgi_accession_id").alias("_id"),
            "mgi_accession_id",
            "marker_name",
            "human_gene_symbol",
            "marker_synonym",
            "assignment_status",
            "crispr_allele_production_status",
            "es_cell_production_status",
            "mouse_production_status",
            "phenotype_status",
            "phenotyping_data_available",
            "tested_parameters",
            col("significant_top_level_mp_terms").alias("significant_phenotype_system"),
            col("not_significant_top_level_mp_terms").alias(
                "non_significant_phenotype_system"
            ),
            "significant_mp_terms",
        )
        gene_search_df = gene_search_df.withColumn(
            "_class", lit("org.mousephenotype.api.models.Gene")
        )
        gene_search_df.write.format("mongo").mode("append").option(
            "spark.mongodb.output.uri",
            f"{self.mongodb_connection_uri}/admin?replicaSet={self.mongodb_replica_set}",
        ).option("database", str(self.mongodb_database)).option(
            "collection", str(self.mongodb_genes_collection) + "_search"
        ).save()
        gene_bundle_df.write.parquet(output_path)
