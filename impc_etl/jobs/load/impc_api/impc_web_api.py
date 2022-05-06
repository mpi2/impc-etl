from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    collect_set,
    lit,
    col,
    array,
    concat,
    when,
    size,
    filter,
    regexp_extract,
)

from impc_etl.jobs.extract import ProductReportExtractor
from impc_etl.jobs.load.observation_mapper import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.gene_mapper import GeneLoader
from impc_etl.jobs.load.solr.genotype_phenotype_mapper import GenotypePhenotypeLoader
from impc_etl.jobs.load.solr.impc_images_mapper import ImpcImagesLoader
from impc_etl.workflow.config import ImpcConfig


class ImpcGeneBundleMapper(PySparkTask):
    name = "IMPC_Web_API_Mapper"
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

        product_df = spark.read.parquet(product_parquet_path)
        gene_df = spark.read.parquet(gene_core_parquet_path)

        order_df = (
            product_df.groupBy(
                *[
                    col_name
                    for col_name in product_df.columns
                    if col_name not in ["type", "tissue_enquiry_links"]
                ]
            )
            .agg(
                collect_set("type").alias("available_products"),
                collect_set("tissue_enquiry_links").alias("tissue_enquiry_links"),
            )
            .withColumn(
                "available_products",
                when(
                    size("tissue_enquiry_links") > 0,
                    concat("available_products", array(lit("tissue"))),
                ).otherwise(col("available_products")),
            )
        )
        gene_id_symbol = gene_df.select("mgi_accession_id", "marker_symbol").distinct()

        order_df = order_df.join(gene_id_symbol, "marker_symbol")

        gene_order_df = order_df.select(
            "marker_symbol", "allele_name", "allele_description", "available_products"
        ).distinct()
        gene_order_df = gene_order_df.withColumn(
            "_class", lit("org.mousephenotype.web.models.gene.Order")
        )
        gene_order_df.write.format("mongo").mode("append").option(
            "spark.mongodb.output.uri",
            f"{self.mongodb_connection_uri}/admin?replicaSet={self.mongodb_replica_set}",
        ).option("database", str(self.mongodb_database)).option(
            "collection", "gene-order"
        ).save()

        links_fields = [
            "genbank_file",
            "allele_image",
            "allele_simple_image",
            "vector_genbank_file",
            "vector_allele_image",
        ]

        for link_field in links_fields:
            allele_summary_df = order_df.withColumn(
                f"{link_field}_url",
                filter("other_links", lambda x: x.startswith(f"{link_field}:")),
            )
            allele_summary_df = allele_summary_df.withColumn(
                f"{link_field}_url",
                when(
                    size(f"{link_field}_url") > 0,
                    regexp_extract(
                        col(f"{link_field}_url").getItem(0), f"{link_field}:(.*)", 1
                    ),
                ).otherwise(lit(None)),
            )

        genetic_info_fields = [
            "strain",
            "cassette",
            "cassette_type",
            "parent_es_cell_line",
        ]

        for genetic_info_field in genetic_info_fields:
            allele_summary_df = order_df.withColumn(
                genetic_info_field,
                filter(
                    "genetic_info", lambda x: x.startswith(f"{genetic_info_field}:")
                ),
            )
            allele_summary_df = allele_summary_df.withColumn(
                genetic_info_field,
                when(
                    size(genetic_info_field) > 0,
                    regexp_extract(
                        col(genetic_info_field).getItem(0), f"{link_field}:(.*)", 1
                    ),
                ).otherwise(lit(None)),
            )
        ## process by type and then join with the metadata dataframe

        mice_df = (
            allele_summary_df.where(col("type") == "mouse")
            .select(
                col("mgi_accession_id"),
                col("allele_name"),
                col("product_id"),
                col("name").alias("colony_name"),
                col("background_colony_strain"),
                col("production_centre"),
                col("qc_data"),
                col("associated_product_es_cell_name").alias(
                    "es_cell_parent_mouse_colony"
                ),
            )
            .distinct()
        )
