import os
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from impc_etl.jobs.load.impc_api.impc_bulk_api_mapper import ImpcBulkApiMapper
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


class ImpcBulkApiMongoLoader(PySparkTask):
    name = "IMPC_Bulk_API_Mongo_Loader"
    embryo_data_json_path = luigi.Parameter()
    mongodb_database = luigi.Parameter()
    output_path = luigi.Parameter()
    #   packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
    mongodb_connection_uri = luigi.Parameter()
    mongodb_genes_collection = luigi.Parameter()
    mongodb_replica_set = luigi.Parameter()

    @property
    def packages(self):
        return super().packages + super(PySparkTask, self).packages

    def requires(self):
        return [
            ImpcBulkApiMapper(),
        ]

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}bulk_api/_LOAD_SUCCESS")

    def app_options(self):
        return [
            self.input()[0].path,
        ]

    def write_to_mongo(self, df: DataFrame, class_name: str, collection_name: str):
        df = df.withColumn("_class", lit(class_name))
        df.write.format("mongodb").mode("overwrite").option(
            "spark.mongodb.write.uri",
            f"{self.mongodb_connection_uri}/admin?replicaSet={self.mongodb_replica_set}",
        ).option("database", str(self.mongodb_database)).option(
            "collection", collection_name
        ).option(
            "writeConcern.w", "majority"
        ).save()

    def main(self, sc: SparkContext, *args: Any):
        # Drop statistical results from the gene bundle
        # Create an experimental data collection with the observations
        output_path = os.path.dirname(args[0])

        spark = SparkSession(sc)
        gene_search_json_path = output_path + "/gene_search_json/"
        gene_search_df = spark.read.json(gene_search_json_path)

        genes_bundles_json_path = output_path + "/gene_bundles_json/"
        genes_bundles_df = spark.read.json(genes_bundles_json_path)

        experimental_data_json_path = output_path + "/experimental_data_json/"
        experimental_data_df = spark.read.json(experimental_data_json_path)

        statistical_results_json_path = output_path + "/statistical_results_json/"
        statistical_results_df = spark.read.json(statistical_results_json_path)
        statistical_results_df = statistical_results_df.repartition(10000)

        self.write_to_mongo(
            gene_search_df,
            "org.mousephenotype.api.models.Gene",
            "gene_search",
        )

        self.write_to_mongo(
            genes_bundles_df,
            "org.mousephenotype.api.models.GeneBundle",
            "gene_bundles",
        )

        self.write_to_mongo(
            experimental_data_df,
            "org.mousephenotype.api.models.Observation",
            "experimental_data",
        )

        self.write_to_mongo(
            statistical_results_df,
            "org.mousephenotype.api.models.StatisticalResult",
            "statistical_results",
        )
        with self.output().open("w"):
            pass
