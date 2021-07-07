import luigi
from luigi.contrib.spark import PySparkTask
from typing import List, Dict
from pyspark.sql.functions import collect_set, struct, lit

from impc_etl.jobs.extract.gene_production_status_extractor import (
    GeneProductionStatusExtractor,
)
from impc_etl.workflow.config import ImpcConfig
from pyspark.sql import SparkSession, DataFrame
from impc_etl.jobs.load.solr.gene_mapper import get_gene_core_df, IMITS_GENE_COLUMNS
from impc_etl.workflow.extraction import (
    GeneExtractor,
    AlleleExtractor,
    MGIHomoloGeneExtractor,
    MGIMrkListExtractor,
    OntologyMetadataExtractor,
    ProductExtractor,
)
from impc_etl.workflow.load import (
    ObservationsMapper,
    StatsResultsCoreLoader,
    GenotypePhenotypeCoreLoader,
    ImpcImagesCoreLoader,
)

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


class ImpcBundleMapper(PySparkTask):
    name = "IMPC_Bundle_Mapper"
    embryo_data_json_path = luigi.Parameter()
    mongodb_database = luigi.Parameter()
    mongodb_collection = luigi.Parameter()
    output_path = luigi.Parameter()
    packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    mongodb_connection_uri = luigi.Parameter()
    mongodb_database = luigi.Parameter()
    mongodb_collection = luigi.Parameter()
    mongodb_replica_set = luigi.Parameter()

    @property
    def packages(self):
        return super().packages + super(PySparkTask, self).packages

    def requires(self):
        return [
            GeneExtractor(),
            AlleleExtractor(),
            MGIHomoloGeneExtractor(),
            MGIMrkListExtractor(),
            ObservationsMapper(),
            StatsResultsCoreLoader(raw_data_in_output="bundled"),
            OntologyMetadataExtractor(),
            GeneProductionStatusExtractor(),
            GenotypePhenotypeCoreLoader(),
            ImpcImagesCoreLoader(),
            ProductExtractor(),
        ]

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}gene_bundle_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.embryo_data_json_path,
            self.input()[4].path,
            self.input()[5].path,
            self.input()[6].path,
            self.input()[7].path,
            self.input()[8].path,
            self.input()[9].path,
            self.input()[10].path,
            self.output().path,
        ]

    def main(self, sc, *argv):
        imits_gene_parquet_path = argv[0]
        imits_allele_parquet_path = argv[1]
        mgi_homologene_report_parquet_path = argv[2]
        mgi_mrk_list_report_parquet_path = argv[3]
        embryo_data_json_path = argv[4]
        observations_parquet_path = argv[5]
        stats_results_parquet_path = argv[6]
        ontology_metadata_parquet_path = argv[7]
        gene_production_status_path = argv[8]
        genotype_phenotype_parquet_path = argv[9]
        impc_images_parquet_path = argv[10]
        product_parquet_path = argv[11]
        output_path = argv[12]
        spark = SparkSession(sc)

        imits_gene_df = spark.read.parquet(imits_gene_parquet_path).select(
            IMITS_GENE_COLUMNS
        )
        imits_allele_df = spark.read.parquet(imits_allele_parquet_path)
        mgi_homologene_df = spark.read.parquet(mgi_homologene_report_parquet_path)
        mgi_mrk_list_df = spark.read.parquet(mgi_mrk_list_report_parquet_path)
        mgi_mrk_list_df = mgi_mrk_list_df.select(
            [
                "mgi_accession_id",
                "chr",
                "genome_coordinate_start",
                "genome_coordinate_end",
                "strand",
            ]
        )
        embryo_data_df = spark.read.json(embryo_data_json_path, mode="FAILFAST")
        observations_df = spark.read.parquet(observations_parquet_path)
        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        ontology_metadata_df = spark.read.parquet(ontology_metadata_parquet_path)
        gene_production_status_df = spark.read.parquet(gene_production_status_path)
        genotype_phenotype_df = spark.read.parquet(genotype_phenotype_parquet_path)
        impc_images_df = spark.read.parquet(impc_images_parquet_path)
        product_df = spark.read.parquet(product_parquet_path)
        gene_df: DataFrame = get_gene_core_df(
            imits_gene_df,
            imits_allele_df,
            mgi_homologene_df,
            mgi_mrk_list_df,
            embryo_data_df,
            observations_df,
            stats_results_df,
            ontology_metadata_df,
            gene_production_status_df,
            False,
        )
        gene_df = gene_df.withColumnRenamed(
            "datasets_raw_data", "gene_statistical_results"
        )
        gene_df = gene_df.withColumn(
            "_class", lit("org.mousephenotype.api.models.GeneBundle")
        )
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
        gene_df = gene_df.join(gp_by_gene_df, "mgi_accession_id", "left_outer")

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
        gene_df.write.format("mongo").mode("append").option(
            "spark.mongodb.output.uri",
            f"{self.mongodb_connection_uri}/admin?replicaSet={self.mongodb_replica_set}",
        ).option("database", str(self.mongodb_database)).option(
            "collection", str(self.mongodb_collection)
        ).save()
        stats_results_df.write.format("mongo").mode("append").option(
            "spark.mongodb.output.uri",
            f"{self.mongodb_connection_uri}/admin?replicaSet={self.mongodb_replica_set}",
        ).option("database", str(self.mongodb_database)).option(
            "collection", "statistical_results"
        ).save()
        gene_df.write.parquet(output_path)
