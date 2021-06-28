import luigi
from luigi.contrib.spark import PySparkTask
from typing import List, Dict
from pyspark.sql.functions import collect_set, struct


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


class ImpcBundleMapper(PySparkTask):
    name = "IMPC_Bundle_Mapper"
    embryo_data_json_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            GeneExtractor(),
            AlleleExtractor(),
            MGIHomoloGeneExtractor(),
            MGIMrkListExtractor(),
            ObservationsMapper(),
            StatsResultsCoreLoader(),
            OntologyMetadataExtractor(),
            GeneProductionStatusExtractor(),
            GenotypePhenotypeCoreLoader(),
            ImpcImagesCoreLoader(),
            ProductExtractor(),
        ]

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}_gene_bundle_parquet")

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
            self.input()[8].path,
            self.input()[10].path,
            self.output().path,
        ]

    def main(self, sc, *argv):
        print(argv)
        imits_gene_parquet_path = argv[1]
        imits_allele_parquet_path = argv[2]
        mgi_homologene_report_parquet_path = argv[3]
        mgi_mrk_list_report_parquet_path = argv[4]
        embryo_data_json_path = argv[5]
        observations_parquet_path = argv[6]
        stats_results_parquet_path = argv[7]
        ontology_metadata_parquet_path = argv[8]
        gene_production_status_path = argv[9]
        genotype_phenotype_parquet_path = argv[10]
        impc_images_parquet_path = argv[11]
        product_parquet_path = argv[12]
        output_path = argv[13]

        spark = SparkSession.builder.getOrCreate()
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
            ).alias("significant_phenotype_associations")
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
            ).alias("impc_images")
        )
        gene_df = gene_df.join(images_by_gene_df, "mgi_accession_id", "left_outer")

        products_by_gene = product_df.groupBy("mgi_accession_id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in product_df.columns
                        if col_name != "mgi_accession_id"
                    ]
                )
            ).alias("gene_products")
        )

        gene_df = gene_df.join(products_by_gene, "mgi_accession_id", "left_outer")
        gene_df.write.parquet(output_path)
