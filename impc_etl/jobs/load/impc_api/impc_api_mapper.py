import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, explode, zip_with, struct

from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.gene_mapper import GeneLoader
from impc_etl.jobs.load.solr.genotype_phenotype_mapper import GenotypePhenotypeLoader
from impc_etl.jobs.load.solr.stats_results_mapper import StatsResultsMapper
from impc_etl.workflow.config import ImpcConfig

GENE_SUMMARY_MAPPINGS = {
    "mgi_accession_id": "geneAccessionId",
    "marker_symbol": "geneSymbol",
    "marker_name": "geneName",
    "marker_synonym": "synonyms",
    "significant_top_level_mp_terms": "significantTopLevelPhenotypes",
    "not_significant_top_level_mp_terms": "notSignificantTopLevelPhenotypes",
    "embryo_data_available": "hasEmbryoImagingData",
}


class ImpcGeneSummaryMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "Impc_Gene_Summary_Mapper"

    #: Path to the CSV gene disease association report
    gene_disease_association_csv_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            GeneLoader(),
            GenotypePhenotypeLoader(),
            ExperimentToObservationMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_summary_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.gene_disease_association_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        gene_parquet_path = args[0]
        genotype_phenotype_parquet_path = args[1]
        observations_parquet_path = args[2]
        gene_disease_association_csv_path = args[3]
        output_path = args[4]

        gene_df = spark.read.parquet(gene_parquet_path)
        genotype_phenotype_df = spark.read.parquet(genotype_phenotype_parquet_path)
        observations_df = spark.read.parquet(observations_parquet_path)
        gene_disease_association_df = spark.read.csv(
            gene_disease_association_csv_path, header=True
        )

        gene_df = gene_df.withColumn("id", col("mgi_accession_id"))
        for col_name in GENE_SUMMARY_MAPPINGS.keys():
            gene_df = gene_df.withColumnRenamed(
                col_name, GENE_SUMMARY_MAPPINGS[col_name]
            )
        genotype_phenotype_df = genotype_phenotype_df.withColumnRenamed(
            "marker_accession_id", "id"
        )
        gp_call_by_gene = genotype_phenotype_df.groupBy("id").count()
        gp_call_by_gene = gp_call_by_gene.withColumnRenamed(
            "count", "significantPhenotypesCount"
        )
        gene_df = gene_df.join(gp_call_by_gene, "id", "left_outer")

        adult_lacz_observations_by_gene = get_lacz_expression_count(
            observations_df, "adult"
        )
        gene_df = gene_df.join(adult_lacz_observations_by_gene, "id", "left_outer")

        embryo_lacz_observations_by_gene = get_lacz_expression_count(
            observations_df, "embryo"
        )
        gene_df = gene_df.join(embryo_lacz_observations_by_gene, "id", "left_outer")

        gene_disease_association_df = gene_disease_association_df.where(
            col("type") == "disease_gene_summary"
        )

        gene_disease_association_df = gene_disease_association_df.groupBy(
            "marker_id"
        ).count()
        gene_disease_association_df = gene_disease_association_df.withColumnRenamed(
            "marker_id", "id"
        )
        gene_disease_association_df = gene_disease_association_df.withColumnRenamed(
            "count", "associatedDiseasesCount"
        )
        gene_df = gene_df.join(gene_disease_association_df, "id", "left_outer")

        gene_df = gene_df.withColumn(
            "hasLacZData",
            (col("adultExpressionObservationsCount") > 0)
            | (col("embryoExpressionObservationsCount") > 0),
        )

        gene_images_flag = observations_df.where(
            col("observation_type") == "image_record"
        )
        gene_images_flag = gene_images_flag.groupBy("gene_accession_id").agg(
            first("observation_id").alias("obs_id")
        )
        gene_images_flag = gene_images_flag.withColumn(
            "hasImagingData", col("obs_id").isNotNull()
        )
        gene_images_flag = gene_images_flag.withColumnRenamed("gene_accession_id", "id")
        gene_df = gene_df.join(gene_images_flag, "id", "left_outer")

        gene_hist_flag = observations_df.where(
            col("procedure_stable_id").contains("HIS")
        )
        gene_hist_flag = gene_hist_flag.groupBy("gene_accession_id").agg(
            first("observation_id").alias("obs_id")
        )
        gene_hist_flag = gene_hist_flag.withColumn(
            "hasHistopathologyData", col("obs_id").isNotNull()
        )
        gene_hist_flag = gene_hist_flag.withColumnRenamed("gene_accession_id", "id")

        gene_df = gene_df.join(gene_hist_flag, "id", "left_outer")

        gene_via_flag = observations_df.where(
            col("parameter_stable_id").isin(
                [
                    "IMPC_VIA_001_001",
                    "IMPC_VIA_002_001",
                    "IMPC_EVL_001_001",
                    "IMPC_EVM_001_001",
                    "IMPC_EVP_001_001",
                    "IMPC_EVO_001_001",
                    "IMPC_VIA_063_001",
                    "IMPC_VIA_064_001",
                    "IMPC_VIA_065_001",
                    "IMPC_VIA_066_001",
                    "IMPC_VIA_067_001",
                    "IMPC_VIA_056_001",
                ]
            )
        )
        gene_via_flag = gene_via_flag.groupBy("gene_accession_id").agg(
            first("observation_id").alias("obs_id")
        )
        gene_via_flag = gene_via_flag.withColumn(
            "hasViabilityData", col("obs_id").isNotNull()
        )
        gene_via_flag = gene_via_flag.withColumnRenamed("gene_accession_id", "id")
        gene_df = gene_df.join(gene_via_flag, "id", "left_outer")

        gene_bw_flag = observations_df.where(
            col("parameter_stable_id") == "IMPC_BWT_008_001"
        )
        gene_bw_flag = gene_bw_flag.groupBy("gene_accession_id").agg(
            first("observation_id").alias("obs_id")
        )
        gene_bw_flag = gene_bw_flag.withColumn(
            "hasBodyWeightData", col("obs_id").isNotNull()
        )
        gene_bw_flag = gene_bw_flag.withColumnRenamed("gene_accession_id", "id")
        gene_df = gene_df.join(gene_bw_flag, "id", "left_outer")

        gene_df.write.partitionBy("id").json(output_path)


def get_lacz_expression_count(observations_df, lacz_lifestage):
    procedure_name = "Adult LacZ" if lacz_lifestage == "adult" else "Embryo LacZ"
    lacz_observations_by_gene = observations_df.where(
        (col("procedure_name") == procedure_name)
        & (col("observation_type") == "categorical")
        & (col("parameter_name") != "LacZ Images Section")
        & (col("parameter_name") != "LacZ Images Wholemount")
    )
    lacz_observations_by_gene = lacz_observations_by_gene.select(
        "gene_accession_id", "zygosity", "parameter_name"
    ).distinct()
    lacz_observations_by_gene = lacz_observations_by_gene.groupBy(
        "gene_accession_id"
    ).count()
    lacz_observations_by_gene = lacz_observations_by_gene.withColumnRenamed(
        "count", f"{lacz_lifestage}ExpressionObservationsCount"
    )
    lacz_observations_by_gene = lacz_observations_by_gene.withColumnRenamed(
        "gene_accession_id", "id"
    )

    return lacz_observations_by_gene


def to_camel_case(snake_str):
    components = snake_str.split("_")
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


class ImpcGeneStatsResultsMapper(PySparkTask):

    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcGeneStatsResultsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            StatsResultsMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_stats_results_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        stats_results_parquet_path = args[0]
        output_path = args[1]

        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        explode_cols = ["procedure_stable_id", "procedure_name", "project_name"]

        for col_name in explode_cols:
            stats_results_df = stats_results_df.withColumn(col_name, explode(col_name))
        stats_results_df = stats_results_df.select(
            "marker_accession_id",
            "pipeline_stable_id",
            "procedure_stable_id",
            "procedure_name",
            "parameter_stable_id",
            "parameter_name",
            "allele_accession_id",
            "allele_name",
            "allele_symbol",
            "metadata_group",
            "zygosity",
            "phenotyping_center",
            "phenotype_sex",
            "project_name",
            "male_mutant_count",
            "female_mutant_count",
            "p_value",
            "effect_size",
            "significant",
            "mp_term_id",
            "mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
        )

        stats_results_df = stats_results_df.withColumn(
            "phenotype",
            struct(col("mp_term_id").alias("id"), col("mp_term_name").alias("name")),
        )

        stats_results_df = stats_results_df.withColumn(
            "topLevelPhenotype",
            zip_with(
                "top_level_mp_term_id",
                "top_level_mp_term_name",
                lambda x, y: struct(x.alias("id"), y.alias("name")),
            ),
        )

        stats_results_df = stats_results_df.drop(
            "mp_term_id",
            "mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
        )

        stats_results_df = stats_results_df.withColumnRenamed(
            "marker_accession_id", "geneAccessionId"
        )

        for col_name in stats_results_df.columns:
            stats_results_df = stats_results_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        stats_results_df.write.partitionBy("geneAccessionId").json(output_path)


class ImpcGenePhenotypeHitsMapper(PySparkTask):

    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcGenePhenotypeHitsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            GenotypePhenotypeLoader(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_significant_phenotypes_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        gp_parquet_path = args[0]
        output_path = args[1]

        gp_df = spark.read.parquet(gp_parquet_path)
        explode_cols = ["procedure_stable_id", "procedure_name", "project_name"]

        for col_name in explode_cols:
            gp_df = gp_df.withColumn(col_name, explode(col_name))
        gp_df = gp_df.select(
            "marker_accession_id",
            "pipeline_stable_id",
            "procedure_stable_id",
            "procedure_name",
            "parameter_stable_id",
            "parameter_name",
            "allele_accession_id",
            "allele_name",
            "allele_symbol",
            "zygosity",
            "phenotyping_center",
            "sex",
            "project_name",
            "male_mutant_count",
            "female_mutant_count",
            "p_value",
            "effect_size",
            "significant",
            "mp_term_id",
            "mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
        )

        gp_df = gp_df.withColumn(
            "phenotype",
            struct(col("mp_term_id").alias("id"), col("mp_term_name").alias("name")),
        )

        gp_df = gp_df.withColumn(
            "topLevelPhenotype",
            zip_with(
                "top_level_mp_term_id",
                "top_level_mp_term_name",
                lambda x, y: struct(x.alias("id"), y.alias("name")),
            ),
        )

        gp_df = gp_df.drop(
            "mp_term_id",
            "mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
        )

        gp_df = gp_df.withColumnRenamed("marker_accession_id", "geneAccessionId")

        for col_name in gp_df.columns:
            gp_df = gp_df.withColumnRenamed(col_name, to_camel_case(col_name))

        gp_df.write.partitionBy("geneAccessionId").json(output_path)


# class ImpcWebApiMapper(luigi.Task):
#     name = "IMPC_Web_Api_Mapper"
#
#     def requires(self):
#         return [
#             ImpcGeneSummaryMapper(),
#             ImpcGeneStatsResultsMapper(),
#             ImpcGenePhenotypeHitsMapper(),
#             # ImpcGeneExpressionMapper(),
#             # ImpcGeneImagesMapper(),
#             # ImpcGeneHistopathologyMapper(),
#             # ImpcGeneDiseasesMapper(),
#             # ImpcGenePublicationsMapper(),
#             # ImpcGeneOrderMapper()
#             # ImpcAlleleMapper(),
#             # ImpcPhenotypeMapper(),
#         ]
