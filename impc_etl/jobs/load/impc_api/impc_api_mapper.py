import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import (
    col,
    first,
    explode,
    zip_with,
    struct,
    when,
    sum,
    collect_set,
    lit,
    concat,
    count,
    max,
)

from impc_etl.jobs.extract import ProductReportExtractor
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.gene_mapper import GeneLoader
from impc_etl.jobs.load.solr.genotype_phenotype_mapper import GenotypePhenotypeLoader
from impc_etl.jobs.load.solr.impc_images_mapper import ImpcImagesLoader
from impc_etl.jobs.load.solr.stats_results_mapper import StatsResultsMapper
from impc_etl.workflow.config import ImpcConfig

GENE_SUMMARY_MAPPINGS = {
    "mgi_accession_id": "mgiGeneAccessionId",
    "marker_symbol": "geneSymbol",
    "marker_name": "geneName",
    "marker_synonym": "synonyms",
    "significant_top_level_mp_terms": "significantTopLevelPhenotypes",
    "not_significant_top_level_mp_terms": "notSignificantTopLevelPhenotypes",
    "embryo_data_available": "hasEmbryoImagingData",
    "human_gene_symbol": "human_gene_symbols",
    "human_symbol_synonym": "human_symbol_synonyms",
    "production_centre": "production_centres",
    "phenotyping_centre": "phenotyping_centres",
    "allele_name": "allele_names",
    "ensembl_gene_id": "ensembl_gene_ids",
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
        gene_df = gene_df.drop("ccds_ids")
        gene_df = gene_df.withColumnRenamed("ccds_id", "ccds_ids")
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
        ).agg(sum(when(col("disease_id").isNotNull(), 1).otherwise(0)).alias("count"))
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
        gene_images_flag = gene_images_flag.drop("obs_id")
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
        gene_hist_flag = gene_hist_flag.drop("obs_id")

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
        gene_via_flag = gene_via_flag.drop("obs_id")
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
        gene_bw_flag = gene_bw_flag.drop("obs_id")
        gene_df = gene_df.join(gene_bw_flag, "id", "left_outer")

        gene_df = gene_df.drop("datasets_raw_data")

        for col_name in gene_df.columns:
            gene_df = gene_df.withColumnRenamed(col_name, to_camel_case(col_name))

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
    ).agg(sum(when(col("parameter_name").isNotNull(), 1).otherwise(0)).alias("count"))
    lacz_observations_by_gene = lacz_observations_by_gene.withColumnRenamed(
        "count", f"{lacz_lifestage}ExpressionObservationsCount"
    )
    lacz_observations_by_gene = lacz_observations_by_gene.withColumnRenamed(
        "gene_accession_id", "id"
    )

    return lacz_observations_by_gene


def get_lacz_expression_data(observations_df, lacz_lifestage):
    procedure_name = "Adult LacZ" if lacz_lifestage == "adult" else "Embryo LacZ"

    lacz_observations = observations_df.where(
        (col("procedure_name") == procedure_name)
        & (col("observation_type") == "categorical")
        & (col("parameter_name") != "LacZ Images Section")
        & (col("parameter_name") != "LacZ Images Wholemount")
    )
    categories = [
        "expression",
        "tissue not available",
        "no expression",
        "imageOnly",
        "ambiguous",
    ]
    lacz_observations_by_gene = lacz_observations.groupBy(
        "strain_accession_id",
        "gene_accession_id",
        "zygosity",
        "parameter_stable_id",
        "parameter_name",
    ).agg(
        *[
            sum(when(col("category") == category, 1).otherwise(0)).alias(
                to_camel_case(category.replace(" ", "_"))
            )
            for category in categories
        ]
    )
    lacz_observations_by_gene = lacz_observations_by_gene.withColumn(
        "mutantCounts",
        struct(*[to_camel_case(category.replace(" ", "_")) for category in categories]),
    )

    lacz_observations_by_gene = lacz_observations_by_gene.select(
        "gene_accession_id",
        "zygosity",
        "parameter_stable_id",
        "parameter_name",
        "mutantCounts",
    )

    wt_lacz_observations_by_strain = lacz_observations.where(
        col("biological_sample_group") == "control"
    )

    wt_lacz_observations_by_strain = wt_lacz_observations_by_strain.groupBy(
        "parameter_stable_id", "parameter_name"
    ).agg(
        *[
            sum(when(col("category") == category, 1).otherwise(0)).alias(
                to_camel_case(category.replace(" ", "_"))
            )
            for category in categories
        ]
    )

    wt_lacz_observations_by_strain = wt_lacz_observations_by_strain.withColumn(
        "controlCounts",
        struct(*[to_camel_case(category.replace(" ", "_")) for category in categories]),
    )

    wt_lacz_observations_by_strain = wt_lacz_observations_by_strain.select(
        "parameter_stable_id", "parameter_name", "controlCounts"
    )

    lacz_observations_by_gene = lacz_observations_by_gene.join(
        wt_lacz_observations_by_strain,
        ["parameter_stable_id", "parameter_name"],
        "left_outer",
    )

    lacz_images_by_gene = observations_df.where(
        (col("procedure_name") == procedure_name)
        & (col("observation_type") == "image_record")
        & (
            (col("parameter_name") != "LacZ Images Section")
            | (col("parameter_name") != "LacZ Images Wholemount")
        )
    )

    lacz_images_by_gene = lacz_images_by_gene.select(
        struct(
            "parameter_stable_id",
            "parameter_name",
        ).alias("expression_image_parameter"),
        "gene_accession_id",
        "zygosity",
        explode("parameter_association_name").alias("parameter_association_name"),
    ).distinct()
    lacz_images_by_gene = lacz_images_by_gene.groupBy(
        "gene_accession_id", "zygosity", "parameter_association_name"
    ).agg(
        collect_set("expression_image_parameter").alias("expression_image_parameters")
    )
    lacz_images_by_gene = lacz_images_by_gene.withColumnRenamed(
        "parameter_association_name", "parameter_name"
    )
    lacz_observations_by_gene = lacz_observations_by_gene.join(
        lacz_images_by_gene,
        ["gene_accession_id", "zygosity", "parameter_name"],
        "left_outer",
    )
    lacz_observations_by_gene = lacz_observations_by_gene.withColumn(
        "lacZLifestage", lit(lacz_lifestage)
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
        explode_cols = [
            "procedure_stable_id",
            "procedure_name",
            "project_name",
            "life_stage_name",
        ]

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
            "life_stage_name",
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
            "topLevelPhenotypes",
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
            "marker_accession_id", "mgiGeneAccessionId"
        )
        stats_results_df = stats_results_df.withColumn("id", col("mgiGeneAccessionId"))

        for col_name in stats_results_df.columns:
            stats_results_df = stats_results_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        stats_results_df = stats_results_df.withColumnRenamed(
            "phenotypingCenter", "phenotypingCentre"
        )
        stats_results_df = stats_results_df.withColumnRenamed(
            "phenotypeSex", "phenotypeSexes"
        )

        stats_results_df = stats_results_df.withColumn(
            "maleMutantCount", col("maleMutantCount").astype(IntegerType())
        )
        stats_results_df = stats_results_df.withColumn(
            "femaleMutantCount", col("femaleMutantCount").astype(IntegerType())
        )
        stats_results_df.write.partitionBy("id").json(output_path)


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
        (e.g. impc/dr15.2/parquet/gene_significant_phenotypes_json)
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
            "p_value",
            "life_stage_name",
            "effect_size",
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

        gp_df = gp_df.withColumnRenamed("marker_accession_id", "mgiGeneAccessionId")
        gp_df = gp_df.withColumn("id", col("mgiGeneAccessionId"))

        for col_name in gp_df.columns:
            gp_df = gp_df.withColumnRenamed(col_name, to_camel_case(col_name))

        gp_df = gp_df.withColumn("lifeStageName", explode("lifeStageName"))
        gp_df = gp_df.withColumnRenamed("topLevelPhenotype", "topLevelPhenotypes")
        gp_df = gp_df.withColumnRenamed("phenotypingCenter", "phenotypingCentre")
        gp_df = gp_df.withColumn("pValue", col("pValue").astype(DoubleType()))
        gp_df = gp_df.withColumn("effectSize", col("effectSize").astype(DoubleType()))
        gp_df.write.json(output_path)


class ImpcLacZExpressionMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcGeneStatsResultsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            ExperimentToObservationMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/lacz_expression_json"
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
        observations_parquet_path = args[0]
        output_path = args[1]

        observations_df = spark.read.parquet(observations_parquet_path)

        adult_lacz_expression_data = get_lacz_expression_data(observations_df, "adult")
        embryo_lacz_expression_data = get_lacz_expression_data(
            observations_df, "embryo"
        )

        lacz_expression_data = adult_lacz_expression_data.union(
            embryo_lacz_expression_data
        )
        lacz_expression_data = lacz_expression_data.withColumn(
            "id", col("gene_accession_id")
        )
        for col_name in lacz_expression_data.columns:
            lacz_expression_data = lacz_expression_data.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )
        lacz_expression_data = lacz_expression_data.groupBy("id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in lacz_expression_data.columns
                        if col_name != "id"
                    ]
                )
            ).alias("expressionData")
        )
        lacz_expression_data.write.partitionBy("id").json(output_path)


class ImpcPublicationsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcPublicationsMapper"

    #: Path to the CSV gene publications association report
    gene_publications_json_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/publications_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.gene_publications_json_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        gene_publications_json_path = args[0]
        output_path = args[1]

        publications_df = spark.read.json(gene_publications_json_path)
        publications_df = publications_df.withColumn("allele", explode("alleles"))
        publications_df = publications_df.select(
            "allele.*",
            "journalInfo.*",
            *[col_name for col_name in publications_df.columns if col_name != "allele"],
        )
        publications_df = publications_df.withColumn(
            "journalTitle", col("journal.title")
        )
        publications_df = publications_df.drop("journal")
        publications_df = publications_df.withColumnRenamed("gacc", "geneAccessionId")
        publications_df = publications_df.withColumn("id", col("geneAccessionId"))
        publications_df = publications_df.groupBy("id").agg(
            collect_set(
                struct(
                    "alleleSymbol",
                    "doi",
                    "monthOfPublication",
                    "yearOfPublication",
                    "journalTitle",
                    "title",
                    "pmcid",
                )
            ).alias("publications")
        )
        publications_df.write.partitionBy("id").json(output_path)


class ImpcProductsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcProductsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ProductReportExtractor()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}/impc_web_api/order_json")

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
        products_parquet_path = args[0]
        output_path = args[1]

        products_df = spark.read.parquet(products_parquet_path)
        products_df = products_df.select(
            "mgi_accession_id",
            "marker_symbol",
            "allele_name",
            "allele_description",
            "type",
        )
        products_df = products_df.withColumn(
            "allele_symbol",
            concat(col("marker_symbol"), lit("<"), col("allele_name"), lit(">")),
        )
        products_df = products_df.groupBy(
            "mgi_accession_id", "allele_symbol", "allele_description"
        ).agg(collect_set("type").alias("product_types"))

        products_df = products_df.withColumnRenamed("mgi_accession_id", "id")
        for col_name in products_df.columns:
            products_df = products_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        products_df = products_df.groupBy("id").agg(
            collect_set(
                struct(
                    *[col_name for col_name in products_df.columns if col_name != "id"]
                )
            ).alias("order")
        )

        products_df.write.partitionBy("id").json(output_path)


class ImpcGeneImagesMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcGeneImagesMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ImpcImagesLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_images_json"
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
        impc_images_parquet_path = args[0]
        output_path = args[1]

        impc_images_df = spark.read.parquet(impc_images_parquet_path)

        impc_images_df = impc_images_df.groupBy(
            "gene_accession_id",
            "strain_accession_id",
            "procedure_stable_id",
            "procedure_name",
            "parameter_stable_id",
            "parameter_name",
        ).agg(
            count("observation_id").alias("count"),
            first("thumbnail_url").alias("thumbnail_url"),
            first("file_type").alias("file_type"),
        )

        impc_images_df = impc_images_df.withColumnRenamed("gene_accession_id", "id")

        for col_name in impc_images_df.columns:
            impc_images_df = impc_images_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        impc_images_df = impc_images_df.groupBy("id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in impc_images_df.columns
                        if col_name != "id"
                    ]
                )
            ).alias("gene_images")
        )

        impc_images_df.write.partitionBy("id").json(output_path)


class ImpcGeneDiseasesMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcGeneDiseasesMapper"

    #: Path to the CSV gene disease association report
    disease_model_summary_csv_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_diseases_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.disease_model_summary_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        disease_model_summary_csv_path = args[0]
        output_path = args[1]

        disease_df = spark.read.csv(disease_model_summary_csv_path, header=True)

        disease_df = disease_df.withColumn(
            "phenodigm_score",
            (col("disease_model_avg_norm") + col("disease_model_max_norm")) / 2,
        )

        max_values = disease_df.groupBy("disease_id", "marker_id").agg(
            max("phenodigm_score").alias("phenodigm_score")
        )
        max_disease_df = disease_df.join(
            max_values, ["disease_id", "marker_id", "phenodigm_score"]
        )

        max_disease_df = max_disease_df.withColumnRenamed("marker_id", "id")

        for col_name in max_disease_df.columns:
            max_disease_df = max_disease_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        max_disease_df = max_disease_df.groupBy("id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in max_disease_df.columns
                        if col_name != "id"
                    ]
                )
            ).alias("gene_diseases")
        )

        max_disease_df.write.partitionBy("id").json(output_path)


class ImpcGeneHistopathologyMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcGeneHistopathologyMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            GenotypePhenotypeLoader(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/gene_histopath_json)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_histopath_json"
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
        explode_cols = [
            "procedure_stable_id",
            "procedure_name",
            "project_name",
            "life_stage_name",
        ]

        for col_name in explode_cols:
            gp_df = gp_df.withColumn(col_name, explode(col_name))

        gp_df = gp_df.where(col("parameter_stable_id").contains("_HIS_"))

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
            "life_stage_name",
            "mpath_term_id",
            "mpath_term_name",
        )

        for col_name in gp_df.columns:
            gp_df = gp_df.withColumnRenamed(col_name, to_camel_case(col_name))
        gp_df = gp_df.withColumnRenamed("markerAccessionId", "geneAccessionId")
        gp_df = gp_df.withColumn("id", col("geneAccessionId"))

        gp_df = gp_df.groupBy("id").agg(
            collect_set(
                struct(*[col_name for col_name in gp_df.columns if col_name != "id"])
            ).alias("gene_histopathology")
        )

        gp_df.write.partitionBy("id").json(output_path)


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
