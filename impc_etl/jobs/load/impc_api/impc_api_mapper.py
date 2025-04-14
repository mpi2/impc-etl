import csv
import json
import os
import re

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
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
    max,
    min,
    regexp_replace,
    split,
    arrays_zip,
    expr,
    concat_ws,
    countDistinct,
    array_contains,
    array_union,
    array,
    udf,
    row_number,
    avg,
    stddev,
    count,
    quarter,
    regexp_extract,
    array_distinct,
    lower,
    size,
    array_intersect,
    trim,
    explode_outer,
    desc,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    BooleanType,
    ArrayType,
    StringType,
    StructType,
    StructField,
)

from impc_etl.jobs.clean.specimen_cleaner import (
    MouseSpecimenCleaner,
    EmbryoSpecimenCleaner,
)
from impc_etl.jobs.extract import MGIStrainReportExtractor
from impc_etl.jobs.extract.ontology_hierarchy_extractor import (
    OntologyTermHierarchyExtractor,
)
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.gene_mapper import GeneLoader
from impc_etl.jobs.load.solr.genotype_phenotype_mapper import GenotypePhenotypeLoader
from impc_etl.jobs.load.solr.impc_images_mapper import ImpcImagesLoader
from impc_etl.jobs.load.solr.mp_mapper import MpLoader
from impc_etl.jobs.load.solr.pipeline_mapper import ImpressToParameterMapper
from impc_etl.jobs.load.solr.stats_results_mapper import StatsResultsMapper
from impc_etl.workflow import SmallPySparkTask
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


# stats_df.select("doc_id", "procedure_stable_id", "parameter_stable_id", "marker_accession_id").withColumn("procedure_stable_id", explode("procedure_stable_id")).where(col("parameter_stable_id") == "IMPC_BWT_008_001").join(raw_data, "doc_id", "left_outer").withColumn("observation_id", explode("observation_id")).drop("window_weight").join(exp_df.drop("procedure_stable_id", "parameter_stable_id"), "observation_id", "left_outer").select("doc_id", "procedure_stable_id", "parameter_stable_id", "marker_accession_id", "biological_sample_group", "sex", "zygosity", "discrete_point", "data_point").groupBy("doc_id", "procedure_stable_id", "parameter_stable_id", "marker_accession_id", "biological_sample_group", "sex", "zygosity", "discrete_point").agg(avg("data_point").alias("mean"), stddev("data_point").alias("std"), count("data_point").alias("count")).groupBy(col("doc_id").alias("datasetId"), col("procedure_stable_id").alias("procedureStableId"), col("parameter_stable_id").alias("parameterStableId"), col("marker_accession_id").alias("mgiGeneAccessionId")).agg(collect_set(struct( col("biological_sample_group").alias("sampleGroup"), "sex", "zygosity", col("discrete_point").alias("ageInWeeks"), "mean", "std", "count")).alias("dataPoints")).write.json("/nfs/production/tudor/komp/data-releases/latest-input/dr19.2/output/impc_web_api/bwt_curve_service_json")


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
            f"{self.output_path}/impc_web_api/gene_summary_service_json"
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
        gp_call_by_gene = genotype_phenotype_df.groupBy("id").agg(
            countDistinct("mp_term_id").alias("count")
        )
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
        gene_images_flag = gene_images_flag.select(
            "gene_accession_id", lit(True).alias("hasImagingData")
        ).distinct()
        gene_images_flag = gene_images_flag.withColumnRenamed("gene_accession_id", "id")
        gene_df = gene_df.join(gene_images_flag, "id", "left_outer")
        gene_df = gene_df.withColumn(
            "hasImagingData",
            when(col("hasImagingData").isNotNull(), lit(True)).otherwise(lit(False)),
        )

        gene_hist_flag = observations_df.where(
            col("procedure_stable_id").contains("HIS")
        )
        gene_hist_flag = gene_hist_flag.select(
            "gene_accession_id", lit(True).alias("hasHistopathologyData")
        ).distinct()
        gene_hist_flag = gene_hist_flag.withColumnRenamed("gene_accession_id", "id")

        gene_df = gene_df.join(gene_hist_flag, "id", "left_outer")
        gene_df = gene_df.withColumn(
            "hasHistopathologyData",
            when(col("hasHistopathologyData").isNotNull(), lit(True)).otherwise(
                lit(False)
            ),
        )

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
        gene_via_flag = gene_via_flag.select(
            "gene_accession_id", lit(True).alias("hasViabilityData")
        ).distinct()
        gene_via_flag = gene_via_flag.withColumnRenamed("gene_accession_id", "id")
        gene_df = gene_df.join(gene_via_flag, "id", "left_outer")
        gene_df = gene_df.withColumn(
            "hasViabilityData",
            when(col("hasViabilityData").isNotNull(), lit(True)).otherwise(lit(False)),
        )

        gene_bw_flag = observations_df.where(
            col("parameter_stable_id") == "IMPC_BWT_008_001"
        )
        gene_bw_flag = gene_bw_flag.select(
            "gene_accession_id", lit(True).alias("hasBodyWeightData")
        ).distinct()
        gene_bw_flag = gene_bw_flag.withColumnRenamed("gene_accession_id", "id")
        gene_df = gene_df.join(gene_bw_flag, "id", "left_outer")
        gene_df = gene_df.withColumn(
            "hasBodyWeightData",
            when(col("hasBodyWeightData").isNotNull(), lit(True)).otherwise(lit(False)),
        )

        gene_df = gene_df.drop("datasets_raw_data")

        for col_name in gene_df.columns:
            gene_df = gene_df.withColumnRenamed(col_name, to_camel_case(col_name))

        gene_avg_df = gene_df.where(col("phenotypingDataAvailable") == True).select(
            avg("significantPhenotypesCount").alias("significantPhenotypesAverage"),
            avg("associatedDiseasesCount").alias("associatedDiseasesAverage"),
            avg("adultExpressionObservationsCount").alias(
                "adultExpressionObservationsAverage"
            ),
            avg("embryoExpressionObservationsCount").alias(
                "embryoExpressionObservationsAverage"
            ),
        )
        gene_avg_df.repartition(1).write.option("ignoreNullFields", "false").json(
            output_path + "_avgs"
        )
        gene_df.repartition(100).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcGeneSearchMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "Impc_Gene_Search_Mapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return GeneLoader()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_search_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input().path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        gene_parquet_path = args[0]
        gene_df = spark.read.parquet(gene_parquet_path)
        output_path = args[1]

        for col_name in GENE_SUMMARY_MAPPINGS.keys():
            gene_df = gene_df.withColumnRenamed(
                col_name, GENE_SUMMARY_MAPPINGS[col_name]
            )

        for col_name in gene_df.columns:
            gene_df = gene_df.withColumnRenamed(col_name, to_camel_case(col_name))

        gene_search_df = gene_df.select(
            "mgiGeneAccessionId",
            "geneName",
            "geneSymbol",
            "synonyms",
            "humanGeneSymbols",
            "humanSymbolSynonyms",
            "esCellProductionStatus",
            "mouseProductionStatus",
            "phenotypeStatus",
            "phenotypingDataAvailable",
        )
        gene_search_df.repartition(1).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcPhenotypeSummaryMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "Impc_Phenotype_Summary_Mapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            MpLoader(),
            ImpressToParameterMapper(),
            StatsResultsMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/phenotype_summary_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        mp_parquet_path = args[0]
        impress_parameter_parquet_path = args[1]
        stats_results_parquet_path = args[2]
        output_path = args[3]

        mp_df = spark.read.parquet(mp_parquet_path)
        impress_df = spark.read.parquet(impress_parameter_parquet_path)
        stats_results_df = spark.read.parquet(stats_results_parquet_path)

        stats_results_df = (
            stats_results_df.select(
                "marker_accession_id",
                "status",
                "top_level_mp_term_id",
                "intermediate_mp_term_id",
                when(col("mp_term_id").isNotNull(), array("mp_term_id"))
                .otherwise(col("mp_term_id_options"))
                .alias("mp_term_id_options"),
                "significant",
            )
            .where(col("status") != lit("NotProcessed"))
            .distinct()
        )

        stats_results_df = stats_results_df.withColumn(
            "mp_ids",
            array_union(
                "top_level_mp_term_id",
                array_union(
                    "intermediate_mp_term_id",
                    "mp_term_id_options",
                ),
            ),
        ).drop(
            "status",
            "top_level_mp_term_id",
            "intermediate_mp_term_id",
            "mp_term_id_options",
        )

        stats_results_df = (
            stats_results_df.withColumn("mp_id", explode("mp_ids"))
            .drop("mp_ids")
            .distinct()
        )

        stats_results_df = stats_results_df.groupBy("marker_accession_id", "mp_id").agg(
            max("significant").alias("significant")
        )

        stats_results_df = stats_results_df.groupBy("mp_id").agg(
            sum(when(col("significant") == lit(True), 1).otherwise(0)).alias(
                "significant_genes"
            ),
            sum(when(col("significant") == lit(False), 1).otherwise(0)).alias(
                "not_significant_genes"
            ),
        )

        phenotype_summary_df = mp_df.join(stats_results_df, "mp_id", "left")

        impress_df = (
            impress_df.select(
                "pipeline_stable_id",
                "pipeline_name",
                "procedure_stable_id",
                "procedure_name",
                "procedure_stable_key",
                "procedure.description",
                array_union(
                    "mp_id",
                    array_union(
                        "top_level_mp_id",
                        array_union(
                            "intermediate_mp_id",
                            array_union(
                                "abnormal_mp_id",
                                array_union("increased_mp_id", "decreased_mp_id"),
                            ),
                        ),
                    ),
                ).alias("phenotype_ids"),
            )
            .where(col("phenotype_ids").isNotNull())
            .distinct()
        )

        for col_name in impress_df.columns:
            impress_df = impress_df.withColumnRenamed(col_name, to_camel_case(col_name))
        impress_df = impress_df.withColumn("mp_id", explode("phenotypeIds"))
        impress_df = impress_df.drop("phenotypeIds")
        impress_df = impress_df.groupBy("mp_id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in impress_df.columns
                        if col_name != "mp_id"
                    ]
                )
            ).alias("procedures")
        )
        phenotype_summary_df = phenotype_summary_df.join(impress_df, "mp_id", "left")

        phenotype_summary_mappings = {
            "mp_id": "phenotypeId",
            "mp_term": "phenotypeName",
            "mp_definition": "phenotypeDefinition",
            "mp_term_synonym": "phenotypeSynonyms",
        }

        for col_name in phenotype_summary_mappings.keys():
            phenotype_summary_df = phenotype_summary_df.withColumnRenamed(
                col_name, phenotype_summary_mappings[col_name]
            )

        for col_name in phenotype_summary_df.columns:
            phenotype_summary_df = phenotype_summary_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        phenotype_summary_df = phenotype_summary_df.withColumn(
            "topLevelPhenotypes",
            zip_with(
                "topLevelMpId",
                "topLevelMpTerm",
                lambda x, y: struct(x.alias("id"), y.alias("name")),
            ),
        )

        phenotype_summary_df.select(
            "phenotypeId",
            "phenotypeName",
            "phenotypeDefinition",
            "phenotypeSynonyms",
            "significantGenes",
            "notSignificantGenes",
            "procedures",
            "topLevelPhenotypes",
        ).distinct().repartition(100).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcPhenotypeGenotypeHitsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "Impc_Phenotype_Genotype_Hits_Mapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [MpLoader(), GenotypePhenotypeLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/phenotype_genotype_hits_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        mp_parquet_path = args[0]
        impress_parameter_parquet_path = args[1]
        stats_results_parquet_path = args[2]
        output_path = args[3]

        mp_df = spark.read.parquet(mp_parquet_path)
        impress_df = spark.read.parquet(impress_parameter_parquet_path)
        stats_results_df = spark.read.parquet(stats_results_parquet_path)

        stats_results_df = (
            stats_results_df.select(
                "marker_accession_id",
                "status",
                "top_level_mp_term_id",
                "intermediate_mp_term_id",
                when(col("mp_term_id").isNotNull(), array("mp_term_id"))
                .otherwise(col("mp_term_id_options"))
                .alias("mp_term_id_options"),
                "significant",
            )
            .where(col("status") != "NotProcessed")
            .distinct()
        )

        stats_results_df = stats_results_df.groupBy(
            "marker_accession_id",
            "status",
            "top_level_mp_term_id",
            "intermediate_mp_term_id",
            "mp_term_id_options",
        ).agg(max("significant").alias("significant"))

        stats_results_df = stats_results_df.withColumn(
            "mp_ids",
            array_union(
                "top_level_mp_term_id",
                array_union(
                    "intermediate_mp_term_id",
                    "mp_term_id_options",
                ),
            ),
        ).drop(
            "status",
            "top_level_mp_term_id",
            "intermediate_mp_term_id",
            "mp_term_id_options",
        )
        stats_results_df = (
            stats_results_df.withColumn("mp_id", explode("mp_ids"))
            .drop("mp_ids")
            .distinct()
        )
        stats_results_df = stats_results_df.groupBy("mp_id").agg(
            sum(when(col("significant") == True, 1).otherwise(0)).alias(
                "significant_genes"
            ),
            sum(when(col("significant") == False, 1).otherwise(0)).alias(
                "not_significant_genes"
            ),
        )

        phenotype_summary_df = mp_df.join(stats_results_df, "mp_id", "left")

        impress_df = (
            impress_df.select(
                "pipeline_stable_id",
                "pipeline_name",
                "procedure_stable_id",
                "procedure_name",
                "procedure_stable_key",
                "procedure.description",
                array_union(
                    "mp_id",
                    array_union(
                        "top_level_mp_id",
                        array_union(
                            "intermediate_mp_id",
                            array_union(
                                "abnormal_mp_id",
                                array_union("increased_mp_id", "decreased_mp_id"),
                            ),
                        ),
                    ),
                ).alias("phenotype_ids"),
            )
            .where(col("phenotype_ids").isNotNull())
            .distinct()
        )

        for col_name in impress_df.columns:
            impress_df = impress_df.withColumnRenamed(col_name, to_camel_case(col_name))
        impress_df = impress_df.withColumn("mp_id", explode("phenotypeIds"))
        impress_df = impress_df.drop("phenotypeIds")
        impress_df = impress_df.groupBy("mp_id").agg(
            collect_set(
                struct(
                    *[
                        col_name
                        for col_name in impress_df.columns
                        if col_name != "mp_id"
                    ]
                )
            ).alias("procedures")
        )
        phenotype_summary_df = phenotype_summary_df.join(impress_df, "mp_id", "left")

        phenotype_summary_mappings = {
            "mp_id": "phenotypeId",
            "mp_term": "phenotypeName",
            "mp_definition": "phenotypeDefinition",
            "mp_term_synonym": "phenotypeSynonyms",
        }

        for col_name in phenotype_summary_mappings.keys():
            phenotype_summary_df = phenotype_summary_df.withColumnRenamed(
                col_name, phenotype_summary_mappings[col_name]
            )

        for col_name in phenotype_summary_df.columns:
            phenotype_summary_df = phenotype_summary_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        phenotype_summary_df = phenotype_summary_df.withColumn(
            "topLevelPhenotypes",
            zip_with(
                "topLevelMpId",
                "topLevelMpTerm",
                lambda x, y: struct(x.alias("id"), y.alias("name")),
            ),
        )

        phenotype_summary_df.select(
            "mpId",
            "mpName",
            "mpDefinition",
            "mpSynonyms",
            "significantGenes",
            "notSignificantGenes",
            "procedures",
            "topLevelPhenotypes",
        ).distinct().repartition(100).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcPhenotypeSearchMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "Impc_Phenotype_Search_Mapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [MpLoader(), GenotypePhenotypeLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/phenotype_search_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        mp_parquet_path = args[0]
        genotype_phenotype_parquet_path = args[1]
        output_path = args[2]

        mp_df = spark.read.parquet(mp_parquet_path)
        genotype_phenotype_df = spark.read.parquet(genotype_phenotype_parquet_path)
        genotype_phenotype_df = genotype_phenotype_df.select(
            "mp_term_id",
            "intermediate_mp_term_id",
            "top_level_mp_term_id",
            "marker_accession_id",
        ).distinct()

        phenotype_gene_count_df = (
            mp_df.join(
                genotype_phenotype_df,
                (col("mp_id") == col("mp_term_id"))
                | (array_contains(col("intermediate_mp_term_id"), col("mp_id")))
                | (
                    array_contains(
                        genotype_phenotype_df["top_level_mp_term_id"], col("mp_id")
                    )
                ),
            )
            .groupBy("mp_id")
            .agg(countDistinct("marker_accession_id").alias("geneCount"))
        )

        phenotype_search_df = mp_df.join(phenotype_gene_count_df, "mp_id", "left")
        phenotype_search_df = phenotype_search_df.withColumn(
            "topLevelParents",
            zip_with(
                "top_level_mp_term_id",
                "top_level_mp_term",
                lambda x, y: struct(x.alias("key"), y.alias("value")),
            ),
        )

        phenotype_search_df = phenotype_search_df.withColumn(
            "intermediateLevelParents",
            zip_with(
                "intermediate_mp_id",
                "intermediate_mp_term",
                lambda x, y: struct(x.alias("key"), y.alias("value")),
            ),
        )

        for col_name in phenotype_search_df.columns:
            phenotype_search_df = phenotype_search_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        phenotype_search_df = phenotype_search_df.select(
            "mpId",
            col("mpTerm").alias("phenotypeName"),
            col("mpTermSynonym").alias("synonyms"),
            col("mpDefinition").alias("definition"),
            "topLevelParents",
            "intermediateLevelParents",
            "geneCount",
        )

        phenotype_search_df.repartition(1).write.option(
            "ignoreNullFields", "false"
        ).json(output_path)


def get_lacz_expression_count(observations_df, lacz_lifestage):
    procedure_name = "Adult LacZ" if lacz_lifestage == "adult" else "Embryo LacZ"
    lacz_observations_by_gene = observations_df.where(
        (col("procedure_name") == procedure_name)
        & (col("observation_type") == "categorical")
        & (col("parameter_name") != "LacZ Images Section")
        & (col("parameter_name") != "LacZ Images Wholemount")
    )
    lacz_observations_by_gene = lacz_observations_by_gene.select(
        "gene_accession_id", "zygosity", lower("parameter_name").alias("parameter_name")
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

    observations_df = observations_df.withColumn(
        "parameter_name", lower("parameter_name")
    )

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
        "gene_accession_id",
        "zygosity",
        "parameter_name",
    ).agg(
        *[
            sum(when(col("category") == category, 1).otherwise(0)).alias(
                to_camel_case(category.replace(" ", "_"))
            )
            for category in categories
        ],
        collect_set(
            "parameter_stable_id",
        ).alias("mutant_parameter_stable_ids"),
    )
    lacz_observations_by_gene = lacz_observations_by_gene.withColumn(
        "mutantCounts",
        struct(*[to_camel_case(category.replace(" ", "_")) for category in categories]),
    )

    lacz_observations_by_gene = lacz_observations_by_gene.select(
        "gene_accession_id",
        "zygosity",
        "mutant_parameter_stable_ids",
        "parameter_name",
        "mutantCounts",
    ).distinct()

    wt_lacz_observations_by_strain = lacz_observations.where(
        col("biological_sample_group") == "control"
    )

    wt_lacz_observations_by_strain = wt_lacz_observations_by_strain.groupBy(
        "parameter_name"
    ).agg(
        *[
            sum(when(col("category") == category, 1).otherwise(0)).alias(
                to_camel_case(category.replace(" ", "_"))
            )
            for category in categories
        ],
        collect_set(
            "parameter_stable_id",
        ).alias("control_parameter_stable_ids"),
    )

    wt_lacz_observations_by_strain = wt_lacz_observations_by_strain.withColumn(
        "controlCounts",
        struct(*[to_camel_case(category.replace(" ", "_")) for category in categories]),
    )

    wt_lacz_observations_by_strain = wt_lacz_observations_by_strain.select(
        "parameter_name", "controlCounts"
    )

    lacz_observations_by_gene = lacz_observations_by_gene.join(
        wt_lacz_observations_by_strain,
        ["parameter_name"],
        "left_outer",
    )

    lacz_images_by_gene = observations_df.where(
        (col("procedure_name") == procedure_name)
        & (col("observation_type") == "image_record")
        & (
            (lower(col("parameter_name")) == "lacz images section")
            | (lower(col("parameter_name")) == "lacz images wholemount")
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
    lacz_images_by_gene = lacz_images_by_gene.withColumn(
        "parameter_name", lower("parameter_name")
    )
    lacz_observations_by_gene = lacz_observations_by_gene.join(
        lacz_images_by_gene,
        ["gene_accession_id", "zygosity", "parameter_name"],
        "left_outer",
    )
    lacz_observations_by_gene = lacz_observations_by_gene.withColumn(
        "lacZLifestage", lit(lacz_lifestage)
    )
    return lacz_observations_by_gene.distinct()


def to_camel_case(snake_str):
    components = snake_str.split("_")
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + "".join(x.title() for x in components[1:])


def phenotype_term_zip_udf(x, y):
    return when(x.isNotNull(), struct(x.alias("id"), y.alias("name"))).otherwise(
        lit(None)
    )


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
            OntologyTermHierarchyExtractor(),
            ImpressToParameterMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_statistical_results_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        stats_results_parquet_path = args[0]
        ontology_term_hierarchy_parquet_path = args[1]
        impress_parquet_path = args[2]
        output_path = args[3]

        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        ontology_term_hierarchy_df = spark.read.parquet(
            ontology_term_hierarchy_parquet_path
        )
        explode_cols = [
            "procedure_stable_id",
            "procedure_name",
            "project_name",
            "life_stage_name",
        ]

        parent_df = ontology_term_hierarchy_df.where(size(col("child_ids")) > 0).select(
            "child_ids", "id", "term"
        )

        parent_df = parent_df.withColumnRenamed("id", "parent_id").withColumnRenamed(
            "term", "parent_term"
        )

        stats_results_df = stats_results_df.distinct()

        stats_results_with_phenotype_df = stats_results_df.join(
            parent_df,
            size(array_intersect("child_ids", "mp_term_id_options"))
            == size("mp_term_id_options"),
            "left_outer",
        )
        stats_results_with_phenotype_df = stats_results_with_phenotype_df.withColumn(
            "potentialPhenotypes",
            zip_with(
                "mp_term_id_options",
                "mp_term_name_options",
                phenotype_term_zip_udf,
            ),
        )
        stats_results_with_phenotype_df = stats_results_with_phenotype_df.withColumn(
            "potentialPhenotype", explode("potentialPhenotypes")
        ).drop("potentialPhenotypes")
        ontology_level_df = ontology_term_hierarchy_df.select(
            "id", size("intermediate_ids").alias("ontology_level")
        ).distinct()
        stats_results_with_phenotype_df = stats_results_with_phenotype_df.join(
            ontology_level_df, col("potentialPhenotype.id") == col("id"), "left_outer"
        )
        window_spec = Window.partitionBy("doc_id")
        stats_results_with_phenotype_df = stats_results_with_phenotype_df.withColumn(
            "max_level", min("ontology_level").over(window_spec)
        )
        stats_results_with_phenotype_df = (
            stats_results_with_phenotype_df.groupBy(
                *[
                    col_name
                    for col_name in stats_results_with_phenotype_df.columns
                    if col_name
                    not in [
                        "potentialPhenotype",
                        "ontology_level",
                        "level_id",
                        "max_level",
                        "id",
                    ]
                ]
            )
            .agg(
                collect_set("potentialPhenotype").alias("potentialPhenotypes"),
                collect_set(
                    when(
                        col("ontology_level") == col("max_level"),
                        col("potentialPhenotype"),
                    ).otherwise(lit(None))
                ).alias("generalPhenotypes"),
            )
            .withColumn(
                "display_phenotype",
                when(
                    size(col("generalPhenotypes")) == 1,
                    col("generalPhenotypes").getItem(0),
                )
                .when(
                    col("parent_id").isNotNull(),
                    struct(
                        col("parent_id").alias("id"),
                        col("parent_term").alias("name"),
                    ),
                )
                .otherwise(lit(None)),
            )
            .drop("child_ids", "parent_id", "parent_term", "generalPhenotypes")
        )

        stats_results_with_phenotype_df = stats_results_with_phenotype_df.select(
            "doc_id", "display_phenotype"
        ).distinct()
        stats_results_df = stats_results_df.join(
            stats_results_with_phenotype_df, "doc_id", "left_outer"
        )

        for col_name in explode_cols:
            stats_results_df = stats_results_df.withColumn(col_name, explode(col_name))
        impress_df = (
            spark.read.parquet(impress_parquet_path)
            .select(
                "fully_qualified_name",
                col("procedure.minAnimals").alias("procedure_min_animals"),
                col("procedure.minFemales").alias("procedure_min_females"),
                col("procedure.minMales").alias("procedure_min_males"),
            )
            .distinct()
        )
        stats_results_df = stats_results_df.withColumn(
            "fully_qualified_name",
            concat_ws(
                "_", "pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"
            ),
        )
        stats_results_df = stats_results_df.join(
            impress_df, "fully_qualified_name", "left_outer"
        )
        stats_results_df = stats_results_df.drop("fully_qualified_name")
        stats_results_df = stats_results_df.select(
            "doc_id",
            "data_type",
            "marker_accession_id",
            "pipeline_stable_id",
            "procedure_stable_id",
            "procedure_name",
            "procedure_min_animals",
            "procedure_min_females",
            "procedure_min_males",
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
            "mp_term_id_options",
            "mp_term_name_options",
            "intermediate_mp_term_id",
            "intermediate_mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
            "display_phenotype",
            "statistical_method",
            "status",
        )

        stats_results_df = stats_results_df.withColumn(
            "significantPhenotype",
            when(
                col("mp_term_id").isNotNull(),
                struct(
                    col("mp_term_id").alias("id"), col("mp_term_name").alias("name")
                ),
            ).otherwise(lit(None)),
        )

        stats_results_df = stats_results_df.withColumn(
            "intermediatePhenotypes",
            when(
                col("intermediate_mp_term_id").isNotNull(),
                zip_with(
                    "intermediate_mp_term_id",
                    "intermediate_mp_term_name",
                    lambda x, y: struct(x.alias("id"), y.alias("name")),
                ),
            ).otherwise(lit(None)),
        )

        stats_results_df = stats_results_df.withColumn(
            "topLevelPhenotypes",
            when(
                col("top_level_mp_term_id").isNotNull(),
                zip_with(
                    "top_level_mp_term_id",
                    "top_level_mp_term_name",
                    lambda x, y: struct(x.alias("id"), y.alias("name")),
                ),
            ).otherwise(lit(None)),
        )

        stats_results_df = stats_results_df.withColumn(
            "potentialPhenotypes",
            zip_with(
                "mp_term_id_options",
                "mp_term_name_options",
                phenotype_term_zip_udf,
            ),
        )

        stats_results_df = stats_results_df.withColumn(
            "potentialPhenotypes",
            when(
                col("significantPhenotype").isNotNull(),
                array("significantPhenotype"),
            ).otherwise(col("potentialPhenotypes")),
        )

        stats_results_df = stats_results_df.drop(
            "mp_term_id",
            "mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
            "intermediate_mp_term_id",
            "intermediate_mp_term_name",
            "mp_term_id_options",
            "mp_term_name_options",
        )

        stats_results_df = stats_results_df.withColumnRenamed(
            "marker_accession_id", "mgiGeneAccessionId"
        )
        stats_results_df = stats_results_df.withColumnRenamed(
            "doc_id", "statistical_result_id"
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
        stats_results_df.distinct().repartition(1000).write.option(
            "ignoreNullFields", "false"
        ).json(output_path)


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
            f"{self.output_path}/impc_web_api/gene_phenotype_hits_service_json"
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
            "statistical_result_id",
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
            "intermediate_mp_term_id",
            "intermediate_mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
            "assertion_type",
            "data_type",
        )

        gp_df = gp_df.withColumn(
            "phenotype",
            struct(col("mp_term_id").alias("id"), col("mp_term_name").alias("name")),
        )

        gp_df = gp_df.withColumn(
            "intermediatePhenotype",
            zip_with(
                "intermediate_mp_term_id",
                "intermediate_mp_term_name",
                lambda x, y: struct(x.alias("id"), y.alias("name")),
            ),
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
            "intermediate_mp_term_id",
            "intermediate_mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
        )

        gp_df = gp_df.withColumnRenamed("marker_accession_id", "mgiGeneAccessionId")
        gp_df = gp_df.withColumn("id", col("mgiGeneAccessionId"))

        for col_name in gp_df.columns:
            gp_df = gp_df.withColumnRenamed(col_name, to_camel_case(col_name))

        gp_df = gp_df.withColumn("lifeStageName", explode("lifeStageName"))
        gp_df = gp_df.withColumnRenamed("topLevelPhenotype", "topLevelPhenotypes")
        gp_df = gp_df.withColumnRenamed(
            "intermediatePhenotype", "intermediatePhenotypes"
        )
        gp_df = gp_df.withColumnRenamed("phenotypingCenter", "phenotypingCentre")
        gp_df = gp_df.withColumnRenamed("statisticalResultId", "datasetId")
        gp_df = gp_df.withColumn("pValue", col("pValue").astype(DoubleType()))
        gp_df = gp_df.withColumn("effectSize", col("effectSize").astype(DoubleType()))
        gp_df.repartition(100).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcLacZExpressionMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcLacZExpressionMapper"

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
            f"{self.output_path}/impc_web_api/gene_expression_service_json"
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
        lacz_expression_data = lacz_expression_data.withColumnRenamed(
            "geneAccessionId", "mgiGeneAccessionId"
        )
        lacz_expression_data.distinct().repartition(100).write.option(
            "ignoreNullFields", "false"
        ).json(output_path)


class ImpcPublicationsMapper(SmallPySparkTask):
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
            f"{self.output_path}/impc_web_api/publications_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        output_path = args[0]

        publications_df = spark.read.format("mongodb").load()

        publication_service_df = publications_df.where(
            col("status") == "reviewed"
        ).select(
            "title",
            "authorString",
            "consortiumPaper",
            "doi",
            col("firstPublicationDate").alias("publicationDate"),
            col("journalInfo.journal.title").alias("journalTitle"),
            col("alleles.acc").alias("mgiAlleleAccessionId"),
            col("alleles.gacc").alias("mgiGeneAccessionId"),
            col("alleles.geneSymbol"),
            col("alleles.alleleSymbol"),
            col("pmid").alias("pmId"),
            "abstractText",
            "meshHeadingList",
            "grantsList",
        )

        def zip_func(*args):
            return list(zip(*args))

        zip_udf = udf(
            zip_func,
            ArrayType(
                StructType(
                    [
                        StructField("mgiAlleleAccessionId", StringType(), True),
                        StructField("mgiGeneAccessionId", StringType(), True),
                        StructField("geneSymbol", StringType(), True),
                        StructField("alleleSymbol", StringType(), True),
                    ]
                )
            ),
        )

        publication_service_df = publication_service_df.withColumn(
            "alleles",
            zip_udf(
                col("mgiAlleleAccessionId"),
                col("mgiGeneAccessionId"),
                col("geneSymbol"),
                col("alleleSymbol"),
            ),
        )

        publication_service_df = publication_service_df.drop(
            "mgiAlleleAccessionId",
            "mgiGeneAccessionId",
            "geneSymbol",
            "alleleSymbol",
        )

        publication_service_df.repartition(1).write.json(output_path)

        # Incremental count by year
        incremental_counts_by_year = (
            publications_df.where(col("status") == "reviewed")
            .select("pmid", "pubYear")
            .withColumn(
                "count",
                count("pmid")
                .over(Window.partitionBy("pubYear").orderBy("pubYear"))
                .alias("count"),
            )
            .select(col("pubYear").cast("int"), col("count").cast("int"))
            .distinct()
            .sort("pubYear")
            .rdd.map(lambda row: row.asDict())
            .collect()
        )
        aggregations_path = output_path.replace(
            "publications", "publications_aggregation"
        )
        if not os.path.exists(aggregations_path):
            os.makedirs(aggregations_path)

        # Count by year and quarter
        publications_by_quarter = [
            json.loads(s)
            for s in publications_df.where(col("status") == "reviewed")
            .select("pmid", "pubYear", quarter("firstPublicationDate").alias("quarter"))
            .withColumn(
                "countQuarter",
                count("pmid").over(
                    Window.partitionBy("pubYear", "quarter").orderBy(
                        "pubYear", col("quarter").asc()
                    )
                ),
            )
            .withColumn(
                "countYear",
                count("pmid").over(Window.partitionBy("pubYear").orderBy("pubYear")),
            )
            .select(
                col("pubYear").cast("int"),
                "quarter",
                col("countYear"),
                col("countQuarter"),
            )
            .distinct()
            .sort("pubYear", "quarter")
            .groupBy("pubYear", col("countYear").alias("count"))
            .agg(
                collect_set(
                    struct("quarter", col("countQuarter").alias("count"))
                ).alias("byQuarter")
            )
            .sort("pubYear")
            .toJSON()
            .collect()
        ]

        # Count by Grant Agency
        publications_by_grant_agency = (
            publications_df.where(col("status") == "reviewed")
            .select("pmid", explode("grantsList").alias("grantInfo"))
            .select("pmid", "grantInfo.agency")
            .groupBy("agency")
            .agg(countDistinct("pmid").alias("count"))
            .sort(col("count").desc())
            .rdd.map(lambda row: row.asDict())
            .collect()
        )

        with open(
            output_path.replace("publications", "publications_aggregation")
            + "/publications_aggregation.json",
            "w",
        ) as f:
            json.dump(
                {
                    "incrementalCountsByYear": incremental_counts_by_year,
                    "publicationsByQuarter": publications_by_quarter,
                    "publicationsByGrantAgency": publications_by_grant_agency,
                },
                f,
                ensure_ascii=False,
            )


class ImpcMiceProductsMapper(SmallPySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcMiceProductsMapper"

    #: Path to the Mice products report JSON file
    mice_products_report_json_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            MouseSpecimenCleaner(),
            EmbryoSpecimenCleaner(),
            MGIStrainReportExtractor(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gentar-products_mice-latest.json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.mice_products_report_json_path,
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        mice_products_report_json_path = args[0]
        mouse_specimen_parquet_path = args[1]
        embryo_specimen_parquet_path = args[2]
        mgi_strain_parquet_path = args[3]
        output_path = args[4]

        products_df = spark.read.json(mice_products_report_json_path)
        mouse_specimen_df = spark.read.parquet(mouse_specimen_parquet_path)
        embryo_specimen_df = spark.read.parquet(embryo_specimen_parquet_path)
        mgi_strain_df = spark.read.parquet(mgi_strain_parquet_path)

        mouse_specimen_df = mouse_specimen_df.select("_colonyID", "_strainID")
        embryo_specimen_df = embryo_specimen_df.select("_colonyID", "_strainID")
        specimen_df = mouse_specimen_df.union(embryo_specimen_df).distinct()
        specimen_df = specimen_df.withColumn(
            "strain_id",
            when(col("_strainID").rlike("^[0-9]+$"), col("_strainID")).otherwise(
                lit(None)
            ),
        )
        specimen_df = specimen_df.withColumn(
            "strain_name",
            when(~col("_strainID").rlike("^[0-9]+$"), col("_strainID")).otherwise(
                lit(None)
            ),
        )

        specimen_df = specimen_df.join(
            mgi_strain_df,
            concat(lit("MGI:"), col("_strainID")) == col("mgiStrainID"),
            "left_outer",
        )

        specimen_df = specimen_df.withColumn(
            "strain_name",
            when(col("strain_name").isNull(), col("strainName")).otherwise(
                col("strain_name")
            ),
        )

        specimen_df = specimen_df.withColumnRenamed(
            "_colonyID", "associatedProductColonyName"
        )
        specimen_df = specimen_df.withColumnRenamed("strain_name", "displayStrainName")
        specimen_df = specimen_df.select(
            "associatedProductColonyName", "displayStrainName"
        ).distinct()

        products_df = products_df.join(
            specimen_df, "associatedProductColonyName", "left_outer"
        )

        products_df.repartition(1).write.option("ignoreNullFields", "false").json(
            output_path
        )


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
            f"{self.output_path}/impc_web_api/gene_images_service_json"
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
        window_spec = Window.partitionBy(
            "gene_accession_id",
            "strain_accession_id",
            "procedure_stable_id",
            "procedure_name",
            "parameter_stable_id",
            "parameter_name",
        ).orderBy("omero_id", "file_type")

        impc_images_df = (
            impc_images_df.withColumn(
                "min_omero_id", first("omero_id").over(window_spec)
            )
            .groupBy(
                "gene_accession_id",
                "strain_accession_id",
                "procedure_stable_id",
                "procedure_name",
                "parameter_stable_id",
                "parameter_name",
            )
            .agg(
                count("observation_id").alias("count"),
                first("thumbnail_url").alias("thumbnail_url"),
                first("file_type").alias("file_type"),
                when(first("file_type") == "application/pdf", lit(True))
                .when(first("min_omero_id") == -1, lit(True))
                .otherwise(lit(False))
                .alias("is_special_format"),
            )
        )

        impc_images_df = impc_images_df.withColumn(
            "thumbnail_url",
            when(col("is_special_format") == True, lit(None)).otherwise(
                col("thumbnail_url")
            ),
        )

        impc_images_df = impc_images_df.withColumnRenamed(
            "gene_accession_id", "mgiGeneAccessionId"
        )

        impc_images_df = impc_images_df.where(col("mgiGeneAccessionId").isNotNull())

        for col_name in impc_images_df.columns:
            impc_images_df = impc_images_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        impc_images_df.repartition(500).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcImagesMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcImagesMapper"

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
            f"{self.output_path}/impc_web_api/images_service_json"
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

        impc_images_df = impc_images_df.withColumnRenamed(
            "parameter_association_stable_id", "stableId"
        )
        impc_images_df = impc_images_df.withColumnRenamed(
            "parameter_association_sequence_id", "associationSequenceId"
        )
        impc_images_df = impc_images_df.withColumnRenamed(
            "parameter_association_name", "name"
        )
        impc_images_df = impc_images_df.withColumnRenamed(
            "parameter_association_value", "value"
        )
        impc_images_df = impc_images_df.withColumnRenamed(
            "life_stage_name", "lifeStageName"
        )

        impc_images_df = impc_images_df.withColumnRenamed(
            "gene_accession_id", "mgiGeneAccessionId"
        )

        impc_images_df = impc_images_df.withColumn(
            "anatomyTerms",
            zip_with(
                "anatomy_id",
                "anatomy_term",
                lambda x, y: when(
                    x.isNotNull(), struct(x.alias("anatomyTermId"), y.alias("name"))
                ).otherwise(lit(None)),
            ),
        )

        for col_name in impc_images_df.columns:
            impc_images_df = impc_images_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        impc_images_df = impc_images_df.withColumn(
            "thumbnailUrl",
            when(
                (col("omeroId") == -1) | (col("fileType") == "application/pdf"),
                lit(None),
            ).otherwise(col("thumbnailUrl")),
        )

        impc_images_df = impc_images_df.withColumn(
            "downloadUrl",
            when(
                (col("omeroId") != -1) & (col("fileType") != "application/pdf"),
                col("downloadUrl"),
            ).otherwise(col("downloadFilePath")),
        )

        impc_images_df = impc_images_df.withColumn(
            "associatedParameters",
            arrays_zip("stableId", "associationSequenceId", "name", "value"),
        ).drop("id", "associationSequenceId", "name", "value")

        impc_images_experimental_df = (
            impc_images_df.where(col("biologicalSampleGroup") == "experimental")
            .groupBy(
                "mgiGeneAccessionId",
                "geneSymbol",
                "strainAccessionId",
                "pipelineStableId",
                "procedureStableId",
                "procedureName",
                "parameterStableId",
                "parameterName",
                "biologicalSampleGroup",
                "metadataGroup",
                col("phenotypingCenter").alias("phenotypingCentre"),
            )
            .agg(
                collect_set(
                    struct(
                        "thumbnailUrl",
                        "downloadUrl",
                        "jpegUrl",
                        "fileType",
                        "observationId",
                        "specimenId",
                        "colonyId",
                        "sex",
                        "zygosity",
                        "ageInWeeks",
                        "alleleSymbol",
                        "associatedParameters",
                        "dateOfExperiment",
                        "anatomyTerms",
                        "imageLink",
                    )
                ).alias("images")
            )
        )

        window_controls = Window.partitionBy(
            "strainAccessionId",
            "procedureStableId",
            "procedureName",
            "parameterStableId",
            "parameterName",
            "metadataGroup",
            "phenotypingCenter",
        ).orderBy(col("observationId"))

        impc_images_control_df = impc_images_df.where(
            col("biologicalSampleGroup") == "control"
        )

        impc_images_control_df = impc_images_control_df.withColumn(
            "row", row_number().over(window_controls)
        )

        impc_images_control_df = impc_images_control_df.drop("row")

        impc_images_control_df = impc_images_control_df.groupBy(
            "mgiGeneAccessionId",
            "geneSymbol",
            "strainAccessionId",
            "pipelineStableId",
            "procedureStableId",
            "procedureName",
            "parameterStableId",
            "parameterName",
            "biologicalSampleGroup",
            "metadataGroup",
            col("phenotypingCenter").alias("phenotypingCentre"),
        ).agg(
            collect_set(
                struct(
                    "thumbnailUrl",
                    "downloadUrl",
                    "jpegUrl",
                    "fileType",
                    "observationId",
                    "specimenId",
                    "colonyId",
                    "sex",
                    "zygosity",
                    "ageInWeeks",
                    "alleleSymbol",
                    "associatedParameters",
                    "dateOfExperiment",
                    "anatomyTerms",
                    "imageLink",
                )
            ).alias("images")
        )
        impc_images_df = impc_images_experimental_df.union(impc_images_control_df)

        impc_images_df.repartition(500).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcGeneDiseasesMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcGeneDiseasesMapper"

    #: Path to the CSV gene disease association report
    disease_model_summary_csv_path = luigi.Parameter()

    #: Path to the CSV gene disease association report
    mouse_model_phenodigm_csv_path = luigi.Parameter()

    #: Path to the CSV gene disease association report
    disease_phenodigm_csv_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/gene_diseases_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.disease_model_summary_csv_path,
            self.mouse_model_phenodigm_csv_path,
            self.disease_phenodigm_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        disease_model_summary_csv_path = args[0]
        mouse_model_phenodigm_csv_path = args[1]  # model_id, model_phenotypes
        disease_phenodigm_csv_path = args[2]  # disease_id, disease_phenotypes
        output_path = args[3]

        disease_df = spark.read.csv(disease_model_summary_csv_path, header=True).drop(
            "disease_phenotypes", "model_phenotypes"
        )
        mouse_model_df = spark.read.csv(
            mouse_model_phenodigm_csv_path, header=True
        ).select("model_id", "model_phenotypes")
        disease_phenodigm_df = spark.read.csv(
            disease_phenodigm_csv_path, header=True
        ).select("disease_id", "disease_phenotypes")

        disease_df = disease_df.withColumn(
            "phenodigm_score",
            (col("disease_model_avg_norm") + col("disease_model_max_norm")) / 2,
        )

        disease_df = disease_df.join(disease_phenodigm_df, "disease_id", "left_outer")
        disease_df = disease_df.join(mouse_model_df, "model_id", "left_outer")

        window_spec = Window.partitionBy("disease_id", "marker_id").orderBy(
            col("phenodigm_score").desc()
        )

        max_disease_df = disease_df.withColumn(
            "row_number", row_number().over(window_spec)
        )

        max_disease_df = max_disease_df.withColumn(
            "isMaxPhenodigmScore", col("row_number") == 1
        ).drop("row_number")

        max_disease_df = max_disease_df.withColumnRenamed(
            "marker_id", "mgiGeneAccessionId"
        )

        for col_name in max_disease_df.columns:
            max_disease_df = max_disease_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        double_cols = [
            "diseaseModelAvgNorm",
            "diseaseModelAvgRaw",
            "diseaseModelMaxRaw",
            "diseaseModelMaxNorm",
        ]

        for col_name in double_cols:
            max_disease_df = max_disease_df.withColumn(
                col_name, col(col_name).astype(DoubleType())
            )

        max_disease_df = max_disease_df.withColumn(
            "markerNumModels", col("markerNumModels").astype(IntegerType())
        )
        max_disease_df = max_disease_df.withColumn(
            "associationCurated", col("associationCurated").astype(BooleanType())
        )

        max_disease_df.repartition(500).write.option("ignoreNullFields", "false").json(
            output_path
        )


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
            f"{self.output_path}/impc_web_api/gene_histopathology_service_json"
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
        gp_df = gp_df.withColumnRenamed("markerAccessionId", "mgiGeneAccessionId")

        gp_df.repartition(500).write.option("ignoreNullFields", "false").json(
            output_path
        )


class ImpcDatasetsMetadataMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcDatasetsMetadataMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            ImpressToParameterMapper(),
            StatsResultsMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/datasets_metadata_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        impress_parameter_parquet_path = args[0]
        stats_results_parquet_path = args[1]
        output_path = args[2]

        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        explode_cols = [
            "procedure_stable_key",
            "procedure_stable_id",
            "procedure_name",
            "parameter_stable_key",
            "project_name",
            "life_stage_name",
            "life_stage_acc",
        ]
        impress_parameter_df = spark.read.parquet(impress_parameter_parquet_path)
        unit_df = impress_parameter_df.select(
            "fully_qualified_name",
            col("unit_x").alias("x"),
            col("unit_y").alias("y"),
            col("categories").alias("parameter_category_list"),
        )
        unit_df = unit_df.withColumn("unit", struct("x", "y"))
        unit_df = unit_df.drop("x", "y")
        stats_results_df = stats_results_df.withColumn(
            "fully_qualified_name",
            concat_ws(
                "_", "pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"
            ),
        )
        stats_results_df = stats_results_df.join(
            unit_df, "fully_qualified_name", "left_outer"
        ).drop("fully_qualified_name")
        col_name_map = {
            "doc_id": "datasetId",
            "marker_accession_id": "mgiGeneAccessionId",
            "marker_symbol": "geneSymbol",
            "metadata": "metadataValues",
            "production_center": "productionCentre",
            "phenotyping_center": "phenotypingCentre",
            "resource_fullname": "resourceFullName",
            "statistical_method": "name",
            "soft_windowing_bandwidth": "bandwidth",
            "soft_windowing_shape": "shape",
            "soft_windowing_peaks": "peaks",
            "soft_windowing_min_obs_required": "minObsRequired",
            "soft_windowing_total_obs_or_weight": "totalObsOrWeight",
            "soft_windowing_threshold": "threshold",
            "soft_windowing_number_of_doe": "numberOfDoe",
            "soft_windowing_doe_note": "doeNote",
            "effect_size": "reportedEffectSize",
            "p_value": "reportedPValue",
        }

        new_structs_dict = {
            "summaryStatistics": [
                "female_control_count",
                "female_control_mean",
                "female_control_sd",
                "male_control_count",
                "male_control_mean",
                "male_control_sd",
                "female_mutant_count",
                "female_mutant_mean",
                "female_mutant_sd",
                "male_mutant_count",
                "male_mutant_mean",
                "male_mutant_sd",
                "both_mutant_count",
                "both_mutant_mean",
                "both_mutant_sd",
            ],
            "statisticalMethod": [
                "statistical_method",
                {
                    "attributes": [
                        "female_ko_effect_p_value",
                        "female_ko_effect_stderr_estimate",
                        "female_ko_parameter_estimate",
                        "female_percentage_change",
                        "male_ko_effect_p_value",
                        "male_ko_effect_stderr_estimate",
                        "male_ko_parameter_estimate",
                        "male_percentage_change",
                        "genotype_effect_p_value",
                        "genotype_effect_stderr_estimate",
                        "group_1_genotype",
                        "group_1_residuals_normality_test",
                        "group_2_genotype",
                        "group_2_residuals_normality_test",
                        "interaction_effect_p_value",
                        "interaction_significant",
                        "intercept_estimate",
                        "intercept_estimate_stderr_estimate",
                        "sex_effect_p_value",
                        "sex_effect_parameter_estimate",
                        "sex_effect_stderr_estimate",
                        "male_effect_size",
                        "female_effect_size",
                        "batch_significant",
                        "variance_significant",
                    ]
                },
            ],
            "softWindowing": [
                "soft_windowing_bandwidth",
                "soft_windowing_shape",
                "soft_windowing_peaks",
                "soft_windowing_min_obs_required",
                "soft_windowing_total_obs_or_weight",
                "soft_windowing_threshold",
                "soft_windowing_number_of_doe",
                "soft_windowing_doe_note",
            ],
        }

        int_columns = [
            "female_control_count",
            "male_control_count",
            "female_mutant_count",
            "male_mutant_count",
            "both_mutant_count",
        ]
        double_columns = [
            "p_value",
            "effect_size",
            "female_ko_effect_p_value",
            "female_ko_parameter_estimate",
            "female_percentage_change",
            "genotype_effect_p_value",
            "male_ko_effect_p_value",
            "male_ko_parameter_estimate",
            "male_percentage_change",
        ]

        for col_name in int_columns:
            stats_results_df = stats_results_df.withColumn(
                col_name, col(col_name).astype(IntegerType())
            )
        for col_name in double_columns:
            stats_results_df = stats_results_df.withColumn(
                col_name, col(col_name).astype(DoubleType())
            )

        for col_name in explode_cols:
            stats_results_df = stats_results_df.withColumn(col_name, explode(col_name))
        stats_results_df = stats_results_df.select(
            "doc_id",  # statisticalResultId
            "strain_accession_id",
            "strain_name",
            "genetic_background",
            "colony_id",
            "marker_accession_id",  # mgiGeneAccessionId
            "marker_symbol",  # geneSymbol
            "allele_accession_id",
            "allele_name",
            "allele_symbol",
            "metadata_group",
            "metadata",  # metadataValues
            "life_stage_acc",  # explode
            "life_stage_name",  # explode
            "data_type",
            "production_center",  # productionCentre
            "phenotyping_center",  # phenotypingCentre
            "project_name",
            "resource_name",
            "resource_fullname",  # resourceFullName
            "pipeline_stable_key",
            "pipeline_stable_id",
            "pipeline_name",
            "procedure_stable_key",  # explode
            "procedure_stable_id",  # explode
            "procedure_name",  # explode
            "procedure_group",
            "parameter_stable_key",  # explode
            "parameter_stable_id",
            "parameter_name",
            "female_control_count",  # group under summaryStatistics
            "female_control_mean",  # group under summaryStatistics
            "female_control_sd",  # group under summaryStatistics
            "male_control_count",  # group under summaryStatistics
            "male_control_mean",  # group under summaryStatistics
            "male_control_sd",  # group under summaryStatistics
            "female_mutant_count",  # group under summaryStatistics
            "female_mutant_mean",  # group under summaryStatistics
            "female_mutant_sd",  # group under summaryStatistics
            "male_mutant_count",  # group under summaryStatistics
            "male_mutant_mean",  # group under summaryStatistics
            "male_mutant_sd",  # group under summaryStatistics
            "both_mutant_count",  # group under summaryStatistics
            "both_mutant_mean",  # group under summaryStatistics
            "both_mutant_sd",  # group under summaryStatistics
            "status",
            "sex",  # phenotypeSex
            "zygosity",
            "phenotype_sex",  # testedSexes
            "significant",
            "classification_tag",
            "p_value",  # reportedPValue
            "effect_size",  # reportedEffectSize
            "statistical_method",  # name, group under statisticalMethod
            "female_ko_effect_p_value",  # group under statisticalMethod.attributes
            "female_ko_effect_stderr_estimate",  # group under statisticalMethod.attributes
            "female_ko_parameter_estimate",  # group under statisticalMethod.attributes
            "female_percentage_change",  # group under statisticalMethod.attributes
            "male_ko_effect_p_value",  # group under statisticalMethod.attributes
            "male_ko_effect_stderr_estimate",  # group under statisticalMethod.attributes
            "male_ko_parameter_estimate",  # group under statisticalMethod.attributes
            "male_percentage_change",  # group under statisticalMethod.attributes
            "genotype_effect_p_value",  # group under statisticalMethod.attributes
            "genotype_effect_stderr_estimate",  # group under statisticalMethod.attributes
            "group_1_genotype",  # group under statisticalMethod.attributes
            "group_1_residuals_normality_test",  # group under statisticalMethod.attributes
            "group_2_genotype",  # group under statisticalMethod.attributes
            "group_2_residuals_normality_test",  # group under statisticalMethod.attributes
            "interaction_effect_p_value",  # group under statisticalMethod.attributes
            "interaction_significant",  # group under statisticalMethod.attributes
            "intercept_estimate",  # group under statisticalMethod.attributes
            "intercept_estimate_stderr_estimate",  # group under statisticalMethod.attributes
            "sex_effect_p_value",  # group under statisticalMethod.attributes
            "sex_effect_parameter_estimate",  # group under statisticalMethod.attributes
            "sex_effect_stderr_estimate",  # group under statisticalMethod.attributes
            "male_effect_size",  # group under statisticalMethod.attributes
            "female_effect_size",  # group under statisticalMethod.attributes
            "batch_significant",  # group under statisticalMethod.attributes
            "variance_significant",  # group under statisticalMethod.attributes
            "soft_windowing_bandwidth",  # bandwidth, group under softWindowing
            "soft_windowing_shape",  # shape, group under softWindowing
            "soft_windowing_peaks",  # peaks, group under softWindowing
            "soft_windowing_min_obs_required",  # minObsRequired, group under softWindowing
            "soft_windowing_total_obs_or_weight",  # totalObsOrWeight, group under softWindowing
            "soft_windowing_threshold",  # threshold, group under softWindowing
            "soft_windowing_number_of_doe",  # numberOfDoe, group under softWindowing
            "soft_windowing_doe_note",  # doeNote, group under softWindowing
            "mp_term_id",
            "mp_term_name",
            "intermediate_mp_term_id",
            "intermediate_mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
            "mp_term_id_options",
            "mp_term_name_options",
            "unit",
            "parameter_category_list",
        )

        stats_results_df = stats_results_df.withColumn(
            "soft_windowing_peaks",
            split(regexp_replace("soft_windowing_peaks", "\[|\]", ""), ",").cast(
                "array<int>"
            ),
        )

        stats_results_df = stats_results_df.withColumn(
            "significantPhenotype",
            when(
                col("mp_term_id").isNotNull(),
                struct(
                    col("mp_term_id").alias("id"), col("mp_term_name").alias("name")
                ),
            ).otherwise(lit(None)),
        )

        stats_results_df = stats_results_df.withColumn(
            "intermediatePhenotypes",
            zip_with(
                "intermediate_mp_term_id",
                "intermediate_mp_term_name",
                phenotype_term_zip_udf,
            ),
        )

        stats_results_df = stats_results_df.withColumn(
            "topLevelPhenotypes",
            zip_with(
                "top_level_mp_term_id",
                "top_level_mp_term_name",
                phenotype_term_zip_udf,
            ),
        )

        stats_results_df = stats_results_df.withColumn(
            "potentialPhenotypes",
            zip_with(
                "mp_term_id_options",
                "mp_term_name_options",
                phenotype_term_zip_udf,
            ),
        )

        stats_results_df = stats_results_df.drop(
            "mp_term_id",
            "mp_term_name",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
            "intermediate_mp_term_id",
            "intermediate_mp_term_name",
            "mp_term_id_options",
            "mp_term_name_options",
        )

        stats_results_df = stats_results_df.withColumnRenamed(
            "marker_accession_id", "mgiGeneAccessionId"
        )
        stats_results_df = stats_results_df.withColumnRenamed("doc_id", "datasetId")
        stats_results_df = stats_results_df.withColumn("id", col("datasetId"))

        drop_list = []
        for struct_col_name in new_structs_dict.keys():
            columns = new_structs_dict[struct_col_name]
            fields = []
            for column in columns:
                if type(column) == str:
                    fields.append(
                        col(column).alias(col_name_map[column])
                        if column in col_name_map
                        else col(column).alias(to_camel_case(column))
                    )
                    drop_list.append(column)
                else:
                    for sub_field_name in column.keys():
                        fields.append(
                            struct(
                                *[
                                    col(sub_column).alias(col_name_map[sub_column])
                                    if sub_column in col_name_map
                                    else col(sub_column).alias(
                                        to_camel_case(sub_column)
                                    )
                                    for sub_column in column[sub_field_name]
                                ]
                            ).alias(sub_field_name)
                        )
                        for sub_column in column[sub_field_name]:
                            drop_list.append(sub_column)

            stats_results_df = stats_results_df.withColumn(
                struct_col_name,
                struct(*fields),
            )

        stats_results_df = stats_results_df.drop(*drop_list)

        for col_name in stats_results_df.columns:
            stats_results_df = stats_results_df.withColumnRenamed(
                col_name,
                col_name_map[col_name]
                if col_name in col_name_map
                else to_camel_case(col_name),
            )
        stats_results_df.repartition(1000).write.option(
            "ignoreNullFields", "false"
        ).json(output_path)


class ImpcDatasetsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcDatasetsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            StatsResultsMapper(raw_data_in_output="bundled"),
            ExperimentToObservationMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/datasets_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        dataset_observation_index_parquet_path = args[0] + "_raw_data_ids"
        observations_parquet_path = args[1]
        output_path = args[2]

        dataset_observation_index_df = spark.read.parquet(
            dataset_observation_index_parquet_path
        )

        line_level_procedures = [
            "IMPC_VIA_002",
            "IMPC_VIA_001",
            "IMPC_EVM_001",
            "IMPC_FER_001",
            "IMPC_EVL_001",
            "IMPC_EVP_001",
            "IMPC_EVO_001",
        ]
        observations_df = spark.read.parquet(observations_parquet_path)
        line_observations_df = observations_df.where(
            col("procedure_stable_id").isin(line_level_procedures)
        )
        dataset_line_observation_index_df = dataset_observation_index_df.select(
            "doc_id", "observation_id"
        ).distinct()
        observations_df = observations_df.where(
            ~col("procedure_stable_id").isin(line_level_procedures)
        )
        dataset_observation_index_df = dataset_observation_index_df.withColumn(
            "window_weight",
            when(col("window_weight").isNotNull(), col("window_weight")).otherwise(
                expr("transform(observation_id, id -> NULL)")
            ),
        )
        dataset_observation_index_df = dataset_observation_index_df.withColumn(
            "obs_id_ww",
            arrays_zip(
                "observation_id",
                "window_weight",
            ),
        )
        dataset_observation_index_df = dataset_observation_index_df.drop(
            "observation_id", "window_weight"
        )
        dataset_observation_index_df = dataset_observation_index_df.withColumn(
            "obs_id_ww", explode("obs_id_ww")
        )
        dataset_observation_index_df = dataset_observation_index_df.select(
            "doc_id", "obs_id_ww.*"
        )
        observations_df = observations_df.select(
            "observation_id",
            "biological_sample_group",
            "date_of_experiment",
            "external_sample_id",
            "sex",
            "weight",
            "data_point",
            "category",
            "time_point",
            "discrete_point",
            "date_of_birth",
            "sub_term_id",
            "sub_term_name",
        ).distinct()

        datasets_df = dataset_observation_index_df.join(
            observations_df, "observation_id"
        )
        datasets_df = datasets_df.drop("observation_id")
        datasets_col_map = {
            "doc_id": "datasetId",
            "biological_sample_group": "sampleGroup",
            "sex": "specimenSex",
            "date_of_birth": "specimenDateOfBirth",
            "date_of_experiment": "dateOfExperiment",
            "external_sample_id": "specimenId",
            "weight": "bodyWeight",
            "window_weight": "windowWeight",
            "category": "category",
            "data_point": "dataPoint",
            "time_point": "timePoint",
            "discrete_point": "discretePoint",
        }
        double_type_cols = ["data_point", "discrete_point", "weight", "window_weight"]
        for double_col in double_type_cols:
            datasets_df = datasets_df.withColumn(
                double_col, col(double_col).astype(DoubleType())
            )
        for column_name, new_column_name in datasets_col_map.items():
            datasets_df = datasets_df.withColumnRenamed(column_name, new_column_name)

        datasets_df = datasets_df.groupBy(
            "datasetId", "sampleGroup", "specimenSex"
        ).agg(
            collect_set(
                struct(
                    "specimenDateOfBirth",
                    "dateOfExperiment",
                    "specimenId",
                    "bodyWeight",
                    "windowWeight",
                    "category",
                    "dataPoint",
                    "timePoint",
                    "discretePoint",
                )
            ).alias("observations")
        )

        datasets_df = datasets_df.groupBy("datasetId").agg(
            collect_set(struct("sampleGroup", "specimenSex", "observations")).alias(
                "series"
            )
        )
        datasets_df.repartition(10000).write.parquet(output_path)

        dataset_line_observation_index_df = (
            dataset_line_observation_index_df.withColumn(
                "observation_id", explode("observation_id")
            )
        )

        line_datasets_df = dataset_line_observation_index_df.join(
            line_observations_df, "observation_id"
        )
        line_datasets_df = line_datasets_df.drop("observation_id")
        line_datasets_col_map = {
            "doc_id": "datasetId",
            "parameter_stable_id": "parameterStableId",
            "parameter_name": "parameterName",
            "data_point": "dataPoint",
        }

        for column_name, new_column_name in line_datasets_col_map.items():
            line_datasets_df = line_datasets_df.withColumnRenamed(
                column_name, new_column_name
            )
        line_datasets_df = line_datasets_df.withColumn(
            "dataPoint", col("dataPoint").astype(DoubleType())
        )
        line_datasets_df = line_datasets_df.groupBy("datasetId").agg(
            collect_set(
                struct(
                    "parameterStableId",
                    "parameterName",
                    "dataPoint",
                )
            ).alias("counts")
        )

        line_datasets_df.write.parquet(output_path + "_line")


class ImpcPhenotypeStatisticalResultsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcPhenotypeStatisticalResultsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [StatsResultsMapper(), GeneLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/phenotype_stats_service_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        stats_results_parquet_path = args[0]
        gene_parquet_path = args[1]
        output_path = args[2]

        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        gene_df = spark.read.parquet(gene_parquet_path)

        gene_df = gene_df.select(
            col("mgi_accession_id").alias("marker_accession_id"),
            "seq_region_start",
            "seq_region_end",
            "seq_region_id",
            "chr_name",
            "chr_strand",
            "marker_symbol",
            "marker_name",
        ).distinct()

        phenotype_stats_df = stats_results_df.select(
            "doc_id",
            "marker_accession_id",
            "p_value",
            "effect_size",
            "significant",
            "mp_term_id",
            "mp_term_id_options",
            "intermediate_mp_term_id",
            "top_level_mp_term_id",
            "resource_fullname",
            "mp_term_name",
            "intermediate_mp_term_name",
            "top_level_mp_term_name",
            "mp_term_name_options",
        )

        phenotype_stats_df = phenotype_stats_df.withColumn(
            "significantPhenotype",
            when(
                col("mp_term_id").isNotNull(),
                struct(
                    col("mp_term_id").alias("id"), col("mp_term_name").alias("name")
                ),
            ).otherwise(lit(None)),
        )

        phenotype_stats_df = phenotype_stats_df.withColumn(
            "intermediatePhenotypes",
            zip_with(
                "intermediate_mp_term_id",
                "intermediate_mp_term_name",
                phenotype_term_zip_udf,
            ),
        )

        phenotype_stats_df = phenotype_stats_df.withColumn(
            "topLevelPhenotypes",
            zip_with(
                "top_level_mp_term_id",
                "top_level_mp_term_name",
                phenotype_term_zip_udf,
            ),
        )

        phenotype_stats_df = phenotype_stats_df.withColumn(
            "potentialPhenotypes",
            zip_with(
                "mp_term_id_options",
                "mp_term_name_options",
                phenotype_term_zip_udf,
            ),
        )
        phenotype_stats_df = phenotype_stats_df.drop(
            "top_level_mp_term_id",
            "top_level_mp_term_name",
            "intermediate_mp_term_id",
            "intermediate_mp_term_name",
            "mp_term_id_options",
            "mp_term_name_options",
            "mp_term_id",
            "mp_term_name",
        )

        phenotype_stats_df = phenotype_stats_df.join(gene_df, "marker_accession_id")

        phenotype_stats_map = {
            "marker_accession_id": "mgiGeneAccessionId",
            "effect_size": "reportedEffectSize",
            "p_value": "reportedPValue",
            "resource_fullname": "resourceFullName",
            "doc_id": "datasetId",
        }

        for col_name in phenotype_stats_map.keys():
            phenotype_stats_df = phenotype_stats_df.withColumnRenamed(
                col_name, phenotype_stats_map[col_name]
            )

        for col_name in phenotype_stats_df.columns:
            phenotype_stats_df = phenotype_stats_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        double_cols = [
            "reportedEffectSize",
            "reportedPValue",
        ]

        for col_name in double_cols:
            phenotype_stats_df = phenotype_stats_df.withColumn(
                col_name, col(col_name).astype(DoubleType())
            )

        int_cols = [
            "seqRegionStart",
            "seqRegionEnd",
        ]

        for col_name in int_cols:
            phenotype_stats_df = phenotype_stats_df.withColumn(
                col_name, col(col_name).astype(IntegerType())
            )
        phenotype_stats_df = (
            phenotype_stats_df.withColumn(
                "potentialPhenotypes",
                when(
                    col("significantPhenotype").isNotNull(),
                    array("significantPhenotype"),
                ).otherwise(col("potentialPhenotypes")),
            )
            .withColumn(
                "phenotypes",
                concat(
                    "potentialPhenotypes",
                    "intermediatePhenotypes",
                    "topLevelPhenotypes",
                ),
            )
            .drop(
                "potentialPhenotypes",
                "intermediatePhenotypes",
                "topLevelPhenotypes",
                "significantPhenotype",
            )
            .withColumn("phenotypeId", explode("phenotypes.id"))
            .drop("phenotypes")
            .groupBy("phenotypeId")
            .agg(
                collect_set(
                    struct(
                        "mgiGeneAccessionId",
                        "markerSymbol",
                        "reportedPValue",
                        "reportedEffectSize",
                        "chrName",
                        "chrStrand",
                        "seqRegionStart",
                        "seqRegionEnd",
                        "significant",
                        "resourceFullName",
                    )
                ).alias("results")
            )
        )
        phenotype_stats_df.repartition(1000).write.parquet(output_path)


class ImpcBWTDatasetsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcBWTDatasetsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            StatsResultsMapper(raw_data_in_output="bundled"),
            ExperimentToObservationMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/bwt_curve_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        stats_results_parquet_path = args[0]
        raw_data_parquet_path = args[0] + "_raw_data_ids"
        observations_parquet_path = args[1]
        output_path = args[2]

        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        raw_data_df = spark.read.parquet(raw_data_parquet_path)
        observations_df = spark.read.parquet(observations_parquet_path)
        stats_results_df.select(
            "doc_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "marker_accession_id",
        ).withColumn("procedure_stable_id", explode("procedure_stable_id")).where(
            col("parameter_stable_id") == "IMPC_BWT_008_001"
        ).join(
            raw_data_df, "doc_id", "left_outer"
        ).withColumn(
            "observation_id", explode("observation_id")
        ).drop(
            "window_weight"
        ).join(
            observations_df.drop("procedure_stable_id", "parameter_stable_id"),
            "observation_id",
            "left_outer",
        ).select(
            "doc_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "marker_accession_id",
            "biological_sample_group",
            "sex",
            "zygosity",
            "discrete_point",
            "data_point",
        ).groupBy(
            "doc_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "marker_accession_id",
            "biological_sample_group",
            "sex",
            "zygosity",
            "discrete_point",
        ).agg(
            avg("data_point").alias("mean"),
            stddev("data_point").alias("std"),
            count("data_point").alias("count"),
        ).groupBy(
            col("doc_id").alias("datasetId"),
            col("procedure_stable_id").alias("procedureStableId"),
            col("parameter_stable_id").alias("parameterStableId"),
            col("marker_accession_id").alias("mgiGeneAccessionId"),
        ).agg(
            collect_set(
                struct(
                    col("biological_sample_group").alias("sampleGroup"),
                    "sex",
                    "zygosity",
                    col("discrete_point").alias("ageInWeeks"),
                    "mean",
                    "std",
                    "count",
                )
            ).alias("dataPoints")
        ).write.json(
            output_path
        )


class ImpcExternalLinksMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcExternalLinksMapper"

    mouse_human_ortholog_report_tsv_path: luigi.Parameter = luigi.Parameter()
    umass_early_lethal_report_csv_path: luigi.Parameter = luigi.Parameter()
    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [GeneLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/external_links_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.mouse_human_ortholog_report_tsv_path,
            self.umass_early_lethal_report_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        gene_parquet_path = args[0]
        mouse_human_ortholog_report_tsv_path = args[1]
        umass_early_lethal_report_csv_path = args[2]
        output_path = args[3]

        gene_df = spark.read.parquet(gene_parquet_path)
        mouse_human_ortholog_report_df = spark.read.csv(
            mouse_human_ortholog_report_tsv_path, sep="\t", header=True
        )
        umass_early_lethal_report_df = spark.read.csv(
            umass_early_lethal_report_csv_path, header=True, multiLine=True
        )
        umass_early_lethal_report_df = umass_early_lethal_report_df.withColumnRenamed(
            "MGI Number", "mgi_accession_id"
        )
        umass_early_lethal_report_df = umass_early_lethal_report_df.withColumnRenamed(
            "Description only", "description"
        )
        umass_early_lethal_report_df = umass_early_lethal_report_df.withColumn(
            "Link", concat(lit("https://"), col("Link"))
        )
        umass_early_lethal_report_df = umass_early_lethal_report_df.withColumnRenamed(
            "Link", "href"
        )
        umass_early_lethal_report_df = umass_early_lethal_report_df.withColumn(
            "description", regexp_replace("description", "[\b\n\r]", " ")
        )

        umass_early_lethal_report_df = umass_early_lethal_report_df.withColumn(
            "mgi_accession_id",
            concat_ws(":", lit("MGI"), trim("mgi_accession_id")),
        ).select("mgi_accession_id", "description", "href")
        for col_name in mouse_human_ortholog_report_df.columns:
            mouse_human_ortholog_report_df = (
                mouse_human_ortholog_report_df.withColumnRenamed(
                    col_name, col_name.replace(" ", "_").lower()
                )
            )

        mouse_human_ortholog_report_df = mouse_human_ortholog_report_df.select(
            "human_gene_symbol", "mgi_gene_acc_id"
        )

        mouse_human_ortholog_report_df = (
            mouse_human_ortholog_report_df.withColumnRenamed(
                "mgi_gene_acc_id", "mgi_gene_accession_id"
            )
        )

        gene_mgi_accession_df = (
            gene_df.select("mgi_accession_id")
            .withColumnRenamed("mgi_accession_id", "mgi_gene_accession_id")
            .dropDuplicates()
        )

        gwas_external_links_df = gene_mgi_accession_df.join(
            mouse_human_ortholog_report_df, "mgi_gene_accession_id"
        )

        gwas_external_links_df = gwas_external_links_df.withColumnRenamed(
            "mgi_gene_accession_id", "mgiGeneAccessionId"
        )

        gwas_external_links_df = gwas_external_links_df.withColumnRenamed(
            "human_gene_symbol", "label"
        )

        gwas_external_links_df = gwas_external_links_df.withColumn(
            "href",
            concat(
                lit("https://www.ebi.ac.uk/gwas/genes/"), gwas_external_links_df.label
            ),
        )
        gwas_external_links_df = gwas_external_links_df.withColumn(
            "providerName", lit("GWAS Catalog")
        )

        gwas_external_links_df = gwas_external_links_df.withColumn(
            "description", lit(None)
        )

        embryo_data_df = gene_df.select(
            "mgi_accession_id",
            "marker_symbol",
        ).distinct()
        embryo_data_df = embryo_data_df.join(
            umass_early_lethal_report_df, "mgi_accession_id"
        )

        umass_external_links_df = embryo_data_df.withColumnRenamed(
            "mgi_accession_id", "mgiGeneAccessionId"
        )
        umass_external_links_df = umass_external_links_df.withColumnRenamed(
            "marker_symbol", "label"
        )
        umass_external_links_df = umass_external_links_df.withColumn(
            "providerName", lit("Mager Lab Early Lethal Phenotypes")
        )
        umass_external_links_df = umass_external_links_df.select(
            "mgiGeneAccessionId", "label", "href", "providerName", "description"
        )

        external_links_df = gwas_external_links_df.union(umass_external_links_df)
        external_links_df.write.json(output_path, mode="overwrite")


class ImpcPathologyDatasetsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcPathologyDatasetsMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ExperimentToObservationMapper(), ImpressToParameterMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/pathology_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        observations_parquet_path = args[0]
        parameter_parquet_path = args[1]
        output_path = args[2]

        observations_df = spark.read.parquet(observations_parquet_path)
        parameter_df = (
            spark.read.parquet(parameter_parquet_path)
            .select(
                "pipeline_stable_id",
                "pipeline_stable_key",
                "procedure_stable_id",
                "procedure_stable_key",
                "parameter_stable_id",
                "parameter_stable_key",
            )
            .distinct()
        )
        observations_df = observations_df.join(
            parameter_df,
            [
                "pipeline_stable_id",
                "procedure_stable_id",
                "parameter_stable_id",
            ],
        )
        pathology_datasets_cols = [
            "gene_accession_id",
            "gene_symbol",
            "allele_accession_id",
            "allele_symbol",
            "zygosity",
            "pipeline_stable_id",
            "pipeline_stable_key",
            "procedure_stable_id",
            "procedure_stable_key",
            "procedure_name",
            "parameter_stable_id",
            "parameter_stable_key",
            "parameter_name",
            "life_stage_name",
            "sub_term_id",
            "sub_term_name",
            "text_value",
            "category",
            "data_point",
            "external_sample_id",
            "phenotyping_center",
            "metadata",
            "strain_name",
            "strain_accession_id",
        ]
        observations_df = observations_df.select(*pathology_datasets_cols)
        pathology_datasets_df = observations_df.where(
            col("parameter_stable_id").contains("PAT")
        )
        pathology_datasets_df = pathology_datasets_df.withColumnRenamed(
            "gene_accession_id", "mgi_gene_accession_id"
        )
        for col_name in pathology_datasets_df.columns:
            pathology_datasets_df = pathology_datasets_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        pathology_datasets_df = pathology_datasets_df.withColumnRenamed(
            "subTermId", "termId"
        )
        pathology_datasets_df = pathology_datasets_df.withColumnRenamed(
            "subTermName", "termName"
        )

        pathology_datasets_df = pathology_datasets_df.withColumnRenamed(
            "external_sample_id", "sampleId"
        )

        pathology_datasets_df = pathology_datasets_df.withColumn(
            "ontologyTerms", arrays_zip("termId", "termName")
        )
        pathology_datasets_df = pathology_datasets_df.drop("termId", "termName")
        common_columns = [
            "mgiGeneAccessionId",
            "geneSymbol",
            "pipelineStableId",
            "pipelineStableKey",
            "procedureStableId",
            "procedureStableKey",
            "procedureName",
            "parameterStableId",
            "parameterStableKey",
            "parameterName",
        ]
        pathology_datasets_df = pathology_datasets_df.groupBy(*common_columns).agg(
            collect_set(
                struct(
                    *[
                        to_camel_case(col_name)
                        for col_name in pathology_datasets_df.columns
                        if col_name not in common_columns
                    ]
                )
            ).alias("datasets")
        )
        pathology_datasets_df.write.json(output_path, mode="overwrite")


class ImpcHistopathologyDatasetsMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcHistopathologyDatasetsMapper"

    ma_metadata_csv_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ExperimentToObservationMapper(), ImpcImagesLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/histopathology_service_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.ma_metadata_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        observations_parquet_path = args[0]
        impc_images_parquet_path = args[1]
        ma_metadata_csv_path = args[2]
        output_path = args[3]

        observations_df = spark.read.parquet(observations_parquet_path)
        impc_images_df = spark.read.parquet(impc_images_parquet_path)
        ma_metadata_df = spark.read.csv(ma_metadata_csv_path, header=True).select(
            "curie", "name"
        )

        ma_metadata_df = ma_metadata_df.withColumnRenamed("curie", "sub_term_id")
        ma_metadata_df = ma_metadata_df.withColumnRenamed("name", "sub_term_name")

        histopathology_datasets_cols = [
            "gene_accession_id",
            "allele_accession_id",
            "allele_symbol",
            "zygosity",
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "parameter_name",
            "life_stage_name",
            "specimen_id",
            "external_sample_id",
            "phenotyping_center",
            "text_value",
            "category",
            lit(None).astype(StringType()).alias("omero_id"),
            lit(None).astype(StringType()).alias("jpeg_url"),
            lit(None).astype(StringType()).alias("thumbnail_url"),
            "sub_term_id",
            "sub_term_name",
        ]
        observations_df = observations_df.select(*histopathology_datasets_cols)
        histopathology_datasets_df = observations_df.where(
            col("parameter_stable_id").contains("HIS")
            & ~col("parameter_name").contains("Images")
        )

        histopathology_images_df = impc_images_df.where(
            col("parameter_stable_id").contains("HIS")
        )

        histopathology_images_df = histopathology_images_df.select(
            "gene_accession_id",
            "allele_accession_id",
            "allele_symbol",
            "zygosity",
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "parameter_name",
            "life_stage_name",
            "specimen_id",
            "external_sample_id",
            "phenotyping_center",
            "parameter_association_name",
            "parameter_association_value",
            lit(None).astype(StringType()).alias("text_value"),
            lit(None).astype(StringType()).alias("category"),
            "omero_id",
            "jpeg_url",
            "thumbnail_url",
        )

        histopathology_images_df = histopathology_images_df.withColumn(
            "parameter_association_name",
            explode(array_distinct("parameter_association_name")),
        )

        histopathology_images_df = histopathology_images_df.withColumn(
            "parameter_association_value",
            explode(array_distinct("parameter_association_value")),
        )

        histopathology_images_df = histopathology_images_df.withColumn(
            "parameter_name",
            concat(
                regexp_extract(col("parameter_association_name"), r"(.*) - .*", 1),
                lit(" - "),
                "parameter_name",
            ),
        ).drop("parameter_association_name")

        histopathology_images_df = histopathology_images_df.join(
            ma_metadata_df,
            col("parameter_association_value") == col("sub_term_id"),
            "left_outer",
        ).drop("parameter_association_value")

        histopathology_images_df = histopathology_images_df.withColumn(
            "sub_term_id", array("sub_term_id")
        )
        histopathology_images_df = histopathology_images_df.withColumn(
            "sub_term_name", array("sub_term_name")
        )

        histopathology_datasets_df = histopathology_datasets_df.union(
            histopathology_images_df
        )
        histopathology_datasets_df = histopathology_datasets_df.withColumnRenamed(
            "gene_accession_id", "mgi_gene_accession_id"
        )

        for col_name in histopathology_datasets_df.columns:
            histopathology_datasets_df = histopathology_datasets_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        histopathology_datasets_df = histopathology_datasets_df.withColumnRenamed(
            "subTermId", "termId"
        )
        histopathology_datasets_df = histopathology_datasets_df.withColumnRenamed(
            "subTermName", "termName"
        )

        histopathology_datasets_df = histopathology_datasets_df.withColumnRenamed(
            "external_sample_id", "sampleId"
        )

        histopathology_datasets_df = histopathology_datasets_df.withColumn(
            "ontologyTerms", arrays_zip("termId", "termName")
        )
        histopathology_datasets_df = histopathology_datasets_df.drop(
            "termId", "termName"
        )

        histopathology_datasets_df = histopathology_datasets_df.withColumn(
            "tissue", regexp_extract(col("parameterName"), r"(.*) - .*", 1)
        )
        histopathology_datasets_df = histopathology_datasets_df.groupBy(
            "mgiGeneAccessionId", "tissue"
        ).agg(
            collect_set(
                struct(
                    *[
                        to_camel_case(col_name)
                        for col_name in histopathology_datasets_df.columns
                        if col_name not in ["mgiGeneAccessionId", "tissue"]
                    ]
                )
            ).alias("observations")
        )
        histopathology_datasets_df = (
            histopathology_datasets_df.groupBy(
                "mgiGeneAccessionId",
            )
            .agg(collect_set(struct("tissue", "observations")).alias("datasets"))
            .where(col("mgiGeneAccessionId").isNotNull())
        )
        histopathology_datasets_df.write.json(output_path, mode="overwrite")


class ImpcReleaseMetadataMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcReleaseMetadataMapper"
    release_version: luigi.Parameter = luigi.Parameter()
    release_date: luigi.Parameter = luigi.Parameter()
    release_notes_md_path: luigi.Parameter = luigi.Parameter()
    statistical_analysis_package_name: str = luigi.Parameter()
    statistical_analysis_package_version: str = luigi.Parameter()
    genome_assembly_species: str = luigi.Parameter()
    genome_assembly_assembly_version: str = luigi.Parameter()
    gentar_gene_status_json_path: luigi.Parameter = luigi.Parameter()
    mp_calls_historic_csv_path: str = luigi.Parameter()
    data_points_historic_csv_path: str = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ExperimentToObservationMapper(), GenotypePhenotypeLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/release_metadata.json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.release_version,
            self.release_date,
            self.release_notes_md_path,
            self.statistical_analysis_package_name,
            self.statistical_analysis_package_version,
            self.genome_assembly_species,
            self.genome_assembly_assembly_version,
            self.gentar_gene_status_json_path,
            self.mp_calls_historic_csv_path,
            self.data_points_historic_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        observations_parquet_path = args[0]
        gene_phenotype_parquet_path = args[1]
        release_version = args[2]
        release_date = args[3]
        release_notes_md_path = args[4]
        statistical_analysis_package_name = args[5]
        statistical_analysis_package_version = args[6]
        genome_assembly_species = args[7]
        genome_assembly_version = args[8]
        gentar_gene_status_json_path = args[9]
        mp_calls_historic_csv_path = args[10]
        data_points_historic_csv_path = args[11]
        output_path = args[12]

        observations_df = spark.read.parquet(observations_parquet_path)
        gene_phenotype_df = spark.read.parquet(gene_phenotype_parquet_path)
        gentar_gene_status_df = spark.read.json(gentar_gene_status_json_path)

        with open(release_notes_md_path, "r", encoding="utf-8") as file:
            md_content = file.read()

        release_notes = json.dumps(md_content)

        phenotyped_genes = (
            observations_df.where(col("biological_sample_group") == lit("experimental"))
            .where(col("datasource_name") == lit("IMPC"))
            .select("gene_accession_id")
            .distinct()
            .count()
        )

        phenotyped_lines = (
            observations_df.where(col("biological_sample_group") == lit("experimental"))
            .where(col("datasource_name") == lit("IMPC"))
            .select("colony_id")
            .distinct()
            .count()
        )

        significant_calls = gene_phenotype_df.count()
        significant_calls = (
            significant_calls
            + observations_df.where(col("procedure_stable_id").contains("ELZ"))
            .where(col("category") == lit("expression"))
            .select(
                "colony_id",
                "external_sample_id",
                "parameter_stable_id",
                "category",
                "sex",
            )
            .count()
        )

        lines_by_phenotyping_center = (
            observations_df.where(col("datasource_name") == lit("IMPC"))
            .groupBy(col("phenotyping_center").alias("phenotypingCentre"))
            .agg(
                countDistinct(
                    when(
                        col("biological_sample_group") == lit("experimental"),
                        col("colony_id"),
                    ).otherwise(lit(None))
                ).alias("mutantLines"),
                countDistinct(
                    when(
                        col("biological_sample_group") == lit("experimental"),
                        col("external_sample_id"),
                    ).otherwise(lit(None))
                ).alias("mutantSpecimens"),
                countDistinct(
                    when(
                        col("biological_sample_group") == lit("control"),
                        col("external_sample_id"),
                    ).otherwise(lit(None))
                ).alias("controlSpecimens"),
            )
            .rdd.map(lambda row: row.asDict())
            .collect()
        )

        data_quality_counts = (
            observations_df.where(col("datasource_name") == lit("IMPC"))
            .groupBy(col("observation_type").alias("dataType"))
            .count()
            .rdd.map(lambda row: row.asDict())
            .collect()
        )

        phenotype_annotations_df = (
            gene_phenotype_df.withColumn(
                "topLevelPhenotype", explode("top_level_mp_term_name")
            )
            .groupBy("topLevelPhenotype", "zygosity")
            .agg(count("*").alias("count"))
        )

        phenotype_annotations = (
            phenotype_annotations_df.groupBy("topLevelPhenotype")
            .agg(
                sum("count").alias("total"),
                collect_set(struct("zygosity", "count")).alias("counts"),
            )
            .rdd.map(lambda row: row.asDict(True))
            .collect()
        )

        gentar_column_map = {
            "Gene Symbol": "marker_symbol",
            "MGI ID": "mgi_accession_id",
            "Assignment Status": "assignment_status",
            "ES Cell Null Production Status": "null_allele_production_status",
            "ES Cell Conditional Production Status": "conditional_allele_production_status",
            "Crispr Production Status": "crispr_allele_production_status",
            "Crispr Conditional Production Status": "crispr_conditional_allele_production_status",
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

        def get_overall_status_count(status_col, status_order_map):
            genes_by_production_status_overall_df = (
                gentar_gene_status_df.select("mgi_accession_id", status_col)
                .where(size(status_col) > 0)
                .distinct()
            )

            genes_by_production_status_overall_df = (
                genes_by_production_status_overall_df.withColumn(
                    status_col,
                    explode(status_col),
                )
            )

            genes_by_production_status_overall_df = (
                genes_by_production_status_overall_df.withColumn(
                    "production_centre",
                    split(col(status_col), "\|").getItem(0),
                )
            )

            genes_by_production_status_overall_df = (
                genes_by_production_status_overall_df.withColumn(
                    status_col,
                    split(col(status_col), "\|").getItem(1),
                )
            )

            genes_by_production_status_overall_df = (
                genes_by_production_status_overall_df.withColumn(
                    "production_status_order",
                    udf(lambda status: status_order_map.get(status.lower(), 0))(
                        status_col
                    ),
                )
            )

            window_spec = Window.partitionBy("mgi_accession_id").orderBy(
                desc("production_status_order")
            )
            genes_by_production_status_ranked = (
                genes_by_production_status_overall_df.withColumn(
                    "rank", row_number().over(window_spec)
                )
            )

            genes_by_production_status_overall_df = (
                genes_by_production_status_ranked.filter(col("rank") == 1).drop("rank")
            )

            genes_by_production_status_overall_df = (
                genes_by_production_status_overall_df.withColumnRenamed(
                    status_col, "status"
                )
            )

            genes_by_production_status_overall = (
                genes_by_production_status_overall_df.groupBy("status")
                .agg(countDistinct("mgi_accession_id").alias("count"))
                .rdd.map(lambda row: row.asDict())
                .collect()
            )
            return genes_by_production_status_overall

        def get_by_center_status_count(status_col, status_order_map):
            genes_by_production_status_by_center_df = (
                gentar_gene_status_df.select("mgi_accession_id", status_col)
                .where(size(status_col) > 0)
                .distinct()
            )

            genes_by_production_status_by_center_df = (
                genes_by_production_status_by_center_df.withColumn(
                    status_col,
                    explode(status_col),
                )
            )

            genes_by_production_status_by_center_df = (
                genes_by_production_status_by_center_df.withColumn(
                    "production_centre",
                    split(col(status_col), "\|").getItem(0),
                )
            )

            genes_by_production_status_by_center_df = (
                genes_by_production_status_by_center_df.withColumn(
                    status_col,
                    split(col(status_col), "\|").getItem(1),
                )
            )

            genes_by_production_status_by_center_df = (
                genes_by_production_status_by_center_df.withColumn(
                    "production_status_order",
                    udf(lambda status: status_order_map.get(status.lower(), 0))(
                        status_col
                    ),
                )
            )

            window_spec = Window.partitionBy(
                "mgi_accession_id", "production_centre"
            ).orderBy(desc("production_status_order"))
            genes_by_production_status_ranked = (
                genes_by_production_status_by_center_df.withColumn(
                    "rank", row_number().over(window_spec)
                )
            )

            genes_by_production_status_by_center_df = (
                genes_by_production_status_ranked.filter(col("rank") == 1).drop("rank")
            )

            genes_by_production_status_by_center_df = (
                genes_by_production_status_by_center_df.withColumnRenamed(
                    status_col, "status"
                )
            )
            genes_by_production_status_by_center_df = (
                genes_by_production_status_by_center_df.withColumnRenamed(
                    "production_centre", "centre"
                )
            )

            genes_by_production_status_by_center = (
                genes_by_production_status_by_center_df.groupBy("status", "centre")
                .agg(countDistinct("mgi_accession_id").alias("count"))
                .rdd.map(lambda row: row.asDict())
                .collect()
            )
            return genes_by_production_status_by_center

        es_prod_status_map = {
            "attempt in progress": 1,
            "micro-injection in progress": 2,
            "chimeras/founder obtained": 3,
            "rederivation started": 4,
            "rederivation complete": 5,
            "cre excision started": 6,
            "cre excision complete": 7,
            "genotype in progress": 8,
            "mouse allele modification genotype confirmed": 9,
            "genotype confirmed": 10,
            "phenotype attempt registered": 11,
        }
        genes_by_production_status_es_null_overall = get_overall_status_count(
            "null_allele_production_status", es_prod_status_map
        )

        genes_by_production_status_es_null_by_center = get_by_center_status_count(
            "null_allele_production_status", es_prod_status_map
        )

        genes_by_production_status_es_conditional_overall = get_overall_status_count(
            "conditional_allele_production_status", es_prod_status_map
        )

        genes_by_production_status_es_conditional_by_center = (
            get_by_center_status_count(
                "conditional_allele_production_status", es_prod_status_map
            )
        )

        crispr_prod_status_map = {
            "attempt in progress": 1,
            "embryos obtained": 2,
            "founders obtained": 3,
            "genotype in progress": 4,
            "mouse allele modification genotype confirmed": 5,
            "genotype confirmed": 6,
        }

        genes_by_production_status_crispr_null_overall = get_overall_status_count(
            "crispr_allele_production_status", crispr_prod_status_map
        )

        genes_by_production_status_crispr_null_by_center = get_by_center_status_count(
            "crispr_allele_production_status", crispr_prod_status_map
        )

        genes_by_production_status_crispr_conditional_overall = (
            get_overall_status_count(
                "crispr_conditional_allele_production_status", crispr_prod_status_map
            )
        )

        genes_by_production_status_crispr_conditional_by_center = (
            get_by_center_status_count(
                "crispr_conditional_allele_production_status", crispr_prod_status_map
            )
        )

        phenotyping_status_map = {
            "phenotyping registered": 1,
            "phenotyping started": 2,
            "rederivation started": 3,
            "rederivation complete": 4,
            "phenotyping complete": 5,
            "phenotyping finished": 6,
            "phenotyping all data sent": 7,
        }

        genes_by_phenotyping_status_overall = get_overall_status_count(
            "phenotyping_status", phenotyping_status_map
        )

        genes_by_phenotyping_status_by_center = get_by_center_status_count(
            "phenotyping_status", phenotyping_status_map
        )

        phenotype_associations_by_procedure_df = gene_phenotype_df.withColumn(
            "procedure_name", explode("procedure_name")
        )
        phenotype_associations_by_procedure_df = (
            phenotype_associations_by_procedure_df.withColumn(
                "life_stage_name", explode("life_stage_name")
            )
        )

        phenotype_associations_by_procedure_df = (
            phenotype_associations_by_procedure_df.groupBy(
                col("procedure_name"), col("life_stage_name")
            ).count()
        )
        phenotype_associations_by_procedure_df = (
            phenotype_associations_by_procedure_df.withColumnRenamed(
                "life_stage_name", "lifeStage"
            )
        )
        phenotype_associations_by_procedure_df = (
            phenotype_associations_by_procedure_df.groupBy("procedure_name").agg(
                sum("count").alias("total"),
                collect_set(struct("lifeStage", "count")).alias("counts"),
            )
        )
        phenotype_associations_by_procedure = (
            phenotype_associations_by_procedure_df.rdd.map(
                lambda row: row.asDict(True)
            ).collect()
        )

        summary_counts = {
            release_version: {
                "phenotypedGenes": phenotyped_genes,
                "phenotypedLines": phenotyped_lines,
                "phentoypeCalls": significant_calls,
            }
        }
        with open(mp_calls_historic_csv_path, mode="r", encoding="utf-8-sig") as f:
            old_release_counts = [
                {k: v for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)
            ]
            for row in old_release_counts:
                old_release_version = row["Category"]
                phenotyped_genes = int(row["Phenotyped genes"])
                phenotyped_lines = int(row["Phenotyped lines"])
                phenotype_calls = int(row["MP calls"])

                summary_counts[old_release_version] = {
                    "phenotypedGenes": phenotyped_genes,
                    "phenotypedLines": phenotyped_lines,
                    "phenotypeCalls": phenotype_calls,
                }

        data_points_count = {release_version: data_quality_counts}

        column_to_data_type = {
            "Categorical (QC passed)": "categorical",
            "Image record (QC passed)": "image_record",
            "ontological_datapoints_QC_passed": "ontological",
            "Text (QC passed)": "text",
            "Time series (QC passed)": "time_series",
            "Unidimensional (QC passed)": "unidimensional",
        }

        with open(data_points_historic_csv_path, mode="r", encoding="utf-8-sig") as f:
            old_release_counts = [
                {k: v for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)
            ]

            for row in old_release_counts:
                old_release_version = row["Category"]
                data_points_count[old_release_version] = []

                for column, data_type in column_to_data_type.items():
                    data_points_count[old_release_version].append(
                        {"dataType": data_type, "count": int(row[column])}
                    )

        release_metadata_dict = {
            "dataReleaseVersion": release_version,
            "dataReleaseDate": release_date,
            "dataReleaseNotes": release_notes,
            "statisticalAnalysisPackage": {
                "name": statistical_analysis_package_name,
                "version": statistical_analysis_package_version,
            },
            "genomeAssembly": {
                "species": genome_assembly_species,
                "version": genome_assembly_version,
            },
            "summaryCounts": summary_counts,
            "dataPointCount": data_points_count,
            "sampleCounts": lines_by_phenotyping_center,
            "dataQualityChecks": data_quality_counts,
            "phenotypeAnnotations": phenotype_annotations,
            "productionStatusOverall": [
                {
                    "statusType": "productionESCellNull",
                    "counts": genes_by_production_status_es_null_overall,
                },
                {
                    "statusType": "productionESCellConditional",
                    "counts": genes_by_production_status_es_conditional_overall,
                },
                {
                    "statusType": "productionCrisprNull",
                    "counts": genes_by_production_status_crispr_null_overall,
                },
                {
                    "statusType": "productionCrisprConditional",
                    "counts": genes_by_production_status_crispr_conditional_overall,
                },
                {
                    "statusType": "phenotyping",
                    "counts": genes_by_phenotyping_status_overall,
                },
            ],
            "productionStatusByCenter": [
                {
                    "statusType": "productionESCellNull",
                    "counts": genes_by_production_status_es_null_by_center,
                },
                {
                    "statusType": "productionESCellConditional",
                    "counts": genes_by_production_status_es_conditional_by_center,
                },
                {
                    "statusType": "productionCrisprNull",
                    "counts": genes_by_production_status_crispr_null_by_center,
                },
                {
                    "statusType": "productionCrisprConditional",
                    "counts": genes_by_production_status_crispr_conditional_by_center,
                },
                {
                    "statusType": "phenotyping",
                    "counts": genes_by_phenotyping_status_by_center,
                },
            ],
            "phenotypeAssociationsByProcedure": phenotype_associations_by_procedure,
        }

        with open(output_path, mode="w") as output_file:
            output_file.write(json.dumps(release_metadata_dict))


class ImpcLateAdultLandingPageMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcLateAdultLandingPageMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            StatsResultsMapper(raw_data_in_output="bundled"),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/late_adult_landing"
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
        statistical_results_parquet_path = args[0]
        output_path = args[1]

        stat_results_df = spark.read.parquet(statistical_results_parquet_path)
        late_adult_stats_results_df = stat_results_df.where(
            stat_results_df.pipeline_stable_id.contains("LA_")
        ).withColumn("procedure_name", explode("procedure_name"))
        late_adult_procedure_data = (
            late_adult_stats_results_df.withColumn(
                "significantInt",
                when(col("significant") == True, 2)
                .when(col("significant") == False, 1)
                .otherwise(0),
            )
            .select(
                "marker_accession_id",
                "marker_symbol",
                "procedure_name",
                "significantInt",
            )
            .sort("marker_symbol", "procedure_name")
            .distinct()
        )
        late_adult_procedure_data = (
            late_adult_procedure_data.groupBy(
                "marker_accession_id",
                "marker_symbol",
            )
            .pivot("procedure_name")
            .max("significantInt")
            .na.fill(0)
            .where(col("marker_symbol").isNotNull())
        )
        procedure_list = sorted(
            [
                str(row.procedure_name)
                for row in late_adult_stats_results_df.select("procedure_name")
                .sort("procedure_name")
                .distinct()
                .collect()
            ]
        )

        procedure_late_adult_data_dict = {
            "columns": procedure_list,
            "rows": sorted(
                [
                    {
                        "mgiGeneAccessionId": row.marker_accession_id,
                        "markerSymbol": row.marker_symbol,
                        "significance": [
                            int(row[procedureName]) for procedureName in procedure_list
                        ],
                    }
                    for row in late_adult_procedure_data.collect()
                ],
                key=lambda x: x["markerSymbol"],
            ),
        }

        late_adult_parameter_data = (
            late_adult_stats_results_df.withColumn(
                "significantInt",
                when(col("significant") == True, 2)
                .when(col("significant") == False, 1)
                .otherwise(0),
            )
            .select(
                "marker_accession_id",
                "marker_symbol",
                "procedure_name",
                concat_ws("_", col("procedure_name"), col("parameter_name")).alias(
                    "procedure_parameter"
                ),
                "significantInt",
            )
            .sort("marker_symbol", "procedure_parameter")
            .distinct()
        )
        procedure_parameter_list = sorted(
            [
                str(row.procedure_parameter)
                for row in late_adult_parameter_data.select("procedure_parameter")
                .sort("procedure_parameter")
                .distinct()
                .collect()
            ]
        )
        late_adult_parameter_data = (
            late_adult_parameter_data.groupBy(
                "marker_accession_id",
                "marker_symbol",
                "procedure_name",
            )
            .pivot("procedure_parameter")
            .max("significantInt")
            .na.fill(0)
            .where(col("marker_symbol").isNotNull())
        )

        parameter_late_adult_data_dicts = [
            {
                "columns": [
                    p.replace(procedure_name + "_", "")
                    for p in procedure_parameter_list
                    if p.startswith(procedure_name)
                ],
                "rows": sorted(
                    [
                        {
                            "mgiGeneAccessionId": row.marker_accession_id,
                            "markerSymbol": row.marker_symbol,
                            "significance": [
                                int(row[p])
                                for p in procedure_parameter_list
                                if p.startswith(procedure_name)
                            ],
                        }
                        for row in late_adult_parameter_data.where(
                            col("procedure_name") == procedure_name
                        ).collect()
                    ],
                    key=lambda x: x["markerSymbol"],
                ),
            }
            for procedure_name in procedure_list
        ]

        if not os.path.exists(output_path):
            # Create the directory
            os.makedirs(output_path)

        with open(f"{output_path}/procedure_level_data.json", mode="w") as output_file:
            output_file.write(json.dumps(procedure_late_adult_data_dict))

        def sanitize_filename(filename):
            # Define a regex pattern to match invalid characters
            pattern = r'[<>:"/\\|?*\s\(\)-]+'

            # Replace invalid characters with an underscore
            sanitized_filename = re.sub(pattern, "_", filename)

            # Optionally strip leading/trailing whitespace and dots
            sanitized_filename = sanitized_filename.strip().strip(".").strip("_")

            return sanitized_filename.lower()

        for index, procedure_name in enumerate(procedure_list):
            with open(
                f"{output_path}/{sanitize_filename(procedure_name)}_data.json",
                mode="w",
            ) as output_file:
                output_file.write(json.dumps(parameter_late_adult_data_dicts[index]))


class ImpcCardiovascularLandingPageMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcCardiovascularLandingPageMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            StatsResultsMapper(raw_data_in_output="bundled"),
            GenotypePhenotypeLoader(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/cardiovascular_landing.json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        statistical_results_parquet_path = args[0]
        genotype_phenotype_parquet_path = args[1]
        output_path = args[1]

        stat_results_df = spark.read.parquet(statistical_results_parquet_path)
        genotype_phenotype_df = spark.read.parquet(genotype_phenotype_parquet_path)

        cardiovascular_system_mp_id = "MP:0005385"

        tested_genes = (
            stat_results_df.where(
                (col("mp_term_id") == lit(cardiovascular_system_mp_id))
                | array_contains("mp_term_id_options", lit(cardiovascular_system_mp_id))
                | array_contains(
                    "intermediate_mp_term_id", lit(cardiovascular_system_mp_id)
                )
                | array_contains(
                    "top_level_mp_term_id", lit(cardiovascular_system_mp_id)
                )
            )
            .where(~(col("status") == "NotProcessed"))
            .select("marker_accession_id")
            .distinct()
        )

        female_significant_genes = (
            genotype_phenotype_df.where(
                (col("mp_term_id") == lit(cardiovascular_system_mp_id))
                | array_contains(
                    "intermediate_mp_term_id", lit(cardiovascular_system_mp_id)
                )
                | array_contains(
                    "top_level_mp_term_id", lit(cardiovascular_system_mp_id)
                )
            )
            .where(col("sex") == "female")
            .select("marker_accession_id")
            .distinct()
        )

        male_significant_genes = (
            genotype_phenotype_df.where(
                (col("mp_term_id") == lit(cardiovascular_system_mp_id))
                | array_contains(
                    "intermediate_mp_term_id", lit(cardiovascular_system_mp_id)
                )
                | array_contains(
                    "top_level_mp_term_id", lit(cardiovascular_system_mp_id)
                )
            )
            .where(col("sex") == "male")
            .select("marker_accession_id")
            .distinct()
        )

        both_significant_genes = male_significant_genes.intersect(
            female_significant_genes
        )

        female_significant_genes = female_significant_genes.subtract(
            both_significant_genes
        )
        male_significant_genes = male_significant_genes.subtract(both_significant_genes)

        significant_genes_by_phenotype = (
            genotype_phenotype_df.where(
                (col("mp_term_id") == lit(cardiovascular_system_mp_id))
                | array_contains(
                    "intermediate_mp_term_id", lit(cardiovascular_system_mp_id)
                )
                | array_contains(
                    "top_level_mp_term_id", lit(cardiovascular_system_mp_id)
                )
            )
            .groupBy("mp_term_id", "mp_term_name")
            .agg(countDistinct("marker_accession_id").alias("count"))
        )

        genotype_phenotype_distribution = (
            genotype_phenotype_df.select(
                "marker_accession_id",
                "mp_term_id",
                "intermediate_mp_term_id",
                "top_level_mp_term_id",
            )
            .distinct()
            .groupBy("marker_accession_id", "marker_symbol")
            .agg(
                sum(
                    when(
                        (col("mp_term_id") == lit(cardiovascular_system_mp_id))
                        | array_contains(
                            "intermediate_mp_term_id", lit(cardiovascular_system_mp_id)
                        )
                        | array_contains(
                            "top_level_mp_term_id", lit(cardiovascular_system_mp_id)
                        ),
                        lit(1),
                    ).otherwise(lit(0))
                ).alias("cardiovascularPhenotypeCount"),
                sum(
                    when(
                        (col("mp_term_id") == lit(cardiovascular_system_mp_id))
                        | array_contains(
                            "intermediate_mp_term_id", lit(cardiovascular_system_mp_id)
                        )
                        | array_contains(
                            "top_level_mp_term_id", lit(cardiovascular_system_mp_id)
                        ),
                        lit(0),
                    ).otherwise(lit(1))
                ).alias("otherPhenotypeCount"),
            )
        )

        gene_by_top_level_phenotype_df = (
            genotype_phenotype_df.withColumn(
                "top_level_phenotype",
                zip_with(
                    "top_level_mp_term_id",
                    "top_level_mp_term_name",
                    lambda x, y: struct(x.alias("id"), y.alias("name")),
                ),
            )
            .withColumn("top_level_phenotype", explode("top_level_phenotype"))
            .groupBy("top_level_phenotype")
            .agg(collect_set("marker_accession_id").alias("marker_accession_ids"))
            .select("top_level_phenotype.*", "marker_accession_ids")
        )

        cardiovascular_system_gene_list = (
            gene_by_top_level_phenotype_df.where(
                col("id") == lit(cardiovascular_system_mp_id)
            )
            .rdd.map(lambda row: row.asDict(True))
            .collect()[0]
        )

        other_system_gene_list = (
            gene_by_top_level_phenotype_df.where(
                ~(col("id") == lit(cardiovascular_system_mp_id))
            )
            .rdd.map(lambda row: row.asDict(True))
            .collect()
        )
        chord_diagram_data = {
            "keys": [cardiovascular_system_gene_list["name"]],
            "matrix": [],
        }
        cardio_only_genes = set(cardiovascular_system_gene_list["marker_accession_ids"])
        cardio_system_intersections = []
        for other_system_genes in other_system_gene_list:
            gene_intersection = list(
                set(cardiovascular_system_gene_list["marker_accession_ids"])
                & set(other_system_genes["marker_accession_ids"])
            )
            if len(gene_intersection) > 0:
                cardio_only_genes = cardio_only_genes.difference(
                    set(other_system_genes["marker_accession_ids"])
                )
                cardio_system_intersections.append(len(gene_intersection))
                chord_diagram_data["keys"].append(other_system_genes["name"])
        cardio_system_intersections.insert(0, len(cardio_only_genes))
        chord_diagram_data["matrix"].append(cardio_system_intersections)
        for value in cardio_system_intersections[1:]:
            chord_diagram_data["matrix"].append(
                [value] + [0 for _ in range(0, len(cardio_system_intersections[1:]))]
            )

        cardiovascular_landing_data_dict = {
            "pieChart": {
                "maleOnly": male_significant_genes.count(),
                "femaleOnly": female_significant_genes.count(),
                "both": both_significant_genes.count(),
                "total": tested_genes.count(),
            },
            "table": significant_genes_by_phenotype.rdd.map(
                lambda row: row.asDict(True)
            ).collect(),
            "phenotypeDistribution": genotype_phenotype_distribution.rdd.map(
                lambda row: row.asDict(True)
            ).collect(),
            "chordDiagram": chord_diagram_data,
        }

        with open(output_path, mode="w") as output_file:
            output_file.write(json.dumps(cardiovascular_landing_data_dict))


class ImpcHistopathologyLandingPageMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcHistopathologyLandingPageMapper"

    product_report_json_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            ExperimentToObservationMapper(),
            StatsResultsMapper(raw_data_in_output="bundled"),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/histopathology_landing.json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.product_report_json_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        statistical_results_parquet_path = args[1]
        product_report_json_path = args[2]
        output_path = args[3]

        stat_results_df = spark.read.parquet(statistical_results_parquet_path)
        product_df = spark.read.json(product_report_json_path)
        product_df = product_df.where(array_contains("productTypes", lit("tissue")))
        product_df = (
            product_df.select("mgiGeneAccessionId", "alleleSymbol")
            .groupBy("mgiGeneAccessionId")
            .agg(collect_set("alleleSymbol").alias("allelesWithTissue"))
        )
        product_df = product_df.withColumn("hasTissue", size("allelesWithTissue") > 0)

        histopath_stat_results_df = stat_results_df.where(
            (stat_results_df.data_type == "histopathology")
            & col("marker_symbol").isNotNull()
        )

        anatomy_df = (
            histopath_stat_results_df.withColumn(
                "anatomy",
                split(histopath_stat_results_df.parameter_name, " - ").getItem(0),
            )
            .select("anatomy")
            .distinct()
            .sort("anatomy")
        )

        anatomy_list = [str(row.anatomy) for row in anatomy_df.collect()]

        significance_df = (
            histopath_stat_results_df.withColumn(
                "significantInt",
                when(col("significant") == True, 4)
                .when(col("significant") == False, 2)
                .otherwise(0),
            )
            .withColumn(
                "anatomy",
                split(histopath_stat_results_df.parameter_name, " - ")
                .getItem(0)
                .alias("anatomy"),
            )
            .sort("anatomy")
            .groupBy("marker_accession_id", "marker_symbol")
            .pivot("anatomy")
            .max("significantInt")
            .na.fill(0)
            .sort("marker_symbol")
        )

        significance_df = significance_df.join(
            product_df,
            col("marker_accession_id") == col("mgiGeneAccessionId"),
            "left_outer",
        )

        significance_df = significance_df.withColumn(
            "hasTissue",
            when(col("hasTissue").isNull(), lit(False)).otherwise(col("hasTissue")),
        )

        significance_data = significance_df.collect()

        gene_list = [
            {
                "markerSymbol": row.marker_symbol,
                "mgiGeneAccessionId": str(row.marker_accession_id),
                "hasTissue": bool(row.hasTissue),
                "allelesWithTissue": [str(val) for val in row["allelesWithTissue"]]
                if row["allelesWithTissue"] is not None
                else None,
                "significance": [int(row[col_name]) for col_name in anatomy_list],
            }
            for row in significance_data
        ]

        histopath_data_dict = {
            "columns": anatomy_list,
            "rows": sorted(gene_list, key=lambda k: k["markerSymbol"]),
        }

        with open(output_path, mode="w") as output_file:
            output_file.write(json.dumps(histopath_data_dict))


class ImpcPhenotypePleiotropyMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcPhenotypePleiotropyMapper"

    product_report_json_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [GenotypePhenotypeLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/phenotype_pleiotropy.json"
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
        genotype_phenotype_parquet_path = args[0]
        output_path = args[1]

        genotype_phenotype_df = spark.read.parquet(genotype_phenotype_parquet_path)
        top_level_phenotypes = (
            genotype_phenotype_df.select(
                explode("top_level_mp_term_id").alias("top_level_mp_term_id")
            )
            .groupBy()
            .agg(collect_set("top_level_mp_term_id"))
            .collect()[0][0]
        )

        pleiotropy_json = {}

        for top_level_phenotype in top_level_phenotypes:
            pleiotropy_json[top_level_phenotype] = (
                genotype_phenotype_df.select(
                    "marker_accession_id",
                    "marker_symbol",
                    "mp_term_id",
                    "intermediate_mp_term_id",
                    "top_level_mp_term_id",
                )
                .distinct()
                .groupBy("marker_accession_id", "marker_symbol")
                .agg(
                    collect_set("top_level_mp_term_id").alias("top_level_mp_term_ids"),
                    sum(
                        when(
                            (col("mp_term_id") == lit(top_level_phenotype))
                            | array_contains(
                                "intermediate_mp_term_id", lit(top_level_phenotype)
                            )
                            | array_contains(
                                "top_level_mp_term_id", lit(top_level_phenotype)
                            ),
                            lit(1),
                        ).otherwise(lit(0))
                    ).alias("phenotypeCount"),
                    sum(
                        when(
                            (col("mp_term_id") == lit(top_level_phenotype))
                            | array_contains(
                                "intermediate_mp_term_id", lit(top_level_phenotype)
                            )
                            | array_contains(
                                "top_level_mp_term_id", lit(top_level_phenotype)
                            ),
                            lit(0),
                        ).otherwise(lit(1))
                    ).alias("otherPhenotypeCount"),
                )
                .where(col("phenotypeCount") > 0)
                .drop("top_level_mp_term_ids")
                .rdd.map(lambda row: row.asDict(True))
                .collect()
            )
        with open(output_path, mode="w") as output_file:
            output_file.write(json.dumps(pleiotropy_json))


class ImpcEmbryoLandingMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcEmbryoLandingMapper"

    embryo_data_json_path = luigi.Parameter()
    embryo_viability_calls_tsv_path = luigi.Parameter()
    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            ExperimentToObservationMapper(),
            ImpressToParameterMapper(),
            GeneLoader(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/embryo_landing.json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.embryo_data_json_path,
            self.embryo_viability_calls_tsv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        observations_parquet_path = args[0]
        impress_parameter_parquet_path = args[1]
        gene_parquet_path = args[2]
        embryo_data_json_path = args[3]
        embryo_viability_calls_tsv_path = args[4]
        output_path = args[5]

        observations_df = spark.read.parquet(observations_parquet_path)
        impc_viability_outcomes_df = (
            observations_df.where(
                col("parameter_stable_id").isin(
                    ["IMPC_VIA_001_001", "IMPC_VIA_065_001", "IMPC_VIA_067_001"]
                )
            )
            .where(col("datasource_name") == "IMPC")
            .where(
                (~(col("text_value") == "Cannot be calculated"))
                | col("text_value").isNull()
            )
        )

        impc_viability_outcomes_df = impc_viability_outcomes_df.withColumnRenamed(
            "gene_accession_id", "mgi_gene_accession_id"
        )

        impc_viability_outcomes_chart_df = impc_viability_outcomes_df.withColumn(
            "outcome",
            when(col("category").isNotNull(), col("category")).otherwise(
                concat(
                    split(col("text_value"), " - ").getItem(0),
                    lit(" - "),
                    when(lower(col("text_value")).contains("lethal"), "Lethal")
                    .when(lower(col("text_value")).contains("subviable"), "Subviable")
                    .when(
                        lower(col("text_value")).contains("reduced life span"),
                        "Subviable",
                    )
                    .otherwise("Viable"),
                )
            ),
        )

        impc_viability_outcomes_chart_df = impc_viability_outcomes_chart_df.groupBy(
            "outcome"
        ).agg(
            collect_set(struct("mgi_gene_accession_id", "gene_symbol")).alias("genes")
        )

        impc_viability_outcomes_table_df = impc_viability_outcomes_df.withColumn(
            "outcome",
            when(
                col("category").isNotNull(), split(col("category"), " - ").getItem(1)
            ).otherwise(
                when(lower(col("text_value")).contains("lethal"), "Lethal")
                .when(lower(col("text_value")).contains("subviable"), "Subviable")
                .when(
                    lower(col("text_value")).contains("reduced life span"),
                    "Subviable",
                )
                .otherwise("Viable")
            ),
        )

        impc_viability_outcomes_table_df = impc_viability_outcomes_table_df.groupBy(
            "outcome"
        ).agg(
            collect_set(struct("mgi_gene_accession_id", "gene_symbol")).alias("genes")
        )

        impc_secondary_viability_windows_df = spark.read.csv(
            embryo_viability_calls_tsv_path, header=True, sep="\t"
        )

        impc_secondary_viability_windows_df = (
            impc_secondary_viability_windows_df.withColumnRenamed(
                "gene_id", "mgi_gene_accession_id"
            )
        )

        impc_secondary_viability_windows_df = (
            impc_secondary_viability_windows_df.groupBy("FUSIL").agg(
                collect_set(struct("mgi_gene_accession_id", "gene_symbol")).alias(
                    "genes"
                )
            )
        )

        impc_secondary_viability_windows_df = (
            impc_secondary_viability_windows_df.withColumnRenamed(
                "FUSIL", "windowOfLethality"
            )
        )

        embryo_data_df = spark.read.json(embryo_data_json_path, mode="FAILFAST")
        embryo_data_df = embryo_data_df.withColumn("colonies", explode("colonies"))
        embryo_data_df = embryo_data_df.select("colonies.*")
        embryo_data_df = embryo_data_df.withColumn(
            "procedures_parameters", explode("procedures_parameters")
        )
        embryo_data_df = embryo_data_df.withColumn(
            "procedure_key", col("procedures_parameters.procedure_id")
        )

        impress_df = spark.read.parquet(impress_parameter_parquet_path)
        impress_df = impress_df.select(
            "procedure_stable_key", "procedure_name"
        ).distinct()
        embryo_data_df = embryo_data_df.join(
            impress_df, col("procedure_key") == col("procedure_stable_key")
        )
        gene_df = (
            spark.read.parquet(gene_parquet_path)
            .select("mgi_accession_id", "marker_symbol", "is_umass_gene")
            .distinct()
        )
        embryo_data_df = embryo_data_df.join(
            gene_df, embryo_data_df["mgi"] == col("mgi_accession_id")
        )
        embryo_data_df = embryo_data_df.select(
            col("mgi_accession_id").alias("mgi_gene_accession_id"),
            col("marker_symbol").alias("gene_symbol"),
            col("procedure_name"),
            col("analysis_view_url"),
            col("has_automated_analysis"),
            col("url").alias("embryo_viewer_url"),
            col("is_umass_gene"),
        ).distinct()

        for col_name in embryo_data_df.columns:
            embryo_data_df = embryo_data_df.withColumnRenamed(
                col_name, to_camel_case(col_name)
            )

        genes_with_embryo_vignettes = [
            "MGI:1913761",
            "MGI:1916804",
            "MGI:102806",
            "MGI:1195985",
            "MGI:1915138",
            "MGI:1337104",
            "MGI:3039593",
            "MGI:1922814",
            "MGI:97491",
            "MGI:1928849",
            "MGI:2151064",
            "MGI:104606",
            "MGI:103226",
            "MGI:1920939",
            "MGI:95698",
            "MGI:1915091",
            "MGI:1924285",
            "MGI:1914797",
            "MGI:1351614",
            "MGI:2147810",
        ]

        embryo_data_df = embryo_data_df.withColumn(
            "hasVignettes",
            col(to_camel_case("mgi_gene_accession_id")).isin(
                genes_with_embryo_vignettes
            ),
        )

        embryo_data_df = embryo_data_df.groupBy(
            to_camel_case("mgi_gene_accession_id"),
            to_camel_case("gene_symbol"),
            to_camel_case("analysis_view_url"),
            to_camel_case("has_automated_analysis"),
            to_camel_case("embryo_viewer_url"),
            to_camel_case("is_umass_gene"),
            to_camel_case("has_vignettes"),
        ).agg(collect_set(to_camel_case("procedure_name")).alias("procedureNames"))

        embryo_landing_json = {
            "primaryViabilityTable": [
                {
                    **row.asDict(),
                    "genes": [
                        dict(zip(["mgiGeneAccessionId", "geneSymbol"], gene_tuple))
                        for gene_tuple in row.genes
                    ],
                }
                for row in impc_viability_outcomes_table_df.collect()
            ],
            "primaryViabilityChart": [
                {
                    **row.asDict(),
                    "genes": [
                        dict(zip(["mgiGeneAccessionId", "geneSymbol"], gene_tuple))
                        for gene_tuple in row.genes
                    ],
                }
                for row in impc_viability_outcomes_chart_df.collect()
            ],
            "secondaryViabilityData": [
                {
                    **row.asDict(),
                    "genes": [
                        dict(zip(["mgiGeneAccessionId", "geneSymbol"], gene_tuple))
                        for gene_tuple in row.genes
                    ],
                }
                for row in impc_secondary_viability_windows_df.collect()
            ],
            "embryoDataAvailabilityGrid": [
                row.asDict() for row in embryo_data_df.collect()
            ],
        }

        with open(output_path, mode="w") as output_file:
            output_file.write(json.dumps(embryo_landing_json))


class ImpcIDGMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcIDGMapper"

    ortholog_mapping_report_tsv_path = luigi.Parameter()
    idg_family_mapping_report_json_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [GeneLoader()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/idg_landing.json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.idg_family_mapping_report_json_path,
            self.ortholog_mapping_report_tsv_path,
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        idg_family_mapping_report_json_path = args[0]
        ortholog_mapping_report_tsv_path = args[1]
        gene_parquet_path = args[2]
        output_path = args[3]

        idg_family_df = spark.read.json(idg_family_mapping_report_json_path)
        ortholog_mapping_df = spark.read.csv(
            ortholog_mapping_report_tsv_path, sep="\t", header=True
        )
        gene_df = spark.read.parquet(gene_parquet_path)

        gene_df = gene_df.select(
            "mgi_accession_id",
            "marker_symbol",
            "significant_top_level_mp_terms",
            "not_significant_top_level_mp_terms",
            "phenotype_status",
            "mouse_production_status",
            "es_cell_production_status",
        ).distinct()

        ortholog_mapping_df = ortholog_mapping_df.select(
            col("Mgi Gene Acc Id").alias("mgi_accession_id"),
            col("Human Gene Symbol").alias("human_gene_symbol"),
        ).distinct()

        gene_df = gene_df.join(
            ortholog_mapping_df,
            "mgi_accession_id",
        )
        idg_family_df = idg_family_df.withColumnRenamed("Gene", "human_gene_symbol")
        idg_family_df = idg_family_df.withColumnRenamed("IDGFamily", "idg_family")
        gene_df = gene_df.join(idg_family_df, "human_gene_symbol")

        idg_landing_json = gene_df.rdd.map(lambda row: row.asDict(True)).collect()

        with open(output_path, mode="w") as output_file:
            output_file.write(json.dumps(idg_landing_json))


class ImpcBatchQueryMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcBatchQueryMapper"

    ortholog_mapping_report_tsv_path = luigi.Parameter()
    mp_hp_matches_csv_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ImpcGeneStatsResultsMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_web_api/batch_query_data_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.ortholog_mapping_report_tsv_path,
            self.mp_hp_matches_csv_path,
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        ortholog_mapping_report_tsv_path = args[0]
        mp_hp_matches_csv_path = args[1]
        gene_stats_results_json_path = args[2]
        output_path = args[3]

        ortholog_mapping_df = spark.read.csv(
            ortholog_mapping_report_tsv_path, sep="\t", header=True
        )
        stats_results = spark.read.json(gene_stats_results_json_path)

        ortholog_mapping_df = ortholog_mapping_df.select(
            col("Mgi Gene Acc Id").alias("mgiGeneAccessionId"),
            col("Human Gene Symbol").alias("humanGeneSymbol"),
            col("Hgnc Acc Id").alias("hgncGeneAccessionId"),
        ).distinct()

        stats_results = stats_results.join(
            ortholog_mapping_df, "mgiGeneAccessionId", how="left_outer"
        )

        mp_matches_df = spark.read.csv(mp_hp_matches_csv_path, header=True)
        mp_matches_df = mp_matches_df.select(
            col("curie_x").alias("id"),
            col("curie_y").alias("hp_term_id"),
            col("label_y").alias("hp_term_name"),
        ).distinct()

        stats_mp_hp_df = stats_results.select(
            "statisticalResultId",
            "potentialPhenotypes",
            "intermediatePhenotypes",
            "topLevelPhenotypes",
            "significantPhenotype",
        )
        for phenotype_list_col in [
            "potentialPhenotypes",
            "intermediatePhenotypes",
            "topLevelPhenotypes",
        ]:
            stats_mp_hp_df = stats_mp_hp_df.withColumn(
                phenotype_list_col[:-1], explode_outer(phenotype_list_col)
            )

        stats_mp_hp_df = stats_mp_hp_df.join(
            mp_matches_df,
            (
                (col("significantPhenotype.id") == col("id"))
                | (col("potentialPhenotype.id") == col("id"))
                | (col("intermediatePhenotype.id") == col("id"))
                | (col("topLevelPhenotype.id") == col("id"))
            ),
            how="left_outer",
        )
        stats_mp_hp_df = stats_mp_hp_df.withColumn(
            "humanPhenotype",
            phenotype_term_zip_udf(col("hp_term_id"), col("hp_term_name")),
        )
        stats_mp_hp_df = (
            stats_mp_hp_df.groupBy("statisticalResultId")
            .agg(collect_set("humanPhenotype").alias("humanPhenotypes"))
            .select("statisticalResultId", "humanPhenotypes")
            .distinct()
        )

        stats_results = stats_results.join(stats_mp_hp_df, "statisticalResultId")

        stats_results.write.parquet(output_path)
