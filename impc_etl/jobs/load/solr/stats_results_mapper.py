import base64
import gzip
import json
import sys
from typing import List, Any

import luigi
import pyspark.sql.functions as f
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    Row,
)
from pyspark.sql.window import Window

from impc_etl.jobs.extract import ImpressExtractor
from impc_etl.jobs.extract.allele_ref_extractor import ExtractAlleleRef
from impc_etl.jobs.extract.ontology_hierarchy_extractor import (
    OntologyTermHierarchyExtractor,
)
from impc_etl.jobs.extract.open_stats_extractor import StatisticalAnalysisOutputMapper
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.mp_chooser_mapper import MPChooserGenerator
from impc_etl.jobs.load.solr.pipeline_mapper import ImpressToParameterMapper
from impc_etl.jobs.load.solr.stats_results_mapping_helper import *
from impc_etl.shared.utils import convert_to_row

# TODO missing strain name and genetic background
from impc_etl.workflow.config import ImpcConfig


class StatsResultsMapper(PySparkTask):
    """
    PySpark task to map the output of the statistical analysis, the manual gene/phenotype calls into one collection
    of self-contained and self-explanatory statistical results, so they can be interpreted/used isolated from any
    other datasource. It includes marker data, metadata about the parameter measured and analysed,
    and some description of the dataset.

    This task depends on:
    - `impc_etl.jobs.extract.open_stats_extractor.StatisticalAnalysisOutputMapper`
    - `impc_etl.jobs.load.observation_mapper.ExperimentToObservationMapper`
    - `impc_etl.jobs.extract.ontology_hierarchy_extractor.OntologyTermHierarchyExtractor`
    - `impc_etl.jobs.load.solr.pipeline_mapper.ImpressToParameterMapper`
    - `impc_etl.jobs.extract.allele_ref_extractor.ExtractAlleleRef`
    - `impc_etl.jobs.load.mp_chooser_mapper.MPChooserGenerator`
    """

    threei_stats_csv_path = luigi.Parameter()
    pwg_stats_csv_path = luigi.Parameter()
    mpath_metadata_path = luigi.Parameter()
    raw_data_in_output = luigi.Parameter()
    extract_windowed_data = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            StatisticalAnalysisOutputMapper(),
            ExperimentToObservationMapper(),
            OntologyTermHierarchyExtractor(),
            ImpressExtractor(),
            ImpressToParameterMapper(),
            ExtractAlleleRef(),
            MPChooserGenerator(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr16.0/parquet/allele_ref_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}statistical_results_raw_data_{self.raw_data_in_output}_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.input()[4].path,
            self.input()[5].path,
            self.input()[6].path,
            self.pwg_stats_csv_path,
            self.threei_stats_csv_path,
            self.mpath_metadata_path,
            self.raw_data_in_output,
            self.extract_windowed_data,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Solr Core loader
        """
        open_stats_parquet_path = args[0]
        observations_parquet_path = args[1]
        ontology_parquet_path = args[2]
        pipeline_parquet_path = args[3]
        pipeline_core_parquet_path = args[4]
        allele_parquet_path = args[5]
        mp_chooser_path = args[6]
        pwg_stats_csv_path = args[7]
        threei_stats_csv_path = args[8]
        mpath_metadata_path = args[9]
        raw_data_in_output = args[10]
        extract_windowed_data = args[11] == "true"
        output_path = args[12]

        spark = SparkSession(sc)
        open_stats_complete_df = spark.read.parquet(open_stats_parquet_path)
        ontology_df = spark.read.parquet(ontology_parquet_path)
        allele_df = spark.read.parquet(allele_parquet_path)
        pipeline_df = spark.read.parquet(pipeline_parquet_path)
        pipeline_core_df = spark.read.parquet(pipeline_core_parquet_path)
        observations_df = spark.read.parquet(observations_parquet_path)
        pwg_df = spark.read.csv(pwg_stats_csv_path, header=True)
        threei_df = spark.read.csv(threei_stats_csv_path, header=True)
        mpath_metadata_df = spark.read.csv(mpath_metadata_path, header=True)

        mp_chooser_txt = spark.sparkContext.wholeTextFiles(mp_chooser_path).collect()[
            0
        ][1]
        mp_chooser = json.loads(mp_chooser_txt)

        open_stats_df = self.get_stats_results_core(
            open_stats_complete_df,
            ontology_df,
            allele_df,
            pipeline_df,
            pipeline_core_df,
            observations_df,
            pwg_df,
            threei_df,
            mpath_metadata_df,
            mp_chooser,
            extract_windowed_data,
            raw_data_in_output,
        )

        if extract_windowed_data:
            stats_results_column_list = STATS_RESULTS_COLUMNS + [
                col_name
                for col_name in WINDOW_COLUMNS
                if col_name != "observations_window_weight"
            ]
            stats_results_df = open_stats_df.select(*stats_results_column_list)
        elif raw_data_in_output == "bundled":
            stats_results_column_list = STATS_RESULTS_COLUMNS + ["raw_data"]
            stats_results_df = open_stats_df.select(*stats_results_column_list)
            stats_results_df = stats_results_df.repartition(20000)
        else:
            stats_results_df = open_stats_df.select(*STATS_RESULTS_COLUMNS)
        for col_name in stats_results_df.columns:
            if dict(stats_results_df.dtypes)[col_name] == "null":
                stats_results_df = stats_results_df.withColumn(
                    col_name, f.lit(None).astype(StringType())
                )
        stats_results_df.write.parquet(output_path)
        if raw_data_in_output == "include":
            raw_data_df = open_stats_df.select("doc_id", "raw_data")
            raw_data_df.distinct().write.parquet(output_path + "_raw_data")

    def get_stats_results_core(
        self,
        open_stats_complete_df,
        ontology_df,
        allele_df,
        pipeline_df,
        pipeline_core_df,
        observations_df,
        pwg_df,
        threei_df,
        mpath_metadata_df,
        mp_chooser,
        extract_windowed_data=False,
        raw_data_in_output="include",
    ):
        threei_df = self.standardize_threei_schema(threei_df)

        embryo_stat_packets = open_stats_complete_df.where(
            (
                (f.col("procedure_group").contains("IMPC_GPL"))
                | (f.col("procedure_group").contains("IMPC_GEL"))
                | (f.col("procedure_group").contains("IMPC_GPM"))
                | (f.col("procedure_group").contains("IMPC_GEM"))
                | (f.col("procedure_group").contains("IMPC_GPO"))
                | (f.col("procedure_group").contains("IMPC_GEO"))
                | (f.col("procedure_group").contains("IMPC_GPP"))
                | (f.col("procedure_group").contains("IMPC_GEP"))
            )
        )

        open_stats_df = open_stats_complete_df.where(
            ~(
                f.col("procedure_stable_id").contains("IMPC_FER_001")
                | (f.col("procedure_stable_id").contains("IMPC_VIA_001"))
                | (f.col("procedure_stable_id").contains("IMPC_VIA_002"))
                | (f.col("procedure_group").contains("_PAT"))
                | (f.col("procedure_group").contains("_EVL"))
                | (f.col("procedure_group").contains("_EVM"))
                | (f.col("procedure_group").contains("_EVO"))
                | (f.col("procedure_group").contains("_EVP"))
                | (f.col("procedure_group").contains("_ELZ"))
                | (f.col("procedure_name").startswith("Histopathology"))
                | (f.col("procedure_group").contains("IMPC_GPL"))
                | (f.col("procedure_group").contains("IMPC_GEL"))
                | (f.col("procedure_group").contains("IMPC_GPM"))
                | (f.col("procedure_group").contains("IMPC_GEM"))
                | (f.col("procedure_group").contains("IMPC_GPO"))
                | (f.col("procedure_group").contains("IMPC_GEO"))
                | (f.col("procedure_group").contains("IMPC_GPP"))
                | (f.col("procedure_group").contains("IMPC_GEP"))
                | (f.col("procedure_group").startswith("ALT"))
                | (f.col("procedure_group").isin(PWG_PROCEDURES))
            )
        )

        manual_hits_observations_df = observations_df.where(
            ~f.col("procedure_stable_id").startswith("ALT")
        )

        fertility_stats = self._fertility_stats_results(
            manual_hits_observations_df, pipeline_df
        )

        for col_name in open_stats_df.columns:
            if col_name not in fertility_stats.columns:
                fertility_stats = fertility_stats.withColumn(col_name, f.lit(None))
        fertility_stats = fertility_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(fertility_stats)

        pwg_stats = self._pwg_stats(pwg_df, observations_df)

        for col_name in open_stats_df.columns:
            if col_name not in pwg_stats.columns:
                pwg_stats = pwg_stats.withColumn(col_name, f.lit(None))

        pwg_stats = pwg_stats.select(open_stats_df.columns)

        open_stats_df = open_stats_df.union(pwg_stats)

        viability_stats = self._viability_stats_results(
            manual_hits_observations_df, pipeline_df
        )
        for col_name in open_stats_df.columns:
            if col_name not in viability_stats.columns:
                viability_stats = viability_stats.withColumn(col_name, f.lit(None))
        viability_stats = viability_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(viability_stats)

        gross_pathology_stats = self._gross_pathology_stats_results(
            manual_hits_observations_df
        )
        for col_name in open_stats_df.columns:
            if col_name not in gross_pathology_stats.columns:
                gross_pathology_stats = gross_pathology_stats.withColumn(
                    col_name, f.lit(None)
                )
        gross_pathology_stats = gross_pathology_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(gross_pathology_stats)

        histopathology_stats = self._histopathology_stats_results(
            manual_hits_observations_df
        )
        for col_name in open_stats_df.columns:
            if col_name not in histopathology_stats.columns:
                histopathology_stats = histopathology_stats.withColumn(
                    col_name, f.lit(None)
                )
        histopathology_stats = histopathology_stats.select(
            open_stats_df.columns
        ).distinct()
        open_stats_df = open_stats_df.union(histopathology_stats)

        embryo_viability_stats = self._embryo_viability_stats_results(
            manual_hits_observations_df, pipeline_df
        )
        for col_name in open_stats_df.columns:
            if col_name not in embryo_viability_stats.columns:
                embryo_viability_stats = embryo_viability_stats.withColumn(
                    col_name, f.lit(None)
                )
        embryo_viability_stats = embryo_viability_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(embryo_viability_stats)

        embryo_stats = self._embryo_stats_results(
            observations_df, pipeline_df, embryo_stat_packets
        )
        for col_name in open_stats_df.columns:
            if col_name not in embryo_stats.columns:
                embryo_stats = embryo_stats.withColumn(col_name, f.lit(None))
        embryo_stats = embryo_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(embryo_stats)

        observations_metadata_df = observations_df.select(
            STATS_OBSERVATIONS_JOIN + list(set(OBSERVATIONS_STATS_MAP.values()))
        ).dropDuplicates()
        observations_metadata_df = observations_metadata_df.groupBy(
            *[
                col_name
                for col_name in observations_metadata_df.columns
                if col_name != "sex"
            ]
        ).agg(f.collect_set("sex").alias("sex"))

        aggregation_expresion = []

        for col_name in list(set(OBSERVATIONS_STATS_MAP.values())):
            if col_name not in ["datasource_name", "production_center"]:
                if col_name == "sex":
                    aggregation_expresion.append(
                        f.array_distinct(f.flatten(f.collect_set(col_name))).alias(
                            col_name
                        )
                    )
                elif col_name in ["strain_name", "genetic_background"]:
                    aggregation_expresion.append(
                        f.first(f.col(col_name)).alias(col_name)
                    )
                else:
                    aggregation_expresion.append(
                        f.collect_set(col_name).alias(col_name)
                    )

        observations_metadata_df = observations_metadata_df.groupBy(
            STATS_OBSERVATIONS_JOIN + ["datasource_name", "production_center"]
        ).agg(*aggregation_expresion)

        open_stats_df = self.map_to_stats(
            open_stats_df,
            observations_metadata_df,
            STATS_OBSERVATIONS_JOIN,
            OBSERVATIONS_STATS_MAP,
            "observation",
        )

        open_stats_df = open_stats_df.withColumn(
            "pipeline_stable_id",
            f.when(
                f.col("procedure_stable_id") == "ESLIM_022_001",
                f.lit("ESLIM_001"),
            ).otherwise(f.col("pipeline_stable_id")),
        )
        open_stats_df = open_stats_df.withColumn(
            "procedure_stable_id",
            f.when(
                f.col("procedure_stable_id").contains("~"),
                f.split(f.col("procedure_stable_id"), "~"),
            ).otherwise(f.array(f.col("procedure_stable_id"))),
        )
        open_stats_df = open_stats_df.alias("stats")

        mp_ancestors_df = ontology_df.select(
            "id",
            f.struct("parent_ids", "intermediate_ids", "top_level_ids").alias(
                "ancestors"
            ),
        )
        mp_ancestors_df_1 = mp_ancestors_df.alias("mp_term_1")
        mp_ancestors_df_2 = mp_ancestors_df.alias("mp_term_2")
        open_stats_df = open_stats_df.join(
            mp_ancestors_df_1,
            (f.expr("mp_term[0].term_id") == f.col("mp_term_1.id")),
            "left_outer",
        )

        open_stats_df = open_stats_df.join(
            mp_ancestors_df_2,
            (f.expr("mp_term[1].term_id") == f.col("mp_term_2.id")),
            "left_outer",
        )

        mp_term_schema = ArrayType(
            StructType(
                [
                    StructField("event", StringType(), True),
                    StructField("otherPossibilities", StringType(), True),
                    StructField("sex", StringType(), True),
                    StructField("term_id", StringType(), True),
                ]
            )
        )
        select_collapsed_mp_term_udf = f.udf(
            lambda mp_term_array, pipeline, procedure_group, parameter, data_type, first_term_ancestors, second_term_ancestors: self._select_collapsed_mp_term(
                mp_term_array,
                pipeline,
                procedure_group,
                parameter,
                mp_chooser,
                data_type,
                first_term_ancestors,
                second_term_ancestors,
            ),
            mp_term_schema,
        )
        open_stats_df = open_stats_df.withColumn(
            "collapsed_mp_term",
            f.when(
                f.expr(
                    "exists(mp_term.sex, sex -> sex = 'male') AND exists(mp_term.sex, sex -> sex = 'female')"
                )
                & (f.col("data_type").isin(["categorical", "unidimensional"])),
                select_collapsed_mp_term_udf(
                    "mp_term",
                    "pipeline_stable_id",
                    "procedure_group",
                    "parameter_stable_id",
                    "data_type",
                    "mp_term_1.ancestors",
                    "mp_term_2.ancestors",
                ),
            ).otherwise(f.col("mp_term")),
        )
        open_stats_df = open_stats_df.drop("mp_term_1.*", "mp_term_2.*")
        open_stats_df = open_stats_df.withColumn(
            "collapsed_mp_term", f.expr("collapsed_mp_term[0]")
        )
        open_stats_df = open_stats_df.withColumn("significant", f.lit(False))

        open_stats_df = open_stats_df.join(
            threei_df,
            [
                "resource_name",
                "colony_id",
                "marker_symbol",
                "procedure_stable_id",
                "parameter_stable_id",
                "zygosity",
            ],
            "left_outer",
        )
        open_stats_df = self.map_three_i(open_stats_df)
        open_stats_df = open_stats_df.join(
            pwg_df,
            [
                "resource_name",
                "colony_id",
                "marker_accession_id",
                "procedure_stable_id",
                "parameter_stable_id",
                "zygosity",
            ],
            "left_outer",
        )
        print("Before map full:")
        print(open_stats_df.where(f.col("resource_name") == "pwg").count())
        print("Before map join:")
        print(open_stats_df.where(f.col("pwg_significant").isNotNull()).count())
        open_stats_df.where(f.col("pwg_significant").isNotNull()).select(
            "colony_id", "parameter_stable_id", "zygosity"
        ).distinct().show(truncate=False)
        open_stats_df = self.map_pwg(open_stats_df)
        open_stats_df.where(f.col("resource_name") == "pwg").show(
            vertical=True, truncate=False
        )
        raise ValueError

        open_stats_df = open_stats_df.withColumn(
            "collapsed_mp_term",
            f.when(
                f.col("threei_collapsed_mp_term").isNotNull(),
                f.col("threei_collapsed_mp_term"),
            ).otherwise(f.col("collapsed_mp_term")),
        )

        open_stats_df = open_stats_df.drop("threei_collapsed_mp_term")

        open_stats_df = open_stats_df.withColumn(
            "mp_term_id",
            f.regexp_replace("collapsed_mp_term.term_id", " ", ""),
        )
        for bad_mp in BAD_MP_MAP.keys():
            open_stats_df = open_stats_df.withColumn(
                "mp_term_id",
                f.when(
                    f.col("mp_term_id") == bad_mp,
                    f.lit(BAD_MP_MAP[bad_mp]),
                ).otherwise(f.col("mp_term_id")),
            )
        open_stats_df = open_stats_df.withColumn(
            "mp_term_event", f.col("collapsed_mp_term.event")
        )
        open_stats_df = open_stats_df.withColumn(
            "mp_term_sex", f.col("collapsed_mp_term.sex")
        )
        open_stats_df = open_stats_df.withColumnRenamed("mp_term", "full_mp_term")
        open_stats_df = open_stats_df.withColumn(
            "full_mp_term",
            f.when(
                f.col("full_mp_term").isNull() & f.col("collapsed_mp_term").isNotNull(),
                f.array(f.col("collapsed_mp_term")),
            )
            .when(
                f.col("full_mp_term").isNull() & f.col("collapsed_mp_term").isNull(),
                f.lit(None),
            )
            .otherwise(f.col("full_mp_term")),
        )

        if extract_windowed_data:
            stats_results_column_list = (
                STATS_RESULTS_COLUMNS + WINDOW_COLUMNS + RAW_DATA_COLUMNS
            )
        elif raw_data_in_output == "exclude":
            stats_results_column_list = STATS_RESULTS_COLUMNS
        else:
            stats_results_column_list = STATS_RESULTS_COLUMNS + RAW_DATA_COLUMNS

        for col_name in stats_results_column_list:
            if col_name not in open_stats_df.columns:
                open_stats_df = open_stats_df.withColumn(col_name, f.lit(None))

        ontology_df = ontology_df.withColumnRenamed("id", "mp_term_id")
        open_stats_df = self.map_to_stats(
            open_stats_df, ontology_df, ["mp_term_id"], ONTOLOGY_STATS_MAP, "ontology"
        )

        pipeline_core_join = [
            "parameter_stable_id",
            "pipeline_stable_id",
            "procedure_stable_id",
        ]
        pipeline_core_df = (
            pipeline_core_df.select(
                [
                    col_name
                    for col_name in pipeline_core_df.columns
                    if col_name in pipeline_core_join
                    or col_name in PIPELINE_STATS_MAP.values()
                ]
            )
            .groupBy(
                [
                    "parameter_stable_id",
                    "pipeline_stable_id",
                    "procedure_stable_id",
                    "pipeline_stable_key",
                ]
            )
            .agg(
                *[
                    f.array_distinct(f.flatten(f.collect_set(col_name))).alias(col_name)
                    if col_name
                    in [
                        "mp_id",
                        "mp_term",
                        "top_level_mp_id",
                        "top_level_mp_term",
                        "intermediate_mp_id",
                        "intermediate_mp_term",
                    ]
                    else f.collect_set(col_name).alias(col_name)
                    for col_name in list(set(PIPELINE_STATS_MAP.values()))
                    if col_name != "pipeline_stable_key"
                ]
            )
            .dropDuplicates()
        )
        pipeline_core_df = pipeline_core_df.withColumnRenamed(
            "procedure_stable_id", "proc_id"
        )
        pipeline_core_df = pipeline_core_df.withColumn(
            "procedure_stable_id",
            f.array(f.col("proc_id")),
        )
        # Fix for VIA_002 missing mp terms
        pipeline_core_df = self._add_via_002_mp_term_options(pipeline_core_df)

        open_stats_df = self.map_to_stats(
            open_stats_df,
            pipeline_core_df,
            pipeline_core_join,
            PIPELINE_STATS_MAP,
            "impress",
        )

        open_stats_df = open_stats_df.withColumn(
            "top_level_mp_term_id",
            f.when(
                f.col("top_level_mp_term_id").isNull(),
                f.col("top_level_mp_id_options"),
            ).otherwise(f.col("top_level_mp_term_id")),
        )
        open_stats_df = open_stats_df.withColumn(
            "top_level_mp_term_name",
            f.when(
                f.col("top_level_mp_term_name").isNull(),
                f.col("top_level_mp_term_options"),
            ).otherwise(f.col("top_level_mp_term_name")),
        )

        open_stats_df = open_stats_df.withColumn(
            "intermediate_mp_term_id",
            f.when(
                f.col("intermediate_mp_term_id").isNull(),
                f.col("intermediate_mp_id_options"),
            ).otherwise(f.col("intermediate_mp_term_id")),
        )
        open_stats_df = open_stats_df.withColumn(
            "intermediate_mp_term_name",
            f.when(
                f.col("intermediate_mp_term_name").isNull(),
                f.col("intermediate_mp_term_options"),
            ).otherwise(f.col("intermediate_mp_term_name")),
        )

        allele_df = allele_df.select(
            ["allele_symbol"] + list(ALLELE_STATS_MAP.values())
        ).dropDuplicates()

        open_stats_df = self.map_to_stats(
            open_stats_df, allele_df, ["allele_symbol"], ALLELE_STATS_MAP, "allele"
        )

        open_stats_df = open_stats_df.withColumn("sex", f.col("mp_term_sex"))
        open_stats_df = open_stats_df.withColumn(
            "phenotype_sex",
            f.when(
                f.col("phenotype_sex").isNull(),
                f.lit(None),
            )
            .when(
                f.col("phenotype_sex").contains("Both sexes included"),
                f.array(
                    f.lit("male"),
                    f.lit("female"),
                ),
            )
            .otherwise(
                f.array(
                    f.lower(
                        f.regexp_extract(
                            f.col("phenotype_sex"),
                            r"Only one sex included in the analysis; (.*)\[.*\]",
                            1,
                        )
                    )
                )
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "phenotype_sex",
            f.when(
                f.col("phenotype_sex").isNull() & f.col("mp_term_sex").isNotNull(),
                f.when(
                    f.col("mp_term_sex") == "not_considered",
                    f.array(
                        f.lit("male"),
                        f.lit("female"),
                    ),
                ).otherwise(f.array(f.col("mp_term_sex"))),
            ).otherwise(f.col("phenotype_sex")),
        )
        open_stats_df = open_stats_df.withColumn(
            "zygosity",
            f.when(
                f.col("zygosity") == "homozygous",
                f.lit("homozygote"),
            ).otherwise(f.col("zygosity")),
        )

        open_stats_df = self.map_ontology_prefix(open_stats_df, "MA:", "anatomy_")
        open_stats_df = self.map_ontology_prefix(open_stats_df, "EMAP:", "anatomy_")
        open_stats_df = self.map_ontology_prefix(open_stats_df, "EMAPA:", "anatomy_")
        open_stats_df = open_stats_df.withColumn(
            "significant",
            f.when(
                f.col("mp_term_id").isNotNull(),
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        open_stats_df = open_stats_df.withColumn(
            "p_value",
            f.when(
                f.col("statistical_method").startswith("Reference Range"),
                f.least(
                    f.col("female_pvalue_low_normal_vs_high"),
                    f.col("female_pvalue_low_vs_normal_high"),
                    f.col("male_pvalue_low_normal_vs_high"),
                    f.col("male_pvalue_low_vs_normal_high"),
                    f.col("genotype_pvalue_low_normal_vs_high"),
                    f.col("genotype_pvalue_low_vs_normal_high"),
                ),
            ).otherwise(f.col("p_value")),
        )
        open_stats_df = open_stats_df.withColumn(
            "effect_size",
            f.when(
                f.col("statistical_method").startswith("Reference Range"),
                f.greatest(
                    f.col("female_effect_size_low_normal_vs_high"),
                    f.col("female_effect_size_low_vs_normal_high"),
                    f.col("male_effect_size_low_normal_vs_high"),
                    f.col("male_effect_size_low_vs_normal_high"),
                    f.col("genotype_effect_size_low_normal_vs_high"),
                    f.col("genotype_effect_size_low_vs_normal_high"),
                ),
            ).otherwise(f.col("effect_size")),
        )
        open_stats_df = self.map_ontology_prefix(open_stats_df, "MPATH:", "mpath_")
        mpath_metadata_df = mpath_metadata_df.select(
            f.col("acc").alias("mpath_term_id"),
            f.col("name").alias("mpath_metadata_term_name"),
        ).distinct()
        open_stats_df = open_stats_df.join(
            mpath_metadata_df, "mpath_term_id", "left_outer"
        )
        open_stats_df = open_stats_df.withColumn(
            "mpath_term_name", f.col("mpath_metadata_term_name")
        )
        open_stats_df = open_stats_df.withColumn(
            "metadata",
            f.expr(
                "transform(metadata, metadata_values -> concat_ws('|', metadata_values))"
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "significant",
            f.when(
                f.col("data_type") == "time_series",
                f.lit(False),
            ).otherwise(f.col("significant")),
        )
        open_stats_df = open_stats_df.withColumn(
            "status",
            f.when(
                f.col("data_type") == "time_series",
                f.lit("NotProcessed"),
            ).otherwise(f.col("status")),
        )

        open_stats_df = open_stats_df.withColumn(
            "procedure_stable_id_str",
            f.concat_ws(",", "procedure_stable_id"),
        )
        identifying_cols = [
            "colony_id",
            "pipeline_stable_id",
            "procedure_stable_id_str",
            "parameter_stable_id",
            "phenotyping_center",
            "production_center",
            "metadata_group",
            "zygosity",
            "strain_accession_id",
            "sex",
        ]
        identifying_cols = [
            f.when(
                f.col(col_name).isNotNull(),
                f.col(col_name),
            ).otherwise(f.lit(""))
            for col_name in identifying_cols
        ]
        open_stats_df = open_stats_df.withColumn(
            "doc_id",
            f.md5(f.concat(*identifying_cols)),
        )
        if raw_data_in_output == "include" or raw_data_in_output == "bundled":
            specimen_dobs = (
                observations_df.select("external_sample_id", "date_of_birth")
                .dropDuplicates()
                .collect()
            )
            specimen_dob_dict = [row.asDict() for row in specimen_dobs]
            specimen_dob_dict = {
                row["external_sample_id"]: row["date_of_birth"]
                for row in specimen_dob_dict
            }
            open_stats_df = self._parse_raw_data(
                open_stats_df,
                extract_windowed_data,
                specimen_dob_dict,
                raw_data_in_output != "bundled",
            )
        open_stats_df = open_stats_df.withColumn(
            "data_type",
            f.when(
                f.col("procedure_group").rlike(
                    "|".join(
                        [
                            "IMPC_GPL",
                            "IMPC_GEL",
                            "IMPC_GPM",
                            "IMPC_GEM",
                            "IMPC_GPO",
                            "IMPC_GEO",
                            "IMPC_GPP",
                            "IMPC_GEP",
                        ]
                    )
                )
                & (f.col("data_type") == "categorical"),
                f.lit("embryo"),
            ).otherwise(f.col("data_type")),
        )
        return open_stats_df

    def _compress_and_encode(self, json_text):
        if json_text is None:
            return None
        else:
            return str(
                base64.b64encode(gzip.compress(bytes(json_text, "utf-8"))), "utf-8"
            )

    def _parse_raw_data(
        self, open_stats_df, extract_windowed_data, specimen_dob_dict, compress=True
    ):
        compress_and_encode = f.udf(self._compress_and_encode, StringType())
        open_stats_df = open_stats_df.withColumnRenamed(
            "observations_biological_sample_group", "biological_sample_group"
        )
        open_stats_df = open_stats_df.withColumnRenamed(
            "observations_external_sample_id", "external_sample_id"
        )
        open_stats_df = open_stats_df.withColumnRenamed(
            "observations_date_of_experiment", "date_of_experiment"
        )
        open_stats_df = open_stats_df.withColumnRenamed(
            "observations_sex", "specimen_sex"
        )
        open_stats_df = open_stats_df.withColumnRenamed(
            "observations_body_weight", "body_weight"
        )
        open_stats_df = open_stats_df.withColumnRenamed(
            "observations_time_point", "time_point"
        )
        open_stats_df = open_stats_df.withColumnRenamed(
            "observations_discrete_point", "discrete_point"
        )
        if extract_windowed_data:
            open_stats_df = open_stats_df.withColumnRenamed(
                "observations_window_weight", "window_weight"
            )
        for col_name in [
            "biological_sample_group",
            "date_of_experiment",
            "external_sample_id",
            "specimen_sex",
        ]:
            open_stats_df = open_stats_df.withColumn(
                col_name,
                f.when(
                    (
                        f.col("data_type").isin(
                            ["unidimensional", "time_series", "categorical"]
                        )
                        & (f.col(col_name).isNotNull())
                    ),
                    f.col(col_name),
                ).otherwise(f.lit(None)),
            )
        open_stats_df = open_stats_df.withColumn(
            "body_weight",
            f.when(
                (
                    f.col("data_type").isin(
                        ["unidimensional", "time_series", "categorical"]
                    )
                    & (f.col("body_weight").isNotNull())
                ),
                f.col("body_weight"),
            ).otherwise(f.expr("transform(external_sample_id, sample_id -> NULL)")),
        )
        open_stats_df = open_stats_df.withColumn(
            "data_point",
            f.when(
                (f.col("data_type").isin(["unidimensional", "time_series"]))
                & (f.col("observations_response").isNotNull()),
                f.col("observations_response"),
            ).otherwise(f.expr("transform(external_sample_id, sample_id -> NULL)")),
        )
        open_stats_df = open_stats_df.withColumn(
            "category",
            f.when(
                (f.col("data_type") == "categorical")
                & (f.col("observations_response").isNotNull()),
                f.col("observations_response"),
            ).otherwise(f.expr("transform(external_sample_id, sample_id -> NULL)")),
        )
        open_stats_df = open_stats_df.withColumn(
            "time_point",
            f.when(
                (f.col("data_type") == "time_series")
                & (f.col("time_point").isNotNull()),
                f.col("time_point"),
            ).otherwise(f.expr("transform(external_sample_id, sample_id -> NULL)")),
        )
        open_stats_df = open_stats_df.withColumn(
            "discrete_point",
            f.when(
                (f.col("data_type") == "time_series")
                & (f.col("discrete_point").isNotNull()),
                f.col("discrete_point"),
            ).otherwise(f.expr("transform(external_sample_id, sample_id -> NULL)")),
        )

        date_of_birth_udf = (
            lambda specimen_list: [
                specimen_dob_dict[specimen] if specimen in specimen_dob_dict else None
                for specimen in specimen_list
            ]
            if specimen_list is not None
            else []
        )
        date_of_birth_udf = f.udf(date_of_birth_udf, ArrayType(StringType()))
        open_stats_df = open_stats_df.withColumn(
            "date_of_birth", date_of_birth_udf("external_sample_id")
        )
        if extract_windowed_data:
            open_stats_df = open_stats_df.withColumn(
                "window_weight",
                f.when(
                    (f.col("data_type") == "unidimensional")
                    & (f.col("window_weight").isNotNull()),
                    f.col("window_weight"),
                ).otherwise(f.expr("transform(external_sample_id, sample_id -> NULL)")),
            )
        raw_data_cols = [
            "biological_sample_group",
            "date_of_experiment",
            "external_sample_id",
            "specimen_sex",
            "body_weight",
            "data_point",
            "category",
            "time_point",
            "discrete_point",
        ]
        if extract_windowed_data:
            raw_data_cols.append("window_weight")
        open_stats_df = open_stats_df.withColumn(
            "raw_data", f.arrays_zip(*raw_data_cols)
        )

        # to_json_udf = udf(
        #     lambda row: None
        #     if row is None
        #     else json.dumps(
        #         [
        #             {raw_data_cols[int(key)]: value for key, value in item.asDict().items()}
        #             for item in row
        #         ]
        #     ),
        #     StringType(),
        # )
        open_stats_df = open_stats_df.withColumn("raw_data", f.to_json("raw_data"))
        for idx, col_name in enumerate(raw_data_cols):
            open_stats_df = open_stats_df.withColumn(
                "raw_data",
                f.regexp_replace("raw_data", f'"{idx}":', f'"{col_name}":'),
            )
        if compress:
            open_stats_df = open_stats_df.withColumn(
                "raw_data", compress_and_encode("raw_data")
            )
        return open_stats_df

    def map_ontology_prefix(self, open_stats_df, term_prefix, field_prefix):
        mapped_columns = [
            col_name for col_name in STATS_RESULTS_COLUMNS if field_prefix in col_name
        ]
        for col_name in mapped_columns:
            mp_col_name = col_name.replace(field_prefix, "mp_")
            open_stats_df = open_stats_df.withColumn(
                col_name,
                f.when(
                    f.col(col_name).isNull(),
                    f.when(
                        f.col("mp_term_id").startswith(term_prefix),
                        f.col(mp_col_name),
                    ).otherwise(f.lit(None)),
                ).otherwise(f.col(col_name)),
            )
        mapped_id = field_prefix + "term_id"
        for col_name in mapped_columns:
            mp_col_name = col_name.replace(field_prefix, "mp_")
            open_stats_df = open_stats_df.withColumn(
                mp_col_name,
                f.when(
                    f.col(mapped_id).isNotNull(),
                    f.lit(None),
                ).otherwise(f.col(mp_col_name)),
            )
        return open_stats_df

    def map_to_stats(
        self, open_stats_df, metadata_df, join_columns, source_stats_map, source_name
    ):
        for col_name in metadata_df.columns:
            if col_name not in join_columns:
                metadata_df = metadata_df.withColumnRenamed(
                    col_name, f"{source_name}_{col_name}"
                )
        if source_name == "observations":
            open_stats_df_ext = open_stats_df.join(metadata_df, join_columns)
        else:
            open_stats_df_ext = open_stats_df.join(
                metadata_df, join_columns, "left_outer"
            )
        for column_name, source_column in source_stats_map.items():
            open_stats_df_ext = open_stats_df_ext.withColumn(
                column_name, f.col(f"{source_name}_{source_column}")
            )
        for source_column in source_stats_map.values():
            open_stats_df_ext = open_stats_df_ext.drop(f"{source_name}_{source_column}")
        return open_stats_df_ext

    def standardize_threei_schema(self, threei_df: DataFrame):
        threei_df = threei_df.dropDuplicates()
        for col_name, threei_column in THREEI_STATS_MAP.items():
            threei_df = threei_df.withColumnRenamed(threei_column, col_name)
        threei_df = threei_df.withColumn("resource_name", f.lit("3i"))
        threei_df = threei_df.withColumn(
            "procedure_stable_id",
            f.array(f.col("procedure_stable_id")),
        )
        threei_df = threei_df.withColumn(
            "sex",
            f.when(
                f.col("sex") == "both",
                f.lit("not_considered"),
            ).otherwise(f.lower(f.col("sex"))),
        )
        threei_df = threei_df.withColumn(
            "zygosity",
            f.when(
                f.col("zygosity") == "Hom",
                f.lit("homozygote"),
            )
            .when(
                f.col("zygosity") == "Hemi",
                f.lit("hemizygote"),
            )
            .otherwise(f.lit("heterozygote")),
        )
        threei_df = threei_df.withColumn(
            "term_id", f.regexp_replace("mp_id", r"\[", "")
        )

        threei_df = threei_df.withColumn(
            "threei_collapsed_mp_term",
            f.when(
                (f.col("mp_id") != "NA") & (f.col("mp_id").isNotNull()),
                f.struct(
                    f.lit(None).cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    "sex",
                    f.col("term_id").alias("term_id"),
                ),
            ).otherwise(f.lit(None)),
        )
        threei_df = threei_df.withColumn(
            "threei_p_value",
            f.when(
                f.col("classification_tag") == "Significant",
                f.lit(0.0),
            ).otherwise(f.lit(1.0)),
        )
        threei_df = threei_df.withColumn(
            "threei_genotype_effect_p_value",
            f.when(
                f.col("classification_tag") == "Significant",
                f.lit(0.0),
            ).otherwise(f.lit(1.0)),
        )
        threei_df = threei_df.withColumn(
            "threei_genotype_effect_parameter_estimate",
            f.when(
                f.col("classification_tag") == "Significant",
                f.lit(1.0),
            ).otherwise(f.lit(0.0)),
        )
        threei_df = threei_df.withColumn(
            "threei_significant",
            f.when(
                f.col("classification_tag") == "Significant",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        threei_df = threei_df.withColumn(
            "threei_status",
            f.when(
                f.col("classification_tag").isin(["Significant", "Not Significant"]),
                f.lit("Successful"),
            ).otherwise(f.lit("NotProcessed")),
        )
        threei_df = threei_df.withColumn(
            "threei_statistical_method", f.lit("Supplied as data")
        )
        threei_df = threei_df.drop(
            "sex",
            "term_id",
            "mp_id",
            "parameter_name",
            "procedure_name",
            "combine_sex_call",
            "samples",
            "allele_name",
            "classification_tag",
        )
        return threei_df

    def map_three_i(self, open_stats_df):
        open_stats_df = open_stats_df.withColumn(
            "genotype_effect_parameter_estimate",
            f.when(
                f.col("threei_genotype_effect_parameter_estimate").isNotNull(),
                f.col("threei_genotype_effect_parameter_estimate"),
            ).otherwise(f.col("genotype_effect_parameter_estimate")),
        )
        open_stats_df = open_stats_df.drop("threei_genotype_effect_parameter_estimate")

        open_stats_df = open_stats_df.withColumn(
            "significant",
            f.when(
                f.col("threei_significant").isNotNull(),
                f.col("threei_significant"),
            ).otherwise(f.col("significant")),
        )
        open_stats_df = open_stats_df.drop("threei_significant")

        open_stats_df = open_stats_df.withColumn(
            "status",
            f.when(
                f.col("threei_status").isNotNull(),
                f.col("threei_status"),
            ).otherwise(f.col("status")),
        )
        open_stats_df = open_stats_df.drop("threei_status")
        open_stats_df = open_stats_df.withColumn(
            "statistical_method",
            f.when(
                f.col("threei_statistical_method").isNotNull(),
                f.col("threei_statistical_method"),
            ).otherwise(f.col("statistical_method")),
        )
        open_stats_df = open_stats_df.drop("threei_statistical_method")

        open_stats_df = open_stats_df.withColumn(
            "p_value",
            f.when(
                f.col("threei_p_value").isNotNull(),
                f.col("threei_p_value"),
            ).otherwise(f.col("p_value")),
        )
        open_stats_df = open_stats_df.drop("threei_p_value")

        open_stats_df = open_stats_df.withColumn(
            "genotype_effect_p_value",
            f.when(
                f.col("threei_genotype_effect_p_value").isNotNull(),
                f.col("threei_genotype_effect_p_value"),
            ).otherwise(f.col("genotype_effect_p_value")),
        )
        open_stats_df = open_stats_df.drop("threei_genotype_effect_p_value")
        return open_stats_df

    def _pwg_stats(self, pwg_df: DataFrame, observations_df: DataFrame):
        pwg_df = pwg_df.withColumn(
            "zygosity",
            f.when(
                f.col("zygosity") == "homozygous",
                f.lit("homozygote"),
            )
            .when(
                f.col("zygosity") == "hemizygous",
                f.lit("hemizygote"),
            )
            .otherwise(f.lit("heterozygote")),
        )
        observations_df = observations_df.where(
            f.col("procedure_group").isin(PWG_PROCEDURES)
        )
        required_stats_columns = STATS_OBSERVATIONS_JOIN + [
            "procedure_stable_id",
            "pipeline_name",
            "allele_accession_id",
            "parameter_name",
            "allele_symbol",
            "marker_accession_id",
            "marker_symbol",
            "strain_accession_id",
        ]
        pwg_stats_results = (
            observations_df.withColumnRenamed(
                "gene_accession_id", "marker_accession_id"
            )
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
            .distinct()
        )
        pwg_stats_results = pwg_df.join(
            pwg_stats_results,
            [
                "colony_id",
                "marker_accession_id",
                "zygosity",
                "procedure_stable_id",
                "parameter_stable_id",
            ],
        )

        pwg_stats_results = pwg_stats_results.withColumn(
            "sex",
            f.when(
                f.col("sex") == "both",
                f.lit("not_considered"),
            ).otherwise(f.lower(f.col("sex"))),
        )

        pwg_stats_results = pwg_stats_results.withColumn(
            "collapsed_mp_term",
            f.when(
                (f.col("mp_term") != "NA") & (f.col("mp_term").isNotNull()),
                f.struct(
                    f.lit(None).cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    f.col("sex").alias("sex"),
                    f.col("mp_term").alias("term_id"),
                ),
            ).otherwise(f.lit(None)),
        )

        pwg_stats_results = pwg_stats_results.withColumn(
            "significant",
            f.when(
                f.col("significance") == "significant",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        pwg_stats_results = pwg_stats_results.withColumn(
            "batch_significant",
            f.when(
                f.col("batch_significant") == "TRUE",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        pwg_stats_results = pwg_stats_results.withColumn(
            "variance_significant",
            f.when(
                f.col("variance_significant") == "TRUE",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        pwg_stats_results = pwg_stats_results.withColumn(
            "classification_tag",
            f.concat(
                "classification_tag",
                f.lit(" Sexual dimorphism:"),
                "sexual_dimorphism",
            ),
        )
        pwg_stats_results = pwg_stats_results.withColumn("status", f.lit("Successful"))

        pwg_columns = [
            "sex",
            "significant",
            "status",
            "statistical_method",
            "p_value",
            "genotype_effect_p_value",
            "batch_significant",
            "variance_significant",
            "effect_size",
            "male_ko_effect_p_value",
            "female_ko_effect_p_value",
            "female_percentage_change",
            "male_percentage_change",
            "female_control_count",
            "female_mutant_count",
            "male_control_count",
            "male_mutant_count",
            "collapsed_mp_term",
            "classification_tag",
        ]

        pwg_stats_results = pwg_stats_results.select(
            required_stats_columns + pwg_columns
        )
        pwg_stats_results.printSchema()
        pwg_stats_results.show(vertical=True, truncate=False)
        raise ValueError
        return pwg_stats_results

    def standardize_pwg_schema(self, pwg_df: DataFrame):
        pwg_df = pwg_df.dropDuplicates()
        join_cols = [
            "colony_id",
            "marker_accession_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "zygosity",
        ]
        original_cols = [
            col_name for col_name in pwg_df.columns if col_name not in join_cols
        ]
        for col_name in original_cols:
            pwg_df = pwg_df.withColumnRenamed(col_name, f"pwg_{col_name}")
        pwg_df = pwg_df.withColumn("resource_name", f.lit("pwg"))
        pwg_df = pwg_df.withColumn(
            "procedure_stable_id",
            f.array(f.col("procedure_stable_id")),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_sex",
            f.when(
                f.col("pwg_sex") == "both",
                f.lit("not_considered"),
            ).otherwise(f.lower(f.col("pwg_sex"))),
        )
        pwg_df = pwg_df.withColumn(
            "zygosity",
            f.when(
                f.col("zygosity") == "homozygous",
                f.lit("homozygote"),
            )
            .when(
                f.col("zygosity") == "hemizygous",
                f.lit("hemizygote"),
            )
            .otherwise(f.lit("heterozygote")),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_collapsed_mp_term",
            f.when(
                (f.col("pwg_mp_term") != "NA") & (f.col("pwg_mp_term").isNotNull()),
                f.struct(
                    f.lit(None).cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    f.col("pwg_sex").alias("sex"),
                    f.col("pwg_mp_term").alias("term_id"),
                ),
            ).otherwise(f.lit(None)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_significant",
            f.when(
                f.col("pwg_significance") == "significant",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_batch_significant",
            f.when(
                f.col("pwg_batch_significant") == "TRUE",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_variance_significant",
            f.when(
                f.col("pwg_variance_significant") == "TRUE",
                f.lit(True),
            ).otherwise(f.lit(False)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_classification_tag",
            f.concat("pwg_classification_tag", "pwg_sexual_dimorphism"),
        )
        pwg_df = pwg_df.withColumn("pwg_status", f.lit("Successful"))
        pwg_df.printSchema()
        pwg_df.show(vertical=True, truncate=False)

        return pwg_df

    def map_pwg(self, open_stats_df):
        open_stats_df = open_stats_df.where(
            (f.col("resource_name") != "pwg") | (f.col("pwg_status") == "Successful")
        )
        pwg_columns = [
            "significant",
            "status",
            "statistical_method",
            "p_value",
            "genotype_effect_p_value",
            "batch_significant",
            "variance_significant",
            "effect_size",
            "male_ko_effect_p_value",
            "female_ko_effect_p_value",
            "female_percentage_change",
            "male_percentage_change",
            "female_control_count",
            "female_mutant_count",
            "male_control_count",
            "male_mutant_count",
            "collapsed_mp_term",
        ]
        for col_name in pwg_columns:
            open_stats_df = open_stats_df.withColumn(
                col_name,
                f.when(
                    f.col(f"pwg_{col_name}").isNotNull(),
                    f.col(f"pwg_{col_name}"),
                ).otherwise(f.col(col_name)),
            )
            open_stats_df = open_stats_df.drop(f"pwg_{col_name}")
        return open_stats_df

    def _fertility_stats_results(
        self, observations_df: DataFrame, pipeline_df: DataFrame
    ):
        fertility_condition = f.col("parameter_stable_id").isin(
            ["IMPC_FER_001_001", "IMPC_FER_019_001"]
        )

        # mp_chooser = (
        #     pipeline_df.select(
        #         col("pipelineKey").alias("pipeline_stable_id"),
        #         col("procedure.procedureKey").alias("procedure_stable_id"),
        #         col("parameter.parameterKey").alias("parameter_stable_id"),
        #         col("parammpterm.optionText").alias("category"),
        #         col("termAcc"),
        #     )
        #     .withColumn("category", lower(col("category")))
        #     .distinct()
        # )

        required_stats_columns = STATS_OBSERVATIONS_JOIN + [
            "sex",
            "procedure_stable_id",
            "pipeline_name",
            "category",
            "allele_accession_id",
            "parameter_name",
            "allele_symbol",
            "marker_accession_id",
            "marker_symbol",
            "strain_accession_id",
        ]
        fertility_stats_results = (
            observations_df.where(fertility_condition)
            .withColumnRenamed("gene_accession_id", "marker_accession_id")
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "category",
            f.lower(f.col("category")),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "data_type", f.lit("line")
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "effect_size", f.lit(1.0)
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "statistical_method", f.lit("Supplied as data")
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "status", f.lit("Successful")
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "p_value", f.lit(0.0)
        )

        # fertility_stats_results = fertility_stats_results.join(
        #     mp_chooser,
        #     [
        #         "pipeline_stable_id",
        #         "procedure_stable_id",
        #         "parameter_stable_id",
        #         "category",
        #     ],
        #     "left_outer",
        # )

        fertility_stats_results = fertility_stats_results.withColumn(
            "termAcc",
            f.when(
                f.col("category") == "infertile",
                f.when(
                    f.col("parameter_stable_id") == "IMPC_FER_001_001",
                    f.lit("MP:0001925"),
                ).otherwise(f.lit("MP:0001926")),
            ).otherwise(f.lit(None)),
        )

        fertility_stats_results = fertility_stats_results.groupBy(
            required_stats_columns
            + ["data_type", "status", "effect_size", "statistical_method", "p_value"]
        ).agg(
            f.collect_set("category").alias("categories"),
            f.collect_set(
                f.struct(
                    f.lit("ABNORMAL").cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    "sex",
                    f.col("termAcc").alias("term_id"),
                )
            ).alias("mp_term"),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "mp_term",
            f.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "mp_term",
            f.when(
                f.size(f.col("mp_term.term_id")) == 0,
                f.lit(None),
            ).otherwise(f.col("mp_term")),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(1.0),
            ).otherwise(f.col("p_value")),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "effect_size",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(0.0),
            ).otherwise(f.col("effect_size")),
        )
        return fertility_stats_results

    def _embryo_stats_results(
        self,
        observations_df: DataFrame,
        pipeline_df: DataFrame,
        embryo_stats_packets: DataFrame,
    ):

        mp_chooser = pipeline_df.select(
            "pipelineKey",
            "procedure.procedureKey",
            "parameter.parameterKey",
            "parammpterm.optionText",
            "parammpterm.selectionOutcome",
            "termAcc",
        ).distinct()

        mp_chooser = (
            mp_chooser.withColumnRenamed("pipelineKey", "pipeline_stable_id")
            .withColumnRenamed("procedureKey", "procedure_stable_id")
            .withColumnRenamed("parameterKey", "parameter_stable_id")
        )

        mp_chooser = mp_chooser.withColumn(
            "category",
            f.when(
                f.col("optionText").isNull(),
                f.col("selectionOutcome"),
            ).otherwise(f.col("optionText")),
        )

        mp_chooser = mp_chooser.withColumn(
            "category",
            f.lower(f.col("category")),
        )
        mp_chooser = mp_chooser.drop("optionText", "selectionOutcome")

        required_stats_columns = STATS_OBSERVATIONS_JOIN + [
            "sex",
            "procedure_stable_id",
            "pipeline_name",
            "category",
            "allele_accession_id",
            "parameter_name",
            "allele_symbol",
            "marker_accession_id",
            "marker_symbol",
            "strain_accession_id",
            "text_value",
        ]
        embryo_stats_results = (
            observations_df.where(
                f.col("procedure_group").rlike(
                    "|".join(
                        [
                            "IMPC_GPL",
                            "IMPC_GEL",
                            "IMPC_GPM",
                            "IMPC_GEM",
                            "IMPC_GPO",
                            "IMPC_GEO",
                            "IMPC_GPP",
                            "IMPC_GEP",
                        ]
                    )
                )
                & (f.col("biological_sample_group") == "experimental")
                & (f.col("observation_type") == "categorical")
            )
            .withColumnRenamed("gene_accession_id", "marker_accession_id")
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )
        embryo_control_data = observations_df.where(
            f.col("procedure_group").rlike(
                "|".join(
                    [
                        "IMPC_GPL",
                        "IMPC_GEL",
                        "IMPC_GPM",
                        "IMPC_GEM",
                        "IMPC_GPO",
                        "IMPC_GEO",
                        "IMPC_GPP",
                        "IMPC_GEP",
                    ]
                )
            )
            & (f.col("biological_sample_group") == "control")
            & (f.col("observation_type") == "categorical")
            & (f.col("category").isin(["yes", "no"]))
        )
        embryo_control_data = embryo_control_data.select(
            "procedure_stable_id", "parameter_stable_id", "category"
        )
        embryo_control_data = embryo_control_data.groupBy(
            "procedure_stable_id", "parameter_stable_id", "category"
        ).count()
        window = Window.partitionBy(
            "procedure_stable_id", "parameter_stable_id"
        ).orderBy(f.col("count").desc())
        embryo_normal_data = embryo_control_data.select(
            "procedure_stable_id",
            "parameter_stable_id",
            f.first("category").over(window).alias("normal_category"),
        ).distinct()
        embryo_stats_results = embryo_stats_results.withColumn(
            "category",
            f.lower(f.col("category")),
        )
        embryo_stats_results = embryo_stats_results.join(
            embryo_normal_data,
            ["procedure_stable_id", "parameter_stable_id"],
            "left_outer",
        )

        embryo_stats_results = embryo_stats_results.withColumn(
            "category",
            f.when(
                f.col("category").isin(["yes", "no"])
                & (f.col("category") != f.col("normal_category")),
                f.lit("abnormal"),
            ).otherwise(f.col("category")),
        )

        embryo_stats_results = embryo_stats_results.drop("normal_category")

        embryo_stats_results = embryo_stats_results.withColumn(
            "data_type", f.lit("categorical")
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "status", f.lit("Successful")
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "statistical_method", f.lit("Supplied as data")
        )

        embryo_stats_results = embryo_stats_results.join(
            mp_chooser,
            [
                "pipeline_stable_id",
                "procedure_stable_id",
                "parameter_stable_id",
                "category",
            ],
            "left_outer",
        )

        group_by_cols = [
            col_name
            for col_name in required_stats_columns
            if col_name not in ["category", "sex"]
        ]
        group_by_cols += ["data_type", "status", "statistical_method"]

        embryo_stats_results = embryo_stats_results.groupBy(group_by_cols).agg(
            f.collect_set("sex").alias("sex"),
            f.collect_set("category").alias("categories"),
            f.collect_set(
                f.struct(
                    f.lit("ABNORMAL").cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    f.col("sex"),
                    f.col("termAcc").alias("term_id"),
                ),
            ).alias("mp_term"),
            f.collect_list(f.col("termAcc")).alias("abnormalCalls"),
        )

        embryo_stats_results = embryo_stats_results.withColumn(
            "mp_term",
            f.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "abnormalCallsCount",
            f.size(f.expr("filter(abnormalCalls, mp -> mp IS NOT NULL)")),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "mp_term",
            f.when(
                (
                    (f.col("zygosity") == "homozygote")
                    | (f.col("zygosity") == "hemizygote")
                )
                & (f.col("abnormalCallsCount") >= 2),
                f.col("mp_term"),
            )
            .when(
                (f.col("zygosity") == "heterozygote")
                & (f.col("abnormalCallsCount") >= 4),
                f.col("mp_term"),
            )
            .otherwise(f.lit(None)),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(1.0),
            ).otherwise(f.lit(0.0)),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "effect_size",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(0.0),
            ).otherwise(f.lit(1.0)),
        )
        embryo_stats_results = embryo_stats_results.groupBy(
            *[
                col_name
                for col_name in embryo_stats_results.columns
                if col_name not in ["mp_term", "p_value", "effect_size"]
            ]
        ).agg(
            f.flatten(f.collect_set("mp_term")).alias("mp_term"),
            f.min("p_value").alias("p_value"),
            f.max("effect_size").alias("effect_size"),
        )
        for col_name in embryo_stats_packets.columns:
            if (
                col_name in embryo_stats_results.columns
                and col_name not in STATS_OBSERVATIONS_JOIN
            ):
                embryo_stats_packets = embryo_stats_packets.drop(col_name)
        embryo_stats_results = embryo_stats_results.join(
            embryo_stats_packets, STATS_OBSERVATIONS_JOIN, "left_outer"
        )
        return embryo_stats_results

    def _embryo_viability_stats_results(
        self, observations_df: DataFrame, pipeline_df: DataFrame
    ):

        mp_chooser = (
            pipeline_df.select(
                "pipelineKey",
                "procedure.procedureKey",
                "parameter.parameterKey",
                "parammpterm.optionText",
                "termAcc",
            )
            .distinct()
            .withColumnRenamed("pipelineKey", "pipeline_stable_id")
            .withColumnRenamed("procedureKey", "procedure_stable_id")
            .withColumnRenamed("parameterKey", "parameter_stable_id")
            .withColumnRenamed("optionText", "category")
        ).withColumn(
            "category",
            f.lower(f.col("category")),
        )

        required_stats_columns = STATS_OBSERVATIONS_JOIN + [
            "sex",
            "procedure_stable_id",
            "pipeline_name",
            "category",
            "allele_accession_id",
            "parameter_name",
            "allele_symbol",
            "marker_accession_id",
            "marker_symbol",
            "strain_accession_id",
            "text_value",
        ]
        embryo_viability_stats_results = (
            observations_df.where(
                f.col("parameter_stable_id").isin(
                    [
                        "IMPC_EVL_001_001",
                        "IMPC_EVM_001_001",
                        "IMPC_EVO_001_001",
                        "IMPC_EVP_001_001",
                    ]
                )
            )
            .withColumnRenamed("gene_accession_id", "marker_accession_id")
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "category",
            f.lower(f.col("category")),
        )

        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "data_type", f.lit("embryo")
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "status", f.lit("Successful")
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "statistical_method", f.lit("Supplied as data")
        )

        embryo_viability_stats_results = embryo_viability_stats_results.join(
            mp_chooser,
            [
                "pipeline_stable_id",
                "procedure_stable_id",
                "parameter_stable_id",
                "category",
            ],
            "left_outer",
        )

        embryo_viability_stats_results = embryo_viability_stats_results.groupBy(
            required_stats_columns + ["data_type", "status", "statistical_method"]
        ).agg(
            f.collect_set("category").alias("categories"),
            f.collect_set(
                f.struct(
                    f.lit("ABNORMAL").cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    f.lit("not_considered").cast(StringType()).alias("sex"),
                    f.col("termAcc").alias("term_id"),
                )
            ).alias("mp_term"),
        )

        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "mp_term",
            f.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "mp_term",
            f.when(
                f.size(f.col("mp_term.term_id")) == 0,
                f.lit(None),
            ).otherwise(f.col("mp_term")),
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(1.0),
            ).otherwise(f.lit(0.0)),
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "effect_size",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(0.0),
            ).otherwise(f.lit(1.0)),
        )

        return embryo_viability_stats_results

    def _viability_stats_results(
        self, observations_df: DataFrame, pipeline_df: DataFrame
    ):
        mp_chooser = (
            pipeline_df.select(
                "pipelineKey",
                "procedure.procedureKey",
                "parameter.parameterKey",
                "parammpterm.optionText",
                "termAcc",
            )
            .distinct()
            .withColumnRenamed("pipelineKey", "pipeline_stable_id")
            .withColumnRenamed("procedureKey", "procedure_stable_id")
            .withColumnRenamed("parameterKey", "parameter_stable_id")
            .withColumnRenamed("optionText", "category")
        ).withColumn(
            "category",
            f.lower(f.col("category")),
        )

        required_stats_columns = STATS_OBSERVATIONS_JOIN + [
            "sex",
            "procedure_stable_id",
            "pipeline_name",
            "category",
            "allele_accession_id",
            "parameter_name",
            "allele_symbol",
            "marker_accession_id",
            "marker_symbol",
            "strain_accession_id",
            "text_value",
        ]

        viability_stats_results = (
            observations_df.where(
                (
                    (f.col("parameter_stable_id") == "IMPC_VIA_001_001")
                    & (f.col("procedure_stable_id") == "IMPC_VIA_001")
                )
                | (
                    (
                        f.col("parameter_stable_id").isin(
                            [
                                "IMPC_VIA_063_001",
                                "IMPC_VIA_064_001",
                                "IMPC_VIA_065_001",
                                "IMPC_VIA_066_001",
                                "IMPC_VIA_067_001",
                            ]
                        )
                    )
                    & (f.col("procedure_stable_id") == "IMPC_VIA_002")
                )
            )
            .withColumnRenamed("gene_accession_id", "marker_accession_id")
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )

        viability_stats_results = viability_stats_results.withColumn(
            "category",
            f.lower(f.col("category")),
        )

        json_outcome_schema = StructType(
            [
                StructField("outcome", StringType()),
                StructField("n", IntegerType()),
                StructField("P", DoubleType()),
            ]
        )

        viability_stats_results = viability_stats_results.withColumn(
            "viability_outcome",
            f.when(
                f.col("procedure_stable_id") == "IMPC_VIA_002",
                f.from_json(f.col("text_value"), json_outcome_schema),
            ).otherwise(f.lit(None)),
        )

        viability_p_values = observations_df.where(
            f.col("parameter_stable_id") == "IMPC_VIA_032_001"
        ).select(
            "procedure_stable_id",
            "colony_id",
            f.col("data_point").alias("p_value"),
        )

        viability_male_mutants = observations_df.where(
            f.col("parameter_stable_id") == "IMPC_VIA_010_001"
        ).select(
            "procedure_stable_id",
            "colony_id",
            f.col("data_point").alias("male_mutants"),
        )

        viability_female_mutants = observations_df.where(
            f.col("parameter_stable_id") == "IMPC_VIA_014_001"
        ).select(
            "procedure_stable_id",
            "colony_id",
            f.col("data_point").alias("female_mutants"),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "data_type", f.lit("line")
        )
        viability_stats_results = viability_stats_results.withColumn(
            "effect_size", f.lit(1.0)
        )
        viability_stats_results = viability_stats_results.join(
            viability_p_values, ["colony_id", "procedure_stable_id"], "left_outer"
        )
        viability_stats_results = viability_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("procedure_stable_id") == "IMPC_VIA_002",
                f.col("viability_outcome.P"),
            ).otherwise(f.col("p_value")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("p_value").isNull() & ~f.col("category").contains("Viable"),
                f.lit(0.0),
            ).otherwise(f.col("p_value")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "male_controls", f.lit(None)
        )
        viability_stats_results = viability_stats_results.join(
            viability_male_mutants, ["colony_id", "procedure_stable_id"], "left_outer"
        )
        viability_stats_results = viability_stats_results.withColumn(
            "male_mutants",
            f.when(
                (f.col("procedure_stable_id") == "IMPC_VIA_002")
                & (f.col("parameter_name").contains(" males ")),
                f.col("viability_outcome.n"),
            ).otherwise(f.col("male_mutants")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "female_controls", f.lit(None)
        )
        viability_stats_results = viability_stats_results.join(
            viability_female_mutants, ["colony_id", "procedure_stable_id"], "left_outer"
        )

        viability_stats_results = viability_stats_results.withColumn(
            "female_mutants",
            f.when(
                (f.col("procedure_stable_id") == "IMPC_VIA_002")
                & (f.col("parameter_name").contains(" females ")),
                f.col("viability_outcome.n"),
            ).otherwise(f.col("female_mutants")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "statistical_method",
            f.when(
                f.col("procedure_stable_id") == "IMPC_VIA_002",
                f.lit("Binomial distribution probability"),
            ).otherwise(f.lit("Supplied as data")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "status", f.lit("Successful")
        )

        viability_stats_results = viability_stats_results.join(
            mp_chooser,
            [
                "pipeline_stable_id",
                "procedure_stable_id",
                "parameter_stable_id",
                "category",
            ],
            "left_outer",
        )

        viability_stats_results = viability_stats_results.groupBy(
            required_stats_columns
            + [
                "data_type",
                "status",
                "effect_size",
                "statistical_method",
                "p_value",
                "male_mutants",
                "female_mutants",
                "viability_outcome",
            ]
        ).agg(
            f.collect_set("category").alias("categories"),
            f.collect_set(
                f.struct(
                    f.lit("ABNORMAL").cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    f.lit("not_considered").cast(StringType()).alias("sex"),
                    f.col("termAcc").alias("term_id"),
                )
            ).alias("mp_term"),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "mp_term",
            f.when(
                (f.col("procedure_stable_id") == "IMPC_VIA_002"),
                f.array(
                    f.struct(
                        f.lit("ABNORMAL").cast(StringType()).alias("event"),
                        f.lit(None).cast(StringType()).alias("otherPossibilities"),
                        f.when(
                            f.col("parameter_name").contains(" males "),
                            f.lit("male"),
                        )
                        .when(
                            f.col("parameter_name").contains(" females "),
                            f.lit("female"),
                        )
                        .otherwise(f.lit("not_considered"))
                        .cast(StringType())
                        .alias("sex"),
                        f.when(
                            f.col("viability_outcome.outcome").contains("subviable"),
                            f.lit("MP:0011110"),
                        )
                        .when(
                            f.col("viability_outcome.outcome").contains("lethal"),
                            f.lit("MP:0011100"),
                        )
                        .otherwise(f.lit(None).cast(StringType()))
                        .cast(StringType())
                        .alias("term_id"),
                    )
                ),
            ).otherwise(f.col("mp_term")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "mp_term",
            f.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "mp_term",
            f.when(
                f.size(f.col("mp_term.term_id")) == 0,
                f.lit(None),
            ).otherwise(f.col("mp_term")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(1.0),
            ).otherwise(f.col("p_value")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "effect_size",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(0.0),
            ).otherwise(f.col("effect_size")),
        )
        return viability_stats_results

    def _histopathology_stats_results(self, observations_df: DataFrame):
        histopathology_stats_results = observations_df.where(
            f.expr("exists(sub_term_id, term -> term LIKE 'MPATH:%')")
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "term_set",
            f.array_sort(f.array_distinct("sub_term_name")),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "is_normal",
            (f.size("term_set") == 1)
            & f.expr("exists(term_set, term -> term = 'normal')"),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "tissue_name",
            f.regexp_extract("parameter_name", "(.*)( - .*)", 1),
        )

        histopathology_significance_scores = observations_df.where(
            f.col("parameter_name").endswith("Significance score")
        ).where(f.col("biological_sample_group") == "experimental")

        histopathology_significance_scores = (
            histopathology_significance_scores.withColumn(
                "tissue_name",
                f.regexp_extract("parameter_name", "(.*)( - .*)", 1),
            )
        )

        histopathology_significance_scores = (
            histopathology_significance_scores.withColumn(
                "significance",
                f.when(
                    f.col("category") == "1",
                    f.lit(True),
                ).otherwise(f.lit(False)),
            )
        )

        significance_stats_join = [
            "pipeline_stable_id",
            "procedure_stable_id",
            "specimen_id",
            "experiment_id",
            "tissue_name",
        ]

        histopathology_significance_scores = histopathology_significance_scores.select(
            significance_stats_join + ["significance"]
        )
        histopathology_stats_results = histopathology_stats_results.join(
            histopathology_significance_scores, significance_stats_join, "left_outer"
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "significance",
            f.when(
                f.col("significance") & ~f.col("is_normal"),
                f.lit(True),
            ).otherwise(f.lit(False)),
        )

        required_stats_columns = STATS_OBSERVATIONS_JOIN + [
            "sex",
            "procedure_stable_id",
            "pipeline_name",
            "allele_accession_id",
            "parameter_name",
            "allele_symbol",
            "marker_accession_id",
            "marker_symbol",
            "strain_accession_id",
            "sub_term_id",
            "sub_term_name",
            "specimen_id",
            "significance",
        ]

        histopathology_stats_results = (
            histopathology_stats_results.withColumnRenamed(
                "gene_accession_id", "marker_accession_id"
            )
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )

        histopathology_stats_results = histopathology_stats_results.withColumn(
            "sub_term_id",
            f.expr("filter(sub_term_id, mp -> mp LIKE 'MPATH:%')"),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "term_id", f.explode_outer("sub_term_id")
        )

        histopathology_stats_results = histopathology_stats_results.groupBy(
            *[
                col_name
                for col_name in required_stats_columns
                if col_name not in ["sex", "term_id"]
            ]
        ).agg(
            f.collect_set(
                f.struct(
                    f.lit("ABNORMAL").cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    f.col("sex"),
                    f.col("term_id"),
                )
            ).alias("mp_term")
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "mp_term",
            f.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "mp_term",
            f.when(
                f.col("significance").isNull() | ~f.col("significance"),
                f.lit(None),
            ).otherwise(f.col("mp_term")),
        )

        histopathology_stats_results = histopathology_stats_results.withColumn(
            "statistical_method", f.lit("Supplied as data")
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "status", f.lit("Successful")
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("significance").isNull() | ~f.col("significance"),
                f.lit(1.0),
            ).otherwise(f.lit(0.0)),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "effect_size",
            f.when(
                f.col("significance").isNull() | ~f.col("significance"),
                f.lit(0.0),
            ).otherwise(f.lit(1.0)),
        )

        histopathology_stats_results = histopathology_stats_results.withColumn(
            "data_type", f.lit("histopathology")
        )
        histopathology_stats_results = histopathology_stats_results.drop("significance")
        histopathology_stats_results = histopathology_stats_results.dropDuplicates()

        return histopathology_stats_results

    def _gross_pathology_stats_results(self, observations_df: DataFrame):
        gross_pathology_stats_results = observations_df.where(
            (f.col("biological_sample_group") != "control")
            & f.col("parameter_stable_id").like("%PAT%")
            & (f.expr("exists(sub_term_id, term -> term LIKE 'MP:%')"))
        )
        required_stats_columns = STATS_OBSERVATIONS_JOIN + [
            "sex",
            "procedure_stable_id",
            "pipeline_name",
            "category",
            "allele_accession_id",
            "parameter_name",
            "allele_symbol",
            "marker_accession_id",
            "marker_symbol",
            "strain_accession_id",
            "sub_term_id",
            "sub_term_name",
            "specimen_id",
        ]
        gross_pathology_stats_results = (
            gross_pathology_stats_results.withColumnRenamed(
                "gene_accession_id", "marker_accession_id"
            )
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )

        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "sub_term_id",
            f.expr("filter(sub_term_id, mp -> mp LIKE 'MP:%')"),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "term_id", f.explode_outer("sub_term_id")
        )

        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "term_id",
            f.when(
                f.expr(
                    "exists(sub_term_name, term -> term = 'no abnormal phenotype detected')"
                )
                | f.expr("exists(sub_term_name, term -> term = 'normal')"),
                f.lit(None),
            ).otherwise(f.col("term_id")),
        )

        gross_pathology_stats_results = gross_pathology_stats_results.groupBy(
            *[
                col_name
                for col_name in required_stats_columns
                if col_name not in ["sex", "sub_term_id", "sub_term_name"]
            ]
        ).agg(
            f.collect_set(
                f.struct(
                    f.lit("ABNORMAL").cast(StringType()).alias("event"),
                    f.lit(None).cast(StringType()).alias("otherPossibilities"),
                    f.col("sex"),
                    f.col("term_id"),
                )
            ).alias("mp_term")
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "mp_term",
            f.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "mp_term",
            f.when(
                f.size(f.col("mp_term.term_id")) == 0,
                f.lit(None),
            ).otherwise(f.col("mp_term")),
        )

        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "statistical_method", f.lit("Supplied as data")
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "status", f.lit("Successful")
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "p_value",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(1.0),
            ).otherwise(f.lit(0.0)),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "effect_size",
            f.when(
                f.col("mp_term").isNull(),
                f.lit(0.0),
            ).otherwise(f.lit(1.0)),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "data_type", f.lit("adult-gross-path")
        )
        return gross_pathology_stats_results

    def _select_collapsed_mp_term(
        self,
        mp_term_array: List[Row],
        pipeline,
        procedure_group,
        parameter,
        mp_chooser,
        data_type,
        first_term_ancestors,
        second_term_ancestors,
    ):
        if (
            mp_term_array is None
            or data_type not in ["categorical", "unidimensional"]
            or len(mp_term_array) == 0
        ):
            return mp_term_array
        mp_term = mp_term_array[0].asDict()
        mp_term["sex"] = "not_considered"
        mp_terms = [mp["term_id"] for mp in mp_term_array]
        try:
            if len(set(mp_terms)) > 1:
                print(mp_term_array)
                print(f"{pipeline} {procedure_group} {parameter}")
                mp_term["term_id"] = mp_chooser[pipeline][procedure_group][parameter][
                    "UNSPECIFIED"
                ]["ABNORMAL"]["OVERALL"]["MPTERM"]
            else:
                mp_term["term_id"] = mp_term_array[0]["term_id"]
        except KeyError:
            ancestor_types = ["parent", "intermediate", "top_level"]
            closest_common_ancestor = None
            for ancestor_type in ancestor_types:
                ancestor_intersect = set(
                    first_term_ancestors[f"{ancestor_type}_ids"]
                ) & set(second_term_ancestors[f"{ancestor_type}_ids"])
                if len(ancestor_intersect) > 0:
                    closest_common_ancestor = list(ancestor_intersect)[0]
                    break
            if closest_common_ancestor is None:
                print(mp_term_array)
                print(f"{pipeline} {procedure_group} {parameter}")
                print("Unexpected error:", sys.exc_info()[0])
                raise Exception(
                    str(mp_term_array)
                    + f" | {pipeline} {procedure_group} {parameter} | "
                    + f"[{str(first_term_ancestors)}] [{str(second_term_ancestors)}]"
                    + str(sys.exc_info()[0])
                )
            else:
                mp_term["term_id"] = closest_common_ancestor
        return [convert_to_row(mp_term)]

    def _add_via_002_mp_term_options(self, pipeline_core_df):
        pipeline_core_df = pipeline_core_df.withColumn(
            "top_level_mp_id",
            f.when(
                f.array_contains(f.col("procedure_stable_id"), "IMPC_VIA_002"),
                f.array(f.lit("MP:0010768")),
            ).otherwise(f.col("top_level_mp_id")),
        )
        pipeline_core_df = pipeline_core_df.withColumn(
            "top_level_mp_term",
            f.when(
                f.array_contains(f.col("procedure_stable_id"), "IMPC_VIA_002"),
                f.array(f.lit("mortality/aging")),
            ).otherwise(f.col("top_level_mp_term")),
        )
        pipeline_core_df = pipeline_core_df.withColumn(
            "mp_id",
            f.when(
                f.array_contains(f.col("procedure_stable_id"), "IMPC_VIA_002"),
                f.array(
                    f.lit("MP:0011100"),
                    f.lit("MP:0011110"),
                ),
            ).otherwise(f.col("mp_id")),
        )
        pipeline_core_df = pipeline_core_df.withColumn(
            "mp_term",
            f.when(
                f.array_contains(f.col("procedure_stable_id"), "IMPC_VIA_002"),
                f.array(
                    f.lit("preweaning lethality, complete penetrance"),
                    f.lit("preweaning lethality, incomplete penetrance"),
                ),
            ).otherwise(f.col("mp_term")),
        )
        return pipeline_core_df

    def stop_and_count(self, df):
        print(df.count())
        raise ValueError
