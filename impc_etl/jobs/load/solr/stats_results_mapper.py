import base64
import gzip
import json
import sys
from typing import List, Any

import luigi
import pyspark.sql.functions
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
                    col_name, pyspark.sql.functions.lit(None).astype(StringType())
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
        pwg_df = self.standardize_pwg_schema(pwg_df)

        embryo_stat_packets = open_stats_complete_df.where(
            (
                (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPL"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEL"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPM"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEM"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPO"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEO"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPP"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEP"))
            )
        )

        open_stats_df = open_stats_complete_df.where(
            ~(
                pyspark.sql.functions.col("procedure_stable_id").contains(
                    "IMPC_FER_001"
                )
                | (
                    pyspark.sql.functions.col("procedure_stable_id").contains(
                        "IMPC_VIA_001"
                    )
                )
                | (
                    pyspark.sql.functions.col("procedure_stable_id").contains(
                        "IMPC_VIA_002"
                    )
                )
                | (pyspark.sql.functions.col("procedure_group").contains("_PAT"))
                | (pyspark.sql.functions.col("procedure_group").contains("_EVL"))
                | (pyspark.sql.functions.col("procedure_group").contains("_EVM"))
                | (pyspark.sql.functions.col("procedure_group").contains("_EVO"))
                | (pyspark.sql.functions.col("procedure_group").contains("_EVP"))
                | (pyspark.sql.functions.col("procedure_group").contains("_ELZ"))
                | (
                    pyspark.sql.functions.col("procedure_name").startswith(
                        "Histopathology"
                    )
                )
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPL"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEL"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPM"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEM"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPO"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEO"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GPP"))
                | (pyspark.sql.functions.col("procedure_group").contains("IMPC_GEP"))
                | (pyspark.sql.functions.col("procedure_group").startswith("ALT"))
            )
        )

        manual_hits_observations_df = observations_df.where(
            ~pyspark.sql.functions.col("procedure_stable_id").startswith("ALT")
        )

        fertility_stats = self._fertility_stats_results(
            manual_hits_observations_df, pipeline_df
        )

        for col_name in open_stats_df.columns:
            if col_name not in fertility_stats.columns:
                fertility_stats = fertility_stats.withColumn(
                    col_name, pyspark.sql.functions.lit(None)
                )
        fertility_stats = fertility_stats.select(open_stats_df.columns)

        open_stats_df = open_stats_df.union(fertility_stats)

        viability_stats = self._viability_stats_results(
            manual_hits_observations_df, pipeline_df
        )
        for col_name in open_stats_df.columns:
            if col_name not in viability_stats.columns:
                viability_stats = viability_stats.withColumn(
                    col_name, pyspark.sql.functions.lit(None)
                )
        viability_stats = viability_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(viability_stats)

        gross_pathology_stats = self._gross_pathology_stats_results(
            manual_hits_observations_df
        )
        for col_name in open_stats_df.columns:
            if col_name not in gross_pathology_stats.columns:
                gross_pathology_stats = gross_pathology_stats.withColumn(
                    col_name, pyspark.sql.functions.lit(None)
                )
        gross_pathology_stats = gross_pathology_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(gross_pathology_stats)

        histopathology_stats = self._histopathology_stats_results(
            manual_hits_observations_df
        )
        for col_name in open_stats_df.columns:
            if col_name not in histopathology_stats.columns:
                histopathology_stats = histopathology_stats.withColumn(
                    col_name, pyspark.sql.functions.lit(None)
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
                    col_name, pyspark.sql.functions.lit(None)
                )
        embryo_viability_stats = embryo_viability_stats.select(open_stats_df.columns)
        open_stats_df = open_stats_df.union(embryo_viability_stats)

        embryo_stats = self._embryo_stats_results(
            observations_df, pipeline_df, embryo_stat_packets
        )
        for col_name in open_stats_df.columns:
            if col_name not in embryo_stats.columns:
                embryo_stats = embryo_stats.withColumn(
                    col_name, pyspark.sql.functions.lit(None)
                )
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
        ).agg(pyspark.sql.functions.collect_set("sex").alias("sex"))

        aggregation_expresion = []

        for col_name in list(set(OBSERVATIONS_STATS_MAP.values())):
            if col_name not in ["datasource_name", "production_center"]:
                if col_name == "sex":
                    aggregation_expresion.append(
                        pyspark.sql.functions.array_distinct(
                            pyspark.sql.functions.flatten(
                                pyspark.sql.functions.collect_set(col_name)
                            )
                        ).alias(col_name)
                    )
                elif col_name in ["strain_name", "genetic_background"]:
                    aggregation_expresion.append(
                        pyspark.sql.functions.first(
                            pyspark.sql.functions.col(col_name)
                        ).alias(col_name)
                    )
                else:
                    aggregation_expresion.append(
                        pyspark.sql.functions.collect_set(col_name).alias(col_name)
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("procedure_stable_id") == "ESLIM_022_001",
                pyspark.sql.functions.lit("ESLIM_001"),
            ).otherwise(pyspark.sql.functions.col("pipeline_stable_id")),
        )
        open_stats_df = open_stats_df.withColumn(
            "procedure_stable_id",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("procedure_stable_id").contains("~"),
                pyspark.sql.functions.split(
                    pyspark.sql.functions.col("procedure_stable_id"), "~"
                ),
            ).otherwise(
                pyspark.sql.functions.array(
                    pyspark.sql.functions.col("procedure_stable_id")
                )
            ),
        )
        open_stats_df = open_stats_df.alias("stats")

        mp_ancestors_df = ontology_df.select(
            "id",
            pyspark.sql.functions.struct(
                "parent_ids", "intermediate_ids", "top_level_ids"
            ).alias("ancestors"),
        )
        mp_ancestors_df_1 = mp_ancestors_df.alias("mp_term_1")
        mp_ancestors_df_2 = mp_ancestors_df.alias("mp_term_2")
        open_stats_df = open_stats_df.join(
            mp_ancestors_df_1,
            (
                pyspark.sql.functions.expr("mp_term[0].term_id")
                == pyspark.sql.functions.col("mp_term_1.id")
            ),
            "left_outer",
        )

        open_stats_df = open_stats_df.join(
            mp_ancestors_df_2,
            (
                pyspark.sql.functions.expr("mp_term[1].term_id")
                == pyspark.sql.functions.col("mp_term_2.id")
            ),
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
        select_collapsed_mp_term_udf = pyspark.sql.functions.udf(
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.expr(
                    "exists(mp_term.sex, sex -> sex = 'male') AND exists(mp_term.sex, sex -> sex = 'female')"
                )
                & (
                    pyspark.sql.functions.col("data_type").isin(
                        ["categorical", "unidimensional"]
                    )
                ),
                select_collapsed_mp_term_udf(
                    "mp_term",
                    "pipeline_stable_id",
                    "procedure_group",
                    "parameter_stable_id",
                    "data_type",
                    "mp_term_1.ancestors",
                    "mp_term_2.ancestors",
                ),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )
        open_stats_df = open_stats_df.drop("mp_term_1.*", "mp_term_2.*")
        open_stats_df = open_stats_df.withColumn(
            "collapsed_mp_term", pyspark.sql.functions.expr("collapsed_mp_term[0]")
        )
        open_stats_df = open_stats_df.withColumn(
            "significant", pyspark.sql.functions.lit(False)
        )

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
        open_stats_df = self.map_pwg(open_stats_df)
        open_stats_df = open_stats_df.withColumn(
            "collapsed_mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("threei_collapsed_mp_term").isNotNull(),
                pyspark.sql.functions.col("threei_collapsed_mp_term"),
            ).otherwise(pyspark.sql.functions.col("collapsed_mp_term")),
        )

        open_stats_df = open_stats_df.drop("threei_collapsed_mp_term")

        open_stats_df = open_stats_df.withColumn(
            "mp_term_id",
            pyspark.sql.functions.regexp_replace("collapsed_mp_term.term_id", " ", ""),
        )
        for bad_mp in BAD_MP_MAP.keys():
            open_stats_df = open_stats_df.withColumn(
                "mp_term_id",
                pyspark.sql.functions.when(
                    pyspark.sql.functions.col("mp_term_id") == bad_mp,
                    pyspark.sql.functions.lit(BAD_MP_MAP[bad_mp]),
                ).otherwise(pyspark.sql.functions.col("mp_term_id")),
            )
        open_stats_df = open_stats_df.withColumn(
            "mp_term_event", pyspark.sql.functions.col("collapsed_mp_term.event")
        )
        open_stats_df = open_stats_df.withColumn(
            "mp_term_sex", pyspark.sql.functions.col("collapsed_mp_term.sex")
        )
        open_stats_df = open_stats_df.withColumnRenamed("mp_term", "full_mp_term")
        open_stats_df = open_stats_df.withColumn(
            "full_mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("full_mp_term").isNull()
                & pyspark.sql.functions.col("collapsed_mp_term").isNotNull(),
                pyspark.sql.functions.array(
                    pyspark.sql.functions.col("collapsed_mp_term")
                ),
            )
            .when(
                pyspark.sql.functions.col("full_mp_term").isNull()
                & pyspark.sql.functions.col("collapsed_mp_term").isNull(),
                pyspark.sql.functions.lit(None),
            )
            .otherwise(pyspark.sql.functions.col("full_mp_term")),
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
                open_stats_df = open_stats_df.withColumn(
                    col_name, pyspark.sql.functions.lit(None)
                )

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
                    pyspark.sql.functions.array_distinct(
                        pyspark.sql.functions.flatten(
                            pyspark.sql.functions.collect_set(col_name)
                        )
                    ).alias(col_name)
                    if col_name
                    in [
                        "mp_id",
                        "mp_term",
                        "top_level_mp_id",
                        "top_level_mp_term",
                        "intermediate_mp_id",
                        "intermediate_mp_term",
                    ]
                    else pyspark.sql.functions.collect_set(col_name).alias(col_name)
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
            pyspark.sql.functions.array(pyspark.sql.functions.col("proc_id")),
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("top_level_mp_term_id").isNull(),
                pyspark.sql.functions.col("top_level_mp_id_options"),
            ).otherwise(pyspark.sql.functions.col("top_level_mp_term_id")),
        )
        open_stats_df = open_stats_df.withColumn(
            "top_level_mp_term_name",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("top_level_mp_term_name").isNull(),
                pyspark.sql.functions.col("top_level_mp_term_options"),
            ).otherwise(pyspark.sql.functions.col("top_level_mp_term_name")),
        )

        open_stats_df = open_stats_df.withColumn(
            "intermediate_mp_term_id",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("intermediate_mp_term_id").isNull(),
                pyspark.sql.functions.col("intermediate_mp_id_options"),
            ).otherwise(pyspark.sql.functions.col("intermediate_mp_term_id")),
        )
        open_stats_df = open_stats_df.withColumn(
            "intermediate_mp_term_name",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("intermediate_mp_term_name").isNull(),
                pyspark.sql.functions.col("intermediate_mp_term_options"),
            ).otherwise(pyspark.sql.functions.col("intermediate_mp_term_name")),
        )

        allele_df = allele_df.select(
            ["allele_symbol"] + list(ALLELE_STATS_MAP.values())
        ).dropDuplicates()

        open_stats_df = self.map_to_stats(
            open_stats_df, allele_df, ["allele_symbol"], ALLELE_STATS_MAP, "allele"
        )

        open_stats_df = open_stats_df.withColumn(
            "sex", pyspark.sql.functions.col("mp_term_sex")
        )
        open_stats_df = open_stats_df.withColumn(
            "phenotype_sex",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("phenotype_sex").isNull(),
                pyspark.sql.functions.lit(None),
            )
            .when(
                pyspark.sql.functions.col("phenotype_sex").contains(
                    "Both sexes included"
                ),
                pyspark.sql.functions.array(
                    pyspark.sql.functions.lit("male"),
                    pyspark.sql.functions.lit("female"),
                ),
            )
            .otherwise(
                pyspark.sql.functions.array(
                    pyspark.sql.functions.lower(
                        pyspark.sql.functions.regexp_extract(
                            pyspark.sql.functions.col("phenotype_sex"),
                            r"Only one sex included in the analysis; (.*)\[.*\]",
                            1,
                        )
                    )
                )
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "phenotype_sex",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("phenotype_sex").isNull()
                & pyspark.sql.functions.col("mp_term_sex").isNotNull(),
                pyspark.sql.functions.when(
                    pyspark.sql.functions.col("mp_term_sex") == "not_considered",
                    pyspark.sql.functions.array(
                        pyspark.sql.functions.lit("male"),
                        pyspark.sql.functions.lit("female"),
                    ),
                ).otherwise(
                    pyspark.sql.functions.array(
                        pyspark.sql.functions.col("mp_term_sex")
                    )
                ),
            ).otherwise(pyspark.sql.functions.col("phenotype_sex")),
        )
        open_stats_df = open_stats_df.withColumn(
            "zygosity",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("zygosity") == "homozygous",
                pyspark.sql.functions.lit("homozygote"),
            ).otherwise(pyspark.sql.functions.col("zygosity")),
        )

        open_stats_df = self.map_ontology_prefix(open_stats_df, "MA:", "anatomy_")
        open_stats_df = self.map_ontology_prefix(open_stats_df, "EMAP:", "anatomy_")
        open_stats_df = self.map_ontology_prefix(open_stats_df, "EMAPA:", "anatomy_")
        open_stats_df = open_stats_df.withColumn(
            "significant",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term_id").isNotNull(),
                pyspark.sql.functions.lit(True),
            ).otherwise(pyspark.sql.functions.lit(False)),
        )
        open_stats_df = open_stats_df.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("statistical_method").startswith(
                    "Reference Range"
                ),
                pyspark.sql.functions.least(
                    pyspark.sql.functions.col("female_pvalue_low_normal_vs_high"),
                    pyspark.sql.functions.col("female_pvalue_low_vs_normal_high"),
                    pyspark.sql.functions.col("male_pvalue_low_normal_vs_high"),
                    pyspark.sql.functions.col("male_pvalue_low_vs_normal_high"),
                    pyspark.sql.functions.col("genotype_pvalue_low_normal_vs_high"),
                    pyspark.sql.functions.col("genotype_pvalue_low_vs_normal_high"),
                ),
            ).otherwise(pyspark.sql.functions.col("p_value")),
        )
        open_stats_df = open_stats_df.withColumn(
            "effect_size",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("statistical_method").startswith(
                    "Reference Range"
                ),
                pyspark.sql.functions.greatest(
                    pyspark.sql.functions.col("female_effect_size_low_normal_vs_high"),
                    pyspark.sql.functions.col("female_effect_size_low_vs_normal_high"),
                    pyspark.sql.functions.col("male_effect_size_low_normal_vs_high"),
                    pyspark.sql.functions.col("male_effect_size_low_vs_normal_high"),
                    pyspark.sql.functions.col(
                        "genotype_effect_size_low_normal_vs_high"
                    ),
                    pyspark.sql.functions.col(
                        "genotype_effect_size_low_vs_normal_high"
                    ),
                ),
            ).otherwise(pyspark.sql.functions.col("effect_size")),
        )
        open_stats_df = self.map_ontology_prefix(open_stats_df, "MPATH:", "mpath_")
        mpath_metadata_df = mpath_metadata_df.select(
            pyspark.sql.functions.col("acc").alias("mpath_term_id"),
            pyspark.sql.functions.col("name").alias("mpath_metadata_term_name"),
        ).distinct()
        open_stats_df = open_stats_df.join(
            mpath_metadata_df, "mpath_term_id", "left_outer"
        )
        open_stats_df = open_stats_df.withColumn(
            "mpath_term_name", pyspark.sql.functions.col("mpath_metadata_term_name")
        )
        open_stats_df = open_stats_df.withColumn(
            "metadata",
            pyspark.sql.functions.expr(
                "transform(metadata, metadata_values -> concat_ws('|', metadata_values))"
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "significant",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("data_type") == "time_series",
                pyspark.sql.functions.lit(False),
            ).otherwise(pyspark.sql.functions.col("significant")),
        )
        open_stats_df = open_stats_df.withColumn(
            "status",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("data_type") == "time_series",
                pyspark.sql.functions.lit("NotProcessed"),
            ).otherwise(pyspark.sql.functions.col("status")),
        )

        open_stats_df = open_stats_df.withColumn(
            "procedure_stable_id_str",
            pyspark.sql.functions.concat_ws(",", "procedure_stable_id"),
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col(col_name).isNotNull(),
                pyspark.sql.functions.col(col_name),
            ).otherwise(pyspark.sql.functions.lit(""))
            for col_name in identifying_cols
        ]
        open_stats_df = open_stats_df.withColumn(
            "doc_id",
            pyspark.sql.functions.md5(pyspark.sql.functions.concat(*identifying_cols)),
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("procedure_group").rlike(
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
                & (pyspark.sql.functions.col("data_type") == "categorical"),
                pyspark.sql.functions.lit("embryo"),
            ).otherwise(pyspark.sql.functions.col("data_type")),
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
        compress_and_encode = pyspark.sql.functions.udf(
            self._compress_and_encode, StringType()
        )
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
                pyspark.sql.functions.when(
                    (
                        pyspark.sql.functions.col("data_type").isin(
                            ["unidimensional", "time_series", "categorical"]
                        )
                        & (pyspark.sql.functions.col(col_name).isNotNull())
                    ),
                    pyspark.sql.functions.col(col_name),
                ).otherwise(pyspark.sql.functions.lit(None)),
            )
        open_stats_df = open_stats_df.withColumn(
            "body_weight",
            pyspark.sql.functions.when(
                (
                    pyspark.sql.functions.col("data_type").isin(
                        ["unidimensional", "time_series", "categorical"]
                    )
                    & (pyspark.sql.functions.col("body_weight").isNotNull())
                ),
                pyspark.sql.functions.col("body_weight"),
            ).otherwise(
                pyspark.sql.functions.expr(
                    "transform(external_sample_id, sample_id -> NULL)"
                )
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "data_point",
            pyspark.sql.functions.when(
                (
                    pyspark.sql.functions.col("data_type").isin(
                        ["unidimensional", "time_series"]
                    )
                )
                & (pyspark.sql.functions.col("observations_response").isNotNull()),
                pyspark.sql.functions.col("observations_response"),
            ).otherwise(
                pyspark.sql.functions.expr(
                    "transform(external_sample_id, sample_id -> NULL)"
                )
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "category",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("data_type") == "categorical")
                & (pyspark.sql.functions.col("observations_response").isNotNull()),
                pyspark.sql.functions.col("observations_response"),
            ).otherwise(
                pyspark.sql.functions.expr(
                    "transform(external_sample_id, sample_id -> NULL)"
                )
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "time_point",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("data_type") == "time_series")
                & (pyspark.sql.functions.col("time_point").isNotNull()),
                pyspark.sql.functions.col("time_point"),
            ).otherwise(
                pyspark.sql.functions.expr(
                    "transform(external_sample_id, sample_id -> NULL)"
                )
            ),
        )
        open_stats_df = open_stats_df.withColumn(
            "discrete_point",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("data_type") == "time_series")
                & (pyspark.sql.functions.col("discrete_point").isNotNull()),
                pyspark.sql.functions.col("discrete_point"),
            ).otherwise(
                pyspark.sql.functions.expr(
                    "transform(external_sample_id, sample_id -> NULL)"
                )
            ),
        )

        date_of_birth_udf = (
            lambda specimen_list: [
                specimen_dob_dict[specimen] if specimen in specimen_dob_dict else None
                for specimen in specimen_list
            ]
            if specimen_list is not None
            else []
        )
        date_of_birth_udf = pyspark.sql.functions.udf(
            date_of_birth_udf, ArrayType(StringType())
        )
        open_stats_df = open_stats_df.withColumn(
            "date_of_birth", date_of_birth_udf("external_sample_id")
        )
        if extract_windowed_data:
            open_stats_df = open_stats_df.withColumn(
                "window_weight",
                pyspark.sql.functions.when(
                    (pyspark.sql.functions.col("data_type") == "unidimensional")
                    & (pyspark.sql.functions.col("window_weight").isNotNull()),
                    pyspark.sql.functions.col("window_weight"),
                ).otherwise(
                    pyspark.sql.functions.expr(
                        "transform(external_sample_id, sample_id -> NULL)"
                    )
                ),
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
            "raw_data", pyspark.sql.functions.arrays_zip(*raw_data_cols)
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
        open_stats_df = open_stats_df.withColumn(
            "raw_data", pyspark.sql.functions.to_json("raw_data")
        )
        for idx, col_name in enumerate(raw_data_cols):
            open_stats_df = open_stats_df.withColumn(
                "raw_data",
                pyspark.sql.functions.regexp_replace(
                    "raw_data", f'"{idx}":', f'"{col_name}":'
                ),
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
                pyspark.sql.functions.when(
                    pyspark.sql.functions.col(col_name).isNull(),
                    pyspark.sql.functions.when(
                        pyspark.sql.functions.col("mp_term_id").startswith(term_prefix),
                        pyspark.sql.functions.col(mp_col_name),
                    ).otherwise(pyspark.sql.functions.lit(None)),
                ).otherwise(pyspark.sql.functions.col(col_name)),
            )
        mapped_id = field_prefix + "term_id"
        for col_name in mapped_columns:
            mp_col_name = col_name.replace(field_prefix, "mp_")
            open_stats_df = open_stats_df.withColumn(
                mp_col_name,
                pyspark.sql.functions.when(
                    pyspark.sql.functions.col(mapped_id).isNotNull(),
                    pyspark.sql.functions.lit(None),
                ).otherwise(pyspark.sql.functions.col(mp_col_name)),
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
            open_stats_df = open_stats_df.join(metadata_df, join_columns)
        else:
            open_stats_df = open_stats_df.join(metadata_df, join_columns, "left_outer")
        for column_name, source_column in source_stats_map.items():
            open_stats_df = open_stats_df.withColumn(
                column_name, pyspark.sql.functions.col(f"{source_name}_{source_column}")
            )
        for source_column in source_stats_map.values():
            open_stats_df = open_stats_df.drop(f"{source_name}_{source_column}")
        return open_stats_df

    def standardize_threei_schema(self, threei_df: DataFrame):
        threei_df = threei_df.dropDuplicates()
        for col_name, threei_column in THREEI_STATS_MAP.items():
            threei_df = threei_df.withColumnRenamed(threei_column, col_name)
        threei_df = threei_df.withColumn(
            "resource_name", pyspark.sql.functions.lit("3i")
        )
        threei_df = threei_df.withColumn(
            "procedure_stable_id",
            pyspark.sql.functions.array(
                pyspark.sql.functions.col("procedure_stable_id")
            ),
        )
        threei_df = threei_df.withColumn(
            "sex",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("sex") == "both",
                pyspark.sql.functions.lit("not_considered"),
            ).otherwise(pyspark.sql.functions.lower(pyspark.sql.functions.col("sex"))),
        )
        threei_df = threei_df.withColumn(
            "zygosity",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("zygosity") == "Hom",
                pyspark.sql.functions.lit("homozygote"),
            )
            .when(
                pyspark.sql.functions.col("zygosity") == "Hemi",
                pyspark.sql.functions.lit("hemizygote"),
            )
            .otherwise(pyspark.sql.functions.lit("heterozygote")),
        )
        threei_df = threei_df.withColumn(
            "term_id", pyspark.sql.functions.regexp_replace("mp_id", r"\[", "")
        )

        threei_df = threei_df.withColumn(
            "threei_collapsed_mp_term",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("mp_id") != "NA")
                & (pyspark.sql.functions.col("mp_id").isNotNull()),
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit(None).cast(StringType()).alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    "sex",
                    pyspark.sql.functions.col("term_id").alias("term_id"),
                ),
            ).otherwise(pyspark.sql.functions.lit(None)),
        )
        threei_df = threei_df.withColumn(
            "threei_p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("classification_tag") == "Significant",
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.lit(1.0)),
        )
        threei_df = threei_df.withColumn(
            "threei_genotype_effect_p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("classification_tag") == "Significant",
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.lit(1.0)),
        )
        threei_df = threei_df.withColumn(
            "threei_genotype_effect_parameter_estimate",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("classification_tag") == "Significant",
                pyspark.sql.functions.lit(1.0),
            ).otherwise(pyspark.sql.functions.lit(0.0)),
        )
        threei_df = threei_df.withColumn(
            "threei_significant",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("classification_tag") == "Significant",
                pyspark.sql.functions.lit(True),
            ).otherwise(pyspark.sql.functions.lit(False)),
        )
        threei_df = threei_df.withColumn(
            "threei_status",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("classification_tag").isin(
                    ["Significant", "Not Significant"]
                ),
                pyspark.sql.functions.lit("Successful"),
            ).otherwise(pyspark.sql.functions.lit("NotProcessed")),
        )
        threei_df = threei_df.withColumn(
            "threei_statistical_method", pyspark.sql.functions.lit("Supplied as data")
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col(
                    "threei_genotype_effect_parameter_estimate"
                ).isNotNull(),
                pyspark.sql.functions.col("threei_genotype_effect_parameter_estimate"),
            ).otherwise(
                pyspark.sql.functions.col("genotype_effect_parameter_estimate")
            ),
        )
        open_stats_df = open_stats_df.drop("threei_genotype_effect_parameter_estimate")

        open_stats_df = open_stats_df.withColumn(
            "significant",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("threei_significant").isNotNull(),
                pyspark.sql.functions.col("threei_significant"),
            ).otherwise(pyspark.sql.functions.col("significant")),
        )
        open_stats_df = open_stats_df.drop("threei_significant")

        open_stats_df = open_stats_df.withColumn(
            "status",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("threei_status").isNotNull(),
                pyspark.sql.functions.col("threei_status"),
            ).otherwise(pyspark.sql.functions.col("status")),
        )
        open_stats_df = open_stats_df.drop("threei_status")
        open_stats_df = open_stats_df.withColumn(
            "statistical_method",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("threei_statistical_method").isNotNull(),
                pyspark.sql.functions.col("threei_statistical_method"),
            ).otherwise(pyspark.sql.functions.col("statistical_method")),
        )
        open_stats_df = open_stats_df.drop("threei_statistical_method")

        open_stats_df = open_stats_df.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("threei_p_value").isNotNull(),
                pyspark.sql.functions.col("threei_p_value"),
            ).otherwise(pyspark.sql.functions.col("p_value")),
        )
        open_stats_df = open_stats_df.drop("threei_p_value")

        open_stats_df = open_stats_df.withColumn(
            "genotype_effect_p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("threei_genotype_effect_p_value").isNotNull(),
                pyspark.sql.functions.col("threei_genotype_effect_p_value"),
            ).otherwise(pyspark.sql.functions.col("genotype_effect_p_value")),
        )
        open_stats_df = open_stats_df.drop("threei_genotype_effect_p_value")
        return open_stats_df

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
        pwg_df = pwg_df.withColumn("resource_name", pyspark.sql.functions.lit("pwg"))
        pwg_df = pwg_df.withColumn(
            "procedure_stable_id",
            pyspark.sql.functions.array(
                pyspark.sql.functions.col("procedure_stable_id")
            ),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_sex",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("pwg_sex") == "both",
                pyspark.sql.functions.lit("not_considered"),
            ).otherwise(
                pyspark.sql.functions.lower(pyspark.sql.functions.col("pwg_sex"))
            ),
        )
        pwg_df = pwg_df.withColumn(
            "zygosity",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("zygosity") == "homozygous",
                pyspark.sql.functions.lit("homozygote"),
            )
            .when(
                pyspark.sql.functions.col("zygosity") == "hemizygous",
                pyspark.sql.functions.lit("hemizygote"),
            )
            .otherwise(pyspark.sql.functions.lit("heterozygote")),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_collapsed_mp_term",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("pwg_mp_term") != "NA")
                & (pyspark.sql.functions.col("pwg_mp_term").isNotNull()),
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit(None).cast(StringType()).alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    "pwg_sex",
                    pyspark.sql.functions.col("pwg_mp_term").alias("mp_term"),
                ),
            ).otherwise(pyspark.sql.functions.lit(None)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_significant",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("pwg_significance") == "significant",
                pyspark.sql.functions.lit(True),
            ).otherwise(pyspark.sql.functions.lit(False)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_batch_significant",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("pwg_batch_significant") == "TRUE",
                pyspark.sql.functions.lit(True),
            ).otherwise(pyspark.sql.functions.lit(False)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_variance_significant",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("pwg_variance_significant") == "TRUE",
                pyspark.sql.functions.lit(True),
            ).otherwise(pyspark.sql.functions.lit(False)),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_classification_tag",
            pyspark.sql.functions.concat(
                "pwg_classification_tag", "pwg_sexual_dimorphism"
            ),
        )
        pwg_df = pwg_df.withColumn(
            "pwg_status", pyspark.sql.functions.lit("Successful")
        )

        return pwg_df

    def map_pwg(self, open_stats_df):
        pwg_columns = [
            "significant",
            "status",
            "statistical_method",
            "p_value",
            "genotype_effect_p_value",
            "batch_significant",
            "variance_significant",
            "genotype_effect_size",
            "male_ko_effect_p_value",
            "female_ko_effect_p_value",
            "female_percentage_change",
            "male_percentage_change",
            "sex",
        ]
        for col_name in pwg_columns:
            open_stats_df = open_stats_df.withColumn(
                col_name,
                pyspark.sql.functions.when(
                    pyspark.sql.functions.col(f"pwg_{col_name}").isNotNull(),
                    pyspark.sql.functions.col(f"pwg_{col_name}"),
                ).otherwise(pyspark.sql.functions.col(col_name)),
            )
            open_stats_df = open_stats_df.drop(f"pwg_{col_name}")
        open_stats_df = open_stats_df.withColumn(
            "status",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("status").isNull()
                & (pyspark.sql.functions.col("resource_name") == "pwg"),
                pyspark.sql.functions.lit("NotProcessed"),
            ).otherwise(pyspark.sql.functions.col("status")),
        )
        return open_stats_df

    def _fertility_stats_results(
        self, observations_df: DataFrame, pipeline_df: DataFrame
    ):
        fertility_condition = pyspark.sql.functions.col("parameter_stable_id").isin(
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
            pyspark.sql.functions.lower(pyspark.sql.functions.col("category")),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "data_type", pyspark.sql.functions.lit("line")
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "effect_size", pyspark.sql.functions.lit(1.0)
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "statistical_method", pyspark.sql.functions.lit("Supplied as data")
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "status", pyspark.sql.functions.lit("Successful")
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "p_value", pyspark.sql.functions.lit(0.0)
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("category") == "infertile",
                pyspark.sql.functions.when(
                    pyspark.sql.functions.col("parameter_stable_id")
                    == "IMPC_FER_001_001",
                    pyspark.sql.functions.lit("MP:0001925"),
                ).otherwise(pyspark.sql.functions.lit("MP:0001926")),
            ).otherwise(pyspark.sql.functions.lit(None)),
        )

        fertility_stats_results = fertility_stats_results.groupBy(
            required_stats_columns
            + ["data_type", "status", "effect_size", "statistical_method", "p_value"]
        ).agg(
            pyspark.sql.functions.collect_set("category").alias("categories"),
            pyspark.sql.functions.collect_set(
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit("ABNORMAL")
                    .cast(StringType())
                    .alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    "sex",
                    pyspark.sql.functions.col("termAcc").alias("term_id"),
                )
            ).alias("mp_term"),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.size(pyspark.sql.functions.col("mp_term.term_id"))
                == 0,
                pyspark.sql.functions.lit(None),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(1.0),
            ).otherwise(pyspark.sql.functions.col("p_value")),
        )
        fertility_stats_results = fertility_stats_results.withColumn(
            "effect_size",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.col("effect_size")),
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("optionText").isNull(),
                pyspark.sql.functions.col("selectionOutcome"),
            ).otherwise(pyspark.sql.functions.col("optionText")),
        )

        mp_chooser = mp_chooser.withColumn(
            "category",
            pyspark.sql.functions.lower(pyspark.sql.functions.col("category")),
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
                pyspark.sql.functions.col("procedure_group").rlike(
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
                & (
                    pyspark.sql.functions.col("biological_sample_group")
                    == "experimental"
                )
                & (pyspark.sql.functions.col("observation_type") == "categorical")
            )
            .withColumnRenamed("gene_accession_id", "marker_accession_id")
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )
        embryo_control_data = observations_df.where(
            pyspark.sql.functions.col("procedure_group").rlike(
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
            & (pyspark.sql.functions.col("biological_sample_group") == "control")
            & (pyspark.sql.functions.col("observation_type") == "categorical")
            & (pyspark.sql.functions.col("category").isin(["yes", "no"]))
        )
        embryo_control_data = embryo_control_data.select(
            "procedure_stable_id", "parameter_stable_id", "category"
        )
        embryo_control_data = embryo_control_data.groupBy(
            "procedure_stable_id", "parameter_stable_id", "category"
        ).count()
        window = Window.partitionBy(
            "procedure_stable_id", "parameter_stable_id"
        ).orderBy(pyspark.sql.functions.col("count").desc())
        embryo_normal_data = embryo_control_data.select(
            "procedure_stable_id",
            "parameter_stable_id",
            pyspark.sql.functions.first("category")
            .over(window)
            .alias("normal_category"),
        ).distinct()
        embryo_stats_results = embryo_stats_results.withColumn(
            "category",
            pyspark.sql.functions.lower(pyspark.sql.functions.col("category")),
        )
        embryo_stats_results = embryo_stats_results.join(
            embryo_normal_data,
            ["procedure_stable_id", "parameter_stable_id"],
            "left_outer",
        )

        embryo_stats_results = embryo_stats_results.withColumn(
            "category",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("category").isin(["yes", "no"])
                & (
                    pyspark.sql.functions.col("category")
                    != pyspark.sql.functions.col("normal_category")
                ),
                pyspark.sql.functions.lit("abnormal"),
            ).otherwise(pyspark.sql.functions.col("category")),
        )

        embryo_stats_results = embryo_stats_results.drop("normal_category")

        embryo_stats_results = embryo_stats_results.withColumn(
            "data_type", pyspark.sql.functions.lit("categorical")
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "status", pyspark.sql.functions.lit("Successful")
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "statistical_method", pyspark.sql.functions.lit("Supplied as data")
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
            pyspark.sql.functions.collect_set("sex").alias("sex"),
            pyspark.sql.functions.collect_set("category").alias("categories"),
            pyspark.sql.functions.collect_set(
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit("ABNORMAL")
                    .cast(StringType())
                    .alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    pyspark.sql.functions.col("sex"),
                    pyspark.sql.functions.col("termAcc").alias("term_id"),
                ),
            ).alias("mp_term"),
            pyspark.sql.functions.collect_list(
                pyspark.sql.functions.col("termAcc")
            ).alias("abnormalCalls"),
        )

        embryo_stats_results = embryo_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "abnormalCallsCount",
            pyspark.sql.functions.size(
                pyspark.sql.functions.expr(
                    "filter(abnormalCalls, mp -> mp IS NOT NULL)"
                )
            ),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                (
                    (pyspark.sql.functions.col("zygosity") == "homozygote")
                    | (pyspark.sql.functions.col("zygosity") == "hemizygote")
                )
                & (pyspark.sql.functions.col("abnormalCallsCount") >= 2),
                pyspark.sql.functions.col("mp_term"),
            )
            .when(
                (pyspark.sql.functions.col("zygosity") == "heterozygote")
                & (pyspark.sql.functions.col("abnormalCallsCount") >= 4),
                pyspark.sql.functions.col("mp_term"),
            )
            .otherwise(pyspark.sql.functions.lit(None)),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(1.0),
            ).otherwise(pyspark.sql.functions.lit(0.0)),
        )
        embryo_stats_results = embryo_stats_results.withColumn(
            "effect_size",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.lit(1.0)),
        )
        embryo_stats_results = embryo_stats_results.groupBy(
            *[
                col_name
                for col_name in embryo_stats_results.columns
                if col_name not in ["mp_term", "p_value", "effect_size"]
            ]
        ).agg(
            pyspark.sql.functions.flatten(
                pyspark.sql.functions.collect_set("mp_term")
            ).alias("mp_term"),
            pyspark.sql.functions.min("p_value").alias("p_value"),
            pyspark.sql.functions.max("effect_size").alias("effect_size"),
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
            pyspark.sql.functions.lower(pyspark.sql.functions.col("category")),
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
                pyspark.sql.functions.col("parameter_stable_id").isin(
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
            pyspark.sql.functions.lower(pyspark.sql.functions.col("category")),
        )

        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "data_type", pyspark.sql.functions.lit("embryo")
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "status", pyspark.sql.functions.lit("Successful")
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "statistical_method", pyspark.sql.functions.lit("Supplied as data")
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
            pyspark.sql.functions.collect_set("category").alias("categories"),
            pyspark.sql.functions.collect_set(
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit("ABNORMAL")
                    .cast(StringType())
                    .alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    pyspark.sql.functions.lit("not_considered")
                    .cast(StringType())
                    .alias("sex"),
                    pyspark.sql.functions.col("termAcc").alias("term_id"),
                )
            ).alias("mp_term"),
        )

        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.size(pyspark.sql.functions.col("mp_term.term_id"))
                == 0,
                pyspark.sql.functions.lit(None),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(1.0),
            ).otherwise(pyspark.sql.functions.lit(0.0)),
        )
        embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
            "effect_size",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.lit(1.0)),
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
            pyspark.sql.functions.lower(pyspark.sql.functions.col("category")),
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
                    (
                        pyspark.sql.functions.col("parameter_stable_id")
                        == "IMPC_VIA_001_001"
                    )
                    & (
                        pyspark.sql.functions.col("procedure_stable_id")
                        == "IMPC_VIA_001"
                    )
                )
                | (
                    (
                        pyspark.sql.functions.col("parameter_stable_id").isin(
                            [
                                "IMPC_VIA_063_001",
                                "IMPC_VIA_064_001",
                                "IMPC_VIA_065_001",
                                "IMPC_VIA_066_001",
                                "IMPC_VIA_067_001",
                            ]
                        )
                    )
                    & (
                        pyspark.sql.functions.col("procedure_stable_id")
                        == "IMPC_VIA_002"
                    )
                )
            )
            .withColumnRenamed("gene_accession_id", "marker_accession_id")
            .withColumnRenamed("gene_symbol", "marker_symbol")
            .select(required_stats_columns)
        )

        viability_stats_results = viability_stats_results.withColumn(
            "category",
            pyspark.sql.functions.lower(pyspark.sql.functions.col("category")),
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("procedure_stable_id") == "IMPC_VIA_002",
                pyspark.sql.functions.from_json(
                    pyspark.sql.functions.col("text_value"), json_outcome_schema
                ),
            ).otherwise(pyspark.sql.functions.lit(None)),
        )

        viability_p_values = observations_df.where(
            pyspark.sql.functions.col("parameter_stable_id") == "IMPC_VIA_032_001"
        ).select(
            "procedure_stable_id",
            "colony_id",
            pyspark.sql.functions.col("data_point").alias("p_value"),
        )

        viability_male_mutants = observations_df.where(
            pyspark.sql.functions.col("parameter_stable_id") == "IMPC_VIA_010_001"
        ).select(
            "procedure_stable_id",
            "colony_id",
            pyspark.sql.functions.col("data_point").alias("male_mutants"),
        )

        viability_female_mutants = observations_df.where(
            pyspark.sql.functions.col("parameter_stable_id") == "IMPC_VIA_014_001"
        ).select(
            "procedure_stable_id",
            "colony_id",
            pyspark.sql.functions.col("data_point").alias("female_mutants"),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "data_type", pyspark.sql.functions.lit("line")
        )
        viability_stats_results = viability_stats_results.withColumn(
            "effect_size", pyspark.sql.functions.lit(1.0)
        )
        viability_stats_results = viability_stats_results.join(
            viability_p_values, ["colony_id", "procedure_stable_id"], "left_outer"
        )
        viability_stats_results = viability_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("procedure_stable_id") == "IMPC_VIA_002",
                pyspark.sql.functions.col("viability_outcome.P"),
            ).otherwise(pyspark.sql.functions.col("p_value")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("p_value").isNull()
                & ~pyspark.sql.functions.col("category").contains("Viable"),
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.col("p_value")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "male_controls", pyspark.sql.functions.lit(None)
        )
        viability_stats_results = viability_stats_results.join(
            viability_male_mutants, ["colony_id", "procedure_stable_id"], "left_outer"
        )
        viability_stats_results = viability_stats_results.withColumn(
            "male_mutants",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("procedure_stable_id") == "IMPC_VIA_002")
                & (pyspark.sql.functions.col("parameter_name").contains(" males ")),
                pyspark.sql.functions.col("viability_outcome.n"),
            ).otherwise(pyspark.sql.functions.col("male_mutants")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "female_controls", pyspark.sql.functions.lit(None)
        )
        viability_stats_results = viability_stats_results.join(
            viability_female_mutants, ["colony_id", "procedure_stable_id"], "left_outer"
        )

        viability_stats_results = viability_stats_results.withColumn(
            "female_mutants",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("procedure_stable_id") == "IMPC_VIA_002")
                & (pyspark.sql.functions.col("parameter_name").contains(" females ")),
                pyspark.sql.functions.col("viability_outcome.n"),
            ).otherwise(pyspark.sql.functions.col("female_mutants")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "statistical_method",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("procedure_stable_id") == "IMPC_VIA_002",
                pyspark.sql.functions.lit("Binomial distribution probability"),
            ).otherwise(pyspark.sql.functions.lit("Supplied as data")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "status", pyspark.sql.functions.lit("Successful")
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
            pyspark.sql.functions.collect_set("category").alias("categories"),
            pyspark.sql.functions.collect_set(
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit("ABNORMAL")
                    .cast(StringType())
                    .alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    pyspark.sql.functions.lit("not_considered")
                    .cast(StringType())
                    .alias("sex"),
                    pyspark.sql.functions.col("termAcc").alias("term_id"),
                )
            ).alias("mp_term"),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                (pyspark.sql.functions.col("procedure_stable_id") == "IMPC_VIA_002"),
                pyspark.sql.functions.array(
                    pyspark.sql.functions.struct(
                        pyspark.sql.functions.lit("ABNORMAL")
                        .cast(StringType())
                        .alias("event"),
                        pyspark.sql.functions.lit(None)
                        .cast(StringType())
                        .alias("otherPossibilities"),
                        pyspark.sql.functions.when(
                            pyspark.sql.functions.col("parameter_name").contains(
                                " males "
                            ),
                            pyspark.sql.functions.lit("male"),
                        )
                        .when(
                            pyspark.sql.functions.col("parameter_name").contains(
                                " females "
                            ),
                            pyspark.sql.functions.lit("female"),
                        )
                        .otherwise(pyspark.sql.functions.lit("not_considered"))
                        .cast(StringType())
                        .alias("sex"),
                        pyspark.sql.functions.when(
                            pyspark.sql.functions.col(
                                "viability_outcome.outcome"
                            ).contains("subviable"),
                            pyspark.sql.functions.lit("MP:0011110"),
                        )
                        .when(
                            pyspark.sql.functions.col(
                                "viability_outcome.outcome"
                            ).contains("lethal"),
                            pyspark.sql.functions.lit("MP:0011100"),
                        )
                        .otherwise(pyspark.sql.functions.lit(None).cast(StringType()))
                        .cast(StringType())
                        .alias("term_id"),
                    )
                ),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )

        viability_stats_results = viability_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.size(pyspark.sql.functions.col("mp_term.term_id"))
                == 0,
                pyspark.sql.functions.lit(None),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(1.0),
            ).otherwise(pyspark.sql.functions.col("p_value")),
        )
        viability_stats_results = viability_stats_results.withColumn(
            "effect_size",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.col("effect_size")),
        )
        return viability_stats_results

    def _histopathology_stats_results(self, observations_df: DataFrame):
        histopathology_stats_results = observations_df.where(
            pyspark.sql.functions.expr(
                "exists(sub_term_id, term -> term LIKE 'MPATH:%')"
            )
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "term_set",
            pyspark.sql.functions.array_sort(
                pyspark.sql.functions.array_distinct("sub_term_name")
            ),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "is_normal",
            (pyspark.sql.functions.size("term_set") == 1)
            & pyspark.sql.functions.expr("exists(term_set, term -> term = 'normal')"),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "tissue_name",
            pyspark.sql.functions.regexp_extract("parameter_name", "(.*)( - .*)", 1),
        )

        histopathology_significance_scores = observations_df.where(
            pyspark.sql.functions.col("parameter_name").endswith("Significance score")
        ).where(pyspark.sql.functions.col("biological_sample_group") == "experimental")

        histopathology_significance_scores = (
            histopathology_significance_scores.withColumn(
                "tissue_name",
                pyspark.sql.functions.regexp_extract(
                    "parameter_name", "(.*)( - .*)", 1
                ),
            )
        )

        histopathology_significance_scores = (
            histopathology_significance_scores.withColumn(
                "significance",
                pyspark.sql.functions.when(
                    pyspark.sql.functions.col("category") == "1",
                    pyspark.sql.functions.lit(True),
                ).otherwise(pyspark.sql.functions.lit(False)),
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("significance")
                & ~pyspark.sql.functions.col("is_normal"),
                pyspark.sql.functions.lit(True),
            ).otherwise(pyspark.sql.functions.lit(False)),
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
            pyspark.sql.functions.expr("filter(sub_term_id, mp -> mp LIKE 'MPATH:%')"),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "term_id", pyspark.sql.functions.explode_outer("sub_term_id")
        )

        histopathology_stats_results = histopathology_stats_results.groupBy(
            *[
                col_name
                for col_name in required_stats_columns
                if col_name not in ["sex", "term_id"]
            ]
        ).agg(
            pyspark.sql.functions.collect_set(
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit("ABNORMAL")
                    .cast(StringType())
                    .alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    pyspark.sql.functions.col("sex"),
                    pyspark.sql.functions.col("term_id"),
                )
            ).alias("mp_term")
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("significance").isNull()
                | ~pyspark.sql.functions.col("significance"),
                pyspark.sql.functions.lit(None),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )

        histopathology_stats_results = histopathology_stats_results.withColumn(
            "statistical_method", pyspark.sql.functions.lit("Supplied as data")
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "status", pyspark.sql.functions.lit("Successful")
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("significance").isNull()
                | ~pyspark.sql.functions.col("significance"),
                pyspark.sql.functions.lit(1.0),
            ).otherwise(pyspark.sql.functions.lit(0.0)),
        )
        histopathology_stats_results = histopathology_stats_results.withColumn(
            "effect_size",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("significance").isNull()
                | ~pyspark.sql.functions.col("significance"),
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.lit(1.0)),
        )

        histopathology_stats_results = histopathology_stats_results.withColumn(
            "data_type", pyspark.sql.functions.lit("histopathology")
        )
        histopathology_stats_results = histopathology_stats_results.drop("significance")
        histopathology_stats_results = histopathology_stats_results.dropDuplicates()

        return histopathology_stats_results

    def _gross_pathology_stats_results(self, observations_df: DataFrame):
        gross_pathology_stats_results = observations_df.where(
            (pyspark.sql.functions.col("biological_sample_group") != "control")
            & pyspark.sql.functions.col("parameter_stable_id").like("%PAT%")
            & (
                pyspark.sql.functions.expr(
                    "exists(sub_term_id, term -> term LIKE 'MP:%')"
                )
            )
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
            pyspark.sql.functions.expr("filter(sub_term_id, mp -> mp LIKE 'MP:%')"),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "term_id", pyspark.sql.functions.explode_outer("sub_term_id")
        )

        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "term_id",
            pyspark.sql.functions.when(
                pyspark.sql.functions.expr(
                    "exists(sub_term_name, term -> term = 'no abnormal phenotype detected')"
                )
                | pyspark.sql.functions.expr(
                    "exists(sub_term_name, term -> term = 'normal')"
                ),
                pyspark.sql.functions.lit(None),
            ).otherwise(pyspark.sql.functions.col("term_id")),
        )

        gross_pathology_stats_results = gross_pathology_stats_results.groupBy(
            *[
                col_name
                for col_name in required_stats_columns
                if col_name not in ["sex", "sub_term_id", "sub_term_name"]
            ]
        ).agg(
            pyspark.sql.functions.collect_set(
                pyspark.sql.functions.struct(
                    pyspark.sql.functions.lit("ABNORMAL")
                    .cast(StringType())
                    .alias("event"),
                    pyspark.sql.functions.lit(None)
                    .cast(StringType())
                    .alias("otherPossibilities"),
                    pyspark.sql.functions.col("sex"),
                    pyspark.sql.functions.col("term_id"),
                )
            ).alias("mp_term")
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)"),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.size(pyspark.sql.functions.col("mp_term.term_id"))
                == 0,
                pyspark.sql.functions.lit(None),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )

        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "statistical_method", pyspark.sql.functions.lit("Supplied as data")
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "status", pyspark.sql.functions.lit("Successful")
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "p_value",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(1.0),
            ).otherwise(pyspark.sql.functions.lit(0.0)),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "effect_size",
            pyspark.sql.functions.when(
                pyspark.sql.functions.col("mp_term").isNull(),
                pyspark.sql.functions.lit(0.0),
            ).otherwise(pyspark.sql.functions.lit(1.0)),
        )
        gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
            "data_type", pyspark.sql.functions.lit("adult-gross-path")
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
            pyspark.sql.functions.when(
                pyspark.sql.functions.array_contains(
                    pyspark.sql.functions.col("procedure_stable_id"), "IMPC_VIA_002"
                ),
                pyspark.sql.functions.array(pyspark.sql.functions.lit("MP:0010768")),
            ).otherwise(pyspark.sql.functions.col("top_level_mp_id")),
        )
        pipeline_core_df = pipeline_core_df.withColumn(
            "top_level_mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.array_contains(
                    pyspark.sql.functions.col("procedure_stable_id"), "IMPC_VIA_002"
                ),
                pyspark.sql.functions.array(
                    pyspark.sql.functions.lit("mortality/aging")
                ),
            ).otherwise(pyspark.sql.functions.col("top_level_mp_term")),
        )
        pipeline_core_df = pipeline_core_df.withColumn(
            "mp_id",
            pyspark.sql.functions.when(
                pyspark.sql.functions.array_contains(
                    pyspark.sql.functions.col("procedure_stable_id"), "IMPC_VIA_002"
                ),
                pyspark.sql.functions.array(
                    pyspark.sql.functions.lit("MP:0011100"),
                    pyspark.sql.functions.lit("MP:0011110"),
                ),
            ).otherwise(pyspark.sql.functions.col("mp_id")),
        )
        pipeline_core_df = pipeline_core_df.withColumn(
            "mp_term",
            pyspark.sql.functions.when(
                pyspark.sql.functions.array_contains(
                    pyspark.sql.functions.col("procedure_stable_id"), "IMPC_VIA_002"
                ),
                pyspark.sql.functions.array(
                    pyspark.sql.functions.lit(
                        "preweaning lethality, complete penetrance"
                    ),
                    pyspark.sql.functions.lit(
                        "preweaning lethality, incomplete penetrance"
                    ),
                ),
            ).otherwise(pyspark.sql.functions.col("mp_term")),
        )
        return pipeline_core_df

    def stop_and_count(self, df):
        print(df.count())
        raise ValueError
