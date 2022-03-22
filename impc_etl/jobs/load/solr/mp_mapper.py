"""
SOLR module
   Generates the required Solr cores
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    collect_set,
    concat,
    flatten,
    monotonically_increasing_id,
)
from pyspark.sql.types import StringType

from impc_etl.jobs.extract.ontology_hierarchy_extractor import (
    OntologyTermHierarchyExtractor,
)
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.pipeline_mapper import ImpressToParameterMapper
from impc_etl.workflow.config import ImpcConfig

ONTOLOGY_MP_MAP = {
    "mp_id": "id",
    "mp_term": "term",
    "mp_definition": "definition",
    "mp_term_synonym": "synonyms",
    "alt_mp_id": "alt_ids",
    "child_mp_id": "child_ids",
    "child_mp_term": "child_terms",
    "parent_mp_id": "parent_ids",
    "parent_mp_term": "parent_terms",
    "intermediate_mp_id": "intermediate_ids",
    "intermediate_mp_term": "intermediate_terms",
    "top_level_mp_id": "top_level_ids",
    "top_level_mp_term": "top_level_terms",
    "top_level_mp_term_id": "top_level_term_id",
    "top_level_mp_term_synonym": "top_level_synonyms",
}

ONTOLOGY_MA_MAP = {
    "inferred_ma_id": "id",
    "inferred_ma_term": "term",
    "inferred_intermediate_ma_id": "intermediate_ids",
    "inferred_intermediate_ma_term": "intermediate_terms",
    "inferred_selected_top_level_ma_id": "top_level_ids",
    "inferred_selected_top_level_ma_term": "top_level_terms",
}


MP_CORE_COLUMNS = [
    "mp_id",
    "mp_term",
    "mp_definition",
    "mp_term_synonym",
    "alt_mp_id",
    "child_mp_id",
    "child_mp_term",
    "parent_mp_id",
    "parent_mp_term",
    "intermediate_mp_id",
    "intermediate_mp_term",
    "top_level_mp_id",
    "top_level_mp_term",
    "top_level_mp_term_synonym",
    "top_level_mp_term_id",
    "inferred_ma_id",
    "inferred_ma_term",
    "inferred_intermediate_ma_id",
    "inferred_intermediate_ma_term",
    "inferred_selected_top_level_ma_id",
    "inferred_selected_top_level_ma_term",
    "hp_id",
    "hp_term",
]


class MpMapper(PySparkTask):
    #: Name of the Spark task
    name = "IMPC_MP_Mapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    impc_search_index_csv_path = luigi.Parameter()
    mp_relation_augmented_metadata_table_csv_path = luigi.Parameter()
    mp_hp_matches_csv_path = luigi.Parameter()

    def requires(self):
        return [
            OntologyTermHierarchyExtractor(),
            ExperimentToObservationMapper(),
            ImpressToParameterMapper(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/output/gene_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}gene_parquet")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.impc_search_index_csv_path,
            self.mp_relation_augmented_metadata_table_csv_path,
            self.mp_hp_matches_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Solr Core loader
        :param list argv: the list elements should be:
                        [1]: source IMPC parquet file
                        [2]: Output Path
        """
        ontology_parquet_path = args[0]
        observations_parquet_path = args[1]
        pipeline_core_parquet_path = args[2]
        impc_search_index_csv_path = args[3]
        mp_relation_augmented_metadata_table_csv_path = args[4]
        mp_hp_matches_csv_path = args[5]
        output_path = args[6]

        spark = SparkSession.builder.getOrCreate()
        ontology_df = spark.read.parquet(ontology_parquet_path)
        pipeline_df = spark.read.parquet(pipeline_core_parquet_path)
        observations_df = spark.read.parquet(observations_parquet_path)
        impc_search_index_df = spark.read.csv(impc_search_index_csv_path, header=True)
        mp_ext_df = spark.read.csv(
            mp_relation_augmented_metadata_table_csv_path, header=True
        )
        mp_hp_matches_df = spark.read.csv(mp_hp_matches_csv_path, header=True)

        impc_search_index_df = (
            impc_search_index_df.groupBy("phenotype")
            .pivot("property")
            .agg(collect_set("value"))
        )
        mp_df = ontology_df.where(col("id").startswith("MP:"))
        for column_name, ontology_column in ONTOLOGY_MP_MAP.items():
            mp_df = mp_df.withColumnRenamed(ontology_column, column_name)
        mp_df = mp_df.select(list(ONTOLOGY_MP_MAP.keys()))
        mp_hp_matches_df = mp_hp_matches_df.withColumnRenamed("curie_y", "mp_id")
        mp_hp_matches_df = mp_hp_matches_df.withColumnRenamed("curie_x", "hp_id")
        mp_hp_matches_df = mp_hp_matches_df.withColumnRenamed("label_x", "hp_term")
        mp_hp_matches_df = mp_hp_matches_df.groupBy("mp_id").agg(
            collect_set("hp_id").alias("hp_id"), collect_set("hp_term").alias("hp_term")
        )
        mp_hp_matches_df = mp_hp_matches_df.select("mp_id", "hp_id", "hp_term")
        mp_df = mp_df.join(mp_hp_matches_df, "mp_id", "left_outer")
        # mp_df = mp_df.withColumn(
        #     "hp_id",
        #     concat(
        #         "impc:childOneLabel",
        #         "impc:childTwoLabel",
        #         "impc:hpExactSynonym",
        #         "impc:hpLabel",
        #     ),
        # )
        # mp_df = mp_df.withColumn(
        #     "hp_term",
        #     concat(
        #         "impc:childOneLabel",
        #         "impc:childTwoLabel",
        #         "impc:hpExactSynonym",
        #         "impc:hpLabel",
        #     ),
        # )
        mp_df = mp_df.join(
            impc_search_index_df, col("mp_id") == col("phenotype"), "left_outer"
        )
        mp_df = mp_df.withColumn("mp_term_synonym", col("oio:hasExactSynonym"))

        ma_df = ontology_df.where(
            (~col("id").startswith("MP:")) & (~col("id").startswith("MPATH:"))
        )
        for column_name, ontology_column in ONTOLOGY_MA_MAP.items():
            ma_df = ma_df.withColumnRenamed(ontology_column, column_name)
        ma_df = ma_df.select(list(ONTOLOGY_MA_MAP.keys()))
        mp_ma_df = (
            mp_ext_df.select(
                col("acc").alias("mp_id"), col("ma").alias("inferred_ma_id")
            )
            .where(col("inferred_ma_id").isNotNull())
            .distinct()
        )
        mp_ma_df = mp_ma_df.join(ma_df, "inferred_ma_id")
        mp_ma_df = mp_ma_df.groupBy("mp_id").agg(
            collect_set("inferred_ma_id").alias("inferred_ma_id"),
            collect_set("inferred_ma_term").alias("inferred_ma_term"),
            *[
                flatten(collect_set(col_name)).alias(col_name)
                for col_name in ma_df.columns
                if col_name not in ["mp_id", "inferred_ma_id", "inferred_ma_term"]
            ],
        )
        mp_df = mp_df.join(mp_ma_df, "mp_id", "left_outer")
        tested_ontology_terms = pipeline_df.withColumn(
            "mp_id", explode(concat("mp_id", "top_level_mp_id", "intermediate_mp_id"))
        ).select("mp_id", "fully_qualified_name")
        ontological_obs_df = observations_df.where(
            col("observation_type") == "ontological"
        )
        ontological_obs_df = ontological_obs_df.withColumn(
            "mp_id", explode("sub_term_id")
        )
        ontological_obs_df = ontological_obs_df.where(col("mp_id").startswith("MP:"))
        ontological_obs_df = ontological_obs_df.withColumn(
            "fully_qualified_name",
            concat("pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"),
        )
        ontological_obs_df = ontological_obs_df.select(
            "mp_id", "fully_qualified_name"
        ).distinct()
        tested_ontology_terms = tested_ontology_terms.union(
            ontological_obs_df
        ).distinct()
        mp_df = mp_df.join(tested_ontology_terms, "mp_id")
        mp_df = mp_df.select(MP_CORE_COLUMNS).distinct()
        mp_df = mp_df.withColumn(
            "doc_id", monotonically_increasing_id().astype(StringType())
        )
        mp_df.write.parquet(output_path)
