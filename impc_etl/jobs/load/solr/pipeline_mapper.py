"""
    Module to hold IMPRESS to Parameter mapping transformation tasks.

    It takes in the output of the IMPRESS crawler and returns a parameter view of the same data, extende with some ontology information.
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    col,
    lit,
    first,
    concat_ws,
    collect_set,
    flatten,
    collect_list,
    udf,
    struct,
    transform,
)
from pyspark.sql.types import ArrayType, StringType

from impc_etl.jobs.extract import ImpressExtractor
from impc_etl.jobs.extract.ontology_hierarchy_extractor import (
    OntologyTermHierarchyExtractor,
)
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.workflow.config import ImpcConfig

COLUMN_MAPPER = {
    "schedule_key": "schedule.scheduleId",
    "stage": "schedule.stage",
    "stage_label": "schedule.timeLabel",
    "pipeline_id": "pipelineId",
    "pipeline_stable_key": "pipelineId",
    "pipeline_stable_id": "pipelineKey",
    "pipeline_name": "name",
    "procedure_id": "procedure.procedureId",
    "procedure_stable_key": "procedure.procedureId",
    "procedure_stable_id": "procedure.procedureKey",
    "procedure_name": "procedure.name",
    "experiment_level": "procedure.level",
    "parameter_id": "parameter.parameterId",
    "parameter_stable_key": "parameter.parameterId",
    "parameter_stable_id": "parameter.parameterKey",
    "parameter_name": "parameter.name",
    "description": "parameter.description",
    "data_type": "parameter.valueType",
    "required": "parameter.isRequired",
    "annotate": "parameter.isAnnotation",
    "media": "parameter.isMedia",
    "has_options": "parameter.isOption",
    "increment": "parameter.isIncrement",
    "comparable_parameter_group": "parameter.comparableParameterGroup",
}


class ImpressToParameterMapper(PySparkTask):
    """
    PySpark task to map the output of the IMPReSS crawler to a view that represents IMPReSS parameters extendend with ontology data.

    This task depends on:
    - `impc_etl.jobs.extract.impress_extractor.ImpressExtractor`
    - `impc_etl.jobs.load.observation_mapper.ExperimentToObservationMapper`
    - `impc_etl.jobs.extract.ontology_hierarchy_extractor.OntologyTermHierarchyExtractor`
    """

    name = "IMPC_Impress_To_Parameter_Mapper"
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ImpressExtractor(),
            ExperimentToObservationMapper(),
            OntologyTermHierarchyExtractor(),
        ]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}impress_parameter_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.emap_emapa_csv_path,
            self.emapa_metadata_csv_path,
            self.ma_metadata_csv_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Pipeline Solr Core loader
        """
        pipeline_parquet_path = args[0]
        observations_parquet_path = args[1]
        ontology_parquet_path = args[2]
        emap_emapa_tsv_path = args[3]
        emapa_metadata_csv_path = args[4]
        ma_metadata_csv_path = args[5]
        output_path = args[6]

        spark = SparkSession(sc)
        pipeline_df = spark.read.parquet(pipeline_parquet_path)
        observations_df = spark.read.parquet(observations_parquet_path)
        ontology_df = spark.read.parquet(ontology_parquet_path)
        emap_emapa_df = spark.read.csv(emap_emapa_tsv_path, header=True, sep="\t")
        for col_name in emap_emapa_df.columns:
            emap_emapa_df = emap_emapa_df.withColumnRenamed(
                col_name, col_name.lower().replace(" ", "_")
            )
        emapa_metadata_df = spark.read.csv(emapa_metadata_csv_path, header=True)
        ma_metadata_df = spark.read.csv(ma_metadata_csv_path, header=True)

        pipeline_df = pipeline_df.withColumnRenamed("increment", "incrementStruct")
        for column, source in COLUMN_MAPPER.items():
            pipeline_df = pipeline_df.withColumn(column, col(source))

        pipeline_df = pipeline_df.withColumn(
            "unit_y",
            when(col("incrementStruct").isNotNull(), col("unitName")).otherwise(
                lit(None)
            ),
        )
        pipeline_df = pipeline_df.withColumn(
            "unit_x",
            when(
                col("incrementStruct").isNotNull(), col("incrementStruct.incrementUnit")
            ).otherwise(col("unitName")),
        )
        pipeline_df = pipeline_df.withColumn(
            "metadata", col("parameter.type") == "procedureMetadata"
        )
        pipeline_df = pipeline_df.withColumn(
            "fully_qualified_name",
            concat_ws(
                "_", "pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"
            ),
        )
        observations_df = observations_df.withColumn(
            "fully_qualified_name",
            concat_ws(
                "_", "pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"
            ),
        )
        observations_df = observations_df.groupBy("fully_qualified_name").agg(
            first(col("observation_type")).alias("observation_type")
        )

        pipeline_df = pipeline_df.join(
            observations_df, "fully_qualified_name", "left_outer"
        )

        pipeline_categories_df = pipeline_df.select(
            "fully_qualified_name",
            when(
                col("option.name").rlike("^\d+$")
                & col("option.description").isNotNull(),
                col("option.description"),
            )
            .otherwise(col("option.name"))
            .alias("name"),
        )
        pipeline_categories_df = pipeline_categories_df.groupBy(
            "fully_qualified_name"
        ).agg(collect_set("name").alias("categories"))

        pipeline_df = pipeline_df.join(
            pipeline_categories_df, "fully_qualified_name", "left_outer"
        )

        pipeline_mp_terms_df = pipeline_df.select(
            "fully_qualified_name", "parammpterm.selectionOutcome", "termAcc"
        ).where(col("termAcc").startswith("MP"))

        pipeline_mp_terms_df = pipeline_mp_terms_df.join(
            ontology_df, col("id") == col("termAcc")
        )

        uniquify = udf(self._uniquify, ArrayType(StringType()))
        phenotype_term = lambda x, y: struct(col(x).alias("id"), col(y).alias("term"))

        pipeline_mp_terms_df = pipeline_mp_terms_df.groupBy("fully_qualified_name").agg(
            collect_set(phenotype_term("id", "term").alias("mp")),
            uniquify(flatten(collect_list("top_level_ids"))).alias("top_level_mp_id"),
            uniquify(flatten(collect_list("top_level_terms"))).alias(
                "top_level_mp_term"
            ),
            uniquify(flatten(collect_list("top_level_synonyms"))).alias(
                "top_level_mp_term_synonym"
            ),
            uniquify(flatten(collect_list("intermediate_ids"))).alias(
                "intermediate_mp_id"
            ),
            uniquify(flatten(collect_list("intermediate_terms"))).alias(
                "intermediate_mp_term"
            ),
            collect_set(
                when(col("selectionOutcome") == "ABNORMAL", col("termAcc")).otherwise(
                    lit(None)
                )
            ).alias("abnormal_mp_id"),
            collect_set(
                when(col("selectionOutcome") == "ABNORMAL", col("term")).otherwise(
                    lit(None)
                )
            ).alias("abnormal_mp_term"),
            collect_set(
                when(col("selectionOutcome") == "INCREASED", col("termAcc")).otherwise(
                    lit(None)
                )
            ).alias("increased_mp_id"),
            collect_set(
                when(col("selectionOutcome") == "INCREASED", col("term")).otherwise(
                    lit(None)
                )
            ).alias("increased_mp_term"),
            collect_set(
                when(col("selectionOutcome") == "DECREASED", col("termAcc")).otherwise(
                    lit(None)
                )
            ).alias("decreased_mp_id"),
            collect_set(
                when(col("selectionOutcome") == "DECREASED", col("term")).otherwise(
                    lit(None)
                )
            ).alias("decreased_mp_term"),
        )

        pipeline_df = (
            pipeline_df.withColumn("mp_id", transform("mp", lambda mp: mp["id"]))
            .withColumn("mp_term", transform("mp", lambda mp: mp["id"]))
            .drop("mp")
        )

        pipeline_df = pipeline_df.join(
            pipeline_mp_terms_df, "fully_qualified_name", "left_outer"
        )

        pipeline_df = pipeline_df.withColumn(
            "embryo_anatomy_id",
            when(col("termAcc").contains("EMAPA:"), col("termAcc")).otherwise(
                lit(None)
            ),
        )
        emapa_metadata_df = emapa_metadata_df.select(
            "acc", col("name").alias("emapaName")
        )
        pipeline_df = pipeline_df.join(
            emapa_metadata_df, col("embryo_anatomy_id") == col("acc"), "left_outer"
        )

        pipeline_df = pipeline_df.withColumn("embryo_anatomy_term", col("emapaName"))
        pipeline_df = pipeline_df.drop(*emapa_metadata_df.columns)

        pipeline_df = pipeline_df.join(
            ontology_df, col("embryo_anatomy_id") == col("id"), "left_outer"
        )
        pipeline_df = pipeline_df.withColumn(
            "top_level_embryo_anatomy_id", col("top_level_ids")
        )
        pipeline_df = pipeline_df.withColumn(
            "top_level_embryo_anatomy_term", col("top_level_terms")
        )
        pipeline_df = pipeline_df.drop(*ontology_df.columns)

        pipeline_df = pipeline_df.withColumn(
            "mouse_anatomy_id",
            when(col("termAcc").startswith("MA:"), col("termAcc")).otherwise(lit(None)),
        )
        ma_metadata_df = ma_metadata_df.withColumnRenamed("name", "maName")
        pipeline_df = pipeline_df.join(
            ma_metadata_df, col("mouse_anatomy_id") == col("curie"), "left_outer"
        )
        pipeline_df = pipeline_df.withColumn("mouse_anatomy_term", col("maName"))
        pipeline_df = pipeline_df.drop(*ma_metadata_df.columns)

        pipeline_df = pipeline_df.join(
            ontology_df, col("mouse_anatomy_id") == col("id"), "left_outer"
        )
        pipeline_df = pipeline_df.withColumn(
            "top_level_mouse_anatomy_id", col("top_level_ids")
        )
        pipeline_df = pipeline_df.withColumn(
            "top_level_mouse_anatomy_term", col("top_level_terms")
        )
        missing_parameter_information_df = pipeline_df.where(
            col("parameter_stable_id").isNull()
        )
        missing_parameter_rows = missing_parameter_information_df.collect()
        if len(missing_parameter_rows) > 0:
            print("MISSING PARAMETERS")
            for missing in missing_parameter_rows:
                print(missing.asDict())
        pipeline_df = pipeline_df.where(col("parameter_stable_id").isNotNull())
        pipeline_df = pipeline_df.drop(*ontology_df.columns)
        pipeline_df.write.parquet(output_path)

    def _uniquify(self, list_col):
        return list(set(list_col))
