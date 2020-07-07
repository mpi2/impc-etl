"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StringType
import sys

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
)

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
}


def main(argv):
    """
    Pipeline Solr Core loader
    :param list argv: the list elements should be:
                    [1]: Pipeline parquet path
                    [2]: Observations parquet
                    [3]: Ontology parquet
                    [4]: EMAP-EMAPA tsv
                    [5]: EMAPA metadata csv
                    [6]: MA metadata csv
                    [7]: Output Path
    """
    pipeline_parquet_path = argv[1]
    observations_parquet_path = argv[2]
    ontology_parquet_path = argv[3]
    emap_emapa_tsv_path = argv[4]
    emapa_metadata_csv_path = argv[5]
    ma_metadata_csv_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
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
        when(col("incrementStruct").isNotNull(), col("unitName")).otherwise(lit(None)),
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

    pipeline_categories_df = pipeline_df.select("fully_qualified_name", "option.name")
    pipeline_categories_df = pipeline_categories_df.groupBy("fully_qualified_name").agg(
        collect_set("name").alias("categories")
    )

    pipeline_df = pipeline_df.join(
        pipeline_categories_df, "fully_qualified_name", "left_outer"
    )

    pipeline_mp_terms_df = pipeline_df.select(
        "fully_qualified_name", "parammpterm.selectionOutcome", "termAcc"
    ).where(col("termAcc").startswith("MP"))

    pipeline_mp_terms_df = pipeline_mp_terms_df.join(
        ontology_df, col("id") == col("termAcc")
    )

    uniquify = udf(_uniquify, ArrayType(StringType()))

    pipeline_mp_terms_df = pipeline_mp_terms_df.groupBy("fully_qualified_name").agg(
        collect_set("termAcc").alias("mp_id"),
        collect_set("term").alias("mp_term"),
        uniquify(flatten(collect_list("top_level_ids"))).alias("top_level_mp_id"),
        uniquify(flatten(collect_list("top_level_terms"))).alias("top_level_mp_term"),
        uniquify(flatten(collect_list("top_level_synonyms"))).alias(
            "top_level_mp_term_synonym"
        ),
        uniquify(flatten(collect_list("intermediate_ids"))).alias("intermediate_mp_id"),
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

    pipeline_df = pipeline_df.join(
        pipeline_mp_terms_df, "fully_qualified_name", "left_outer"
    )

    pipeline_df = pipeline_df.join(
        emap_emapa_df.alias("emap_emapa"),
        col("emap_id") == col("termAcc"),
        "left_outer",
    )
    pipeline_df = pipeline_df.withColumn("anatomy_id", col("emapa_id"))
    pipeline_df = pipeline_df.drop(*emap_emapa_df.columns)

    emapa_metadata_df = emapa_metadata_df.withColumnRenamed("name", "emapaName")
    pipeline_df = pipeline_df.join(
        emapa_metadata_df, col("anatomy_id") == col("curie"), "left_outer"
    )
    pipeline_df = pipeline_df.withColumn("anatomy_term", col("emapaName"))
    pipeline_df = pipeline_df.drop(*emapa_metadata_df.columns)

    pipeline_df = pipeline_df.withColumn(
        "ma_id",
        when(col("termAcc").startswith("MA:"), col("termAcc")).otherwise(lit(None)),
    )
    ma_metadata_df = ma_metadata_df.withColumnRenamed("name", "maName")
    pipeline_df = pipeline_df.join(
        ma_metadata_df, col("ma_id") == col("curie"), "left_outer"
    )
    pipeline_df = pipeline_df.withColumn("ma_term", col("maName"))
    pipeline_df = pipeline_df.drop(*ma_metadata_df.columns)
    pipeline_df.write.parquet(output_path)


def _uniquify(array: list):
    return list(set(array))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
