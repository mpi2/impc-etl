import hashlib
import sys
from typing import List

from pyspark.sql import SparkSession, Window, DataFrame, Column
from pyspark.sql.functions import (
    col,
    lit,
    when,
    lower,
    concat,
    md5,
    explode,
    concat_ws,
    sort_array,
    collect_set,
    max,
    udf,
    array_union,
    array,
)
from pyspark.sql.types import (
    ArrayType,
    StringType,
    Row,
)
from pyspark.sql.utils import AnalysisException

from impc_etl.config.constants import Constants


def main(argv):
    experiment_parquet_path = argv[1]
    mouse_parquet_path = argv[2]
    embryo_parquet_path = argv[3]
    pipeline_parquet_path = argv[4]
    output_path = argv[5]
    spark = SparkSession.builder.getOrCreate()
    experiment_normalized_df = normalize_experiments(
        spark,
        experiment_parquet_path,
        mouse_parquet_path,
        embryo_parquet_path,
        pipeline_parquet_path,
    )
    experiment_normalized_df.write.mode("overwrite").parquet(output_path)


def normalize_experiments(
    spark_session: SparkSession,
    experiment_parquet_path: str,
    mouse_parquet_path: str,
    embryo_parquet_path: str,
    pipeline_parquet_path: str,
) -> DataFrame:
    """
    DCC experiment normalizer

    :param pipeline_parquet_path:
    :param embryo_parquet_path:
    :param mouse_parquet_path:
    :param experiment_parquet_path:
    :param SparkSession spark_session: PySpark session object
    :return: a normalized specimen parquet file
    :rtype: DataFrame
    """
    experiment_df = spark_session.read.parquet(experiment_parquet_path)
    mouse_df = spark_session.read.parquet(mouse_parquet_path)
    try:
        embryo_df = spark_session.read.parquet(embryo_parquet_path)
    except AnalysisException:
        embryo_df = None
    pipeline_df = spark_session.read.parquet(pipeline_parquet_path)

    ## THIS IS NOT OK
    experiment_df = experiment_df.withColumn(
        "_pipeline",
        when(
            (col("_dataSource") == "3i")
            & (col("_procedureID") == "MGP_PBI_001")
            & (col("_pipeline") == "SLM_001"),
            lit("MGP_001"),
        ).otherwise(col("_pipeline")),
    )

    experiment_df = experiment_df.withColumn(
        "_pipeline",
        when(
            (lower(col("_dataSource")).isin(["europhenome", "mgp"]))
            & (col("_procedureID") == "ESLIM_019_001")
            & (col("_pipeline") == "ESLIM_001"),
            lit("ESLIM_002"),
        ).otherwise(col("_pipeline")),
    )
    ## THIS IS NOT OK

    specimen_cols = [
        "_centreID",
        "_specimenID",
        "_colonyID",
        "_isBaseline",
        "_productionCentre",
        "_phenotypingCentre",
        "phenotyping_consortium",
    ]

    mouse_specimen_df = mouse_df.select(*specimen_cols)
    if embryo_df is not None:
        embryo_specimen_df = embryo_df.select(*specimen_cols)
        specimen_df = mouse_specimen_df.union(embryo_specimen_df)
    else:
        specimen_df = mouse_specimen_df
    experiment_df = experiment_df.alias("experiment")
    specimen_df = specimen_df.alias("specimen")
    experiment_specimen_df = experiment_df.join(
        specimen_df,
        (experiment_df["_centreID"] == specimen_df["_centreID"])
        & (experiment_df["specimenID"] == specimen_df["_specimenID"]),
    )

    experiment_specimen_df = drop_null_colony_id(experiment_specimen_df)

    experiment_specimen_df = re_map_europhenome_experiments(experiment_specimen_df)

    experiment_specimen_df = generate_metadata_group(
        experiment_specimen_df, pipeline_df
    )

    experiment_specimen_df = generate_metadata(experiment_specimen_df, pipeline_df)

    experiment_columns = [
        "experiment." + col_name
        for col_name in experiment_df.columns
        if col_name not in ["_dataSource", "_project"]
    ] + ["metadata", "metadataGroup", "_project", "_dataSource"]
    experiment_df = experiment_specimen_df.select(experiment_columns)
    return experiment_df


def drop_null_colony_id(experiment_specimen_df: DataFrame) -> DataFrame:
    experiment_specimen_df = experiment_specimen_df.where(
        (col("specimen._colonyID").isNotNull())
        | (col("specimen._isBaseline") == True)
        | (col("specimen._colonyID") == "baseline")
    )
    return experiment_specimen_df.dropDuplicates()


def re_map_europhenome_experiments(experiment_specimen_df: DataFrame):
    experiment_specimen_df = experiment_specimen_df.transform(
        override_europhenome_datasource
    )
    return experiment_specimen_df


def override_europhenome_datasource(dcc_df: DataFrame) -> DataFrame:
    legacy_entity_cond: Column = (
        (dcc_df["_dataSource"] == "europhenome")
        & (~lower(dcc_df["_colonyID"]).startswith("baseline"))
        & (dcc_df["_colonyID"].isNotNull())
        & (
            (dcc_df["phenotyping_consortium"] == "MGP")
            | (dcc_df["phenotyping_consortium"] == "MGP Legacy")
        )
    )

    dcc_df = dcc_df.withColumn(
        "_project", when(legacy_entity_cond, lit("MGP")).otherwise(dcc_df["_project"])
    )

    dcc_df = dcc_df.withColumn(
        "_dataSource",
        when(legacy_entity_cond, lit("MGP")).otherwise(dcc_df["_dataSource"]),
    )
    return dcc_df


def generate_metadata_group(
    experiment_specimen_df: DataFrame, impress_df: DataFrame, exp_type="experiment"
) -> DataFrame:
    experiment_metadata = experiment_specimen_df.withColumn(
        "procedureMetadata", explode("procedureMetadata")
    )
    impress_df_required = impress_df.where(
        (col("parameter.isImportant") == True)
        & (col("parameter.type") == "procedureMetadata")
    )
    experiment_metadata = experiment_metadata.join(
        impress_df_required,
        (
            (experiment_metadata["_pipeline"] == impress_df_required["pipelineKey"])
            & (
                experiment_metadata["_procedureID"]
                == impress_df_required["procedure.procedureKey"]
            )
            & (
                experiment_metadata["procedureMetadata._parameterID"]
                == impress_df_required["parameter.parameterKey"]
            )
        ),
    )
    experiment_metadata = experiment_metadata.withColumn(
        "metadataItem",
        when(
            col("procedureMetadata.value").isNotNull(),
            concat(col("parameter.name"), lit(" = "), col("procedureMetadata.value")),
        ).otherwise(concat(col("parameter.name"), lit(" = "), lit("null"))),
    )
    if exp_type == "experiment":
        production_centre_col = "_productionCentre"
        phenotyping_centre_col = "_phenotypingCentre"
    else:
        production_centre_col = "production_centre"
        phenotyping_centre_col = "phenotyping_centre"
    window = Window.partitionBy(
        "unique_id", production_centre_col, phenotyping_centre_col
    ).orderBy("parameter.name")

    experiment_metadata_input = experiment_metadata.withColumn(
        "metadataItems", collect_set(col("metadataItem")).over(window)
    )

    experiment_metadata_input = experiment_metadata_input.withColumn(
        "metadataItems",
        when(
            (col(production_centre_col).isNotNull())
            & (col(production_centre_col) != col(phenotyping_centre_col)),
            array_union(
                col("metadataItems"),
                array(concat(lit("ProductionCenter = "), col(production_centre_col))),
            ),
        ).otherwise(col("metadataItems")),
    )

    experiment_metadata = experiment_metadata_input.groupBy(
        "unique_id", production_centre_col, phenotyping_centre_col
    ).agg(
        concat_ws("::", sort_array(max(col("metadataItems")))).alias(
            "metadataGroupList"
        )
    )

    experiment_metadata = experiment_metadata.withColumn(
        "metadataGroup", md5(col("metadataGroupList"))
    )
    experiment_metadata = experiment_metadata.select("unique_id", "metadataGroup")
    experiment_specimen_df = experiment_specimen_df.join(
        experiment_metadata, "unique_id", "left_outer"
    )
    experiment_specimen_df = experiment_specimen_df.withColumn(
        "metadataGroup",
        when(experiment_specimen_df["metadataGroup"].isNull(), md5(lit(""))).otherwise(
            experiment_specimen_df["metadataGroup"]
        ),
    )
    return experiment_specimen_df


def generate_metadata(
    experiment_specimen_df: DataFrame, impress_df: DataFrame, exp_type="experiment"
) -> DataFrame:
    experiment_metadata = experiment_specimen_df.withColumn(
        "procedureMetadata", explode("procedureMetadata")
    )
    impress_df_required = impress_df.where(
        (col("parameter.type") == "procedureMetadata")
    )
    experiment_metadata = experiment_metadata.join(
        impress_df_required,
        (
            (experiment_metadata["_pipeline"] == impress_df_required["pipelineKey"])
            & (
                experiment_metadata["_procedureID"]
                == impress_df_required["procedure.procedureKey"]
            )
            & (
                experiment_metadata["procedureMetadata._parameterID"]
                == impress_df_required["parameter.parameterKey"]
            )
        ),
    )

    process_experimenter_id_udf = udf(_process_experimenter_id, StringType())
    experiment_metadata = experiment_metadata.withColumn(
        "experimenterIdMetadata",
        when(
            lower(col("parameter.name")).contains("experimenter"),
            process_experimenter_id_udf("procedureMetadata"),
        ).otherwise(lit(None)),
    )
    experiment_metadata = experiment_metadata.withColumn(
        "procedureMetadata.value",
        when(
            col("experimenterIdMetadata").isNotNull(), col("experimenterIdMetadata")
        ).otherwise("procedureMetadata.value"),
    )
    experiment_metadata = experiment_metadata.withColumn(
        "metadataItem",
        concat(
            col("parameter.name"),
            lit(" = "),
            when(
                col("procedureMetadata.value").isNotNull(),
                col("procedureMetadata.value"),
            ).otherwise(lit("null")),
        ),
    )
    if exp_type == "experiment":
        production_centre_col = "_productionCentre"
        phenotyping_centre_col = "_phenotypingCentre"
    else:
        production_centre_col = "production_centre"
        phenotyping_centre_col = "phenotyping_centre"
    experiment_metadata = experiment_metadata.groupBy(
        "unique_id", production_centre_col, phenotyping_centre_col
    ).agg(sort_array(collect_set(col("metadataItem"))).alias("metadata"))
    experiment_metadata = experiment_metadata.withColumn(
        "metadata",
        when(
            (col(production_centre_col).isNotNull())
            & (col(production_centre_col) != col(phenotyping_centre_col)),
            udf(_append_phenotyping_centre_to_metadata, ArrayType(StringType()))(
                col("metadata"), col(production_centre_col)
            ),
        ).otherwise(col("metadata")),
    )
    experiment_specimen_df = experiment_specimen_df.join(
        experiment_metadata, "unique_id", "left_outer"
    )
    return experiment_specimen_df


def _append_phenotyping_centre_to_metadata(metadata: List, prod_centre: str):
    if prod_centre is not None:
        metadata.append("ProductionCenter = " + prod_centre)
    return metadata


def _process_experimenter_id(experimenter_metadata: Row):
    experimenter_metadata = experimenter_metadata.asDict()
    if experimenter_metadata["value"] in Constants.EXPERIMENTER_IDS:
        experimenter_metadata["value"] = Constants.EXPERIMENTER_IDS[
            experimenter_metadata["value"]
        ]
    if experimenter_metadata["value"] is not None:
        experimenter_metadata["value"] = (
            hashlib.md5(experimenter_metadata["value"].encode()).hexdigest()[:5].upper()
        )
    return experimenter_metadata["value"]


if __name__ == "__main__":
    sys.exit(main(sys.argv))
