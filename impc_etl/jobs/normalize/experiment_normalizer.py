import sys
import hashlib
import math
from datetime import datetime
from typing import List, Dict
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession, Window, DataFrame, Column
from pyspark.sql.functions import (
    explode_outer,
    col,
    lit,
    when,
    lower,
    concat,
    md5,
    explode,
    concat_ws,
    collect_list,
    collect_list,
    sort_array,
    collect_set,
    struct,
    max,
    udf,
    array_union,
    array,
    sum,
    size,
    expr,
)
from pyspark.sql.types import (
    BooleanType,
    ArrayType,
    StructType,
    StructField,
    IntegerType,
    StringType,
    Row,
    LongType,
)
from impc_etl.config.constants import Constants
from impc_etl.shared.utils import (
    unix_time_millis,
    extract_parameters_from_derivation,
    has_column,
)


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

    experiment_df = get_derived_parameters(spark_session, experiment_df, pipeline_df)

    experiment_df = get_associated_body_weight(experiment_df, mouse_df)

    experiment_df = generate_age_information(experiment_df, mouse_df)
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


def get_derived_parameters(
    spark: SparkSession,
    dcc_experiment_df: DataFrame,
    impress_df: DataFrame,
    exp_type="experiment",
) -> DataFrame:
    # Add missing europhenome derivations
    europhenome_derivations_json = spark.sparkContext.parallelize(
        Constants.EUROPHENOME_DERIVATIONS
    )
    europhenome_derivations_df = spark.read.json(europhenome_derivations_json)
    europhenome_derivations_df = europhenome_derivations_df.alias("europhenome")

    europhenome_parameters = [
        derivation["europhenomeParameter"]
        for derivation in Constants.EUROPHENOME_DERIVATIONS
    ]

    # Filter impress DataFrame to get only the derived parameters, filtering out archive and unimplemented
    derived_parameters: DataFrame = (
        impress_df.where(
            (
                (impress_df["parameter.isDerived"] == True)
                & (impress_df["parameter.isDeprecated"] == False)
                & (~impress_df["parameter.derivation"].contains("archived"))
                & (~impress_df["parameter.derivation"].contains("unimplemented"))
            )
            | (impress_df["parameter.parameterKey"].isin(europhenome_parameters))
        )
        .where(
            ~impress_df["parameter.parameterKey"].isin(
                Constants.DERIVED_PARAMETER_BANLIST
            )
        )
        .select(
            "pipelineKey",
            "procedure.procedureKey",
            "parameter.parameterKey",
            "parameter.derivation",
            "parameter.type",
            "unitName",
        )
        .dropDuplicates()
    )

    derived_parameters = derived_parameters.join(
        europhenome_derivations_df,
        col("parameterKey") == europhenome_derivations_df["europhenomeParameter"],
        "left_outer",
    )
    derived_parameters = derived_parameters.withColumn(
        "derivation",
        when(
            col("europhenomeDerivation").isNotNull(), col("europhenomeDerivation")
        ).otherwise(col("derivation")),
    )
    derived_parameters = derived_parameters.drop("europhenome.*")

    # Use a Python UDF to extract the keys of the parameters involved in the derivations as a list
    extract_parameters_from_derivation_udf = udf(
        extract_parameters_from_derivation, ArrayType(StringType())
    )

    derived_parameters = derived_parameters.withColumn(
        "derivationInputs", extract_parameters_from_derivation_udf("derivation")
    )

    # Explode the derivation inputs
    derived_parameters_ex = derived_parameters.withColumn(
        "derivationInput", explode("derivationInputs")
    ).select(
        "pipelineKey", "procedureKey", "parameterKey", "derivation", "derivationInput"
    )

    # Compute the derivation inputs for simple, procedure and series parameters
    # Each input is has the form <PARAMETER_KEY>$<PARAMETER_VALUE>
    # If the parameter has increments the inputs will have the form
    # <PARAMETER_KEY>$INCREMENT_1$<PARAMETER_VALUE>|INCREMENT_1$<PARAMETER_VALUE>
    experiments_simple = _get_inputs_by_parameter_type(
        dcc_experiment_df, derived_parameters_ex, "simpleParameter"
    )
    experiments_metadata = _get_inputs_by_parameter_type(
        dcc_experiment_df, derived_parameters_ex, "procedureMetadata"
    )
    experiments_vs_derivations = experiments_simple.union(experiments_metadata)

    if has_column(dcc_experiment_df, "seriesParameter"):
        experiments_series = _get_inputs_by_parameter_type(
            dcc_experiment_df, derived_parameters_ex, "seriesParameter"
        )
        experiments_vs_derivations = experiments_vs_derivations.union(
            experiments_series
        )
    # Collect the derivation inputs in a comma separated list
    experiments_vs_derivations = experiments_vs_derivations.groupby(
        "unique_id", "pipelineKey", "procedureKey", "parameterKey", "derivation"
    ).agg(
        concat_ws(
            ",", collect_list(experiments_vs_derivations["derivationInput"])
        ).alias("derivationInputStr")
    )

    experiments_vs_derivations = experiments_vs_derivations.join(
        derived_parameters.drop("derivation"),
        ["pipelineKey", "procedureKey", "parameterKey"],
    )

    # Check if the experiment contains all the parameter values to perform the derivation
    experiments_vs_derivations = experiments_vs_derivations.withColumn(
        "derivationInput", explode("derivationInputs")
    )
    experiments_vs_derivations = experiments_vs_derivations.withColumn(
        "isPresent",
        when(col("derivationInputStr").contains(col("derivationInput")), 1).otherwise(
            0
        ),
    )
    experiments_vs_derivations = experiments_vs_derivations.groupBy(
        [
            "unique_id",
            "pipelineKey",
            "procedureKey",
            "parameterKey",
            "derivationInputStr",
            "derivationInputs",
            "derivation",
        ]
    ).agg(sum("isPresent").alias("presentColumns"))

    experiments_vs_derivations = experiments_vs_derivations.withColumn(
        "isComplete",
        when(
            (size(col("derivationInputs")) == col("presentColumns")), lit(True)
        ).otherwise(lit(False)),
    )
    experiments_vs_derivations = experiments_vs_derivations.withColumn(
        "derivationInputStr", concat("derivation", lit(";"), "derivationInputStr")
    )
    spark.udf.registerJavaFunction(
        "phenodcc_derivator",
        "org.mousephenotype.dcc.derived.parameters.SparkDerivator",
        StringType(),
    )

    results_df = experiments_vs_derivations.select(
        "unique_id",
        "pipelineKey",
        "procedureKey",
        "parameterKey",
        "presentColumns",
        "isComplete",
        "derivationInputStr",
    ).dropDuplicates()
    results_df = results_df.withColumn(
        "result",
        when(
            (col("isComplete") == True)
            | (col("derivationInputStr").contains("retinaCombined"))
            | (col("derivationInputStr").contains("ifElse")),
            expr("phenodcc_derivator(derivationInputStr)"),
        ).otherwise(lit(None)),
    )

    # Filtering not valid numeric values
    results_df = results_df.withColumn(
        "result",
        when(
            (col("result") == "NaN")
            | (col("result") == "Infinity")
            | (col("result") == "-Infinity"),
            lit(None),
        ).otherwise(col("result")),
    )
    results_df = results_df.where(col("result").isNotNull())
    results_df = results_df.join(
        derived_parameters, ["pipelineKey", "procedureKey", "parameterKey"]
    )

    if exp_type == "experiment":
        result_return = struct(
            results_df["parameterKey"].alias("_parameterID"),
            lit(None).cast(LongType()).alias("_sequenceID"),
            results_df["unitName"].alias("_unit"),
            lit(None).cast(StringType()).alias("parameterStatus"),
            results_df["result"].alias("value"),
        )
    else:
        result_return = struct(
            *[
                lit(None).cast(StringType()).alias("_VALUE"),
                results_df["parameterKey"].alias("_parameterID"),
                results_df["unitName"].alias("_unit"),
                lit(None).cast(StringType()).alias("parameterStatus"),
                results_df["result"].alias("value"),
            ]
        )
    results_df = results_df.groupBy("unique_id", "pipelineKey", "procedureKey").agg(
        collect_list(result_return).alias("results")
    )
    results_df = results_df.withColumnRenamed("unique_id", "unique_id_result")

    dcc_experiment_df = dcc_experiment_df.join(
        results_df,
        (dcc_experiment_df["unique_id"] == results_df["unique_id_result"])
        & (dcc_experiment_df["_procedureID"] == results_df["procedureKey"])
        & (dcc_experiment_df["_pipeline"] == results_df["pipelineKey"]),
        "left_outer",
    )

    simple_parameter_type = None

    for c_type in dcc_experiment_df.dtypes:
        if c_type[0] == "simpleParameter":
            simple_parameter_type = c_type[1]
            break
    merge_simple_parameters = udf(_merge_simple_parameters, simple_parameter_type)
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "simpleParameter",
        when(
            (col("results").isNotNull() & col("simpleParameter").isNotNull()),
            merge_simple_parameters(col("simpleParameter"), col("results")),
        )
        .when(col("simpleParameter").isNull(), col("results"))
        .otherwise(col("simpleParameter")),
    )
    dcc_experiment_df = dcc_experiment_df.drop(
        "complete_derivations.unique_id",
        "unique_id_result",
        "pipelineKey",
        "procedureKey",
        "parameterKey",
        "results",
    )

    return dcc_experiment_df


def get_associated_body_weight(dcc_experiment_df: DataFrame, mice_df: DataFrame):
    weight_observations: DataFrame = dcc_experiment_df.withColumn(
        "simpleParameter", explode_outer("simpleParameter")
    )
    weight_observations = weight_observations.where(
        weight_observations["simpleParameter._parameterID"].isin(
            Constants.WEIGHT_PARAMETERS
        )
    )
    weight_observations = weight_observations.select(
        "specimenID",
        "_centreID",
        col("unique_id").alias("sourceExperimentId"),
        col("_dateOfExperiment").alias("weightDate"),
        col("simpleParameter._parameterID").alias("weightParameterID"),
        col("simpleParameter.value").alias("weightValue"),
    )
    weight_observations = weight_observations.where(col("weightValue").isNotNull())
    weight_observations = weight_observations.join(
        mice_df,
        (weight_observations["specimenID"] == mice_df["_specimenID"])
        & (weight_observations["_centreID"] == mice_df["_centreID"]),
    )
    weight_observations = weight_observations.withColumn(
        "weightDaysOld", udf(calculate_age_in_days, StringType())("weightDate", "_DOB")
    )
    weight_observations = weight_observations.groupBy("specimenID").agg(
        collect_set(
            struct(
                "sourceExperimentId",
                "weightDate",
                "weightParameterID",
                "weightValue",
                "weightDaysOld",
            )
        ).alias("weight_observations")
    )

    dcc_experiment_df = dcc_experiment_df.withColumn(
        "procedureGroup",
        udf(lambda prod_id: prod_id[: prod_id.rfind("_")], StringType())(
            col("_procedureID")
        ),
    )
    dcc_experiment_df = dcc_experiment_df.join(
        weight_observations, "specimenID", "left_outer"
    )
    output_weight_schema = StructType(
        [
            StructField("sourceExperimentId", StringType()),
            StructField("weightDate", StringType()),
            StructField("weightParameterID", StringType()),
            StructField("weightValue", StringType()),
            StructField("weightDaysOld", StringType()),
            StructField("error", ArrayType(StringType())),
        ]
    )
    experiment_df_a = dcc_experiment_df.alias("exp")
    mice_df_a = mice_df.alias("mice")
    dcc_experiment_df = experiment_df_a.join(
        mice_df_a,
        (dcc_experiment_df["specimenID"] == mice_df["_specimenID"])
        & (dcc_experiment_df["_centreID"] == mice_df["_centreID"]),
        "left_outer",
    )
    get_associated_body_weight_udf = udf(_get_closest_weight, output_weight_schema)
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "weight",
        get_associated_body_weight_udf(
            col("_dateOfExperiment"), col("procedureGroup"), col("weight_observations")
        ),
    )
    dcc_experiment_df = dcc_experiment_df.select("exp.*", "weight")
    return dcc_experiment_df


def generate_age_information(dcc_experiment_df: DataFrame, mice_df: DataFrame):
    experiment_df_a = dcc_experiment_df.alias("exp")
    mice_df_a = mice_df.alias("mice")
    dcc_experiment_df = experiment_df_a.join(
        mice_df_a,
        (experiment_df_a["specimenID"] == mice_df["_specimenID"])
        & (experiment_df_a["_centreID"] == mice_df["_centreID"]),
        "left_outer",
    )
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "ageInDays",
        udf(calculate_age_in_days, IntegerType())(
            col("exp._dateOfExperiment"), col("mice._DOB")
        ),
    )
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "ageInWeeks",
        udf(lambda x: math.floor(x / 7) if x is not None else None, IntegerType())(
            col("ageInDays")
        ),
    )
    return dcc_experiment_df.select("exp.*", "ageInWeeks", "ageInDays")


def calculate_age_in_days(experiment_date: str, dob: str) -> int:
    if dob is None or experiment_date is None:
        return None
    experiment_date = datetime.strptime(experiment_date, "%Y-%m-%d")
    dob = datetime.strptime(dob, "%Y-%m-%d")
    return (experiment_date - dob).days


def _get_closest_weight(
    experiment_date: str, procedure_group: str, specimen_weights: List[Row]
) -> Dict:
    if specimen_weights is None or len(specimen_weights) == 0:
        return {
            "sourceExperimentId": None,
            "weightDate": None,
            "weightValue": None,
            "weightParameterID": None,
            "weightDaysOld": None,
        }
    experiment_date = datetime.strptime(experiment_date, "%Y-%m-%d")
    nearest_weight = None
    nearest_diff = None
    errors = []
    for candidate_weight in specimen_weights:
        if (
            candidate_weight["weightValue"] == "null"
            or candidate_weight["weightDate"] == "null"
        ):
            continue
        candidate_weight_date = datetime.strptime(
            candidate_weight["weightDate"], "%Y-%m-%d"
        )
        candidate_diff = abs(
            unix_time_millis(experiment_date) - unix_time_millis(candidate_weight_date)
        )
        if nearest_weight is None:
            nearest_weight = candidate_weight
            nearest_diff = candidate_diff
            continue
        try:
            candidate_weight_value = float(candidate_weight["weightValue"])
        except ValueError:
            errors.append("[PARSING] Failed to parse: " + str(candidate_weight))
            continue

        nearest_weight_value = float(nearest_weight["weightValue"])

        if candidate_diff < nearest_diff:
            nearest_weight = candidate_weight
            nearest_diff = candidate_diff
        elif candidate_diff == nearest_diff:
            if (
                procedure_group is not None
                and procedure_group in candidate_weight["weightParameterID"]
            ):
                if candidate_weight_value > nearest_weight_value:
                    nearest_weight = candidate_weight
                    nearest_diff = candidate_diff
            elif "_BWT" in candidate_weight["weightParameterID"]:
                if candidate_weight_value > nearest_weight_value:
                    nearest_weight = candidate_weight
                    nearest_diff = candidate_diff
            elif candidate_weight_value > nearest_weight_value:
                nearest_weight = candidate_weight
                nearest_diff = candidate_diff

    days_diff = nearest_diff / 86400000 if nearest_diff is not None else 6

    if nearest_weight is not None and days_diff < 5:
        return {**nearest_weight.asDict(), "error": errors}
    else:
        return {
            "sourceExperimentId": None,
            "weightDate": None,
            "weightValue": None,
            "weightParameterID": None,
            "weightDaysOld": None,
            "error": errors,
        }


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


def _get_inputs_by_parameter_type(
    dcc_experiment_df, derived_parameters_ex, parameter_type
):
    experiments_by_type = dcc_experiment_df.select(
        "unique_id",
        "_pipeline",
        "_procedureID",
        explode(parameter_type).alias(parameter_type),
    )
    if parameter_type == "seriesParameter":
        experiments_by_type = experiments_by_type.select(
            "unique_id",
            "_pipeline",
            "_procedureID",
            col(parameter_type + "._parameterID").alias("_parameterID"),
            explode(parameter_type + ".value").alias("value"),
        )
        experiments_by_type = experiments_by_type.withColumn(
            "value", concat(col("value._incrementValue"), lit("|"), col("value._VALUE"))
        )
        experiments_by_type = experiments_by_type.groupBy(
            "unique_id", "_pipeline", "_parameterID", "_procedureID"
        ).agg(concat_ws("$", collect_set("value")).alias("value"))

    parameter_id_column = (
        parameter_type + "._parameterID"
        if parameter_type != "seriesParameter"
        else "_parameterID"
    )
    parameter_value_column = (
        parameter_type + ".value" if parameter_type != "seriesParameter" else "value"
    )
    experiments_vs_derivations = derived_parameters_ex.join(
        experiments_by_type,
        (
            (
                experiments_by_type[parameter_id_column]
                == derived_parameters_ex["derivationInput"]
            )
            & (
                experiments_by_type["_procedureID"]
                == derived_parameters_ex.procedureKey
            )
            & (experiments_by_type["_pipeline"] == derived_parameters_ex.pipelineKey)
        ),
    )
    experiments_vs_derivations: DataFrame = experiments_vs_derivations.withColumn(
        "derivationInput",
        concat(col("derivationInput"), lit("$"), col(parameter_value_column)),
    )
    return (
        experiments_vs_derivations.drop(parameter_type, "_procedureID", "_pipeline")
        if parameter_type != "seriesParameter"
        else experiments_vs_derivations.drop(
            "value", "_parameterID", "_procedureID", "_pipeline"
        )
    )


def _check_complete_input(input_list: List[str], input_str: str):
    complete = len(input_list) > 0
    for input_param in input_list:
        complete = complete and (input_param in input_str)
    return complete and "NOT_FOUND" not in input_str


def _merge_simple_parameters(simple_parameters: List[Dict], results: [Dict]):
    merged_array = []
    if results is None or simple_parameters is None:
        return simple_parameters
    result_parameter_keys = {result["_parameterID"]: result for result in results}
    for simple_parameter in simple_parameters:
        parameter_id = simple_parameter["_parameterID"]
        if parameter_id in result_parameter_keys:
            merged_array.append(result_parameter_keys[parameter_id])
        else:
            merged_array.append(simple_parameter)
    simple_parameter_keys = {
        simple_parameter["_parameterID"]: simple_parameter
        for simple_parameter in simple_parameters
    }
    for result in results:
        parameter_id = result["_parameterID"]
        if parameter_id not in simple_parameter_keys:
            merged_array.append(result)
    return merged_array


if __name__ == "__main__":
    sys.exit(main(sys.argv))
