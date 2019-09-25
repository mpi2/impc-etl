import hashlib
import math
from datetime import datetime
from typing import List, Dict

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    explode_outer,
    col,
    concat,
    md5,
    explode,
    concat_ws,
    collect_list,
    create_map,
    sort_array,
    collect_set,
    struct,
    max,
    first,
)
from pyspark.sql.types import (
    BooleanType,
    ArrayType,
    StructType,
    StructField,
    IntegerType,
    Row,
)

from impc_etl.shared.transformations.commons import *
from impc_etl.shared.utils import extract_parameters_from_derivation, unix_time_millis


def map_centre_id(dcc_experiment_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "_centreID", udf(map_centre_ids, StringType())("_centreID")
    )
    return dcc_experiment_df


def map_project_id(dcc_experiment_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "_project", udf(_map_project_ids, StringType())("_project")
    )
    return dcc_experiment_df


def _map_project_ids(centre_id: str):
    return (
        Constants.PROJECT_ID_MAP[centre_id.lower()] if centre_id is not None else None
    )


def standarize_europhenome_experiments(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "specimenID",
        when(
            dcc_df["_dataSource"] == "EuroPhenome",
            udf(truncate_specimen_id, StringType())(dcc_df["specimenID"]),
        ).otherwise(dcc_df["specimenID"]),
    )
    return dcc_df


def drop_skipped_experiments(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.where(
        ~(
            (dcc_df["_centreID"] == "Ucd")
            & (
                (dcc_df["_experimentID"] == "GRS_2013-10-09_4326")
                | (dcc_df["_experimentID"] == "GRS_2014-07-16_8800")
            )
        )
    )


def drop_skipped_procedures(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.where(
        (udf(_skip_procedure, BooleanType())(dcc_df["_procedureID"]))
        | (dcc_df["_dataSource"] == "3i")
    )


def _skip_procedure(procedure_name: str) -> bool:
    return (
        procedure_name[: procedure_name.rfind("_")] not in Constants.SKIPPED_PROCEDURES
    )


def standarize_3i_experiments(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.withColumn(
        "_project",
        when(
            (dcc_df["_dataSource"] == "3i")
            & (~dcc_df["_project"].isin(Constants.VALID_PROJECT_IDS)),
            lit("MGP"),
        ).otherwise(dcc_df["_project"]),
    )


def drop_null_centre_id(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, "_centreID")


def drop_null_data_source(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, "_dataSource")


def drop_null_date_of_experiment(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, "_dateOfExperiment")


def drop_null_pipeline(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, "_pipeline")


def drop_null_project(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, "_project")


def drop_null_specimen_id(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, "specimenID")


def _drop_if_null(dcc_df: DataFrame, column: str) -> DataFrame:
    return dcc_df.where(dcc_df[column].isNotNull())


def generate_unique_id(dcc_experiment_df: DataFrame):
    non_unique_columns = [
        "_type",
        "_sourceFile",
        "_VALUE",
        "procedureMetadata",
        "statusCode",
        "_sequenceID",
        "_project",
    ]
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "_sequenceIDStr",
        when(col("_sequenceID").isNull(), lit("NA")).otherwise(col("_sequenceID")),
    )
    unique_columns = [
        col_name
        for col_name in dcc_experiment_df.columns
        if col_name not in non_unique_columns and not col_name.endswith("Parameter")
    ]
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "unique_id", md5(concat(*unique_columns))
    )
    return dcc_experiment_df


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


def generate_metadata_group(
    experiment_specimen_df: DataFrame, impress_df: DataFrame
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
        experiment_metadata["procedureMetadata._parameterID"]
        == impress_df_required["parameter.parameterKey"],
    )
    experiment_metadata = experiment_metadata.withColumn(
        "metadataItem",
        when(
            col("procedureMetadata.value").isNotNull(),
            concat(col("parameter.name"), lit(" = "), col("procedureMetadata.value")),
        ).otherwise(lit("")),
    )
    window = Window.partitionBy(
        "unique_id", "_productionCentre", "_phenotypingCentre"
    ).orderBy("parameter.name")
    experiment_metadata_input = experiment_metadata.withColumn(
        "metadataItems", collect_set(col("metadataItem")).over(window)
    )
    experiment_metadata = experiment_metadata_input.groupBy(
        "unique_id", "_productionCentre", "_phenotypingCentre"
    ).agg(
        concat_ws("::", sort_array(max(col("metadataItems")))).alias(
            "metadataGroupList"
        )
    )
    experiment_metadata = experiment_metadata.withColumn(
        "metadataGroupList",
        when(
            (col("_productionCentre").isNotNull())
            & (col("_productionCentre") != col("_phenotypingCentre")),
            concat(
                col("metadataGroupList"),
                lit("::ProductionCenter = "),
                col("_productionCentre"),
            ),
        ).otherwise(col("metadataGroupList")),
    )
    experiment_metadata = experiment_metadata.withColumn(
        "metadataGroup", md5(col("metadataGroupList"))
    )
    experiment_metadata = experiment_metadata.drop("metadataGroupList")
    experiment_specimen_df = experiment_specimen_df.join(
        experiment_metadata,
        ["unique_id", "_productionCentre", "_phenotypingCentre"],
        "left_outer",
    )
    experiment_specimen_df = experiment_specimen_df.withColumn(
        "metadataGroup",
        when(experiment_specimen_df["metadataGroup"].isNull(), md5(lit(""))).otherwise(
            experiment_specimen_df["metadataGroup"]
        ),
    )
    return experiment_specimen_df


def generate_metadata(
    experiment_specimen_df: DataFrame, impress_df: DataFrame
) -> DataFrame:
    experiment_metadata = experiment_specimen_df.withColumn(
        "procedureMetadata", explode("procedureMetadata")
    )
    impress_df_required = impress_df.where(
        (col("parameter.type") == "procedureMetadata")
    )
    experiment_metadata = experiment_metadata.join(
        impress_df_required,
        experiment_metadata["procedureMetadata._parameterID"]
        == impress_df_required["parameter.parameterKey"],
    )
    output_metadata = StructType(
        [
            StructField("_parameterID", StringType(), True),
            StructField("parameterStatus", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )
    process_experimenter_id_udf = udf(_process_experimenter_id, output_metadata)
    experiment_metadata = experiment_metadata.withColumn(
        "procedureMetadata",
        when(
            lower(col("parameter.name")).contains("experimenter"),
            process_experimenter_id_udf("procedureMetadata"),
        ).otherwise(col("procedureMetadata").cast(output_metadata)),
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
    experiment_metadata = experiment_metadata.groupBy(
        "unique_id", "_productionCentre", "_phenotypingCentre"
    ).agg(sort_array(collect_set(col("metadataItem"))).alias("metadata"))
    experiment_metadata = experiment_metadata.withColumn(
        "metadata",
        when(
            (col("_productionCentre").isNotNull())
            & (col("_productionCentre") != col("_phenotypingCentre")),
            udf(_append_phenotyping_centre_to_metadata, ArrayType(StringType()))(
                col("metadata"), col("_productionCentre")
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
    return experimenter_metadata


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
        col("_dateOfExperiment").alias("weightDate"),
        col("simpleParameter._parameterID").alias("weightParameterID"),
        col("simpleParameter.value").alias("weightValue"),
    )
    weight_observations = weight_observations.where(col("weightValue").isNotNull())
    weight_observations = weight_observations.join(
        mice_df, weight_observations["specimenID"] == mice_df["_specimenID"]
    )
    weight_observations = weight_observations.withColumn(
        "weightDaysOld", udf(calculate_age_in_days, StringType())("weightDate", "_DOB")
    )
    weight_observations = weight_observations.groupBy("specimenID").agg(
        collect_set(
            struct("weightDate", "weightParameterID", "weightValue", "weightDaysOld")
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
            StructField("weightDate", StringType()),
            StructField("weightParameterID", StringType()),
            StructField("weightValue", StringType()),
            StructField("weightDaysOld", StringType()),
        ]
    )
    experiment_df_a = dcc_experiment_df.alias("exp")
    mice_df_a = mice_df.alias("mice")
    dcc_experiment_df = experiment_df_a.join(
        mice_df_a,
        dcc_experiment_df["specimenID"] == mice_df["_specimenID"],
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
        experiment_df_a["specimenID"] == mice_df_a["_specimenID"],
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
    experiment_date: str, procedure_group: str, specimen_weights: List[Dict]
) -> Dict:
    if specimen_weights is None or len(specimen_weights) == 0:
        return {
            "weightDate": None,
            "weightValue": None,
            "weightParameterID": None,
            "weightDaysOld": None,
        }
    experiment_date = datetime.strptime(experiment_date, "%Y-%m-%d")
    nearest_weight = None
    nearest_diff = None
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
        candidate_weight_value = float(candidate_weight["weightValue"])
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
        return nearest_weight
    else:
        return {
            "weightDate": None,
            "weightValue": None,
            "weightParameterID": None,
            "weightDaysOld": None,
        }


def get_derived_parameters(
    spark: SparkSession, dcc_experiment_df: DataFrame, impress_df: DataFrame
) -> DataFrame:
    derived_parameters: DataFrame = impress_df.where(
        (impress_df["parameter.isDerived"] == True)
        & (impress_df["parameter.isDeprecated"] == False)
        & (~impress_df["parameter.derivation"].contains("archived"))
    ).select(
        "procedure.procedureKey",
        "parameter.parameterKey",
        "parameter.derivation",
        "parameter.type",
        "unitName",
    ).dropDuplicates()

    extract_parameters_from_derivation_udf = udf(
        extract_parameters_from_derivation, ArrayType(StringType())
    )
    derived_parameters = derived_parameters.withColumn(
        "derivationInputs", extract_parameters_from_derivation_udf("derivation")
    )

    derived_parameters_ex = derived_parameters.withColumn(
        "derivationInput", explode("derivationInputs")
    ).select("procedureKey", "parameterKey", "derivation", "derivationInput")
    derived_parameters_ex = derived_parameters_ex.where(
        ~col("derivation").contains("unimplemented")
    )

    experiments_simple = _get_inputs_by_parameter_type(
        dcc_experiment_df, derived_parameters_ex, "simpleParameter"
    )
    experiments_metadata = _get_inputs_by_parameter_type(
        dcc_experiment_df, derived_parameters_ex, "procedureMetadata"
    )
    experiments_series = _get_inputs_by_parameter_type(
        dcc_experiment_df, derived_parameters_ex, "seriesParameter"
    )
    experiments_vs_derivations = experiments_simple.union(experiments_metadata).union(
        experiments_series
    )

    experiments_vs_derivations = experiments_vs_derivations.groupby(
        "unique_id", "parameterKey", "derivation"
    ).agg(
        concat_ws(
            ",",
            collect_list(
                when(
                    experiments_vs_derivations["derivationInput"].isNull(),
                    lit("NOT_FOUND"),
                ).otherwise(experiments_vs_derivations["derivationInput"])
            ),
        ).alias("derivationInputStr")
    )

    provided_derivations = dcc_experiment_df.withColumn(
        "simpleParameter", explode("simpleParameter")
    )
    provided_derivations = provided_derivations.join(
        derived_parameters_ex,
        col("simpleParameter._parameterID") == col("parameterKey"),
        "left_outer",
    )
    provided_derivations = (
        provided_derivations.where(col("parameterKey").isNotNull())
        .select("unique_id", "parameterKey")
        .dropDuplicates()
    )

    provided_derivations = provided_derivations.alias("provided")

    experiments_vs_derivations = (
        experiments_vs_derivations.join(
            provided_derivations, ["parameterKey", "unique_id"], "left_outer"
        )
        .where(col("provided.unique_id").isNull())
        .drop("provided.*")
    )
    experiments_vs_derivations = experiments_vs_derivations.join(
        derived_parameters.drop("derivation"), "parameterKey"
    )

    experiments_vs_derivations = experiments_vs_derivations.withColumn(
        "isComplete",
        udf(_check_complete_input, BooleanType())(
            "derivationInputs", "derivationInputStr"
        ),
    )
    complete_derivations = experiments_vs_derivations.where(
        col("isComplete") == True
    ).withColumn(
        "derivationInputStr", concat("derivation", lit(";"), "derivationInputStr")
    )
    complete_derivations.createOrReplaceTempView("complete_derivations")
    spark.udf.registerJavaFunction(
        "phenodcc_derivator",
        "org.mousephenotype.dcc.derived.parameters.SparkDerivator",
        StringType(),
    )
    results_df = spark.sql(
        """
           SELECT unique_id, procedureKey, parameterKey,
                  derivationInputStr, phenodcc_derivator(derivationInputStr) as result
           FROM complete_derivations
        """
    )

    results_df = results_df.join(
        derived_parameters, ["parameterKey", "procedureKey"], "left"
    )

    results_df = results_df.groupBy("unique_id", "procedureKey").agg(
        collect_list(
            create_map(
                lit("parameter"),
                results_df["parameterKey"],
                lit("value"),
                results_df["result"],
                lit("unit"),
                results_df["unitName"],
            )
        ).alias("results")
    )
    results_df = results_df.withColumnRenamed("unique_id", "unique_id_result")

    simple_parameter_type = None

    for c_type in dcc_experiment_df.dtypes:
        if c_type[0] == "simpleParameter":
            simple_parameter_type = c_type[1]
            break

    dcc_experiment_df = dcc_experiment_df.join(
        results_df,
        (dcc_experiment_df["unique_id"] == results_df["unique_id_result"])
        & (dcc_experiment_df["_procedureID"] == results_df["procedureKey"]),
        "left_outer",
    )
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "simpleParameter",
        when(
            results_df["results"].isNotNull(),
            udf(_append_simple_parameter, simple_parameter_type)(
                "results", "simpleParameter"
            ),
        ).otherwise(col("simpleParameter").cast(simple_parameter_type)),
    )
    dcc_experiment_df = (
        dcc_experiment_df.drop("complete_derivations.unique_id")
        .drop("unique_id_result")
        .drop("results")
    )

    return dcc_experiment_df


def _get_inputs_by_parameter_type(
    dcc_experiment_df, derived_parameters_ex, parameter_type
):
    experiments_by_type = dcc_experiment_df.select(
        "unique_id", "_procedureID", explode(parameter_type).alias(parameter_type)
    )
    if parameter_type == "seriesParameter":
        experiments_by_type = experiments_by_type.select(
            "unique_id",
            "_procedureID",
            col(parameter_type + "._parameterID").alias("_parameterID"),
            explode(parameter_type + ".value").alias("value"),
        )
        experiments_by_type = experiments_by_type.withColumn(
            "value", concat(col("value._incrementValue"), lit("|"), col("value._VALUE"))
        )
        experiments_by_type = experiments_by_type.groupBy(
            "unique_id", "_parameterID", "_procedureID"
        ).agg(concat_ws("$", collect_list("value")).alias("value"))

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
            & (experiments_by_type._procedureID == derived_parameters_ex.procedureKey)
        ),
    )
    experiments_vs_derivations: DataFrame = experiments_vs_derivations.withColumn(
        "derivationInput",
        concat(col("derivationInput"), lit("$"), col(parameter_value_column)),
    )
    return (
        experiments_vs_derivations.drop(parameter_type, "_procedureID")
        if parameter_type != "seriesParameter"
        else experiments_vs_derivations.drop("value", "_parameterID", "_procedureID")
    )


def _check_complete_input(input_list: List[str], input_str: str):
    complete = len(input_list) > 0
    for input_param in input_list:
        complete = complete and (input_param in input_str)
    return complete and "NOT_FOUND" not in input_str


def _append_simple_parameter(results: List[Dict], simple_parameter: List):
    if results is None:
        return simple_parameter
    for result in results:
        if simple_parameter is not None and result is not None:
            simple_parameter.append(
                {
                    "_parameterID": result["parameter"],
                    "value": result["value"],
                    "_sequenceID": None,
                    "_unit": result["unit"],
                    "parameterStatus": None,
                }
            )
    return simple_parameter


def drop_null_procedure_id(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, "_procedureID")
