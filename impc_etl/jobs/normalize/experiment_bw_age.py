import math
from datetime import datetime
from typing import List, Dict

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode_outer,
    col,
    datediff,
    collect_set,
    struct,
    udf,
    when,
    lit,
    expr,
    regexp_replace,
    to_date,
)
from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    IntegerType,
    StringType,
    Row,
    DateType,
)

from impc_etl.config.constants import Constants
from impc_etl.jobs.normalize.experiment_parameter_derivator import (
    ExperimentParameterDerivator,
)
from impc_etl.shared.utils import (
    unix_time_millis,
)
from impc_etl.workflow.config import ImpcConfig
from impc_etl.workflow.extraction import ImpressExtractor
from impc_etl.workflow.normalization import (
    MouseNormalizer,
)


class ExperimentBWAgeProcessor(PySparkTask):
    name = "IMPC_Experiment_BW_AGE_Processor"
    output_path = luigi.Parameter()

    def requires(self):
        return [ExperimentParameterDerivator(), MouseNormalizer(), ImpressExtractor()]

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}experiment_full_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.output().path,
        ]

    def main(self, sc, *args):
        spark = SparkSession(sc)
        experiment_parquet_path = args[0]
        mouse_parquet_path = args[1]
        pipeline_parquet_path = args[2]
        output_path = args[3]
        experiment_df = spark.read.parquet(experiment_parquet_path)
        mouse_df = spark.read.parquet(mouse_parquet_path)
        pipeline_df = spark.read.parquet(pipeline_parquet_path)
        experiment_df = get_associated_body_weight(experiment_df, mouse_df, pipeline_df)
        experiment_df = generate_age_information(experiment_df, mouse_df)
        experiment_df.write.parquet(output_path)


def get_associated_body_weight(
    dcc_experiment_df: DataFrame, mice_df: DataFrame, pipeline_df: DataFrame
):
    weight_observations: DataFrame = dcc_experiment_df.withColumn(
        "simpleParameter", explode_outer("simpleParameter")
    )
    parameters = pipeline_df.select(
        "pipelineKey",
        "procedure.procedureKey",
        "parameter.parameterKey",
        "parameter.analysisWithBodyweight",
    ).distinct()
    weight_parameters = parameters.where(
        col("analysisWithBodyweight").isin(["is_body_weight", "is_fasted_body_weight"])
    )
    weight_observations = weight_observations.join(
        weight_parameters,
        (
            (weight_observations["_pipeline"] == weight_parameters["pipelineKey"])
            & (weight_observations["_procedureID"] == weight_parameters["procedureKey"])
            & (
                weight_observations["simpleParameter._parameterID"]
                == weight_parameters["parameterKey"]
            )
        ),
    )
    weight_observations = weight_observations.withColumn(
        "weightFasted", col("analysisWithBodyweight") == "is_fasted_body_weight"
    )
    weight_observations = weight_observations.select(
        "specimenID",
        "_centreID",
        col("unique_id").alias("sourceExperimentId"),
        col("_dateOfExperiment").alias("weightDate"),
        col("simpleParameter._parameterID").alias("weightParameterID"),
        col("simpleParameter.value").alias("weightValue"),
        "weightFasted",
    )
    weight_observations = weight_observations.where(col("weightValue").isNotNull())
    weight_observations = weight_observations.join(
        mice_df,
        (weight_observations["specimenID"] == mice_df["_specimenID"])
        & (weight_observations["_centreID"] == mice_df["_centreID"]),
    )
    weight_observations = weight_observations.withColumn(
        "weightDaysOld", datediff("weightDate", "_DOB")
    )
    weight_observations = weight_observations.groupBy("specimenID").agg(
        collect_set(
            struct(
                "sourceExperimentId",
                "weightDate",
                "weightParameterID",
                "weightValue",
                "weightDaysOld",
                "weightFasted",
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
            StructField("weightDate", DateType()),
            StructField("weightParameterID", StringType()),
            StructField("weightValue", StringType()),
            StructField("weightDaysOld", IntegerType()),
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
    for col_name, date_prefix in {
        "_dateOfBloodCollection": "Date and time of blood collection = ",
        "_dateOfSacrifice": "Date and time of sacrifice = ",
    }.items():
        dcc_experiment_df = dcc_experiment_df.withColumn(
            col_name + "Array",
            expr(
                f"filter(metadata, metadataValue ->  metadataValue LIKE '{date_prefix}%' )"
            ),
        )
        dcc_experiment_df = dcc_experiment_df.withColumn(
            col_name,
            to_date(
                regexp_replace(
                    col(col_name + "Array").getItem(0),
                    date_prefix,
                    "",
                ),
                "yyyy-MM-ddTHH:mm:ss",
            ),
        )
    get_associated_body_weight_udf = udf(_get_closest_weight, output_weight_schema)
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "weight",
        get_associated_body_weight_udf(
            when(
                col("_dateOfBloodCollection").isNotNull(), col("_dateOfBloodCollection")
            )
            .when(col("_dateOfSacrifice").isNotNull(), col("_dateOfSacrifice"))
            .otherwise(col("_dateOfExperiment")),
            col("procedureGroup"),
            col("weight_observations"),
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
    for col_name, date_prefix in {
        "_dateOfBloodCollection": "Date and time of blood collection = ",
        "_dateOfSacrifice": "Date and time of sacrifice = ",
    }.items():
        dcc_experiment_df = dcc_experiment_df.withColumn(
            col_name + "Array",
            expr(
                f"filter(metadata, metadataValue ->  metadataValue LIKE '{date_prefix}%' )"
            ),
        )
        dcc_experiment_df = dcc_experiment_df.withColumn(
            col_name,
            to_date(
                regexp_replace(
                    col(col_name + "Array").getItem(0),
                    date_prefix,
                    "",
                ),
                "yyyy-MM-ddTHH:mm:ss",
            ),
        )
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "ageInDays",
        datediff(
            when(
                col("_dateOfBloodCollection").isNotNull(), col("_dateOfBloodCollection")
            )
            .when(col("_dateOfSacrifice").isNotNull(), col("_dateOfSacrifice"))
            .otherwise(col("_dateOfExperiment")),
            col("mice._DOB"),
        ),
    )
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "ageInWeeks",
        udf(lambda x: math.floor(x / 7) if x is not None else None, IntegerType())(
            col("ageInDays")
        ),
    )
    return dcc_experiment_df.select("exp.*", "ageInWeeks", "ageInDays")


def _get_closest_weight(
    experiment_date: datetime.date, procedure_group: str, specimen_weights: List[Row]
) -> Dict:
    if specimen_weights is None or len(specimen_weights) == 0:
        return {
            "sourceExperimentId": None,
            "weightDate": None,
            "weightValue": None,
            "weightParameterID": None,
            "weightDaysOld": None,
        }
    nearest_weight = None
    nearest_diff = None
    errors = []
    min_time = datetime.min.time()
    experiment_date_time = datetime.combine(experiment_date, min_time)
    fasted_weights = [w for w in specimen_weights if w["weightFasted"]]
    non_fasted_weights = [w for w in specimen_weights if not w["weightFasted"]]
    if len(fasted_weights) > 0:
        same_procedure_fasted_weights = [
            w for w in fasted_weights if procedure_group in w["weightParameterID"]
        ]
        if len(same_procedure_fasted_weights) > 0:
            specimen_weights = same_procedure_fasted_weights
        else:
            specimen_weights = non_fasted_weights
    for candidate_weight in specimen_weights:
        if (
            candidate_weight["weightValue"] == "null"
            or candidate_weight["weightDate"] == "null"
        ):
            continue
        candidate_weight_date = candidate_weight["weightDate"]
        candidate_weight_time = datetime.combine(candidate_weight_date, min_time)
        # TODO: TypeError: unsupported operand type(s) for -: 'datetime.date' and 'datetime.datetime'
        candidate_diff = abs(
            unix_time_millis(experiment_date_time)
            - unix_time_millis(candidate_weight_time)
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

        try:
            nearest_weight_value = float(nearest_weight["weightValue"])
        except ValueError:
            errors.append(
                "[PARSING] Failed to parse: "
                + str(nearest_weight["weightValue"])
                + " "
                + procedure_group
                + " "
                + str(experiment_date)
            )
            continue

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
