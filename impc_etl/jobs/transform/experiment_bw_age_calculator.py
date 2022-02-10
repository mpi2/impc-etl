"""
    Luigi PySpark task to process experiments in order to calculate the age of the
    specimen and the associated BW information for a given data point.
"""
import math
from datetime import datetime
from typing import List, Dict, Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode_outer,
    col,
    datediff,
    collect_set,
    struct,
    udf,
    when,
    expr,
    regexp_extract,
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

from impc_etl.jobs.extract import ImpressExtractor
from impc_etl.jobs.transform.experiment_parameter_derivator import (
    SpecimenLevelExperimentParameterDerivator,
)
from impc_etl.jobs.transform.specimen_cross_ref import MouseSpecimenCrossRef
from impc_etl.shared.utils import (
    unix_time_millis,
)
from impc_etl.workflow.config import ImpcConfig


class ExperimentBWAgeCalculator(PySparkTask):
    """
    PysPark task to calculate age of specimen and BW data associations for a given experiment.
    This task depends on:

    - `impc_etl.jobs.transform.experiment_parameter_derivator.SpecimenLevelExperimentParameterDerivator`
    - `impc_etl.jobs.transform.specimen_cross_ref.MouseSpecimenCrossRef`
    - `impc_etl.jobs.extract.impress_extractor.ImpressExtractor`
    """

    #: Name of the Spark task
    name: str = "IMPC_Experiment_ADD_BW_AGE_Processor"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return [
            SpecimenLevelExperimentParameterDerivator(),
            MouseSpecimenCrossRef(),
            ImpressExtractor(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/specimen_level_experiment_with_bw_age_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}specimen_level_experiment_with_bw_age_parquet"
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

    def main(self, sc: SparkContext, *args: Any):
        """
        Loads the given the Experiment parquet, the Specimen parquet and the Impress parquet,
        and uses them to calculate the age of the specimen at any given experiment date and to associate
        BW data to every experiment when possible.
        """
        spark = SparkSession(sc)
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")
        experiment_parquet_path = args[0]
        mouse_parquet_path = args[1]
        pipeline_parquet_path = args[2]
        output_path = args[3]
        experiment_df = spark.read.parquet(experiment_parquet_path)
        mouse_df = spark.read.parquet(mouse_parquet_path)
        pipeline_df = spark.read.parquet(pipeline_parquet_path)
        experiment_df = self.get_associated_body_weight(
            experiment_df, mouse_df, pipeline_df
        )
        experiment_df = self.generate_age_information(experiment_df, mouse_df)
        experiment_df.write.parquet(output_path)

    def get_associated_body_weight(
        self,
        specimen_level_experiment_df: DataFrame,
        mouse_specimen_df: DataFrame,
        impress_df: DataFrame,
    ) -> DataFrame:
        """
        Takes in DataFrame with Experimental data, one with Mouse Specimens and one with Impress information,
        and applies the algorithm to select the associated BW to any given experiment
        and calculate the age of experiment for the selected BW measurement.
        """
        # Explode the nested experiment DF structure so every row represents an observation
        weight_observations: DataFrame = specimen_level_experiment_df.withColumn(
            "simpleParameter", explode_outer("simpleParameter")
        )

        # Select the parameter relevant pieces from the IMPReSS DF
        parameters = impress_df.select(
            "pipelineKey",
            "procedure.procedureKey",
            "parameter.parameterKey",
            "parameter.analysisWithBodyweight",
        ).distinct()

        # Filter the IMPReSS using the analysisWithBodyweight flag
        weight_parameters = parameters.where(
            col("analysisWithBodyweight").isin(
                ["is_body_weight", "is_fasted_body_weight"]
            )
        )

        # Join both the  observations DF and the BW parameters DF to obtain the observations that are BW
        weight_observations = weight_observations.join(
            weight_parameters,
            (
                (weight_observations["_pipeline"] == weight_parameters["pipelineKey"])
                & (
                    weight_observations["_procedureID"]
                    == weight_parameters["procedureKey"]
                )
                & (
                    weight_observations["simpleParameter._parameterID"]
                    == weight_parameters["parameterKey"]
                )
            ),
        )
        # Create a boolean flag for fasted BW procedures
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

        # Join the body weight observations so we can determine the  age of the specimen for any BW measurement
        weight_observations = weight_observations.join(
            mouse_specimen_df,
            (weight_observations["specimenID"] == mouse_specimen_df["_specimenID"])
            & (weight_observations["_centreID"] == mouse_specimen_df["_centreID"]),
        )
        weight_observations = weight_observations.withColumn(
            "weightDaysOld", datediff("weightDate", "_DOB")
        )

        #  Group the weight observations by Specimen
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

        # Create a temporary "procedureGroup" column to be used in the  BW selection
        specimen_level_experiment_df = specimen_level_experiment_df.withColumn(
            "procedureGroup",
            udf(lambda prod_id: prod_id[: prod_id.rfind("_")], StringType())(
                col("_procedureID")
            ),
        )

        # Join all the observations with the BW observations grouped by specimen
        specimen_level_experiment_df = specimen_level_experiment_df.join(
            weight_observations, "specimenID", "left_outer"
        )
        # Schema for the struct that is going to group all the associated BW data
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

        # Alias both the experiment and the specimen df so is easier to join and manipulate
        experiment_df_a = specimen_level_experiment_df.alias("exp")
        mice_df_a = mouse_specimen_df.alias("mice")

        specimen_level_experiment_df = experiment_df_a.join(
            mice_df_a,
            (
                specimen_level_experiment_df["specimenID"]
                == mouse_specimen_df["_specimenID"]
            )
            & (
                specimen_level_experiment_df["_centreID"]
                == mouse_specimen_df["_centreID"]
            ),
            "left_outer",
        )

        # Add special dates to the experiment x specimen dataframe
        # for some experiments the date of sacrifice or date of blood collection
        # has to be used as reference for age and  BW calculations
        specimen_level_experiment_df = self._add_special_dates(
            specimen_level_experiment_df
        )
        get_associated_body_weight_udf = udf(
            self._get_closest_weight, output_weight_schema
        )
        specimen_level_experiment_df = specimen_level_experiment_df.withColumn(
            "weight",
            get_associated_body_weight_udf(
                when(
                    col("_dateOfBloodCollection").isNotNull(),
                    col("_dateOfBloodCollection"),
                )
                .when(col("_dateOfSacrifice").isNotNull(), col("_dateOfSacrifice"))
                .otherwise(col("_dateOfExperiment")),
                col("procedureGroup"),
                col("weight_observations"),
            ),
        )
        specimen_level_experiment_df = specimen_level_experiment_df.select(
            "exp.*", "weight"
        )
        return specimen_level_experiment_df

    def generate_age_information(
        self, dcc_experiment_df: DataFrame, mice_df: DataFrame
    ):
        experiment_df_a = dcc_experiment_df.alias("exp")
        mice_df_a = mice_df.alias("mice")
        dcc_experiment_df = experiment_df_a.join(
            mice_df_a,
            (experiment_df_a["specimenID"] == mice_df["_specimenID"])
            & (experiment_df_a["_centreID"] == mice_df["_centreID"]),
            "left_outer",
        )
        dcc_experiment_df = self._add_special_dates(dcc_experiment_df)
        dcc_experiment_df = dcc_experiment_df.withColumn(
            "ageInDays",
            datediff(
                when(
                    col("_dateOfBloodCollection").isNotNull(),
                    col("_dateOfBloodCollection"),
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
        self,
        experiment_date: datetime.date,
        procedure_group: str,
        specimen_weights: List[Row],
    ) -> Dict:
        """
        Takes in date of experiment, a procedure group, and a set of BW observations
        candidates and chooses the closest one.
        The algorithm first tries to get a BW that is on the same procedure group as the given experiment.
        It also takes into consideration if there is any fasted weight on the Specimen BW observations, if any,
        the whole BW observations set should be replaced with only
        the fasted BWs, this is because if the procedure has a fasted BW,
        this means any observation on the procedure should be associated with that
        fasted value instead of using any other, even when they are closer. After that it selects a list of candidate
        weights and tries to find the closest BW that belongs to the same procedure first. If it doesn't find any,
        it tries with BW observations coming from the _BWT procedures, if there is none _BWT procedure observations,
        it looks for any other procedures. Finally, the  algorithm limits the maximum distance for between the closest
        BW observation anf the experiment date to 5 days, if there is not suitable BW assocition in the 5 days window
        around the experiment date, the BW association is marked as null.
        """
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

    def _add_special_dates(self, dcc_experiment_df: DataFrame):
        """
        Takes in a DataFrame with experimental data, parses out the metadata values for special dates,
        and adds those values as new columns.
        """
        for col_name, date_prefixes in {
            "_dateOfBloodCollection": [
                "date and time of blood collection = ",
                "date/time of blood collection = ",
            ],
            "_dateOfSacrifice": [
                "date and time of sacrifice = ",
                "date of sacrifice = ",
            ],
        }.items():
            escaped_prefixes = [prefix.replace("/", ".") for prefix in date_prefixes]
            prefix_regex = f"(?i)(.*)({'|'.join(escaped_prefixes)})(.*)"
            dcc_experiment_df = dcc_experiment_df.withColumn(
                col_name + "Array",
                expr(
                    f'filter(metadata, metadataValue ->  metadataValue rlike "{prefix_regex}" )'
                ),
            )
            dcc_experiment_df = dcc_experiment_df.withColumn(
                col_name,
                regexp_extract(
                    col(col_name + "Array").getItem(0), prefix_regex, 3
                ).astype(DateType()),
            )
        return dcc_experiment_df
