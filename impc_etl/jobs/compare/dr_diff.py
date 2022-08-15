from pyspark.sql.functions import concat_ws, col, lit, when, explode, collect_set, size


from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from impc_etl.jobs.load.observation_mapper import ExperimentToObservationMapper
from impc_etl.jobs.transform import ExperimentBWAgeCalculator
from impc_etl.workflow.config import ImpcConfig


class ImpcDrDiffReportGeneration(PySparkTask):
    """
    PySpark task that take sin the Observations parquet and filter
    them to get the relevant information for the images pipeline.
    """

    name = "IMPC_DR_Diff_Report_Generation"
    previous_dr_observations_parquet_path = luigi.Parameter()
    previous_dr_tag = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [ExperimentToObservationMapper(), ExperimentBWAgeCalculator()]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}dr{self.previous_dr_tag}_diff_csv"
        )

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.previous_dr_observations_parquet_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        observations_current_parquet_path = args[0]
        experiment_parquet_path = args[1]
        observations_previous_parquet_path = args[2]
        output_path = args[3]
        spark = SparkSession(sc)

        observations_df = spark.read.parquet(observations_current_parquet_path)
        observations_previous_df = spark.read.parquet(
            observations_previous_parquet_path
        )
        exp_df = spark.read.parquet(experiment_parquet_path)
        experiment_list = exp_df.select(
            col("_centreID").alias("phenotyping_center"),
            col("_pipeline").alias("pipeline_stable_id"),
            col("_procedureID").alias("procedure_stable_id"),
            col("_sequenceID").alias("procedure_sequence_id"),
            col("specimenID").alias("external_sample_id"),
            explode(col("procedureMetadata.parameterStatus")).alias("parameter_status"),
        )
        experiment_list = experiment_list.groupBy(
            [
                col_name
                for col_name in experiment_list.columns
                if col_name != "parameter_status"
            ]
        ).agg(collect_set("parameter_status").alias("parameter_status"))
        experiment_list = experiment_list.withColumn(
            "has_status", size("parameter_status") > 0
        )
        experiment_with_status_list = experiment_list.where(
            col("has_status") == True
        ).distinct()
        experiments = (
            observations_df.where(col("datasource_name") == "IMPC")
            .select(
                "phenotyping_center",
                "pipeline_stable_id",
                "procedure_stable_id",
                "procedure_sequence_id",
                "external_sample_id",
            )
            .distinct()
        )
        experiments_previous = (
            observations_previous_df.where(col("datasource_name") == "IMPC")
            .select(
                "phenotyping_center",
                "pipeline_stable_id",
                "procedure_stable_id",
                "procedure_sequence_id",
                "external_sample_id",
            )
            .distinct()
        )
        previous_current_diff = experiments_previous.subtract(experiments)
        experiment_with_status_list = experiment_with_status_list.alias("exp")
        previous_current_diff = previous_current_diff.alias("obs")
        previous_current_diff = previous_current_diff.join(
            experiment_with_status_list,
            (
                previous_current_diff["phenotyping_center"]
                == experiment_with_status_list["phenotyping_center"]
            )
            & (
                previous_current_diff["procedure_stable_id"]
                == experiment_with_status_list["procedure_stable_id"]
            )
            & (
                when(
                    previous_current_diff["procedure_sequence_id"].isNull()
                    & experiment_with_status_list["procedure_sequence_id"].isNull(),
                    lit(True),
                )
                .when(
                    (
                        previous_current_diff["procedure_sequence_id"].isNotNull()
                        & experiment_with_status_list["procedure_sequence_id"].isNull()
                    )
                    | (
                        previous_current_diff["procedure_sequence_id"].isNull()
                        & experiment_with_status_list[
                            "procedure_sequence_id"
                        ].isNotNull()
                    ),
                    lit(False),
                )
                .otherwise(
                    previous_current_diff["procedure_sequence_id"]
                    == experiment_with_status_list["procedure_sequence_id"]
                )
            )
            & (
                previous_current_diff["external_sample_id"]
                == experiment_with_status_list["external_sample_id"]
            ),
            "left_outer",
        ).select(
            "obs.*",
            concat_ws(";", "exp.parameter_status").alias("parameter_statuses"),
            "exp.has_status",
        )
        previous_current_diff.repartition(1).write.csv(output_path, header=True)
