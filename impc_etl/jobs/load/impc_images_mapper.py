from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from impc_etl.jobs.load.observation_mapper import ExperimentToObservationMapper
from impc_etl.workflow.config import ImpcConfig


class ImagesPipelineInputGenerator(PySparkTask):
    """
    PySpark task that take sin the Observations parquet and filter
    them to get the relevant information for the images pipeline.
    """

    name = "IMPC_Images_Pipeline_Input_Generation"
    output_path = luigi.Parameter()

    def requires(self):
        return ExperimentToObservationMapper()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}images_pipeline_input_csv")

    def app_options(self):
        return [self.input().path, self.output().path]

    def main(self, sc: SparkContext, *args: Any):
        observations_parquet_path = args[0]
        output_path = args[1]
        spark = SparkSession(sc)
        observations_df: DataFrame = spark.read.parquet(observations_parquet_path)
        observations_df.where(
            (col("observation_type") == "image_record")
            & (col("download_file_path").contains("mousephenotype.org"))
            & (~col("download_file_path").like("%.mov"))
            & (~col("download_file_path").like("%.bz2"))
        ).select(
            "observation_id",
            "increment_value",
            "download_file_path",
            "phenotyping_center",
            "pipeline_stable_id",
            "procedure_stable_id",
            "datasource_name",
            "parameter_stable_id",
        ).repartition(
            1
        ).write.csv(
            output_path, header=True
        )
