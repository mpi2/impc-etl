import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit, when

from impc_etl.jobs.load.observation_mapper import ExperimentToObservationMapper
from impc_etl.workflow.config import ImpcConfig


class StatsPipelineInputMapper(PySparkTask):
    """
    PySpark task to generate the Statistical Analysis Input.
    It basically flattens the Observations multivalued columsn so they can be processed on R.

    This task depends on:

    - `impc_etl.workflow.load.PipelineCoreLoader`
    """

    #: Name of the Spark task
    name: str = "IMPC_Statistical_Analysis_Input_Mapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return ExperimentToObservationMapper()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}flatten_observations_parquet"
        )

    def app_options(self):
        return [self.input().path, self.output_path]

    def main(self, sc, *args):
        observations_parquet_path = args[1]
        output_path = args[2]
        spark = SparkSession(sc)
        observations_df = spark.read.parquet(observations_parquet_path)
        flatten_observations_df = observations_df
        for d_type in observations_df.dtypes:
            if d_type[1] == "array<string>":
                flatten_observations_df = flatten_observations_df.withColumn(
                    d_type[0], concat_ws("::", d_type[0])
                )
                flatten_observations_df = flatten_observations_df.withColumn(
                    d_type[0],
                    when(col(d_type[0]).isNotNull(), col(d_type[0])).otherwise(
                        lit(None)
                    ),
                )
        flatten_observations_df.write.parquet(f"{output_path}_temp/")
        flatten_observations__temp = spark.read.parquet(f"{output_path}_temp/")
        flatten_observations__temp.write.parquet(output_path)
