"""

Luigi PySpark task that takes the colonies tracking system
report and returns it ready to be used on the rest of the ETL.

The cleaning process includes the mapping of legacy colony IDs to newer nomenclature and the generation
of the string representation of the genetic background.

"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, concat
from pyspark.sql.types import StringType

from impc_etl.config.constants import Constants
from impc_etl.jobs.extract import ColonyTrackingExtractor
from impc_etl.shared import utils
from impc_etl.workflow.config import ImpcConfig


class IMPCColonyCleaner(PySparkTask):
    """
    PySpark Task to clean some legacy colony  identifiers so they match the provided on the legacy  Specimen XML files.
    """

    #: Name of the Spark task
    name = "IMPC_Colony_Cleaner"

    #: Path of the output directory where ethe new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return ColonyTrackingExtractor

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/colonies_tracking_clean_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}colonies_tracking_clean_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input().path(),
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        colonies_tracking_parquet_path = args[0]
        output_path = args[1]
        spark = SparkSession.builder.getOrCreate()
        colonies_df = spark.read.parquet(colonies_tracking_parquet_path)
        specimen_clean_df = self.clean_colonies(colonies_df)
        specimen_clean_df.write.mode("overwrite").parquet(output_path)

    def clean_colonies(self, colonies_df: DataFrame) -> DataFrame:
        """
        DCC colonies cleaner. Takes in a DataFrame containing the full list of colonies on the Colony Trakcking Report
        coming form GenTar and returns the  same list
        of colonies applying some sanitizing to legacy  colony identifiers.
        """
        # Maps Centre IDs and Consortium IDs  to the ones used on the website
        colonies_df = colonies_df.transform(self.map_colonies_df_ids)
        # colonies_df = map_strain_names(colonies_df)

        # Generate genetic background String by using the backgroudn strain
        colonies_df = colonies_df.transform(self.generate_genetic_background)
        return colonies_df

    def map_colonies_df_ids(self, colonies_df: DataFrame) -> DataFrame:
        """
        Takes in a DataFrame containing the columns phenotyping_centre, production_centre, phenotyping_consortium and
        production_consortium and maps them using a dictionary provided in the constants provided in `impc_etl.config.constants.Constants.CENTRE_ID_MAP` and `impc_etl.config.constants.Constants.PROJECT_ID_MAP`
        """
        colonies_df = colonies_df.withColumn(
            "phenotyping_centre",
            udf(utils.map_centre_id, StringType())("phenotyping_centre"),
        )
        colonies_df = colonies_df.withColumn(
            "production_centre",
            udf(utils.map_centre_id, StringType())("production_centre"),
        )
        colonies_df = colonies_df.withColumn(
            "phenotyping_consortium",
            udf(utils.map_project_id, StringType())("phenotyping_consortium"),
        )
        colonies_df = colonies_df.withColumn(
            "production_consortium",
            udf(utils.map_project_id, StringType())("production_consortium"),
        )
        return colonies_df

    def map_strain_names(self, colonies_df: DataFrame) -> DataFrame:
        """
        Takes in  a
        """
        map_strain_name_udf = udf(self.map_strain_name, StringType())
        colonies_df = colonies_df.withColumn(
            "colony_background_strain", map_strain_name_udf("colony_background_strain")
        )
        return colonies_df

    def generate_genetic_background(self, colonies_df: DataFrame) -> DataFrame:
        colonies_df = colonies_df.withColumn(
            "genetic_background",
            concat(lit("involves: "), col("colony_background_strain")),
        )
        return colonies_df

    def map_strain_name(self, strain_name: str) -> str:
        if strain_name is None:
            return None

        if "_" in strain_name:
            intermediate_backgrounds = strain_name.split("_")
        elif ";" in strain_name:
            intermediate_backgrounds = strain_name.split(";")
        elif strain_name == "Balb/c.129S2":
            intermediate_backgrounds = "BALB/c;129S2/SvPas".split(";")
        elif strain_name in ["B6N.129S2.B6J", "B6J.129S2.B6N", "B6N.B6J.129S2"]:
            intermediate_backgrounds = "C57BL/6N;129S2/SvPas;C57BL/6J".split(";")
        elif strain_name == "B6J.B6N":
            intermediate_backgrounds = "C57BL/6J;C57BL/6N".split(";")
        else:
            intermediate_backgrounds = [strain_name]
        intermediate_backgrounds = [
            Constants.BACKGROUND_STRAIN_MAPPER[strain]
            if strain in Constants.BACKGROUND_STRAIN_MAPPER.keys()
            else strain
            for strain in intermediate_backgrounds
        ]

        return ";".join(intermediate_backgrounds)
