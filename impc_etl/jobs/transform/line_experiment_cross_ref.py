"""
    Luigi tasks to take care of cross-reference the Line level Experimental data with the specimen data,
    and the IMPReSS data.

    The general cross-reference process is:

    - Load parquet files (i.e. line level experiments, colony data and IMPReSS data) into data frames.
    - Join line level experiments with colony information
    - Adds project value for line level experiments using the colony's phenotyping consortium
    - Override the data source and the project info for MGP and MGP Legacy line experiments
    - Generate metadata group (a hash value to identify experiments with the same experimental conditions)
    - Generate metadata array (a list of "parameter = value" for metadata parameters in a given experiment)
    - Generates line allelic composition
    - Select the columns relevant to experimental data only
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

from impc_etl.jobs.clean import ColonyCleaner
from impc_etl.jobs.clean.experiment_cleaner import LineLevelExperimentCleaner
from impc_etl.jobs.extract import ImpressExtractor
from impc_etl.jobs.transform.cross_ref_helper import (
    generate_allelic_composition,
    override_europhenome_datasource,
)
from impc_etl.jobs.transform.specimen_experiment_cross_ref import (
    generate_metadata_group,
    generate_metadata,
)
from impc_etl.workflow.config import ImpcConfig


class LineLevelExperimentCrossRef(PySparkTask):
    """
    PySpark task for cross-reference Specimen Level experiments.
    This task depends on:

    - `impc_etl.jobs.clean.experiment_cleaner.LineLevelExperimentCleaner`
    - `impc_etl.jobs.clean.colony_cleaner.ColonyCleaner`
    - `impc_etl.jobs.extract.impress_extractor.ImpressExtractor`
    """

    #: Name of the Spark task
    name = "IMPC_Line_Level_Experiment_Cross_Reference"

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return [
            LineLevelExperimentCleaner(),
            ColonyCleaner(),
            ImpressExtractor(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/line_level_experiment_cross_ref_parquet)
        """
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}line_level_experiment_cross_ref_parquet"
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
        Loads the required parquets as DataFrames and applies the cross-reference transformations.
        """
        line_experiment_parquet_path = args[0]
        colony_parquet_path = args[1]
        impress_parquet_path = args[2]
        output_path = args[3]
        spark = SparkSession.builder.getOrCreate()
        line_cross_ref_df = self.cross_reference_lines(
            spark,
            line_experiment_parquet_path,
            colony_parquet_path,
            impress_parquet_path,
        )
        line_cross_ref_df.write.mode("overwrite").parquet(output_path)

    def cross_reference_lines(
        self,
        spark_session: SparkSession,
        line_experiment_parquet_path,
        colony_parquet_path,
        impress_parquet_path,
    ):
        """
        Loads the parquet files into DataFrames and applies cross-reference transformations.
        """
        line_experiment_df = spark_session.read.parquet(line_experiment_parquet_path)
        colony_df = spark_session.read.parquet(colony_parquet_path)
        impress_df = spark_session.read.parquet(impress_parquet_path)

        # Join line level experiments with colony information
        line_colony_df = line_experiment_df.join(
            colony_df, (line_experiment_df["_colonyID"] == colony_df["colony_name"])
        )

        # Line elements on the XML don't contain a project
        # So we add one using the value of the colony phenotyping consortium
        line_colony_df = line_colony_df.withColumn(
            "_project", line_colony_df["phenotyping_consortium"]
        )
        line_colony_df = line_colony_df.transform(override_europhenome_datasource)
        line_colony_df = generate_metadata_group(
            line_colony_df, impress_df, exp_type="line"
        )
        line_colony_df = generate_metadata(line_colony_df, impress_df, exp_type="line")
        line_colony_df = self.generate_line_allelic_composition(line_colony_df)
        line_columns = [
            col_name
            for col_name in line_experiment_df.columns
            if col_name not in ["_dataSource", "_project"]
        ] + [
            "allelicComposition",
            "metadata",
            "metadataGroup",
            "_project",
            "_dataSource",
        ]
        line_experiment_df = line_colony_df.select(line_columns)
        return line_experiment_df

    def generate_line_allelic_composition(self, line_df: DataFrame) -> DataFrame:
        """
        Generates the line allelic composition using the colony marker information.
        """
        generate_allelic_composition_udf = udf(
            generate_allelic_composition, StringType()
        )
        line_df = line_df.withColumn(
            "allelicComposition",
            generate_allelic_composition_udf(
                lit("homozygous"),
                "allele_symbol",
                "marker_symbol",
                lit(False),
                "_colonyID",
            ),
        )
        return line_df
