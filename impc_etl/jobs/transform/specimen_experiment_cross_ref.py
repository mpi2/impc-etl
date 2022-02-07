"""
    Luigi tasks to take care of cross-reference the Specimen Level Experimental data with the specimen data,
    and the IMPReSS data.

    The general cross-reference process is:

    - Load parquet files (i.e. specimen level experiments, mouse specimens,
    embryo specimens, IMPReSS data) into data frames.
    - Change the legacy data outdated pipelines, for 3i data with procedure MGP_PBI_001: SLM_001 -> MGP_001;
    for EuroPhenome data with procedure ESLIM_019_001: ESLIM_001 -> ESLIM_002.
    - Merge mouse specimen data and embryo specimen data in one DataFrame.
    - Join the Experimental data with the Specimen data
    - Drop experiment for specimens without a colony id
    - Override the data source and the project info for MGP and MGP Legacy experiments
    - Generate metadata group (a hash value to identify experiments with the same experimental conditions)
    - Generate metadata array (a list of "parameter = value" for metadata parameters in a given experiment)
    - Select the columns relevant to experimental data only
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    lower,
)
from pyspark.sql.utils import AnalysisException

from impc_etl.jobs.clean.experiment_cleaner import SpecimenLevelExperimentCleaner
from impc_etl.jobs.extract import ImpressExtractor
from impc_etl.jobs.transform.cross_ref_helper import (
    override_europhenome_datasource,
    generate_metadata_group,
    generate_metadata,
)
from impc_etl.jobs.transform.specimen_cross_ref import EmbryoCrossRef, MouseCrossRef
from impc_etl.workflow.config import ImpcConfig


class SpecimenLevelExperimentCrossRef(PySparkTask):
    """
    PySpark task for cross-reference Specimen Level experiments.
    Depends on `impc_etl.jobs.clean.experiment_cleaner.SpecimenLevelExperimentCleaner`,
    `impc_etl.jobs.transform.specimen_cross_ref.MouseCrossRef`,
    `impc_etl.jobs.transform.specimen_cross_ref.EmbryoCrossRef`,
    and `impc_etl.jobs.extract.impress_extractor.ImpressExtractor`.
    """

    #: Name of the Spark task
    name = "IMPC_Specimen_Level_Experiment_Cross_Reference"

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """

        return [
            SpecimenLevelExperimentCleaner(),
            MouseCrossRef(),
            EmbryoCrossRef(),
            ImpressExtractor(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/specimen_level_experiment_cross_ref_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}specimen_level_experiment_cross_ref_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Loads the required parquets as DataFrames and applies the cross-reference transformations.
        """
        experiment_parquet_path = args[0]
        mouse_parquet_path = args[1]
        embryo_parquet_path = args[2]
        impress_parquet_path = args[3]
        output_path = args[4]
        spark = SparkSession(sc)
        experiment_normalized_df = self.cross_reference_experiments(
            spark,
            experiment_parquet_path,
            mouse_parquet_path,
            embryo_parquet_path,
            impress_parquet_path,
        )
        experiment_normalized_df.write.mode("overwrite").parquet(output_path)

    def cross_reference_experiments(
        self,
        spark_session: SparkSession,
        experiment_parquet_path: str,
        mouse_parquet_path: str,
        embryo_parquet_path: str,
        impress_parquet_path: str,
    ) -> DataFrame:
        """
        Takes in the Specimen level experiment, the IMPReSS the mouse
        and embryo specimen parquet paths, loads them into DataFrames and applies
        cross-reference transformations to the Experimental data.
        """
        experiment_df = spark_session.read.parquet(experiment_parquet_path)
        mouse_df = spark_session.read.parquet(mouse_parquet_path)

        # Sometimes the Embryo Specimen parquet could be empty
        try:
            embryo_df = spark_session.read.parquet(embryo_parquet_path)
        except AnalysisException:
            embryo_df = None
        impress_df = spark_session.read.parquet(impress_parquet_path)

        # TODO: Remove this data manipulation and change the source file instead
        # Legacy data manipulation start

        # Some 3i comes with an invalid pipeline SLM_001, this must be replaced by  MGP_001
        experiment_df = experiment_df.withColumn(
            "_pipeline",
            when(
                (col("_dataSource") == "3i")
                & (col("_procedureID") == "MGP_PBI_001")
                & (col("_pipeline") == "SLM_001"),
                lit("MGP_001"),
            ).otherwise(col("_pipeline")),
        )

        # Some EuroPhenome comes with an invalid pipeline ESLIM_001, this must be replaced by  ESLIM_002
        experiment_df = experiment_df.withColumn(
            "_pipeline",
            when(
                (lower(col("_dataSource")).isin(["europhenome", "mgp"]))
                & (col("_procedureID") == "ESLIM_019_001")
                & (col("_pipeline") == "ESLIM_001"),
                lit("ESLIM_002"),
            ).otherwise(col("_pipeline")),
        )
        # Legacy data manipulation end

        specimen_df = self.merge_specimen_data(mouse_df, embryo_df)

        # Add alias to experiment and specimen DF so is easier to operate after the join
        experiment_df = experiment_df.alias("experiment")
        specimen_df = specimen_df.alias("specimen")

        experiment_specimen_df = experiment_df.join(
            specimen_df,
            (experiment_df["_centreID"] == specimen_df["_centreID"])
            & (experiment_df["specimenID"] == specimen_df["_specimenID"]),
        )

        experiment_specimen_df = self.drop_null_colony_id(experiment_specimen_df)

        experiment_specimen_df = experiment_specimen_df.transform(
            override_europhenome_datasource
        )

        experiment_specimen_df = generate_metadata_group(
            experiment_specimen_df, impress_df
        )

        experiment_specimen_df = generate_metadata(experiment_specimen_df, impress_df)

        experiment_columns = [
            "experiment." + col_name
            for col_name in experiment_df.columns
            if col_name not in ["_dataSource", "_project"]
        ] + ["metadata", "metadataGroup", "_project", "_dataSource"]
        experiment_df = experiment_specimen_df.select(experiment_columns)
        return experiment_df

    def drop_null_colony_id(self, experiment_specimen_df: DataFrame) -> DataFrame:
        """
        Drops experiments with a null colony ID, only when the specimen is a mutant.
        """
        experiment_specimen_df = experiment_specimen_df.where(
            (col("specimen._colonyID").isNotNull())
            | (col("specimen._isBaseline") == True)
            | (col("specimen._colonyID") == "baseline")
        )
        return experiment_specimen_df.dropDuplicates()

    def merge_specimen_data(
        self, mouse_df: DataFrame, embryo_df: DataFrame
    ) -> DataFrame:
        """
        Takes in a mouse dataframe and a embryo dataframe and returns a DataFrame
        containing both Mouse and Embryo specimen data.
        """
        specimen_cols = [
            "_centreID",
            "_specimenID",
            "_colonyID",
            "_isBaseline",
            "_productionCentre",
            "_phenotypingCentre",
            "phenotyping_consortium",
        ]

        # Select the relevant columns for specimen data
        mouse_specimen_df = mouse_df.select(*specimen_cols)
        if embryo_df is not None:
            embryo_specimen_df = embryo_df.select(*specimen_cols)

            # Merge both the mouse specimen data and the embryo specimen data in one DataFrame
            specimen_df = mouse_specimen_df.union(embryo_specimen_df)
        else:
            specimen_df = mouse_specimen_df
        return specimen_df
