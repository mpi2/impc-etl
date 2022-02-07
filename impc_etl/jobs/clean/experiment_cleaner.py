"""
    Luigi PySpark task that takes the Experiment data coming from the different  data sources  (e.g. IMPC, 3i, EuroPhenome, PWG)
    and applies some data sanitizing functions.

    The cleaning process includes:

    - mapping identifiers
    - dropping entries with required values as NULL
    - drop a list of predefined skipped experiments
    - creating experiment IDs for line level experiments
    - sanitize identifiers with XML elements (e.g. gt;)
    - cleaning legacy identifiers
    - lastly generate a unique id
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    udf,
    when,
    lit,
    md5,
    concat,
    col,
    regexp_replace,
    regexp_extract,
)
from pyspark.sql.types import StringType

from impc_etl.config.constants import Constants
from impc_etl.jobs.extract.experiment_extractor import (
    SpecimenLevelExperimentExtractor,
    LineLevelExperimentExtractor,
)
from impc_etl.shared import utils
from impc_etl.workflow.config import ImpcConfig


class ExperimentCleaner(PySparkTask):
    """
    PySpark task  to clean the IMPC Experimental data.

    This task depends on `impc_etl.jobs.extract.dcc_experiment_extractor.SpecimenLevelExperimentExtractor`
    for cleaning Specimen Level experiments
    or `impc_etl.jobs.extract.dcc_experiment_extractor.LineLevelExperimentExtractor`
    for cleaning Line Level experiments.
    """

    #: Name of the Spark task
    name: str = "IMPC_Experiment_Cleaner"

    #: Experimental type can be 'specimen_level' or 'line_level'.
    experiment_type: luigi.Parameter = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return (
            SpecimenLevelExperimentExtractor()
            if self.experiment_type == "specimen_level"
            else LineLevelExperimentExtractor()
        )

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/specimen_level_experiment_clean_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}{self.experiment_type}_experiment_clean_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input().path,
            self.experiment_type,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Loads the given  input Experiment  parquet and applies some sanitizing functions to it.
        """
        input_path = args[0]
        experiment_type = args[1]
        output_path = args[2]
        spark = SparkSession(sc)
        dcc_df = spark.read.parquet(input_path)

        if experiment_type == "specimen_level":
            dcc_clean_df = self.clean_experiments(dcc_df)
        else:
            dcc_clean_df = self.clean_lines(dcc_df)

        dcc_clean_df.write.mode("overwrite").parquet(output_path)

    def clean_experiments(self, experiment_df: DataFrame) -> DataFrame:
        """
        Cleaning function  for specimen  level experiments.
        """
        experiment_df = (
            experiment_df.transform(self.map_centre_ids)
            .transform(self.map_project_ids)
            .transform(self.truncate_europhenome_specimen_ids)
            .transform(self.drop_skipped_experiments)
            .transform(self.drop_skipped_procedures)
            .transform(self.map_3i_project_ids)
            .transform(self.prefix_3i_experiment_ids)
            .transform(self.drop_null_centre_id)
            .transform(self.drop_null_data_source)
            .transform(self.drop_null_date_of_experiment)
            .transform(self.drop_null_pipeline)
            .transform(self.drop_null_project)
            .transform(self.drop_null_specimen_id)
            .transform(self.generate_unique_id)
        )
        return experiment_df

    def clean_lines(self, line_df: DataFrame) -> DataFrame:
        """
        DCC cleaner for line level experiments.
        """
        line_df = (
            line_df.transform(self.generate_line_experiment_id)
            .transform(self.map_centre_ids)
            .transform(self.map_project_ids)
            .transform(self.drop_skipped_procedures)
            .transform(self.truncate_europhenome_colony_ids)
            .transform(self.parse_europhenome_colony_xml_entities)
            .transform(self.map_3i_project_ids)
            .transform(self.prefix_3i_experiment_ids)
            .transform(self.drop_null_centre_id)
            .transform(self.drop_null_data_source)
            .transform(self.drop_null_pipeline)
            .transform(self.drop_null_project)
            .transform(self.generate_unique_id)
        )
        return line_df

    def generate_line_experiment_id(self, line_df: DataFrame) -> DataFrame:
        """
        Takes in a DataFrame containing line level experiments and generates an Experiment  ID for each row.
        Line level experiments in the XML files don't have a experiment ID and that is needed for further processing so we generate an artificial one.
        """
        line_df = line_df.withColumn(
            "_experimentID", concat(col("_procedureID"), lit("-"), col("_colonyID"))
        )
        return line_df

    def map_centre_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        Maps the center ids  found in the XML files to a standard list of ids e.g:
            - gmc -> HMGU
            - h -> MRC Harwell
        The full list of mappings can be found at `impc_etl.config.Constants.CENTRE_ID_MAP`
        """
        dcc_df = dcc_df.withColumn(
            "_centreID", udf(utils.map_centre_id, StringType())("_centreID")
        )
        return dcc_df

    def map_project_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        Maps the center ids  found in the XML files to a standard list of ids e.g:
            - dtcc -> DTCC
            - riken brc -> RBRC
        The full list of mappings can be found at `impc_etl.config.Constants.PROJECT_ID_MAP`
        """
        dcc_df = dcc_df.withColumn(
            "_project", udf(utils.map_project_id, StringType())("_project")
        )
        return dcc_df

    def truncate_europhenome_specimen_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        Some EuroPhenome Specimen Ids have a suffix that should be truncated, the legacy identifying strategy for Specimen id included
        a suffix tno reference the production/phenotyping Centre (e.g. 232328312_HRW), that suffix needs to be removed.
        """
        dcc_df = dcc_df.withColumn(
            "specimenID",
            when(
                (dcc_df["_dataSource"] == "europhenome"),
                udf(utils.truncate_specimen_id, StringType())(dcc_df["specimenID"]),
            ).otherwise(dcc_df["specimenID"]),
        )
        return dcc_df

    def truncate_europhenome_colony_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        Some EuroPhenome Colony Ids have a suffix that should be truncated. Same as Specimen IDs some colony IDs used to have a suffix to identify
        the Centre and that has been  removed from the
        tracking system, so we need to truncate them so they  match the corresponding  entry on GenTar.
        """
        dcc_df = dcc_df.withColumn(
            "_colonyID",
            when(
                (dcc_df["_dataSource"] == "europhenome"),
                udf(utils.truncate_colony_id, StringType())(dcc_df["_colonyID"]),
            ).otherwise(dcc_df["_colonyID"]),
        )
        return dcc_df

    def parse_europhenome_colony_xml_entities(self, dcc_df: DataFrame) -> DataFrame:
        """
        Some EuroPhenome Colony Ids have &lt; &gt; values that have to be replaced by the  corresponding characters < and >.
        """
        dcc_df = dcc_df.withColumn(
            "_colonyID",
            when(
                (dcc_df["_dataSource"] == "europhenome"),
                regexp_replace("_colonyID", "&lt;", "<"),
            ).otherwise(dcc_df["_colonyID"]),
        )

        dcc_df = dcc_df.withColumn(
            "_colonyID",
            when(
                (dcc_df["_dataSource"] == "europhenome"),
                regexp_replace("_colonyID", "&gt;", ">"),
            ).otherwise(dcc_df["_colonyID"]),
        )
        return dcc_df

    def drop_skipped_experiments(self, dcc_df: DataFrame) -> DataFrame:
        """
        We have a list of IMPC experiments that should be dropped from the loading process.
        """
        return dcc_df.where(
            ~(
                (dcc_df["_centreID"] == "Ucd")
                & (
                    dcc_df["_experimentID"].isin(
                        ["GRS_2013-10-09_4326", "GRS_2014-07-16_8800"]
                    )
                )
            )
        )

    def drop_skipped_procedures(self, dcc_df: DataFrame) -> DataFrame:
        """
        Drop the experiments corresponding with the list of dropped procedures found
        at `impc_etl.config.constants.Constants.SKIPPED_PROCEDURES`.
        """
        return dcc_df.where(
            (
                ~(
                    regexp_extract(col("_procedureID"), "(.+_.+)_.+", 1).isin(
                        Constants.SKIPPED_PROCEDURES
                    )
                )
            )
            | (dcc_df["_dataSource"] == "3i")
        )

    def map_3i_project_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        Some data from the  3i project contains invalid Project identifiers, those entries should be marked as MGP.
        """
        return dcc_df.withColumn(
            "_project",
            when(
                (dcc_df["_dataSource"] == "3i")
                & (~dcc_df["_project"].isin(Constants.VALID_PROJECT_IDS)),
                lit("MGP"),
            ).otherwise(dcc_df["_project"]),
        )

    def prefix_3i_experiment_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        In  order to avoid collusion  of experiment IDs coming from 3i witht he  ones we generate on the IMPC data,
        we add a prefix the  3i experiment IDs.
        """
        return dcc_df.withColumn(
            "_experimentID",
            when(
                (dcc_df["_dataSource"] == "3i"),
                concat(lit("3i_"), dcc_df["_experimentID"]),
            ).otherwise(dcc_df["_experimentID"]),
        )

    def drop_null_procedure_id(self, dcc_df: DataFrame):
        """
        Drops all the experiments without a Procedure ID. This case only appears on legacy  data.
        """
        return self.drop_if_null(dcc_df, "_procedureID")

    def drop_null_centre_id(self, dcc_df: DataFrame):
        """
        Drops all the experiments without a Centre ID. This case only appears on legacy  data.
        """
        return self.drop_if_null(dcc_df, "_centreID")

    def drop_null_data_source(self, dcc_df: DataFrame):
        """
        Drops all the experiments without a DataSource. This case only appears on legacy  data.
        """
        return self.drop_if_null(dcc_df, "_dataSource")

    def drop_null_date_of_experiment(self, dcc_df: DataFrame):
        """
        Drops all the experiments without a Date of Experiment. This case only appears on legacy  data.
        """
        return self.drop_if_null(dcc_df, "_dateOfExperiment")

    def drop_null_pipeline(self, dcc_df: DataFrame):
        """
        Drops all the experiments without a Pipeline ID. This case only appears on legacy  data.
        """
        return self.drop_if_null(dcc_df, "_pipeline")

    def drop_null_project(self, dcc_df: DataFrame):
        """
        Drops all the experiments without a Project ID. This case only appears on legacy  data.
        """
        return self.drop_if_null(dcc_df, "_project")

    def drop_null_specimen_id(self, dcc_df: DataFrame):
        """
        Drops all the experiments without a Specimen ID. This case only appears on legacy  data.
        """
        return self.drop_if_null(dcc_df, "specimenID")

    def drop_if_null(self, dcc_df: DataFrame, column: str) -> DataFrame:
        """
        Function that takes in a DataFrame and a column_name and returns that DataFrame filtering out the rows that have
        a null value on the specified column.
        """
        return dcc_df.where(dcc_df[column].isNotNull())

    def generate_unique_id(self, dcc_experiment_df: DataFrame):
        """
        Generates a unique_id column using as an input every column
        except from those that have non-unique values and
        the ones that correspond to parameter values.
        Given that _sequenceID could be null, the function transforms it the to the string NA
        when is null to avoid the nullifying the concat.
        It concatenates the unique set of values and then applies
        an MD5 hash function to the resulting string.
        :param dcc_experiment_df:
        :return: DataFrame
        """
        non_unique_columns = [
            "_type",
            "_sourceFile",
            "_VALUE",
            "procedureMetadata",
            "statusCode",
            "_sequenceID",
            "_project",
        ]
        if "_sequenceID" in dcc_experiment_df.columns:
            dcc_experiment_df = dcc_experiment_df.withColumn(
                "_sequenceIDStr",
                when(col("_sequenceID").isNull(), lit("NA")).otherwise(
                    col("_sequenceID")
                ),
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


class SpecimenLevelExperimentCleaner(ExperimentCleaner):
    name: str = "Specimen_Level_Experiment_Cleaner"
    experiment_type: str = "specimen_level"


class LineLevelExperimentCleaner(ExperimentCleaner):
    name: str = "Line_Level_Experiment_Cleaner"
    experiment_type = "line_level"
