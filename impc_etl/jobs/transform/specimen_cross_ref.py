"""
    Luigi tasks to take care of cross-reference the Specimen data with the tracking system reports.
    It also applies some sanitation functions to the data after dong the cross-reference operations.

    The general cross-reference process is:

    - Join the specimen information with the colony information
    - Generate the specimen allelic composition using the colony information
    - Override the data source and the project info for MGP and MGP Legacy colonies
    - Override 3i specimen project using the colony phenotyping consortium
    - Add production centre information when is missing using the colony information
    - Add life stage

"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, lit, udf, when
from pyspark.sql.types import StringType

from impc_etl.config.constants import Constants
from impc_etl.jobs.clean import ColonyCleaner
from impc_etl.jobs.clean.specimen_cleaner import (
    EmbryoSpecimenCleaner,
    MouseSpecimenCleaner,
)
from impc_etl.jobs.transform.cross_ref_helper import (
    generate_allelic_composition,
    override_europhenome_datasource,
)
from impc_etl.workflow.config import ImpcConfig


class SpecimenCrossRef(PySparkTask):
    """
    PysPark task to cross-reference Specimen data with tracking systems reports.
    This task depends on `impc_etl.jobs.clean.specimen_cleaner.SpecimenCleaner`
    (`impc_etl.jobs.clean.specimen_cleaner.MouseSpecimenCleaner`
    or `impc_etl.jobs.clean.specimen_cleaner.EmbryoSpecimenCleaner`)
    and `impc_etl.jobs.clean.colony_cleaner.ColonyCleaner`.
    """

    #: Name of the Spark task
    name = "IMPC_Specimen_Cross_Reference"

    #: Specimen type can be 'mouse' or 'embryo'
    specimen_type: luigi.Parameter = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        specimen_cleaning_dep = (
            MouseSpecimenCleaner()
            if self.specimen_type == "mouse"
            else EmbryoSpecimenCleaner()
        )

        return [specimen_cleaning_dep, ColonyCleaner()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/mouse_specimen_cross_ref_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}{self.specimen_type}_specimen_cross_ref_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.specimen_type,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Load the specified Specimen parquet and Colonies parquet, does some cross-reference between them
        and applies some data sanitation functions after.
        """
        specimen_parquet_path = args[0]
        colonies_parquet_path = args[1]
        specimen_type = args[2]
        output_path = args[3]
        spark = SparkSession(sc)
        specimen_df = spark.read.parquet(specimen_parquet_path)
        colonies_df = spark.read.parquet(colonies_parquet_path)
        specimen_cross_ref_df = self.cross_reference_specimens(
            specimen_df, colonies_df, specimen_type
        )
        specimen_cross_ref_df.write.mode("overwrite").parquet(output_path)

    def cross_reference_specimens(
        self, specimen_df: DataFrame, colonies_df: DataFrame, entity_type: str
    ) -> DataFrame:
        """
        Cross-reference Specimen data with colony tracking report information.
        """
        specimen_df = specimen_df.alias("specimen")
        colonies_df = colonies_df.alias("colony")

        # join the specimen information with the colony information
        specimen_df = specimen_df.join(
            colonies_df,
            (specimen_df["_colonyID"] == colonies_df["colony_name"]),
            "left_outer",
        )

        # generate the specimen allelic composition using the colony information
        # override 3i specimen project using the colony phenotyping consortium
        # override the data source and the project info for MGP and MGP Legacy colonies
        specimen_df = (
            specimen_df.transform(self.generate_specimen_allelic_composition)
            .transform(override_europhenome_datasource)
            .transform(self.override_3i_specimen_project)
        )
        specimen_df = specimen_df.withColumn(
            "_productionCentre",
            when(
                col("_productionCentre").isNull(),
                when(
                    col("_phenotypingCentre").isNotNull(), col("_phenotypingCentre")
                ).otherwise(col("colony.production_centre")),
            ).otherwise(col("_productionCentre")),
        )
        specimen_df = specimen_df.select(
            "specimen.*",
            "_productionCentre",
            "allelicComposition",
            "colony.phenotyping_consortium",
        )

        if entity_type == "embryo":
            specimen_df = specimen_df.transform(self.add_embryo_life_stage_acc)
        if entity_type == "mouse":
            specimen_df = specimen_df.transform(self.add_mouse_life_stage_acc)
        return specimen_df

    def generate_specimen_allelic_composition(
        self, specimen_df: DataFrame
    ) -> DataFrame:
        """
        Takes in a specimen dataframe and adds the specimen allelic composition.
        """
        generate_allelic_composition_udf = udf(
            generate_allelic_composition, StringType()
        )
        specimen_df = specimen_df.withColumn(
            "allelicComposition",
            generate_allelic_composition_udf(
                "specimen._zygosity",
                "colony.allele_symbol",
                "colony.marker_symbol",
                "specimen._isBaseline",
                "specimen._colonyID",
            ),
        )
        return specimen_df

    def override_3i_specimen_project(self, specimen_df: DataFrame):
        """
        Takes in a Specimen dataframe and replaces the _'project'_
        value for the 3i specimens with the value for the _'phenotyping_consortium'_ coming
        from the colonies tracking report.
        """
        specimen_df.withColumn(
            "_project",
            when(
                specimen_df["_dataSource"] == "3i", col("phenotyping_consortium")
            ).otherwise("_project"),
        )
        return specimen_df

    def add_mouse_life_stage_acc(self, specimen_df: DataFrame):
        """
        Adds life stage to mouse specimen dataframe.
        """
        specimen_df = specimen_df.withColumn(
            "developmental_stage_acc", lit("EFO:0002948")
        )
        specimen_df = specimen_df.withColumn(
            "developmental_stage_name", lit("postnatal")
        )
        return specimen_df

    def add_embryo_life_stage_acc(self, specimen_df: DataFrame):
        """
        Adds life stage to embryo specimen dataframe.
        """
        efo_acc_udf = udf(self.resolve_embryo_life_stage, StringType())
        specimen_df = specimen_df.withColumn(
            "developmental_stage_acc", efo_acc_udf("_stage")
        )
        specimen_df = specimen_df.withColumn(
            "developmental_stage_name", concat(lit("embryonic day "), col("_stage"))
        )
        return specimen_df

    def resolve_embryo_life_stage(self, embryo_stage):
        """
        Resolves the life stage of an Embryo specimen using `impc_etl.config.constants.Constants.EFO_EMBRYONIC_STAGES`
        """
        embryo_stage = str(embryo_stage).replace("E", "")
        return (
            Constants.EFO_EMBRYONIC_STAGES[embryo_stage]
            if embryo_stage in Constants.EFO_EMBRYONIC_STAGES
            else "EFO:" + embryo_stage + "NOT_FOUND"
        )


class MouseCrossRef(SpecimenCrossRef):
    #: Name of the Spark task
    name = "IMPC_Mouse_Cross_Reference"
    specimen_type = "mouse"


class EmbryoCrossRef(SpecimenCrossRef):
    #: Name of the Spark task
    name = "IMPC_Embryo_Cross_Reference"
    specimen_type = "embryo"
