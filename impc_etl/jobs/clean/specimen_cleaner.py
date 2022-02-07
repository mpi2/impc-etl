"""
    Luigi PySpark task that takes the Specimen data coming from the different  data sources  (e.g. IMPC, 3i, EuroPhenome, PWG)
    and applies some data sanitizing functions.

    The cleaning process includes:

    - mapping identifiers
    - dropping entries with required values as NULL
    - drop a list of predefined skipped experiments
    - sanitize identifiers with XML elements (e.g. gt;)
    - cleaning legacy identifiers
    - drop duplicate specimen entries
    - lastly generate a unique id

"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, when, regexp_replace, col, lit, md5, concat
from pyspark.sql.types import StringType

from impc_etl.jobs.extract.specimen_extractor import (
    EmbryoSpecimenExtractor,
    MouseSpecimenExtractor,
)
from impc_etl.shared import utils
from impc_etl.workflow.config import ImpcConfig


class SpecimenCleaner(PySparkTask):
    """
    PySpark task to clean IMPC Specimen data.

    This task depends on `impc_etl.jobs.extract.specimen_extractor.MouseSpecimenExtractor`
    for cleaning Mouse specimens
    or `impc_etl.jobs.extract.specimen_extractor.EmbryoSpecimenExtractor`
    for cleaning Embryo specimens.
    """

    #: Name of the Spark task
    name: str = "IMPC_Specimen_Cleaner"

    #: Specimen type can be 'mouse' or 'embryo'
    specimen_type: luigi.Parameter = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return (
            MouseSpecimenExtractor()
            if self.specimen_type == "mouse"
            else EmbryoSpecimenExtractor()
        )

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/mouse_specimen_clean_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}{self.specimen_type}_specimen_clean_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input().path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Loads the given Specimen parquet and applies some data sanitizing functions.
        """
        input_path = args[0]
        output_path = args[1]
        spark_session = SparkSession.builder.getOrCreate()
        specimen_df = spark_session.read.parquet(input_path)
        specimen_clean_df = self.clean_specimens(specimen_df)
        specimen_clean_df = specimen_clean_df.dropDuplicates(
            ["_specimenID", "_centreID"]
        )
        specimen_clean_df.write.mode("overwrite").parquet(output_path)

    def clean_specimens(self, specimen_df: DataFrame) -> DataFrame:
        """
        DCC specimen cleaner
        :return: a clean specimen parquet file
        :rtype: DataFrame
        """
        specimen_df: DataFrame = (
            specimen_df.transform(self.map_centre_ids)
            .transform(self.map_project_ids)
            .transform(self.map_production_centre_ids)
            .transform(self.map_phenotyping_centre_ids)
        )

        specimen_df = specimen_df.transform(self.truncate_europhenome_specimen_ids)
        specimen_df = specimen_df.transform(self.truncate_europhenome_colony_ids)
        specimen_df = specimen_df.transform(self.parse_europhenome_colony_xml_entities)
        specimen_df = specimen_df.transform(self.standardize_strain_ids)
        specimen_df = specimen_df.transform(self.override_3i_specimen_data)
        specimen_df = specimen_df.transform(self.generate_unique_id)

        # Some specimen can be found several times in the XML file submitted by the IMPC
        # So it is necessary to drop the duplicates
        specimen_df = specimen_df.drop_duplicates(
            [col_name for col_name in specimen_df.columns if col_name != "_sourceFile"]
        )
        return specimen_df

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

    def map_production_centre_ids(self, dcc_experiment_df: DataFrame):
        """
        Maps the center ids  found in the XML files to a standard list of ids e.g:
            - gmc -> HMGU
            - h -> MRC Harwell
        The full list of mappings can be found at `impc_etl.config.Constants.CENTRE_ID_MAP`
        """
        if "_productionCentre" not in dcc_experiment_df.columns:
            dcc_experiment_df = dcc_experiment_df.withColumn(
                "_productionCentre", lit(None)
            )
        dcc_experiment_df = dcc_experiment_df.withColumn(
            "_productionCentre",
            udf(utils.map_centre_id, StringType())("_productionCentre"),
        )
        return dcc_experiment_df

    def map_phenotyping_centre_ids(self, dcc_experiment_df: DataFrame):
        """
        Maps the center ids  found in the XML files to a standard list of ids e.g:
            - gmc -> HMGU
            - h -> MRC Harwell
        The full list of mappings can be found at `impc_etl.config.Constants.CENTRE_ID_MAP`
        """
        dcc_experiment_df = dcc_experiment_df.withColumn(
            "_phenotypingCentre",
            udf(utils.map_centre_id, StringType())("_phenotypingCentre"),
        )
        return dcc_experiment_df

    def truncate_europhenome_specimen_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        Some EuroPhenome Specimen Ids have a suffix that should be truncated, the legacy identifying strategy for Specimen id included
        a suffix tno reference the production/phenotyping Centre (e.g. 232328312_HRW), that suffix needs to be removed.
        """
        dcc_df = dcc_df.withColumn(
            "_specimenID",
            when(
                dcc_df["_dataSource"].isin(["europhenome", "MGP"]),
                udf(utils.truncate_specimen_id, StringType())(dcc_df["_specimenID"]),
            ).otherwise(dcc_df["_specimenID"]),
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
                dcc_df["_dataSource"] == "europhenome",
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

    def standardize_strain_ids(self, dcc_df: DataFrame) -> DataFrame:
        """
        Some strain IDs are provided as MGI identifiers and some others don't, some of the ones provided as MGI identifiers lack the  'MGI:',
        here we remove the 'MGI:' from al of them to be able to process them in a consistent way in future tasks.
        """
        dcc_df = dcc_df.withColumn(
            "_strainID", regexp_replace(dcc_df["_strainID"], "MGI:", "")
        )
        return dcc_df

    def override_3i_specimen_data(self, dcc_specimen_df: DataFrame) -> DataFrame:
        """
        Whenever a Specime is presetn both in the 3i project and any other data source (e.g. EuroPhenome or IMPC) the other data source specimen data should be used instead of the 3i one.
        """
        dcc_specimen_df_a = dcc_specimen_df.alias("a")
        dcc_specimen_df_b = dcc_specimen_df.alias("b")
        dcc_specimen_df = dcc_specimen_df_a.join(
            dcc_specimen_df_b,
            (dcc_specimen_df_a["_specimenID"] == dcc_specimen_df_b["_specimenID"])
            & (dcc_specimen_df_a["_centreID"] == dcc_specimen_df_b["_centreID"])
            & (dcc_specimen_df_a["_dataSource"] != dcc_specimen_df_b["_dataSource"]),
            "left_outer",
        )
        dcc_specimen_df = dcc_specimen_df.where(
            col("b._specimenID").isNull()
            | ((col("b._specimenID").isNotNull()) & (col("a._dataSource") != "3i"))
        )
        return dcc_specimen_df.select("a.*").dropDuplicates()

    def generate_unique_id(self, dcc_specimen_df: DataFrame) -> DataFrame:
        """
        Generates an unique identifier for the  Specimen using productin centre, phenotyping centre  and specimen ID.
        """
        dcc_specimen_df = dcc_specimen_df.withColumn(
            "unique_id",
            md5(
                concat(
                    *[
                        when(
                            col("_productionCentre").isNotNull(),
                            col("_productionCentre"),
                        )
                        .when(
                            col("_phenotypingCentre").isNotNull(),
                            col("_phenotypingCentre"),
                        )
                        .otherwise(lit("")),
                        col("_specimenID"),
                    ]
                )
            ),
        )
        return dcc_specimen_df


class MouseSpecimenCleaner(SpecimenCleaner):
    name = "IMPC_Mouse_Specimen_Cleaner"
    specimen_type = "mouse"


class EmbryoSpecimenCleaner(SpecimenCleaner):
    name = "IMPC_Embryo_Specimen_Cleaner"
    specimen_type = "embryo"
