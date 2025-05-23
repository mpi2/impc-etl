"""
DCC Specimen Extractor module
    This module takes care of extracting specimen data from XML DCC files to  Spark DataFrames.
    In the XML files we can find two specimen classes: mouse and embryo.

    The files are expected to be organized by data source in the following directory structure:
    <DCC_XML_PATH>/<DATASOURCE>/*specimen*.xml
    Each directory containing the raw XML in the XML schema defined by the DCC.
"""
import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

from impc_etl.jobs.extract.xml_extraction_helper import (
    extract_dcc_xml_files,
    get_entity_by_type,
)
from impc_etl.shared.exceptions import UnsupportedEntityError
from impc_etl.workflow import SmallPySparkTask
from impc_etl.workflow.config import ImpcConfig


class SpecimenExtractor(SmallPySparkTask):
    """
    PySpark Task class to extract experimental data from the DCC XML files.
    """

    name = "IMPC_DCC_Specimen_Extractor"
    dcc_specimen_xml_path = luigi.Parameter()
    specimen_type = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task (e.g. impc/dr15.2/parquet/mouse_raw_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}{self.specimen_type}_specimen_raw_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.dcc_specimen_xml_path,
            self.specimen_type,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)
        # TODO update time parsing strategy
        # Setting time parser policy to legacy so it behaves as expected
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=LEGACY")

        # Parsing app options
        dcc_specimen_xml_path = args[0]
        specimen_type = args[1]
        output_path = args[2]

        dcc_df = extract_dcc_xml_files(spark, dcc_specimen_xml_path, "specimen")
        specimen_df = get_specimens_by_type(dcc_df, specimen_type)
        specimen_df.write.mode("overwrite").parquet(output_path)


def get_specimens_by_type(dcc_df: DataFrame, entity_type: str) -> DataFrame:
    """
    Takes a DataFrame generated by `impc_etl.jobs.extract.xml_extraction_helper.extract_dcc_xml_files`,
    an entity type ('mouse' or 'embryo') and return a specimen data frame with one row per specimen.
    Can raise an `impc_etl.shared.exceptions.UnsupportedEntityError` when the  given entity_type is not supported.
    """
    if entity_type not in ["mouse", "embryo"]:
        raise UnsupportedEntityError
    return get_entity_by_type(
        dcc_df,
        entity_type,
        ["_centreID", "_sourceFile", "_dataSource", "_sourcePhenotypingStatus"],
    )


class MouseSpecimenExtractor(SpecimenExtractor):
    specimen_type = "mouse"


class EmbryoSpecimenExtractor(SpecimenExtractor):
    specimen_type = "embryo"
