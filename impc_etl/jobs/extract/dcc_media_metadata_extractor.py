"""
    DCC Media Metadata Extractor Task.
    This module provides a Luigi Task class for extracting DCC Media Metadata.
    Input: JSON file(s) containing DCC Media Metadata with this shape
    ```
            {
                "centre": "UC Davis",
                "checksum": "aa0aa3ad911d1050f05be98b65207f07e26ed49c3e7343b2ce2abde964ca22d9",
                "fileName": "2792092.png",
                "parameter": "IMPC_XRY_048_001",
                "pipeline": "UCD_001",
                "procedure": "IMPC_XRY_001"
            }
    ```
    Output: Parquet file containing the DCC Media Metadata.
"""
from typing import Any

import luigi
from pyspark import SparkContext
from pyspark.sql import SparkSession

from impc_etl.shared.utils import to_snake_case
from impc_etl.workflow import SmallPySparkTask
from impc_etl.workflow.config import ImpcConfig


class ExtractDCCMediaMetadata(SmallPySparkTask):
    """
    PySparkTask task to extract DCC Media Metadata from the DCC Media Server JSON response.
    """

    #: Name of the Spark task
    name = "IMPC_Extract_DCC_Media_Metadata"

    #: DCC Media Metadata JSON file(s) path
    dcc_media_metadata_json_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr16.0/parquet/allele_ref_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}dcc_media_metadata_parquet")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.dcc_media_metadata_json_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        DCC Extractor job runner
        :param list args: the list elements should be:
        """
        dcc_media_metadata_json_path = args[0]
        output_path = args[1]

        spark = SparkSession(sc)

        dcc_media_metadata_df = spark.read.json(dcc_media_metadata_json_path)
        for col_name in dcc_media_metadata_df.columns:
            dcc_media_metadata_df = dcc_media_metadata_df.withColumnRenamed(
                col_name, "dcc_media_" + to_snake_case(col_name)
            )
        dcc_media_metadata_df.write.parquet(output_path)
