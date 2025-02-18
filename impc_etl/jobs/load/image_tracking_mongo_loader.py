from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit

from impc_etl.jobs.extract.dcc_media_metadata_extractor import ExtractDCCMediaMetadata
from impc_etl.jobs.load.observation_mapper import ExperimentToObservationMapper
from impc_etl.workflow.config import ImpcConfig


class ImpcImageTrackingMongoLoader(PySparkTask):
    name = "IMPC_Image_Tracking_Mongo_Loader"
    embryo_data_json_path = luigi.Parameter()
    image_mongodb_database = luigi.Parameter()
    output_path = luigi.Parameter()

    image_mongodb_connection_uri = luigi.Parameter()
    image_mongodb_genes_collection = luigi.Parameter()
    image_mongodb_replica_set = luigi.Parameter()

    def requires(self):
        return [
            ExperimentToObservationMapper(),
            ExtractDCCMediaMetadata(),
        ]

    def output(self):
        return ImpcConfig().get_target(
            f"{self.output_path}image_tracking_mongo/_MONGO_LOAD_SUCCESS"
        )

    def app_options(self):
        return [self.input()[0].path, self.input()[1].path, self.output_path]

    def main(self, sc: SparkContext, *args: Any):
        observations_parquet_path = args[0]
        dcc_media_metadata_parquet_path = args[1]
        output_path = args[2]

        spark = SparkSession(sc)

        observations_df: DataFrame = spark.read.parquet(observations_parquet_path)
        image_tracking_df = spark.read.parquet(dcc_media_metadata_parquet_path)

        images_df = observations_df.where(
            (col("observation_type") == "image_record")
            & (
                col("download_file_path").contains("mousephenotype.org")
                | col("download_file_path").contains("images/3i")
            )
        )

        image_tracking_df = image_tracking_df.withColumn(
            "url",
            concat_ws(
                "/",
                lit("https://api.mousephenotype.org/tracker/media/getfile"),
                col("dcc_media_checksum"),
            ),
        )

        images_df = images_df.join(
            image_tracking_df, col("download_file_path") == col("url"), "left_outer"
        )

        images_df.write.format("mongodb").mode("overwrite").option(
            "spark.mongodb.write.uri",
            f"{self.image_mongodb_connection_uri}/admin?replicaSet={self.image_mongodb_replica_set}",
        ).option("database", str(self.image_mongodb_database)).option(
            "collection", "komp_image_tracking"
        ).option(
            "writeConcern.w", "majority"
        ).save()
        with self.output().open("w"):
            pass
