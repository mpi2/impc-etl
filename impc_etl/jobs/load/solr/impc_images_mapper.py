"""
SOLR module
   Generates the required Solr cores
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    collect_set,
    when,
    flatten,
    explode_outer,
    concat,
    lit,
    regexp_extract,
)

from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.pipeline_mapper import ImpressToParameterMapper
from impc_etl.workflow.config import ImpcConfig


class ImpcImagesLoader(PySparkTask):
    name = "IMPC_Images_Core_Loader"
    omero_ids_csv_path = luigi.Parameter()
    output_path = luigi.Parameter()
    imaging_media_json_info_path = luigi.Parameter()

    def requires(self):
        return [
            ExperimentToObservationMapper(),
            ImpressToParameterMapper(),
        ]

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.omero_ids_csv_path,
            self.imaging_media_json_info_path,
            self.output().path,
        ]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}impc_images_core_parquet")

    def main(self, sc: SparkContext, *args: Any):
        """
        Solr Core loader
        :param list argv: the list elements should be:
                        [1]: source IMPC parquet file
                        [2]: Output Path
        """
        observations_parquet_path = args[0]
        pipeline_core_parquet_path = args[1]
        omero_ids_csv_path = args[2]
        imaging_media_json_info_path = args[3]
        output_path = args[4]

        spark = SparkSession.builder.getOrCreate()
        observations_df = spark.read.parquet(observations_parquet_path)
        pipeline_core_df = spark.read.parquet(pipeline_core_parquet_path)
        pipeline_core_df = pipeline_core_df.select(
            "fully_qualified_name",
            "mouse_anatomy_id",
            "mouse_anatomy_term",
            "embryo_anatomy_id",
            "embryo_anatomy_term",
            col("mp_id").alias("impress_mp_id"),
            col("mp_term").alias("impress_mp_term"),
            "top_level_mouse_anatomy_id",
            "top_level_mouse_anatomy_term",
            "top_level_embryo_anatomy_id",
            "top_level_embryo_anatomy_term",
            col("top_level_mp_id").alias("impress_top_level_mp_id"),
            col("top_level_mp_term").alias("impress_top_level_mp_term"),
            col("intermediate_mp_id").alias("impress_intermediate_mp_id"),
            col("intermediate_mp_term").alias("impress_intermediate_mp_term"),
        ).distinct()
        omero_ids_df = spark.read.csv(omero_ids_csv_path, header=True).dropDuplicates()
        media_info_df = (
            spark.read.json(imaging_media_json_info_path, multiLine=True)
            .dropDuplicates()
            .select("checksum", "fileName")
        )
        media_info_df = media_info_df.withColumn(
            "isPdf",
            when(col("fileName").contains(".pdf"), lit(True)).otherwise(lit(False)),
        )
        media_info_df = media_info_df.drop("fileName")

        omero_ids_df = omero_ids_df.alias("omero")
        image_observations_df = observations_df.where(
            col("observation_type") == "image_record"
        )
        image_observations_df = image_observations_df.alias("obs")
        image_observations_df = image_observations_df.join(
            omero_ids_df,
            [
                "observation_id",
                "download_file_path",
                "phenotyping_center",
                "pipeline_stable_id",
                "procedure_stable_id",
                "parameter_stable_id",
                "datasource_name",
            ],
        )
        image_observations_df = image_observations_df.select("obs.*", "omero.omero_id")
        image_observations_df = image_observations_df.withColumn(
            "checksum",
            regexp_extract("download_file_path", r"\/.*\/(.+)", 1),
        )
        image_observations_df = image_observations_df.join(media_info_df, "checksum")
        image_observations_df = image_observations_df.withColumn(
            "file_type",
            when(
                (col("isPdf") == True) & (col("file_type").isNull()),
                lit("application/pdf"),
            ).otherwise(col("file_type")),
        )
        image_observations_df = image_observations_df.drop("checksum", "isPdf")
        image_observations_exp_df = image_observations_df.withColumn(
            "parameter_association_stable_id_exp",
            explode_outer("parameter_association_stable_id"),
        )

        image_observations_x_impress_df = image_observations_exp_df.withColumn(
            "fully_qualified_name",
            concat_ws(
                "_",
                "pipeline_stable_id",
                "procedure_stable_id",
                "parameter_association_stable_id_exp",
            ),
        )

        image_observations_x_impress_df = image_observations_x_impress_df.select(
            "observation_id", "fully_qualified_name"
        ).distinct()

        image_observations_x_impress_df = image_observations_x_impress_df.join(
            pipeline_core_df,
            (
                image_observations_x_impress_df["fully_qualified_name"]
                == pipeline_core_df["fully_qualified_name"]
            ),
            "left_outer",
        )
        group_by_expressions = [
            collect_set(
                when(
                    col("mouse_anatomy_id").isNotNull(), col("mouse_anatomy_id")
                ).otherwise(col("embryo_anatomy_id"))
            ).alias("embryo_anatomy_id_set"),
            collect_set(
                when(
                    col("mouse_anatomy_term").isNotNull(), col("mouse_anatomy_term")
                ).otherwise(col("embryo_anatomy_term"))
            ).alias("embryo_anatomy_term_set"),
            collect_set(
                when(
                    col("mouse_anatomy_id").isNotNull(), col("mouse_anatomy_id")
                ).otherwise(col("embryo_anatomy_id"))
            ).alias("anatomy_id"),
            collect_set(
                when(
                    col("mouse_anatomy_term").isNotNull(), col("mouse_anatomy_term")
                ).otherwise(col("embryo_anatomy_term"))
            ).alias("anatomy_term"),
            flatten(
                collect_set(
                    when(
                        col("mouse_anatomy_id").isNotNull(),
                        col("top_level_mouse_anatomy_id"),
                    ).otherwise(col("top_level_embryo_anatomy_id"))
                )
            ).alias("selected_top_level_anatomy_id"),
            flatten(
                collect_set(
                    when(
                        col("mouse_anatomy_id").isNotNull(),
                        col("top_level_mouse_anatomy_term"),
                    ).otherwise(col("top_level_embryo_anatomy_term"))
                )
            ).alias("selected_top_level_anatomy_term"),
            collect_set("impress_mp_id").alias("mp_id"),
            collect_set("impress_mp_term").alias("mp_term"),
            flatten(collect_set("impress_top_level_mp_id")).alias(
                "top_level_mp_id_set"
            ),
            flatten(collect_set("impress_top_level_mp_term")).alias(
                "top_level_mp_term_set"
            ),
            flatten(collect_set("impress_intermediate_mp_id")).alias(
                "intermediate_mp_id_set"
            ),
            flatten(collect_set("impress_intermediate_mp_term")).alias(
                "intermediate_mp_term_set"
            ),
        ]
        image_observations_x_impress_df = image_observations_x_impress_df.select(
            [
                "observation_id",
                "mouse_anatomy_id",
                "embryo_anatomy_id",
                "mouse_anatomy_term",
                "embryo_anatomy_term",
                "top_level_mouse_anatomy_id",
                "top_level_embryo_anatomy_id",
                "top_level_mouse_anatomy_term",
                "top_level_embryo_anatomy_term",
                "impress_mp_id",
                "impress_mp_term",
                "impress_top_level_mp_id",
                "impress_top_level_mp_term",
                "impress_intermediate_mp_id",
                "impress_intermediate_mp_term",
            ]
        )
        image_observations_x_impress_df = image_observations_x_impress_df.groupBy(
            "observation_id"
        ).agg(*group_by_expressions)

        image_observations_df = image_observations_df.join(
            image_observations_x_impress_df, "observation_id"
        )

        image_observations_df = image_observations_df.withColumn(
            "download_url",
            concat(
                lit(
                    "//www.ebi.ac.uk/mi/media/omero/webgateway/archived_files/download/"
                ),
                col("omero_id"),
            ),
        )
        image_observations_df = image_observations_df.withColumn(
            "jpeg_url",
            concat(
                lit("//www.ebi.ac.uk/mi/media/omero/webgateway/render_image/"),
                col("omero_id"),
            ),
        )
        image_observations_df = image_observations_df.withColumn(
            "thumbnail_url",
            concat(
                lit("//www.ebi.ac.uk/mi/media/omero/webgateway/render_birds_eye_view/"),
                col("omero_id"),
            ),
        )
        image_observations_df.write.parquet(output_path)
