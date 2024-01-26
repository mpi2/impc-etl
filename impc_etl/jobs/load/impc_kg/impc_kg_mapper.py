from typing import List

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    md5,
    when,
    concat,
    lit,
    col,
    arrays_zip,
    collect_set,
    explode,
)

from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.impc_api.impc_api_mapper import to_camel_case
from impc_etl.jobs.load.solr.impc_images_mapper import ImpcImagesLoader
from impc_etl.workflow.config import ImpcConfig


def _add_unique_id(df: DataFrame, id_column_name: str, columns: List[str]) -> DataFrame:
    df = df.withColumn(
        id_column_name,
        md5(
            concat(
                *[
                    when(col(col_name).isNull(), lit("")).otherwise(col(col_name))
                    for col_name in columns
                ]
            )
        ),
    )
    return df


class ImpcKgObservationMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgObservationMapper"

    extra_cols: str = ""
    observation_type = ""

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ExperimentToObservationMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_kg/{self.observation_type}_observation_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.observation_type,
            self.extra_cols,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        input_parquet_path = args[0]
        observation_type = args[1]
        extra_cols = args[2].replace(" ", "").split(",") if args[2] != "" else []
        output_path = args[3]

        input_df = spark.read.parquet(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "parameter_id",
            ["pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"],
        )
        input_df = _add_unique_id(
            input_df, "phenotyping_center_id", ["phenotyping_center"]
        )
        output_cols = [
            "observation_id",
            "phenotyping_center_id",
            "parameter_id",
            "observation_type",
        ]

        output_df = input_df.select(*output_cols + extra_cols).distinct()
        if observation_type != "":
            output_df = output_df.where(col("observation_type") == observation_type)
        output_col_map = {"sub_term_id": "ontology_term_ids"}
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(output_col_map[col_name])
                if col_name in output_col_map
                else to_camel_case(col_name),
            )
        output_df.write.json(output_path, mode="overwrite")


class ImpcKgUnidimensionalObservationMapper(ImpcKgObservationMapper):
    observation_type = "unidimensional"
    extra_cols = "data_point"


class ImpcKgCategoricalObservationMapper(ImpcKgObservationMapper):
    observation_type = "categorical"
    extra_cols = "category"


class ImpcKgTextObservationMapper(ImpcKgObservationMapper):
    observation_type = "text"
    extra_cols = "text_value"


class ImpcKgTimeSeriesObservationObservationMapper(ImpcKgObservationMapper):
    observation_type = "time_series"
    extra_cols = "data_point,time_point,discrete_point"


class ImpcKgOntologicalObservationMapper(ImpcKgObservationMapper):
    observation_type = "ontological"
    extra_cols = "sub_term_id"


class ImpcKgImageRecordObservationObservationMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgImageRecordObservationObservationMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ImpcImagesLoader(), ExperimentToObservationMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_kg/image_record_observation_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        input_parquet_path = args[0]
        observation_parquet_path = args[1]
        output_path = args[2]

        input_df = spark.read.parquet(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "parameter_id",
            ["pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"],
        )
        input_df = _add_unique_id(
            input_df, "phenotyping_center_id", ["phenotyping_center"]
        )
        observation_df = spark.read.parquet(observation_parquet_path)

        associated_observation_df = observation_df.select(
            col("observation_id").alias("related_observation_id"),
            "experiment_id",
            "parameter_stable_id",
        )
        image_association_df = input_df.drop("parameter_stable_id")
        image_association_df = image_association_df.withColumnRenamed(
            "parameter_association_stable_id", "parameter_stable_id"
        )
        image_association_df = image_association_df.select(
            "observation_id",
            "experiment_id",
            explode(
                arrays_zip(
                    "parameter_stable_id",
                    "parameter_association_sequence_id",
                )
            ).alias("parameter_association"),
        ).distinct()

        image_association_df = image_association_df.select(
            "observation_id", "experiment_id", "parameter_association.*"
        )

        image_association_df = image_association_df.join(
            associated_observation_df,
            ["experiment_id", "parameter_stable_id"],
            "left_outer",
        )

        image_association_df = (
            image_association_df.groupBy("observation_id")
            .agg(collect_set("related_observation_id").alias("related_observation_ids"))
            .select("observation_id", "related_observation_ids")
        )

        input_df = input_df.join(image_association_df, "observation_id", "left_outer")

        output_cols = [
            "observation_id",
            "phenotyping_center_id",
            "parameter_id",
            "observation_type",
            "download_url",
            "jpeg_url",
            "thumbnail_url",
            "omero_id",
            "increment_value",
            "related_observation_ids",
        ]

        output_df = input_df.select(*output_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(col_name, to_camel_case(col_name))
        output_df.write.json(output_path, mode="overwrite")
