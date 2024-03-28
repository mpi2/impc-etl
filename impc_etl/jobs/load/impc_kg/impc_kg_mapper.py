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
    first,
    trim,
    struct,
    zip_with,
    expr,
)

from impc_etl.jobs.extract.gene_ref_extractor import ExtractGeneRef
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.impc_api.impc_api_mapper import (
    to_camel_case,
    ImpcDatasetsMetadataMapper,
    ImpcGenePhenotypeHitsMapper,
)
from impc_etl.jobs.load.solr.gene_mapper import GeneLoader
from impc_etl.jobs.load.solr.impc_images_mapper import ImpcImagesLoader
from impc_etl.jobs.load.solr.pipeline_mapper import ImpressToParameterMapper
from impc_etl.jobs.load.solr.stats_results_mapper import StatsResultsMapper
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


def _map_unique_ids(df: DataFrame, id_column_name: str, array_column: str) -> DataFrame:
    df = df.withColumn(
        id_column_name, expr(f"transform({array_column}, x -> md5(CAST(x AS STRING)))")
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
        output_col_map = {"sub_term_id": "ontology_term_curie"}
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


class ImpcKgExperimentMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgExperimentMapper"

    extra_cols: str = ""
    experiment_type = ""

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
            f"{self.output_path}/impc_kg/{self.experiment_type}_experiment_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.experiment_type,
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
        experiment_type = args[1]
        extra_cols = args[2].replace(" ", "").split(",") if args[2] != "" else []
        output_path = args[3]

        input_df = spark.read.parquet(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "procedure_id",
            ["pipeline_stable_id", "procedure_stable_id"],
        )
        input_df = _add_unique_id(
            input_df, "phenotyping_center_id", ["phenotyping_center"]
        )
        output_cols = [
            "experiment_id",
            "phenotyping_center_id",
            "procedure_id",
            "metadata",
            "metadata_group",
            "experiment_type",
        ]

        if experiment_type == "specimen":
            input_df = input_df.where(col("specimen_id").isNotNull())
        else:
            input_df = input_df.where(col("specimen_id").isNull())
            input_df = _add_unique_id(input_df, "colony_id", ["colony_id"])
        input_df = input_df.withColumn("experiment_type", lit(experiment_type))
        output_df = (
            input_df.groupBy(*output_cols + extra_cols)
            .agg(collect_set("observation_id").alias("observation_ids"))
            .distinct()
        )
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name),
            )
        output_df.write.json(output_path, mode="overwrite")


class ImpcKgSpecimenExperimentMapper(ImpcKgExperimentMapper):
    extra_cols = (
        "specimen_id,life_stage_name,weight,weight_date,weight_days_old,age_in_weeks"
    )
    experiment_type = "specimen"


class ImpcKgLineExperimentMapper(ImpcKgExperimentMapper):
    extra_cols = "colony_id"
    experiment_type = "line"


class ImpcKgSpecimenMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgSpecimenMapper"

    extra_cols: str = ""
    specimen_type = ""

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
            f"{self.output_path}/impc_kg/{self.specimen_type}_specimen_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.specimen_type,
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
        specimen_type = args[1]
        extra_cols = args[2].replace(" ", "").split(",") if args[2] != "" else []
        output_path = args[3]

        input_df = spark.read.parquet(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "procedure_id",
            ["pipeline_stable_id", "procedure_stable_id"],
        )
        input_df = _add_unique_id(
            input_df, "phenotyping_center_id", ["phenotyping_center"]
        )
        input_df = _add_unique_id(
            input_df, "production_center_id", ["production_center"]
        )
        input_df = _add_unique_id(input_df, "pipeline_id", ["pipeline_stable_id"])
        input_df = _add_unique_id(input_df, "colony_id", ["colony_id"])
        input_df = _add_unique_id(input_df, "strain_id", ["strain_accession_id"])
        output_cols = [
            "specimen_id",
            "colony_id",
            "genetic_background",
            "zygosity",
            "production_center_id",
            "phenotyping_center_id",
            "project_name",
            "litter_id",
            "biological_sample_group",
            "sex",
            "pipeline_id",
            "strain_id",
        ]
        input_df = input_df.where(col("specimen_id").isNotNull())

        if specimen_type == "mouse":
            input_df = input_df.where(col("date_of_birth").isNotNull())
        else:
            input_df = input_df.where(col("date_of_birth").isNull())
            input_df = input_df.withColumnRenamed(
                "developmental_stage_acc", "developmental_stage_curie"
            )

        input_df = input_df.withColumn("specimen_type", lit(specimen_type))
        output_df = input_df.select(*output_cols + extra_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name),
            )
        output_df.write.json(output_path, mode="overwrite")


class ImpcKgMouseSpecimenMapper(ImpcKgSpecimenMapper):
    extra_cols = "date_of_birth"
    specimen_type = "mouse"


class ImpcKgEmbryoSpecimenMapper(ImpcKgSpecimenMapper):
    extra_cols = "developmental_stage_curie"
    specimen_type = "embryo"


class ImpcKgStatisticalResultMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgStatisticalResultMapper"
    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [
            ImpcDatasetsMetadataMapper(),
            StatsResultsMapper(raw_data_in_output="bundled"),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_kg/statistical_result_json"
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
        dataset_observation_index_parquet_path = args[1] + "_raw_data_ids"
        output_path = args[2]

        input_df = spark.read.json(input_parquet_path)
        dataset_observation_index_df = spark.read.parquet(
            dataset_observation_index_parquet_path
        )
        dataset_observation_index_df = (
            dataset_observation_index_df.select("doc_id", "observation_id")
            .groupBy("doc_id")
            .agg(collect_set("observation_id").alias("observations"))
        )
        dataset_observation_index_df = dataset_observation_index_df.withColumnRenamed(
            "doc_id", "statisticalResultId"
        )
        input_df = _add_unique_id(
            input_df,
            "parameter_id",
            ["pipelineStableId", "procedureStableId", "parameterStableId"],
        )
        input_df = _add_unique_id(
            input_df, "phenotyping_center_id", ["phenotypingCentre"]
        )
        input_df = _add_unique_id(
            input_df, "production_center_id", ["productionCentre"]
        )
        input_df = _add_unique_id(input_df, "colonyId", ["colonyId"])
        output_cols = [
            "alleleAccessionId",
            "classificationTag",
            "colonyId",
            "dataType",
            "datasetId",
            "phenotyping_center_id",
            "production_center_id",
            "lifeStageAcc",
            "metadataGroup",
            "metadataValues",
            "mgiGeneAccessionId",
            "phenotypeSex",
            "potentialPhenotypes",
            "projectName",
            "reportedEffectSize",
            "reportedPValue",
            "resourceName",
            "sex",
            "significant",
            "significantPhenotype",
            "softWindowing",
            "statisticalMethod",
            "status",
            "strainAccessionId",
            "summaryStatistics",
            "zygosity",
            "observations",
        ]
        input_df = input_df.join(
            dataset_observation_index_df, "datasetId", "left_outer"
        )
        output_df = input_df.select(*output_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name),
            )
        output_df.write.json(output_path, mode="overwrite")


class ImpcKgGenePhenotypeAssociationMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgGenePhenotypeAssociationMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ImpcGenePhenotypeHitsMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}/impc_kg/gene_phenotype_association_json"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        input_parquet_path = args[0]
        output_path = args[1]

        input_df = spark.read.json(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "parameter_id",
            ["pipelineStableId", "procedureStableId", "parameterStableId"],
        )
        input_df = _add_unique_id(
            input_df, "phenotyping_center_id", ["phenotypingCentre"]
        )
        input_df = _add_unique_id(input_df, "mouse_gene_id", ["mgiGeneAccessionId"])
        input_df = input_df.withColumnRenamed("id", "genePhenotypeAssociationId")
        input_df = input_df.withColumnRenamed("datasetId", "statisticalResultId")
        output_cols = [
            "genePhenotypeAssociationId",
            "alleleAccessionId",
            "phenotyping_center_id",
            "statisticalResultId",
            "effectSize",
            "lifeStageName",
            "mouse_gene_id",
            "pValue",
            "parameter_id",
            "phenotype",
            "projectName",
            "sex",
            "zygosity",
        ]
        output_df = input_df.select(*output_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name),
            )
        output_df.write.json(output_path, mode="overwrite")


class ImpcKgParameterMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgParameterMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ImpressToParameterMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}/impc_kg/parameter_json")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        input_parquet_path = args[0]
        output_path = args[1]

        input_df = spark.read.json(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "parameter_id",
            ["pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"],
        )

        input_df = input_df.withColumn("unit_x", trim(col("unit_x")))
        input_df = input_df.withColumn("unit_y", trim(col("unit_y")))
        input_df = input_df.withColumnRenamed(
            "mp_id",
            "potentialPhenotypeTermCuries",
        )

        input_df = input_df.withColumnRenamed(
            "parameter_name",
            "name",
        )

        output_cols = [
            "parameter_id",
            "parameter_stable_id",
            "name",
            "data_type",
            "unit_x",
            "unit_y",
            "potentialPhenotypeTermCuries",
        ]
        output_df = input_df.select(*output_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name),
            )
        output_df.distinct().write.json(output_path, mode="overwrite")


class ImpcKgProcedureMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgProcedureMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ImpressToParameterMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}/impc_kg/procedure_json")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        input_parquet_path = args[0]
        output_path = args[1]

        input_df = spark.read.json(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "procedure_id",
            ["pipeline_stable_id", "procedure_stable_id"],
        )

        input_df = _add_unique_id(
            input_df,
            "parameter_id",
            ["pipeline_stable_id", "procedure_stable_id", "parameter_stable_id"],
        )

        input_df = input_df.withColumnRenamed(
            "procedure_name",
            "name",
        )

        input_df = input_df.groupBy("procedure_id", "procedure_stable_id", "name").agg(
            collect_set("parameter_id").alias("parameters")
        )

        output_cols = ["procedure_id", "procedure_stable_id", "name", "parameters"]
        output_df = input_df.select(*output_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name),
            )
        output_df.distinct().write.json(output_path, mode="overwrite")


class ImpcKgPipelineMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgPipelineMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [ImpressToParameterMapper()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}/impc_kg/pipeline_json")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        spark = SparkSession(sc)

        # Parsing app options
        input_parquet_path = args[0]
        output_path = args[1]

        input_df = spark.read.json(input_parquet_path)
        input_df = _add_unique_id(
            input_df,
            "pipeline_id",
            ["pipeline_stable_id"],
        )
        input_df = _add_unique_id(
            input_df,
            "procedure_id",
            ["pipeline_stable_id", "procedure_stable_id"],
        )

        input_df = input_df.withColumnRenamed(
            "pipeline_name",
            "name",
        )

        input_df = input_df.groupBy("pipeline_id", "pipeline_stable_id", "name").agg(
            collect_set("procedure_id").alias("procedures")
        )

        output_cols = ["pipeline_id", "pipeline_stable_id", "name", "procedures"]
        output_df = input_df.select(*output_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name),
            )
        output_df.distinct().write.json(output_path, mode="overwrite")


class ImpcKgMouseGeneMapper(PySparkTask):
    """
    PySpark Task class to extract GenTar Product report data.
    """

    #: Name of the Spark task
    name: str = "ImpcKgMouseGeneMapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def requires(self):
        return [GeneLoader(), ExtractGeneRef()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/product_report_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}/impc_kg/mouse_gene_json")

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
        gene_parquet_path = args[0]
        gene_ref_parquet_path = args[0]
        output_path = args[1]

        gene_df = spark.read.json(gene_parquet_path)
        gene_ref_df = spark.read.json(gene_ref_parquet_path)

        gene_df = _add_unique_id(
            gene_df,
            "mouse_gene_id",
            ["mgi_accession_id"],
        )

        gene_ref_df = gene_ref_df.select("mgi_gene_acc_id", "human_gene_acc_id")

        gene_df = gene_df.join(
            gene_ref_df, col("mgi_accession_id") == col("mgi_gene_acc_id")
        ).drop("mgi_gene_acc_id")

        gene_df = _map_unique_ids(
            gene_df, "human_gene_orthologues", "human_gene_acc_id"
        )

        mouse_gene_col_map = {
            "marker_name": "name",
            "marker_symbol": "symbol",
            "mgi_accession_id": "mgiGeneAccessionId",
            "marker_synonym": "synonyms",
        }

        output_cols = [
            "marker_name",
            "marker_symbol",
            "mgi_accession_id",
            "marker_synonym",
            "seq_region_start",
            "seq_region_end",
            "chr_strand",
            "seq_region_id",
            "entrezgene_id",
            "ensembl_gene_id",
            "ccds_id",
            "ncbi_id",
            "human_gene_orthologues",
        ]
        output_df = gene_df.select(*output_cols).distinct()
        for col_name in output_df.columns:
            output_df = output_df.withColumnRenamed(
                col_name,
                to_camel_case(col_name)
                if col_name not in mouse_gene_col_map
                else to_camel_case(mouse_gene_col_map[col_name]),
            )
        output_df.distinct().write.json(output_path, mode="overwrite")
