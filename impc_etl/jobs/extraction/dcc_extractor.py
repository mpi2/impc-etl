"""
DCC loader module
    extract_observations:
    extract_ontological_observations:
    extract_unidimensional_observations:
    extract_time_series_observations:
    extract_categorical_observations:
    extract_samples:
"""
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, concat_ws


def extract_observations(spark_session: SparkSession, file_path: str) -> Tuple[DataFrame, DataFrame]:
    """

    :param spark_session:
    :param file_path:
    :return:
    """
    experiments_df = _extract_experiment_files(spark_session, file_path)
    unidimensional_observations = extract_unidimensional_observations(experiments_df)
    ontological_observations = extract_ontological_observations(experiments_df)
    return unidimensional_observations, ontological_observations


def extract_ontological_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    ontological_observations = experiments_df \
        .withColumn('ontologyParameter', explode('procedure.ontologyParameter'))
    ontological_observations = ontological_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', ontological_observations['ontologyParameter']['_parameterID']) \
        .withColumn('term', ontological_observations['ontologyParameter']['term']) \
        .drop('procedure') \
        .drop('ontologyParameter')
    return ontological_observations


def extract_unidimensional_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    unidimensional_observations = experiments_df \
        .withColumn('simpleParameter', explode('procedure.simpleParameter'))
    unidimensional_observations = unidimensional_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', unidimensional_observations['simpleParameter']['_parameterID']) \
        .withColumn('value', unidimensional_observations['simpleParameter']['value']) \
        .drop('procedure') \
        .drop('simpleParameter')
    return unidimensional_observations


def extract_time_series_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    time_series_observations = experiments_df \
        .withColumn('seriesParameter', explode('procedure.seriesParameter'))
    time_series_observations = time_series_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', time_series_observations['seriesParameter']['_parameterID']) \
        .withColumn('value', time_series_observations['seriesParameter']['value']) \
        .drop('procedure') \
        .drop('seriesParameter')
    return time_series_observations


def extract_metadata_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    time_series_observations = experiments_df \
        .withColumn('seriesParameter', explode('procedure.seriesParameter'))
    time_series_observations = time_series_observations \
        .withColumn('procedureId', experiments_df['procedure']['_procedureID']) \
        .withColumn('parameterId', time_series_observations['seriesParameter']['_parameterID']) \
        .withColumn('value', time_series_observations['seriesParameter']['value']) \
        .drop('procedure') \
        .drop('seriesParameter')
    return time_series_observations


def extract_categorical_observations(experiments_df: DataFrame) -> DataFrame:
    """

    :param experiments_df:
    :return:
    """
    return experiments_df


def extract_samples(specimens_dataframe: DataFrame) -> DataFrame:
    """
    :param specimens_dataframe:
    :return:
    """
    return specimens_dataframe


def _extract_experiment_files(spark_session: SparkSession,
                              experiment_dir_path: str) -> DataFrame:
    """

    :param spark_session:
    :param experiment_dir_path:
    :return:
    """
    experiments_df = spark_session.read.format("com.databricks.spark.xml") \
        .options(rowTag="experiment").load(experiment_dir_path)
    return experiments_df


def _extract_specimen_files(spark_session: SparkSession,
                            specimen_dir_path: str) -> Tuple[DataFrame, DataFrame]:
    """

    :param spark_session:
    :param specimen_dir_path:
    :return:
    """
    mice_df = spark_session.read.format("com.databricks.spark.xml") \
        .options(rowTag="mouse").load(specimen_dir_path)
    embryos_df = spark_session.read.format("com.databricks.spark.xml") \
        .options(rowTag="embryo").load(specimen_dir_path)
    return mice_df, embryos_df
