"""
DCC loader module
    extract_observations:
    extract_ontological_observations:
    extract_unidimensional_observations:
    extract_time_series_observations:
    extract_categorical_observations:
    extract_samples:
"""

import os
from typing import Tuple, List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, lit, input_file_name
from impc_etl.jobs.extraction.dcc_schemas import get_specimen_centre_schema, flatten_specimen_df


def extract_observations(spark_session: SparkSession,
                         file_path: str) -> Tuple[DataFrame, DataFrame]:
    """

    :param spark_session:
    :param file_path:
    :return:
    """
    experiments_df = extract_experiment_files(spark_session, file_path)
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


def extract_experiment_files2(spark_session: SparkSession,
                              experiment_dir_path: str) -> DataFrame:
    """

    :param spark_session:
    :param experiment_dir_path:
    :return:
    """
    experiments_df = spark_session.read.format("com.databricks.spark.xml") \
        .options(rowTag="experiment", samplingRatio="1").load(experiment_dir_path)
    return experiments_df


def extract_experiment_files(spark_session: SparkSession, schema, xml_inputs: List[Dict]) -> DataFrame:

    result_df: DataFrame = None

    for input_experiments in xml_inputs:
        datasource_short_name = input_experiments.get('ds_short_name')
        path = input_experiments.get('file_path') + "/*experiment*"

        print(f"loading datasource '{datasource_short_name}' from path '{path}'")

        experiments_df = spark_session.read.format("com.databricks.spark.xml") \
            .options(rowTag='experiment')\
            .schema(schema)\
            .load(path)\
            .withColumn('_type', lit('Experiment'))\
            .withColumn('_datasourceShortName', lit(datasource_short_name))

        lines_df = spark_session.read.format("com.databricks.spark.xml") \
            .options(rowTag='line')\
            .load(path)\
            .withColumn('_type', lit('Line'))\
            .withColumn('_datasourceShortName', lit(datasource_short_name))

        # result_df = merge_tables(merge_tables(experiments_df, lines_df), result_df)

    return result_df


def extract_specimen_files(spark_session: SparkSession, xml_inputs: List[Dict]):

    schema = get_specimen_centre_schema()

    printed: bool = False

    specimen_df: DataFrame = None

    for input_specimens in xml_inputs:
        datasource_short_name = input_specimens.get('datasourceShortName')
        path = input_specimens.get('file_path') + "/*specimen*"

        print(f"loading datasource '{datasource_short_name}' from path '{path}'")

        row_tag = 'ns2:centre' if datasource_short_name == '3i' else 'centre'
        centre_df = spark_session.read.format("com.databricks.spark.xml") \
            .options(rowTag=row_tag)\
            .schema(schema)\
            .load(path)

        if not printed:
            print('\ncentre_df schema:')
            centre_df.printSchema()
            printed = True

        flattened_df = flatten_specimen_df(centre_df, input_file_name(), datasource_short_name)
        specimen_df = flattened_df if specimen_df is None else specimen_df.union(flattened_df)

    return specimen_df


def get_inputs(dcc_xml_file_path):

    xml_files_path = []

    datasource_short_names = os.listdir(dcc_xml_file_path)

    for datasource_short_name in datasource_short_names:
        xml_path = dcc_xml_file_path + '/' + datasource_short_name

        for root, directories, filenames in os.walk(xml_path):
            if len(filenames) > 0:
                datasource_part = {'datasourceShortName': datasource_short_name, 'file_path': root}
                xml_files_path.append(datasource_part)

    return xml_files_path
