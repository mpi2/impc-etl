"""
IMPRESS extractor
    extract_impress: Extract Impress data and load it to a dataframe
"""
from typing import List
import json
import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import udf, explode_outer
import requests
from impc_etl.shared.utils import convert_to_row


def extract_impress(spark_session: SparkSession,
                    impress_api_url: str, start_type: str) -> DataFrame:
    """

    :param spark_session:
    :param impress_api_url:
    :param start_type:
    :return:
    """
    root_index = requests.get("{}/{}/list".format(impress_api_url, start_type)).json()
    root_ids = [key for key in root_index.keys()]
    return get_entities_dataframe(spark_session, impress_api_url, start_type, root_ids)


def get_entities_dataframe(spark_session: SparkSession, impress_api_url,
                           impress_type: str, impress_ids: List[str]) -> DataFrame:
    """

    :param spark_session:
    :param impress_api_url:
    :param impress_type:
    :param impress_ids:
    :return:
    """
    entities = [get_impress_entity_by_id(impress_api_url, impress_type, impress_id) for impress_id
                in
                impress_ids]
    entity_df = spark_session.createDataFrame(convert_to_row(entity) for entity in entities)
    current_type = ''
    current_schema = entity_df.schema
    entity_df = process_collection(spark_session, impress_api_url,
                                   current_schema, current_type, entity_df)
    return entity_df


def process_collection(spark_session, impress_api_url, current_schema, current_type, entity_df):
    """

    :param spark_session:
    :param impress_api_url:
    :param current_schema:
    :param current_type:
    :param entity_df:
    :return:
    """
    impress_subtype = ''
    for column_name in current_schema.names:
        if 'Collection' in column_name:
            impress_subtype = column_name.replace('Collection', '')
            if current_type != '':
                column_name = current_type + '.' + column_name
            entity_id_column_name = impress_subtype + 'Id'
            entity_df = entity_df.withColumn(entity_id_column_name,
                                             explode_outer(entity_df[column_name]))
            sub_entity_schema = get_impress_entity_schema(spark_session, impress_api_url,
                                                          impress_subtype)
            get_entity_udf = udf(
                lambda x: get_impress_entity_by_id(impress_api_url, impress_subtype, x),
                StructType(sub_entity_schema))
            entity_column_name = impress_subtype
            entity_df = entity_df.withColumn(entity_column_name,
                                             get_entity_udf(entity_df[entity_id_column_name]))
            entity_df = process_collection(spark_session, impress_api_url, sub_entity_schema,
                                           impress_subtype, entity_df)
    return entity_df


def get_impress_entity_by_id(impress_api_url: str, impress_type: str, impress_id: str, retries=0):
    """

    :param impress_api_url:
    :param impress_type:
    :param impress_id:
    :param retries:
    :return:
    """
    #print('parsing :' + ("{}/{}/{}".format(impress_api_url, impress_type, impress_id)))
    try:
        response = requests.get("{}/{}/{}".format(impress_api_url, impress_type, impress_id))
        entity = response.json()
    except json.decoder.JSONDecodeError:
        print("{}/{}/{}".format(impress_api_url, impress_type, impress_id))
        print("         " + response.text)
        entity = None
    except requests.exceptions.ConnectionError:
        if retries < 4:
            time.sleep(1)
            entity = get_impress_entity_by_id(impress_api_url, impress_type, impress_id, retries+1)
        else:
            print("Max retries for " + "{}/{}/{}".format(impress_api_url, impress_type, impress_id))
            entity = None
    return entity


def get_impress_entity_schema(spark_session: SparkSession, impress_api_url: str, impress_type: str):
    """

    :param spark_session:
    :param impress_api_url:
    :param impress_type:
    :return:
    """
    schema_example = 1 if impress_type not in ['increment', 'option', 'parammpterm'] else 0
    first_entity = requests.get(
        "{}/{}/{}".format(impress_api_url, impress_type, schema_example)).text
    entity_rdd = spark_session.sparkContext.parallelize([first_entity])
    return spark_session.read.json(entity_rdd).schema
