"""
IMPRESS extractor
    extract_impress: Extract Impress data and load it to a dratafram
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import udf, explode
import requests
from typing import List
from impc_etl.shared.utils import convert_to_row


def extract_impress(spark_session: SparkSession, impress_api_url: str, start_type: str) -> DataFrame:
    root_index = requests.get("{}/{}/list".format(impress_api_url, start_type)).json()
    root_ids = [key for key in root_index.keys()]
    return get_entities_dataframe(spark_session, impress_api_url, start_type, root_ids)


def get_entities_dataframe(spark_session: SparkSession, impress_api_url, impress_type: str, impress_ids: List[str]):
    entities = [get_impress_entity_by_id(impress_api_url, impress_type, impress_id) for impress_id in
                impress_ids]
    entity_df = spark_session.createDataFrame(convert_to_row(entity) for entity in entities)
    for column_name in entity_df.schema.names:
        if 'Collection' in column_name:
            impress_subtype = column_name.replace('Collection', '')
            exploded = entity_df.withColumn(impress_subtype + 'Id', explode(entity_df[column_name]))
            exploded.show()
            # impress_entity_schema = get_impress_entity_schema(spark_session, impress_api_url, impress_subtype)
            # get_impress_entity_by_id_udf = udf(lambda x: print(x), ArrayType(impress_entity_schema))
            # entity_df = entity_df.withColumn(impress_subtype, get_impress_entity_by_id_udf(entity_df[column_name]))
    entity_df.show()
    # entity_df.printSchema()
    return entity_df


def get_impress_entity_by_id(impress_api_url: str, impress_type: str, impress_id: str):
    entity = requests.get("{}/{}/{}".format(impress_api_url, impress_type, impress_id)).json()
    return entity


def get_impress_entity_schema(spark_session: SparkSession, impress_api_url: str, impress_type: str):
    first_entity = requests.get("{}/{}/{}".format(impress_api_url, impress_type, 1)).text
    entity_rdd = spark_session.sparkContext.parallelize([first_entity])
    return spark_session.read.json(entity_rdd).schema



# print(json.dumps(get_impress_data('http://sandbox.mousephenotype.org/impress/', 'pipeline')))
