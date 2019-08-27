"""
IMPRESS extractor
    extract_impress: Extract Impress data and load it to a dataframe
"""
from typing import List
import json
import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
import requests
from impc_etl import logger
import sys
import vcr


def extract_impress(impress_api_url: str, start_type: str) -> DataFrame:
    """

    :param spark_session:
    :param impress_api_url:
    :param start_type:
    :return:
    """
    impress_api_url = (
        impress_api_url[:-1] if impress_api_url.endswith("/") else impress_api_url
    )
    root_index = requests.get(
        "{}/{}/list".format(impress_api_url, start_type), timeout=(5, 14)
    ).json()
    root_ids = [key for key in root_index.keys()]
    return get_entities(impress_api_url, start_type, root_ids)


def get_entities(
    impress_api_url, impress_type: str, impress_ids: List[str]
) -> DataFrame:
    """

    :param spark_session:
    :param impress_api_url:
    :param impress_type:
    :param impress_ids:
    :return:
    """
    entities = [
        get_impress_entity_by_id(impress_api_url, impress_type, impress_id)
        for impress_id in impress_ids
    ]
    entity_df = process_collection(impress_api_url, entities)
    return entity_df


def process_collection(impress_api_url, entities):
    """

    :param spark_session:
    :param impress_api_url:
    :param current_schema:
    :param current_type:
    :param entity_df:
    :return:
    """
    for entity in entities:
        for field in entity.keys():
            if "Collection" in field:
                impress_subtype = field.replace("Collection", "")
                process_collection(
                    impress_api_url,
                    get_impress_entity_by_ids(
                        impress_api_url, impress_subtype, entity[field]
                    ),
                )

    return entities


def get_impress_entity_by_id(
    impress_api_url: str, impress_type: str, impress_id: str, retries=0
):
    """

    :param impress_api_url:
    :param impress_type:
    :param impress_id:
    :param retries:
    :return:
    """
    api_call_url = "{}/{}/{}".format(impress_api_url, impress_type, impress_id)
    logger.info("parsing :" + api_call_url)
    if impress_id is None:
        return None
    try:
        response = requests.get(api_call_url, timeout=(5, 14))
        try:
            entity = response.json()
            entity["url"] = api_call_url
            entity["error"] = response.text
        except json.decoder.JSONDecodeError:
            logger.info("{}/{}/{}".format(impress_api_url, impress_type, impress_id))
            logger.info("         " + response.text)
            if response.text == "":
                raise requests.exceptions.RequestException(response=response)
            entity = dict(url=api_call_url, error="Error decoding: " + response.text)
    except requests.exceptions.RequestException as e:
        if retries < 4:
            time.sleep(1)
            entity = get_impress_entity_by_id(
                impress_api_url, impress_type, impress_id, retries + 1
            )
        else:
            logger.info(
                "Max retries for "
                + "{}/{}/{}".format(impress_api_url, impress_type, impress_id)
            )
            entity = dict(
                url=api_call_url, error="Max retries with response: " + str(e.response)
            )
    return entity


def get_impress_entity_by_ids(
    impress_api_url: str, impress_type: str, impress_ids: List[str], retries=0
):
    """

    :param impress_api_url:
    :param impress_type:
    :param impress_id:
    :param retries:
    :return:
    """
    api_call_url = "{}/{}/multiple".format(impress_api_url, impress_type)
    logger.info("parsing :" + api_call_url)
    if impress_ids is None or len(impress_ids) == 0:
        return []
    try:
        response = requests.post(api_call_url, json=impress_ids)
        try:
            entity = response.json()
        except json.decoder.JSONDecodeError:
            logger.info("{}/{}/multiple".format(impress_api_url, impress_type))
            logger.info("         " + response.text)
            if response.text == "":
                raise requests.exceptions.RequestException(response=response)
            entity = dict(url=api_call_url, error="Error decoding: " + response.text)
    except requests.exceptions.RequestException as e:
        if retries < 4:
            time.sleep(1)
            entity = get_impress_entity_by_ids(
                impress_api_url, impress_type, impress_ids, retries + 1
            )
        else:
            logger.info(
                "Max retries for "
                + "{}/{}/multiple".format(impress_api_url, impress_type)
            )
            entity = dict(
                url=api_call_url, error="Max retries with response: " + str(e.response)
            )
    return entity


def get_impress_entity_schema(
    spark_session: SparkSession, impress_api_url: str, impress_type: str
):
    """

    :param spark_session:
    :param impress_api_url:
    :param impress_type:
    :return:
    """
    schema_example = (
        1 if impress_type not in ["increment", "option", "parammpterm"] else 0
    )
    first_entity = requests.get(
        "{}/{}/{}".format(impress_api_url, impress_type, schema_example)
    ).text
    entity_rdd = spark_session.sparkContext.parallelize([first_entity])
    return (
        spark_session.read.json(entity_rdd)
        .withColumn("url", lit(""))
        .withColumn("error", lit(""))
        .schema
    )


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
                    [3]: File type (experiment or specimen)
                    [4]: Entity type (experiment, line, mouse or embryo)
    """
    extract_impress("https://api.mousephenotype.org/impress/", "pipeline")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
