"""
Static Data loader
    load_human_gene_orthologues:
    load_phenotyping_centres:
"""
from pyspark.sql import DataFrame, SparkSession
from impc_etl.shared import utils


def load_phenotyping_centres(spark_session: SparkSession, file_path: str) -> DataFrame:
    """
    :param spark_session:
    :param file_path:
    :return:
    """
    phenotyping_centres_df = utils.load_tsv(spark_session, file_path)
    return phenotyping_centres_df
