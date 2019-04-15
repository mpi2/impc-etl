"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame


def load(experiment_df: DataFrame, impress_df: DataFrame, specimen_df: DataFrame, imits_df: DataFrame):

    return