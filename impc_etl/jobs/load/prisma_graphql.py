"""
Prisma GRAPHQL module
   Generates the JSON files consumed by prisma.io in order to expose a GrahpQL endpoint
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from impc_etl.shared.utils import flatten_struct


def generate_nodes():
    """

    :return:
    """


def load(genes_df: DataFrame, alleles_df: DataFrame, samples_df: DataFrame,
         observation_df: DataFrame, impress_df: DataFrame):
    impress_df.printSchema()
    impress_df.show()
    parameters_df = impress_df.select('parameter')
    parameters_df = parameters_df.select(flatten_struct(parameters_df.schema))
    parameters_df = parameters_df.select("parameterId",
                                         col("parameterKey").alias("paratemerStableId"),
                                         "name", "description",
                                         col("parammptermCollection").alias(
                                             "potentialPhenotypeTerms")
                                         ).sort("parameterId").distinct()
    parameters_df = parameters_df.where(col("parameterId").isNotNull())
    procedures_df = impress_df.select('procedure')
    procedures_df = procedures_df.select(flatten_struct(procedures_df.schema))
    procedures_df = procedures_df.select("procedureId",
                                         col("procedureKey").alias("procedureStableId"),
                                         "name", "description",
                                         col("parameterCollection").alias(
                                             "parameters")
                                         ).sort("procedureId").distinct()
    procedures_df = procedures_df.where(col("procedureId").isNotNull())
    procedures_df.show(100)
    parameters_df.show(100)

    return
