"""
Prisma GRAPHQL module
   Generates the JSON files consumed by prisma.io in order to expose a GrahpQL endpoint
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, collect_set, md5, explode
from impc_etl.shared.utils import flatten_struct
import json


def generate_nodes():
    """

    :return:
    """


def load(genes_df: DataFrame, alleles_df: DataFrame, products_df: DataFrame,
         phenotyping_colonies_df: DataFrame, samples_df: DataFrame,
         observation_df: DataFrame, impress_df: DataFrame, output_path='../tests/data/'):
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

    return


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def get_relations(alleles_df, type_name):
    relations = []
    for field_name, field_type in alleles_df.dtypes:
        if not field_name.endswith('_ref'):
            continue
        ndf_field = field_name.replace('_ref', '')
        if field_type == 'string':
            allele_1to1_relations_df = alleles_df.select('id', col(field_name).alias(
                ndf_field))
            allele_1to1_relations = allele_1to1_relations_df.rdd.map(
                lambda row: row.asDict()).collect()
            allele_1to1_relations = [
                [
                    {"_typeName": type_name, "id": relation['id'], "fieldName": ndf_field},
                    {"_typeName": "%s%s" % (
                        ndf_field[0].upper(), ndf_field[1:]),
                     "id": relation[ndf_field]}

                ] for relation in allele_1to1_relations
            ]
            relations.extend(allele_1to1_relations)
        if field_type == 'array<string>':
            allele_1tom_relations_df = alleles_df.select('id', explode(field_name).alias(
                field_name.replace('_ref', '')))
            allele_1tom_relations = allele_1tom_relations_df.rdd.map(
                lambda row: row.asDict()).collect()
            allele_1tom_relations = [
                [
                    {"_typeName": type_name, "id": relation['id'], "fieldName": ndf_field},
                    {"_typeName": "%s%s" % (
                        ndf_field[0].upper(), ndf_field[1:-1]),
                     "id": relation[ndf_field]}

                ] for relation in allele_1tom_relations
            ]
            relations.extend(allele_1tom_relations)
    return relations
