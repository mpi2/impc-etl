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

    genes_df = genes_df.where(col("marker_name").isNotNull())
    genes_df = genes_df.select(col("marker_mgi_accession_id").alias("geneAccessionId"),
                               col("marker_symbol").alias("geneSymbol"),
                               col("marker_name").alias("geneName"),
                               col("feature_coord_start").alias("seqRegionStart"),
                               col("feature_coord_end").alias("seqRegionEnd"),
                               col("feature_strand").alias("seqRegionStrain"),
                               col("synonym").alias("synonyms")).distinct()
    genes_df = genes_df.withColumn('id', md5('geneAccessionId').substr(0, 25))
    alleles_df = alleles_df.select(col("allele_mgi_accession_id").alias("accessionId"),
                                   col("allele_symbol").alias("symbol"),
                                   col("allele_name").alias("name"),
                                   "marker_mgi_accession_id",
                                   col("mutation_type").alias("mutationType")
                                   ).distinct()
    alleles_df = alleles_df.where(alleles_df.symbol.isNotNull())
    alleles_schema = alleles_df.schema.names

    products_df = products_df.withColumn('allele_symbol',
                                         concat(
                                             col('marker_symbol'),
                                             lit('<'),
                                             col('allele_name'),
                                             lit('>'))
                                         )
    products_df = products_df.select(
        md5(concat("product_id", "name")).substr(0, 25).alias("id"),
        col('product_id').alias('productId'), 'allele_symbol', 'type',
        col('production_centre').alias('repository'),
        col('name').alias('productName'))
    alleles_df = alleles_df.join(genes_df, alleles_df.marker_mgi_accession_id == genes_df.geneAccessionId, 'left')
    alleles_df = alleles_df.groupBy(alleles_schema).agg(collect_set("id").alias('genes_ref'))
    alleles_df = alleles_df.drop('marker_mgi_accession_id')
    alleles_schema = alleles_df.schema.names

    alleles_df = alleles_df.join(products_df, alleles_df.symbol == products_df.allele_symbol, 'left')
    alleles_df = alleles_df.groupBy(alleles_schema).agg(collect_set("id").alias('products_ref'))
    alleles_schema = alleles_df.schema.names

    phenotyping_colonies_df = phenotyping_colonies_df\
        .select('Colony Name', 'Allele Symbol')\
        .withColumnRenamed('Allele Symbol', 'allele_symbol')\
        .withColumnRenamed('Colony Name', 'colony_id')
    samples_df = samples_df.join(phenotyping_colonies_df, samples_df._colonyID == phenotyping_colonies_df.colony_id, 'left')
    samples_df = samples_df.drop('_colonyID')
    samples_df = samples_df.select(
        md5('_specimenID').substr(0, 25).alias('id'),
        col('_specimenID').alias('specimenId'),
        col('colony_id').alias('colonyId'),
        col('_strainID').alias('geneticBackgroundStrain'),
        col('_zygosity').alias('zygosity'),
        col('_productionCentre').alias('productionCentre'),
        col('_phenotypingCentre').alias('phenotypingCentre'),
        col('_project').alias('project'),
        col('_pipeline').alias('pipeline'),
        col('_litterId').alias('litterId'),
        col('_gender').alias('gender'),
        'allele_symbol',
    )
    samples_df = samples_df.where(samples_df.id.isNotNull())
    alleles_df = alleles_df.join(samples_df, alleles_df.symbol == samples_df.allele_symbol, 'left')
    alleles_df.show(10)
    alleles_df = alleles_df.groupBy(alleles_schema).agg(collect_set("id").alias('samples_ref'))
    alleles_df.show(10)
    samples_df = samples_df.drop('allele_symbol')

    products_df = products_df.drop('allele_symbol').withColumnRenamed('productName', 'name')
    genes_df = genes_df.withColumnRenamed('geneAccessionId', 'accessionId')
    genes_df = genes_df.withColumnRenamed('geneSymbol', 'symbol')
    genes_df = genes_df.withColumnRenamed('geneName', 'name')
    alleles_df = alleles_df.withColumn('id', md5('symbol').substr(0, 25)).distinct()
    alleles_df = alleles_df.where(alleles_df.id.isNotNull())

    nodes = []
    gene_nodes_df = genes_df.withColumn('_typeName', lit('Gene'))
    nodes.extend(gene_nodes_df.distinct().toJSON().collect())

    product_nodes_df = products_df.withColumn('_typeName', lit('Product'))
    nodes.extend(product_nodes_df.distinct().toJSON().collect())

    samples_nodes_df = samples_df.withColumn('_typeName', lit('Sample'))
    nodes.extend(samples_nodes_df.distinct().toJSON().collect())

    allele_nodes_df = alleles_df \
        .select([column_name for column_name in alleles_df.schema.names if
                 not column_name.endswith('_ref')]) \
        .withColumn('_typeName', lit('Allele'))
    nodes.extend(allele_nodes_df.distinct().toJSON().collect())

    for index, chunk in enumerate(chunks(nodes, 10000)):
        name = ''
        if index + 1 < 10:
            name = '00'
        elif index + 1 < 100:
            name = '0'
        name += str(index + 1)
        with open(output_path + '/prisma/nodes/{}.json'.format(name), 'w') as fp:
            fp.write('{ "valueType": "nodes", "values": [' + ', '.join(chunk) + '] }')

    relations = get_relations(alleles_df, 'Allele')

    for index, chunk in enumerate(chunks(relations, 10000)):
        name = ''
        if index + 1 < 10:
            name = '00'
        elif index + 1 < 100:
            name = '0'
        name += str(index + 1)
        json_list = [json.dumps(item) for item in chunk]
        with open(output_path + '/prisma/relations/{}.json'.format(name), 'w') as fp:
            fp.write('{ "valueType": "relations", "values": [' + ', '.join(json_list) + '] }')
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
