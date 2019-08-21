from impc_etl.shared.transformations.commons import *
from pyspark.sql.functions import concat, col


def map_strain_names(colonies_df: DataFrame) -> DataFrame:
    #TODO implemente strain name mapping
    return colonies_df


def generate_genetic_background(colonies_df: DataFrame) -> DataFrame:
    #TODO validate Genetic Background generation
    colonies_df = colonies_df.withColumn('genetic_background', concat(lit('involves: '), col('colony_background_strain')))
    return colonies_df


def map_colonies_df_ids(colonies_df: DataFrame) -> DataFrame:
    colonies_df = colonies_df.withColumn('phenotyping_centre',
                                         udf(map_centre_ids, StringType())(
                                             'phenotyping_centre'))
    colonies_df = colonies_df.withColumn('production_centre',
                                         udf(map_centre_ids, StringType())(
                                             'production_centre'))
    colonies_df = colonies_df.withColumn('phenotyping_consortium',
                                         udf(map_project_ids, StringType())(
                                             'phenotyping_consortium'))
    colonies_df = colonies_df.withColumn('production_consortium',
                                         udf(map_project_ids, StringType())(
                                             'production_consortium'))
    return colonies_df
