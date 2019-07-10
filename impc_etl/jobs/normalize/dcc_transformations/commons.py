from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, when, udf, lower
from pyspark.sql.types import StringType

from impc_etl.config import Constants


def map_centre_id(dcc_experiment_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.withColumn('_centreID',
                                                     udf(map_centre_ids, StringType())(
                                                         '_centreID'))
    return dcc_experiment_df


def map_project_id(dcc_experiment_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.withColumn('_project',
                                                     udf(map_project_ids, StringType())(
                                                         '_project'))
    return dcc_experiment_df


def map_centre_ids(centre_id: str):
    return Constants.CENTRE_ID_MAP[centre_id.lower()] if centre_id is not None else None


def map_project_ids(centre_id: str):
    return Constants.PROJECT_ID_MAP[centre_id.lower()] if centre_id is not None else None


def truncate_specimen_id(specimen_id: str) -> str:
    if specimen_id.endswith('_MRC_Harwell'):
        return specimen_id[:specimen_id.rfind('_MRC_Harwell')]
    else:
        return specimen_id[:specimen_id.rfind('_')]


def override_europhenome_datasource(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn('_dataSource',
                               when((dcc_df['_dataSource'] == 'EuroPhenome')
                                    & (~lower(dcc_df['_colonyID']).startswith('baseline'))
                                    & (dcc_df['_colonyID'].isNotNull())
                                    & ((dcc_df['Phenotyping Consortium'] == 'MGP') | (
                                       dcc_df['Phenotyping Consortium'] == 'MGP Legacy')),
                                    lit('MGP'))
                               .otherwise(dcc_df['_dataSource'])
                               )
    dcc_df = dcc_df.withColumn('_project',
                               when((dcc_df['_dataSource'] == 'EuroPhenome')
                                    & (~lower(dcc_df['_colonyID']).startswith('baseline'))
                                    & (dcc_df['_colonyID'].isNotNull())
                                    & ((dcc_df['Phenotyping Consortium'] == 'MGP') | (
                                       dcc_df['Phenotyping Consortium'] == 'MGP Legacy')),
                                    lit('MGP'))
                               .otherwise(dcc_df['_project'])
                               )
    return dcc_df
