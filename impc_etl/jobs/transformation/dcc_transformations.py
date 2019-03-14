"""
DCC Transformations
    Module to group the DCC transformation functions
"""
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql.functions import explode_outer, lit, when, udf

SKIPPED_PROCEDURES = {'SLM_SLM', 'SLM_AGS', 'TRC_TRC', 'DSS_DSS', 'MGP_ANA', 'MGP_BCI', 'MGP_BMI',
                      'MGP_EEI', 'MGP_MLN', 'MGP_PBI', 'MGP_IMM'}
VALID_PROJECT_IDS = {'BaSH', 'DTCC', 'EUMODIC', 'EUCOMM-EUMODIC', 'Helmholtz GMC', 'JAX', 'NING',
                     'MARC', 'MGP', 'MGP Legacy', 'MRC', 'NorCOMM2', 'Phenomin', 'RIKEN BRC',
                     'KMPC', 'Kmpc', '3i', 'IMPC'}


def get_experiments(dcc_df: DataFrame) -> DataFrame:
    return _get_entity_by_type(dcc_df, 'experiment',
                               ['_centreID', '_pipeline', '_project', '_sourceFile', '_dataSource'])


def get_lines(dcc_df: DataFrame) -> DataFrame:
    return _get_entity_by_type(dcc_df, 'line',
                               ['_centreID', '_pipeline', '_project', '_sourceFile', '_dataSource'])


def get_mice(dcc_df: DataFrame) -> DataFrame:
    return _get_entity_by_type(dcc_df, 'mouse', ['_centreID', '_sourceFile', '_dataSource'])


def get_embryo(dcc_df: DataFrame) -> DataFrame:
    return _get_entity_by_type(dcc_df, 'embryo', ['_centreID', '_sourceFile', '_dataSource'])


def standarize_europhenome_experiments(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn('_project',
                               when(
                                   (dcc_df['_project'] == 'MGP Legacy'), 'MGP')
                               .otherwise(dcc_df['_project'])
                               )
    dcc_df = dcc_df.withColumn('specimenID',
                               when(dcc_df['_dataSource'] == 'EuroPhenome',
                                    udf(_truncate_specimen_id, StringType())(dcc_df['specimenID']))
                               .otherwise(dcc_df['specimenID']))
    return dcc_df


def standarize_3i_experiments(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.withColumn('_project',
                             when(
                                 (dcc_df['_dataSource'] == '3i') & (~dcc_df['_project'].isin()),
                                 'MGP')
                             .otherwise(dcc_df['_project']))


def drop_skipped_procedures(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.where(
        udf(_skip_procedure, BooleanType())
        (dcc_df['procedure._procedureID'])
    )


def drop_skipped_experiments(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.where(~
                        (
                                (
                                        dcc_df['_centreID'] == 'Ucd') &
                                (
                                        (dcc_df['_experimentID'] == 'GRS_2013-10-09_4326') |
                                        (dcc_df['_experimentID'] == 'GRS_2014-07-16_8800')
                                )
                        ))


def drop_null_data_source(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, '_dataSource')


def drop_null_centre_id(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, '_centreID')


def drop_null_project(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, '_project')


def drop_null_pipeline(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, '_pipeline')


def drop_null_procedure_id(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, 'procedure._procedureID')


def drop_null_specimen_id(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, 'specimenID')


def drop_null_date_of_experiment(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, '_dateOfExperiment')


def drop_null_colony_id(experiment_df: DataFrame, mice_df: DataFrame, embryo_df: DataFrame) -> DataFrame:
    mouse_specimens_df = mice_df.select('_specimenID', '_colonyID')
    embryo_specimens_df = embryo_df.select('_specimenID', '_colonyID')
    specimen_df = mouse_specimens_df.union(embryo_specimens_df)
    experiment_df_a = experiment_df.alias('exp')
    specimen_df_a = specimen_df.alias('spe')
    return experiment_df_a.join(specimen_df_a,
                                experiment_df_a['specimenID'] == specimen_df_a['_specimenID'])\
        .where(specimen_df_a['_colonyID'].isNotNull()).select('exp.*')


def _drop_if_null(dcc_df: DataFrame, column: str) -> DataFrame:
    return dcc_df.where(dcc_df[column].isNotNull())


def _get_entity_by_type(dcc_df: DataFrame, entity_type: str,
                        centre_columns: List[str]) -> DataFrame:
    centre_columns.append(dcc_df[entity_type])
    entity_df = dcc_df.where(dcc_df[entity_type].isNotNull()) \
        .select(centre_columns + [entity_type])
    entity_df = entity_df \
        .withColumn('tmp', explode_outer(entity_df[entity_type])) \
        .select(['tmp.*'] + centre_columns) \
        .withColumn('_type', lit(entity_type)) \
        .drop(entity_type)
    return entity_df


def _truncate_specimen_id(specimen_id: str) -> str:
    if specimen_id.endswith('_MRC_Harwell'):
        return specimen_id.replace('_MRC_Harwell', '')
    else:
        return specimen_id[:specimen_id.rfind('_')]


def _skip_procedure(procedure_name: str) -> bool:
    return procedure_name[:procedure_name.rfind('_')] not in SKIPPED_PROCEDURES
