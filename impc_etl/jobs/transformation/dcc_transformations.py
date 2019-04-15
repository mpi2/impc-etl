"""
DCC Transformations
    Module to group the DCC transformation functions
"""
from typing import List, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode_outer, lit, when, udf, col, lower, concat, \
    md5, explode, concat_ws, collect_list, create_map, sort_array
from pyspark.sql.types import StringType, BooleanType, ArrayType

from impc_etl import logger
from impc_etl.shared.utils import extract_parameters_from_derivation

SKIPPED_PROCEDURES = {
    'SLM_SLM', 'SLM_AGS', 'TRC_TRC', 'DSS_DSS', 'MGP_ANA', 'MGP_BCI', 'MGP_BMI',
    'MGP_EEI', 'MGP_MLN', 'MGP_PBI', 'MGP_IMM'
}
VALID_PROJECT_IDS = {
    'BaSH', 'DTCC', 'EUMODIC', 'EUCOMM-EUMODIC', 'Helmholtz GMC', 'JAX', 'NING',
    'MARC', 'MGP', 'MGP Legacy', 'MRC', 'NorCOMM2', 'Phenomin', 'RIKEN BRC',
    'KMPC', 'Kmpc', '3i', 'IMPC'
}
EUROPHENOME_VALID_COLONIES = {
    'EPD0013_1_G11_10613', 'EPD0019_1_A05_10494', 'EPD0023_1_F07_10481', 'EPD0033_3_F04_10955',
    'EPD0037_1_E07_10471', 'EPD0037_3_G01_10533', 'EPD0039_1_B01_10470', 'EPD0057_2_C02_10474',
    'EPD0065_2_E04_10968', 'EPD0089_4_F11_10538', 'EPD0100_4_A06_10472', 'EPD0135_1_A05_10967',
    'EPD0156_1_B01_10970', 'EPD0242_4_B03_10958', 'EPD0011_3_B08_10331',
    'EPD0017_3_E02_71', 'EPD0019_1_A05_10574', 'EPD0023_1_F07_10553', 'EPD0033_3_F04_10514',
    'EPD0037_1_B03_10395', 'EPD0037_1_E07_10554', 'EPD0038_2_B10_10557', 'EPD0057_1_H01_10556',
    'EPD0057_2_C02_10549', 'EPD0065_2_E04_10515', 'EPD0065_5_A04_10523', 'EPD0089_4_F11_320',
    'EPD0100_4_A06_10380', 'EPD0135_1_A05_10581', 'EPD0145_4_B09_10343', 'EPD0156_1_B01_10099',
    'EPD0242_4_B03_10521', 'EPD0023_1_F07_10594', 'EPD0037_1_B03_10632', 'EPD0037_1_E07_10595',
    'EPD0038_2_B10_10026', 'EPD0046_2_F02_216', 'EPD0057_1_H01_134', 'EPD0057_2_C02_10630',
    'EPD0065_2_E04_10028', 'EPD0065_5_A04_141', 'EPD0089_4_F11_10578', 'EPD0100_4_A06_10519',
    'EPD0135_1_A05_10631', 'EPD0156_1_B01_10517', 'EPD0242_4_B03_10579', 'EPD0011_3_B08_28',
    'EPD0013_1_G11_10560', 'EPD0017_3_E02_10220', 'EPD0019_1_A05_73', 'EPD0033_3_F04_10232',
    'EPD0037_1_B03_10234', 'EPD0037_3_G01_10649', 'EPD0039_1_B01_157', 'EPD0046_2_F02_10658',
    'EPD0135_1_A05_10562', 'EPD0145_4_B09_10826', 'EPD0242_4_B03_10233',
    'Dll1_C3H_113', 'Dll1_C3H_10333', 'EPD0059_2_C08_10660'
}

centre_map = {
    'J': 'JAX',
    'Gmc': 'HMGU',
    'Bcm': 'BCM',
    'Ics': 'ICS',
    'Tcp': 'TCP',
    'Rbrc': 'RIKEN BRC',
    'Ucd': 'UCD',
    'Kmpc': 'KMPC',
    'Hmgu': 'HMGU',
    'Ning': 'MARC',
    'Wtsi': 'WSI',
    'H': 'Harwell',
    'Ncom': 'TCP'
}


def process_experiments(dcc_experiment_df: DataFrame, mice_df: DataFrame, embryo_df: DataFrame, impress_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.transform(map_centre_id) \
        .transform(standarize_europhenome_experiments) \
        .transform(drop_skipped_experiments) \
        .transform(drop_skipped_procedures) \
        .transform(standarize_3i_experiments) \
        .transform(drop_null_centre_id) \
        .transform(drop_null_data_source) \
        .transform(drop_null_date_of_experiment) \
        .transform(drop_null_pipeline) \
        .transform(drop_null_project) \
        .transform(drop_null_specimen_id) \
        .transform(generate_unique_id)
    dcc_experiment_df = drop_null_colony_id(dcc_experiment_df, mice_df, embryo_df)
    dcc_experiment_df = re_map_europhenome_experiments(dcc_experiment_df, mice_df, embryo_df)
    dcc_experiment_df = generate_metadata_group(dcc_experiment_df, impress_df)
    dcc_experiment_df = generate_metadata(dcc_experiment_df, impress_df)
    return dcc_experiment_df


def map_centre_id(dcc_experiment_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.withColumn('_centreID',
                                                     udf(_map_centre_ids, StringType())(
                                                         '_centreID'))
    return dcc_experiment_df


def generate_unique_id(dcc_experiment_df: DataFrame):
    non_unique_columns = ['_type', '_sourceFile', '_VALUE',
                          'procedureMetadata', 'statusCode', '_sequenceID', '_project']
    dcc_experiment_df = dcc_experiment_df.withColumn('_sequenceIDStr',
                                                     when(col('_sequenceID').isNull(),
                                                          lit('NA')).otherwise(col('_sequenceID')))
    unique_columns = [col_name for col_name in dcc_experiment_df.columns
                      if col_name not in non_unique_columns
                      and not col_name.endswith('Parameter')]
    dcc_experiment_df = dcc_experiment_df.withColumn('unique_id', md5(concat(*unique_columns)))
    return dcc_experiment_df


def generate_metadata_group(dcc_experiment_df: DataFrame, impress_df) -> DataFrame:
    experiment_metadata = dcc_experiment_df.withColumn('procedureMetadata',
                                                     explode('procedureMetadata'))
    impress_df_required = impress_df.where((col('parameter.isImportant') == True))
    experiment_metadata = experiment_metadata.join(impress_df_required,
        experiment_metadata['procedureMetadata._parameterID'] == impress_df_required['parameter.parameterKey']
    )
    experiment_metadata = experiment_metadata.withColumn('metadataItem',
                                                         concat(col('parameter.name'), lit(' = '),
                                                                col('procedureMetadata.value')))
    experiment_metadata = experiment_metadata.groupBy('unique_id').agg(
        concat_ws('::', sort_array(collect_list(col('metadataItem')))).alias('metadataGroupList'))
    experiment_metadata = experiment_metadata.withColumn('metadataGroup',
                                                         md5(col('metadataGroupList')))
    experiment_metadata = experiment_metadata.drop('metadataGroupList')
    dcc_experiment_df = dcc_experiment_df.join(experiment_metadata, 'unique_id', 'left_outer')
    return dcc_experiment_df


def generate_metadata(dcc_experiment_df: DataFrame, impress_df) -> DataFrame:
    experiment_metadata = dcc_experiment_df.withColumn('procedureMetadata',
                                                     explode('procedureMetadata'))
    impress_df_required = impress_df.where((~lower(col('parameter.name')).contains('experimenter')))
    experiment_metadata = experiment_metadata.join(impress_df_required,
        experiment_metadata['procedureMetadata._parameterID'] == impress_df_required['parameter.parameterKey']
    )
    experiment_metadata = experiment_metadata.withColumn('metadataItem',
                                                         concat(col('parameter.name'), lit(' = '),
                                                                col('procedureMetadata.value')))
    experiment_metadata = experiment_metadata.groupBy('unique_id')\
        .agg(collect_list(col('metadataItem')).alias('metadata'))
    dcc_experiment_df = dcc_experiment_df.join(experiment_metadata, 'unique_id', 'left_outer')
    return dcc_experiment_df


def process_specimens(dcc_specimen_df: DataFrame, imits_df: DataFrame):
    dcc_specimen_df = dcc_specimen_df.transform(map_centre_id) \
        .transform(standarize_europhenome_specimen_ids) \
        .transform(standarize_europhenome_colony_ids)
    dcc_specimen_df = dcc_specimen_df.join(imits_df,
                                           (dcc_specimen_df['_colonyID'] == imits_df[
                                               'Colony Name']) & (
                                                   lower(dcc_specimen_df['_centreID']) ==
                                                   lower(imits_df['Phenotyping Centre'])),
                                           'left_outer')
    dcc_specimen_df = dcc_specimen_df.transform(override_europhenome_datasource)
    return dcc_specimen_df


def get_experiments(dcc_df: DataFrame) -> DataFrame:
    experiment_df = _get_entity_by_type(dcc_df, 'experiment',
                                        ['_centreID', '_pipeline', '_project', '_sourceFile',
                                         '_dataSource'])
    return experiment_df.select(
        ['procedure.*'] +
        [column for column in experiment_df.columns if column is not 'procedure'])\
        .drop('procedure')


def get_lines(dcc_df: DataFrame) -> DataFrame:
    return _get_entity_by_type(dcc_df, 'line',
                               ['_centreID', '_pipeline', '_project', '_sourceFile', '_dataSource'])


def get_derived_parameters(spark: SparkSession, dcc_experiment_df: DataFrame,
                           impress_df: DataFrame) -> DataFrame:
    derived_parameters: DataFrame = impress_df.where((impress_df['parameter.isDerived'] == True) & (
            impress_df['parameter.isDeprecated'] == False)) \
        .select('parameter.parameterKey', 'parameter.derivation', 'parameter.type').dropDuplicates()
    extract_parameters_from_derivation_udf = udf(extract_parameters_from_derivation,
                                                 ArrayType(StringType()))
    derived_parameters = derived_parameters \
        .withColumn('derivationInputs', extract_parameters_from_derivation_udf('derivation'))

    derived_parameters_ex = derived_parameters \
        .withColumn('derivationInput', explode('derivationInputs')) \
        .select('parameterKey', 'derivation', 'derivationInput')
    derived_parameters_ex = derived_parameters_ex.where(
        ~col('derivation').contains('unimplemented'))

    experiments_simple = _get_inputs_by_parameter_type(dcc_experiment_df,
                                                       derived_parameters_ex,
                                                       'simpleParameter')
    experiments_metadata = _get_inputs_by_parameter_type(dcc_experiment_df,
                                                         derived_parameters_ex,
                                                         'procedureMetadata')
    experiments_series = _get_inputs_by_parameter_type(dcc_experiment_df,
                                                       derived_parameters_ex,
                                                       'seriesParameter')
    experiments_vs_derivations = experiments_simple \
        .union(experiments_metadata) \
        .union(experiments_series)

    experiments_vs_derivations = experiments_vs_derivations.groupby(
        'unique_id', 'parameterKey', 'derivation').agg(concat_ws(',', collect_list(
        when(experiments_vs_derivations['derivationInput'].isNull(), lit('NOT_FOUND')).otherwise(
            experiments_vs_derivations['derivationInput']))).alias('derivationInputStr'))

    experiments_vs_derivations = experiments_vs_derivations.join(
        derived_parameters.drop('derivation'), 'parameterKey')
    experiments_vs_derivations = experiments_vs_derivations.withColumn('isComplete',
                                                                       udf(_check_complete_input,
                                                                           BooleanType())(
                                                                           'derivationInputs',
                                                                           'derivationInputStr'))
    complete_derivations = experiments_vs_derivations.where(col('isComplete') == True) \
        .withColumn('derivationInputStr', concat('derivation', lit(';'), 'derivationInputStr'))
    complete_derivations.createOrReplaceTempView('complete_derivations')
    spark.udf.registerJavaFunction('phenodcc_derivator',
                                   'org.mousephenotype.dcc.derived.parameters.SparkDerivator',
                                   StringType())
    results_df = spark.sql(
        """
           SELECT unique_id, parameterKey,
                  derivationInputStr,phenodcc_derivator(derivationInputStr) as result
           FROM complete_derivations
        """
    )

    results_df = results_df.groupBy('unique_id').agg(
        collect_list(create_map(results_df['parameterKey'], results_df['result'])).alias('results'))

    simple_parameter_type = None

    for c_type in dcc_experiment_df.dtypes:
        if c_type[0] == 'simpleParameter':
            simple_parameter_type = c_type[1]
            break

    dcc_experiment_df = dcc_experiment_df.join(results_df,
                                               'unique_id', 'left_outer') \
        .withColumn(
        'derivedParameters',
        when(
            results_df['results'].isNotNull(),
            udf(_append_simple_parameter, simple_parameter_type)(
                'results',
                'simpleParameter')
        ).otherwise(col('simpleParameter').cast(simple_parameter_type)))
    dcc_experiment_df.printSchema()
    dcc_experiment_df = dcc_experiment_df.drop('derivedParameters').drop(
        'complete_derivations.unique_id')

    return dcc_experiment_df


def _get_inputs_by_parameter_type(dcc_experiment_df, derived_parameters_ex, parameter_type):
    experiments_by_type = dcc_experiment_df.select('unique_id',
                                                   explode(parameter_type).alias(
                                                       parameter_type))
    if parameter_type == 'seriesParameter':
        experiments_by_type = experiments_by_type.select('unique_id',
                                                         parameter_type + '._parameterID',
                                                         explode(parameter_type + '.value').alias(
                                                             'value'))
        experiments_by_type = experiments_by_type.withColumn('value',
                                                             concat(col('value._incrementValue'),
                                                                    lit('|'),
                                                                    col('value._VALUE')))
        experiments_by_type = experiments_by_type \
            .groupBy('unique_id', '_parameterID').agg(
            concat_ws('$', collect_list('value')).alias('value'))

    parameter_id_column = parameter_type + '._parameterID' if parameter_type != 'seriesParameter' else '_parameterID'
    parameter_value_column = parameter_type + '.value' if parameter_type != 'seriesParameter' else 'value'
    experiments_vs_derivations = derived_parameters_ex \
        .join(experiments_by_type,
              experiments_by_type[parameter_id_column] ==
              derived_parameters_ex[
                  'derivationInput'], 'left_outer')
    experiments_vs_derivations: DataFrame = experiments_vs_derivations \
        .withColumn('derivationInput',
                    concat(
                        col(
                            'derivationInput'),
                        lit('$'), col(parameter_value_column)))
    experiments_vs_derivations.printSchema()
    return experiments_vs_derivations.drop(
        parameter_type) if parameter_type != 'seriesParameter' else experiments_vs_derivations.drop(
        'value', '_parameterID')


def _check_complete_input(input_list: List[str], input_str: str):
    complete = len(input_list) > 0
    for input_param in input_list:
        complete = complete and (input_param in input_str)
    return complete and 'NOT_FOUND' not in input_str


def _append_simple_parameter(results: List[Dict], simple_parameter: List):
    for result in results:
        if simple_parameter is not None and result is not None:
            parameter = result.keys()[0]
            value = result[parameter]
            simple_parameter.append(
                {'_parameterID': parameter, 'value': value, '_sequenceID': None, '_unit': None,
                 'parameterStatus': None})
    return simple_parameter


def get_mice(dcc_df: DataFrame) -> DataFrame:
    return _get_entity_by_type(dcc_df, 'mouse', ['_centreID', '_sourceFile', '_dataSource'])


def get_embryo(dcc_df: DataFrame) -> DataFrame:
    return _get_entity_by_type(dcc_df, 'embryo', ['_centreID', '_sourceFile', '_dataSource'])


def standarize_europhenome_experiments(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn('_project',
                               when(
                                   (dcc_df['_project'] == 'MGP Legacy'), lit('MGP'))
                               .otherwise(dcc_df['_project'])
                               )
    dcc_df = dcc_df.withColumn('specimenID',
                               when(dcc_df['_dataSource'] == 'EuroPhenome',
                                    udf(_truncate_specimen_id, StringType())(dcc_df['specimenID']))
                               .otherwise(dcc_df['specimenID']))
    return dcc_df


def standarize_europhenome_colony_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn('_colonyID',
                               when(dcc_df['_dataSource'] == 'EuroPhenome',
                                    udf(_truncate_colony_id, StringType())(dcc_df['_colonyID']))
                               .otherwise(dcc_df['_colonyID'])
                               )
    return dcc_df


def override_europhenome_datasource(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn('_dataSource',
                               when((dcc_df['_dataSource'] == 'EuroPhenome')
                                    & (~dcc_df['_colonyID'].startswith('baseline'))
                                    & (dcc_df['_colonyID'].isNotNull())
                                    & ((dcc_df['Phenotyping Consortium'] == 'MGP') | (
                                       dcc_df['Phenotyping Consortium'] == 'MGP Legacy')),
                                    lit('MGP'))
                               .otherwise(dcc_df['_dataSource'])
                               )
    dcc_df = dcc_df.withColumn('_project',
                               when((dcc_df['_dataSource'] == 'EuroPhenome')
                                    & (~dcc_df['_colonyID'].startswith('baseline'))
                                    & (dcc_df['_colonyID'].isNotNull())
                                    & ((dcc_df['Phenotyping Consortium'] == 'MGP') | (
                                       dcc_df['Phenotyping Consortium'] == 'MGP Legacy')),
                                    lit('MGP'))
                               .otherwise(dcc_df['_project'])
                               )
    return dcc_df


def standarize_europhenome_specimen_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn('_specimenID',
                               when((dcc_df['_dataSource'] == 'EuroPhenome') | (
                                       dcc_df['_dataSource'] == 'MGP'),
                                    udf(_truncate_specimen_id, StringType())(dcc_df['_specimenID']))
                               .otherwise(dcc_df['_specimenID'])
                               )
    return dcc_df


def standarize_3i_experiments(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.withColumn('_project',
                             when(
                                 (dcc_df['_dataSource'] == '3i') & (
                                     ~dcc_df['_project'].isin(VALID_PROJECT_IDS)),
                                 lit('MGP'))
                             .otherwise(dcc_df['_project']))


def drop_skipped_procedures(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.where(
        (udf(_skip_procedure, BooleanType())(dcc_df['_procedureID'])) | (
                dcc_df['_dataSource'] == '3i')
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
    return _drop_if_null(dcc_df, '_procedureID')


def drop_null_specimen_id(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, 'specimenID')


def drop_null_date_of_experiment(dcc_df: DataFrame):
    return _drop_if_null(dcc_df, '_dateOfExperiment')


def drop_null_colony_id(experiment_df: DataFrame, mice_df: DataFrame,
                        embryo_df: DataFrame) -> DataFrame:
    mouse_specimens_df = mice_df.select('_centreID', '_specimenID', '_colonyID',
                                        '_isBaseline', 'Phenotyping Consortium')
    embryo_specimens_df = embryo_df.select('_centreID', '_specimenID', '_colonyID',
                                           '_isBaseline', 'Phenotyping Consortium')
    specimen_df = mouse_specimens_df.union(embryo_specimens_df)
    specimen_df = specimen_df.where(
        (specimen_df['_colonyID'].isNotNull()) | (specimen_df['_isBaseline'] == True)
    ).dropDuplicates()
    experiment_df_a = experiment_df.alias('exp')
    specimen_df_a = specimen_df.alias('spe')

    experiment_df_a = experiment_df_a.join(specimen_df_a,
                                           (experiment_df_a['_centreID'] == specimen_df_a[
                                               '_centreID'])
                                           & (experiment_df_a['specimenID'] == specimen_df_a[
                                               '_specimenID'])
                                           )
    experiment_df = experiment_df_a.select('exp.*')
    return experiment_df.dropDuplicates()


def re_map_europhenome_experiments(dcc_experiment_df, mice_df, embryo_df):
    mouse_specimens_df = mice_df.select('_centreID', '_specimenID', '_colonyID',
                                        '_isBaseline', '_phenotypingCentre',
                                        'Phenotyping Consortium')
    embryo_specimens_df = embryo_df.select('_centreID', '_specimenID', '_colonyID',
                                           '_isBaseline', '_phenotypingCentre',
                                           'Phenotyping Consortium')
    specimen_df = mouse_specimens_df.union(embryo_specimens_df)
    specimen_df = specimen_df.where(
        (specimen_df['_colonyID'].isNotNull()) | (specimen_df['_isBaseline'] == True)
    ).dropDuplicates()
    experiment_df_a = dcc_experiment_df.alias('exp')
    specimen_df_a = specimen_df.alias('spe')

    experiment_df_a = experiment_df_a.join(specimen_df_a,
                                           (experiment_df_a['_centreID'] == specimen_df_a[
                                               '_centreID'])
                                           & (experiment_df_a['specimenID'] == specimen_df_a[
                                               '_specimenID'])
                                           )
    experiment_df_a = experiment_df_a.transform(override_europhenome_datasource)
    experiment_df_a = experiment_df_a.select('exp.*', '_dataSource')
    return experiment_df_a.dropDuplicates()


def _map_centre_ids(centre_id: str):
    return centre_map[centre_id]


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
        return specimen_id[:specimen_id.rfind('_MRC_Harwell')]
    else:
        return specimen_id[:specimen_id.rfind('_')]


def _truncate_colony_id(colony_id: str) -> str:
    if colony_id in EUROPHENOME_VALID_COLONIES or colony_id is None:
        return colony_id
    else:
        return colony_id[:colony_id.rfind('_')].strip()


def _skip_procedure(procedure_name: str) -> bool:
    return procedure_name[:procedure_name.rfind('_')] not in SKIPPED_PROCEDURES


def _string_contains(a: str, b: str):
    return b in a if a is not None and b is not None else False
