from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, explode_outer, col, array
from pyspark.sql.types import StructField, StringType, StructType, DoubleType, IntegerType, ArrayType, BooleanType

# SPECIMEN SCHEMA
# common.xsd    NOTE: Last parameter of StructField is 'isNullable'
statusCodeField = [StructField('_statusCode', StringType(), True),
                   StructField('_date', StringType(), True)]

# specimen_definition.xsd:
relatedSpecimenField = [StructField('_specimenID', StringType(), True),
                        StructField('_relationship', StringType(), True)]

genotypeField = [StructField('_MGIGeneId', StringType(), True),
                 StructField('_geneSymbol', StringType(), True),
                 StructField('_MGIAlleleId', StringType(), True),
                 StructField('_fatherZygosity', StringType(), True),
                 StructField('_motherZygosity', StringType(), True)]

chromosomeField = [StructField('_start', StringType(), True),
                   StructField('_end', StringType(), True),
                   StructField('_species', StringType(), True)]

chromosomalAlterationField = [StructField('_chromosomeAdded', StructType(chromosomeField), True),
                              StructField('_chromosomeRemoved', StructType(chromosomeField), True)]

parentalStrainField = [StructField('_percentage', DoubleType(), True),
                       StructField('_MGIStrainID', StringType(), True),
                       StructField('_gender', StringType(), True),
                       StructField('_level', IntegerType(), True)]

specimenSequenceField = [StructField('_relatedSpecimen', ArrayType(StructType(relatedSpecimenField)), True),
                         StructField('_genotype', ArrayType(StructType(genotypeField)), True),
                         StructField('_chromosomalAlteration', ArrayType(StructType(chromosomalAlterationField)), True),
                         StructField('_parentalStrain', ArrayType(StructType(parentalStrainField)), True),
                         StructField('_statusCode', ArrayType(StructType(statusCodeField), True))]

# This is an amalgamation of the mouse, embryo, and specimen fields
centreSpecimenCombinedField = [
    # mouse
    StructField('_DOB', StringType(), True),

    # embryo
    StructField('_stage', StringType(), True),
    StructField('_stageUnit', StringType(), True),

    # specimen
    StructField('_colonyID', StringType(), True),
    StructField('_gender', StringType(), True),
    StructField('_isBaseline', BooleanType(), True),
    StructField('_litterId', StringType(), True),
    StructField('_phenotypingCentre', StringType(), True),
    StructField('_pipeline', StringType(), True),
    StructField('_productionCentre', StringType(), True),
    StructField('_project', StringType(), True),
    StructField('_specimenID', StringType(), True),
    StructField('_strainID', StringType(), True),
    StructField('_zygosity', StringType(), True),
    StructField('relatedSpecimen', StructType(relatedSpecimenField), True),
    StructField('genotype', StructType(genotypeField), True),
    StructField('chromosomalAlteration', StructType(chromosomalAlterationField), True),
    StructField('parentalStrain', StructType(parentalStrainField), True),
    StructField('statusCode', StructType(statusCodeField), True)]

centreSpecimenField = [StructField('_centreID', StringType(), True),
                       StructField('embryo', ArrayType(StructType(centreSpecimenCombinedField)), True),
                       StructField('mouse', ArrayType(StructType(centreSpecimenCombinedField)), True),
                       StructField('ns2:embryo', ArrayType(StructType(centreSpecimenCombinedField)), True),
                       StructField('ns2:mouse', ArrayType(StructType(centreSpecimenCombinedField)), True)]


def get_centre_specimen_schema():
    return StructType(centreSpecimenField)


def flatten_specimen_df(centre_df: DataFrame,
                        source_file: str,
                        datasource_short_name: str) -> DataFrame:
    """
    Flattens out the specimen centreID, mouse, and embryo specimen data and adds type ('mouse' or 'embryo'),
    source file, and datasourceShortName (e.g. IMPC, 3i), returning the flattened DataFrame.

    :param centre_df: The hierarchical specimen DataFrame containing centreID and sub components 'mouse' and 'embryo'
    :param source_file: The xml source file name
    :param datasource_short_name: The data source (e.g. IMPC, 3i)
    :return: A dataframe containing, at the top level:
        _centreID
        _type ('mouse' or 'embryo')
        _sourceFile (the xml source file)
        _datasourceShortName (the data source (e.g. IMPC, 3i)
        the flattened contents of the 'mouse' and 'embryo' records

    NOTE: SparkXml does not support namespaces, so hacks have to be made to accommodate 3i (which uses one)

    """
    df: DataFrame

    if centre_df is None:
        return df

    # HACK for Spark Xml's inability to handle NAMESPACES!
    mouse_col_name = 'mouse' if datasource_short_name != '3i' else 'ns2:mouse'
    embryo_col_name = 'embryo' if datasource_short_name != '3i' else 'ns2:embryo'

    mice_df = centre_df.select(centre_df['_centreID'], centre_df[mouse_col_name])
    embryos_df = centre_df.select(centre_df['_centreID'], centre_df[embryo_col_name])

    exploded_mice = mice_df\
        .withColumn('tmp', explode_outer(mice_df[mouse_col_name])).select('tmp.*', '_centreID')\
        .withColumn('_type', lit('mouse'))

    exploded_embryos = embryos_df\
        .withColumn('tmp', explode_outer(embryos_df[embryo_col_name])).select('tmp.*', '_centreID')\
        .withColumn('_type', lit('embryo'))

    df = exploded_mice.union(exploded_embryos)\
        .withColumn('_sourceFile', lit(source_file)) \
        .withColumn('_datasourceShortName', lit(datasource_short_name))

    return df


# procedure_definition.xsd    NOTE: Last parameter of StructField is 'isNullable'

dimensionField = [StructField('_id', StringType(), True),
                  StructField('_origin', StringType(), True),
                  StructField('_unit', StringType(), True)]

parameterAssociationField = [StructField('_dim', ArrayType(StructType(dimensionField)), True),
                             StructField('_parameterID', StringType(), True),
                             StructField('_sequenceID', IntegerType(), True)]

procedureMetadataField = [StructField('_parameterStatus', StringType(), True),
                          StructField('value', StringType(), True),
                          StructField('_parameterID', StringType(), True),
                          StructField('_sequenceID', IntegerType(), True)]

seriesMediaParameterValueField =\
    [StructField('parameterAssociation', ArrayType(StructType(parameterAssociationField)), True),
     StructField('procedureMetadata', ArrayType(StructType(procedureMetadataField)), True),
     StructField('_incrementValue', StringType(), True),
     StructField('_URI', StringType(), True),
     StructField('_fileType', StringType(), True)]

seriesMediaParameterField =\
    [StructField('value', ArrayType(StructType(seriesMediaParameterValueField)), True),
     StructField('_parameterStatus', StringType(), True),
     StructField('procedureMetadata', ArrayType(StructType(procedureMetadataField)), True),
     StructField('_parameterID', StringType(), True)]

mediaFileField = [StructField('procedureMetadata', ArrayType(StructType(procedureMetadataField)), True),
                  StructField('parameterAssociation', ArrayType(StructType(parameterAssociationField)), True),
                  StructField('_localId', StringType(), True),
                  StructField('_URI', StringType(), True),
                  StructField('_fileType', StringType(), True)]

mediaSectionField = [StructField('mediaFile', ArrayType(StructType(mediaFileField)), True),
                     StructField('_localId', StringType(), True)]

mediaSampleField = [StructField('mediaSection', ArrayType(StructType(mediaSectionField)), True),
                    StructField('_localId', StringType(), True)]

mediaSampleParameterField =\
    [StructField('mediaSample', ArrayType(StructType(mediaSampleField)), True),
     StructField('_parameterStatus', StringType(), True),
     StructField('_parameterID', StringType(), True)]

mediaParameterField = [StructField('_parameterStatus', StringType(), True),
                       StructField('parameterAssociation', ArrayType(StructType(parameterAssociationField)), True),
                       StructField('procedureMetadata', ArrayType(StructType(procedureMetadataField)), True),
                       StructField('_parameterID', StringType(), True),
                       StructField('_URI', StringType(), True),
                       StructField('_fileType', StringType(), True)]

seriesParameterValueField = [StructField('_incrementValue', StringType(), True),
                             StructField('_incrementStatus', StringType(), True)]

seriesParameterField = [StructField('value', ArrayType(StructType(seriesParameterValueField)), True),
                        StructField('_parameterStatus', StringType(), True),
                        StructField('_parameterID', StringType(), True),
                        StructField('_sequenceID', IntegerType(), True),
                        StructField('_unit', StringType(), True)]

ontologyParameterField = [StructField('_term', ArrayType(StringType()), True),
                          StructField('_parameterStatus', StringType(), True),
                          StructField('_parameterID', StringType(), True),
                          StructField('_sequenceID', IntegerType(), True)]

simpleParameterField = [StructField('value', StringType(), True),
                        StructField('_parameterStatus', StringType(), True),
                        StructField('_parameterID', StringType(), True),
                        StructField('_unit', StringType(), True),
                        StructField('_sequenceID', IntegerType(), True)]

procedureField = [StructField('simpleParameter', ArrayType(StructType(simpleParameterField)), True),
                  StructField('ontologyParameter', ArrayType(StructType(ontologyParameterField)), True),
                  StructField('seriesParameter', ArrayType(StructType(seriesParameterField)), True),
                  StructField('mediaParameter', ArrayType(StructType(mediaParameterField)), True),
                  StructField('mediaSampleParameter', ArrayType(StructType(mediaSampleParameterField)), True),
                  StructField('seriesMediaParameter', ArrayType(StructType(seriesMediaParameterField)), True),
                  StructField('procedureMetadata', ArrayType(StructType(procedureMetadataField)), True),
                  StructField('_procedureID', StringType(), True)]

# This is an amalgamation of the experiment, line, and housing fields
centreProcedureCombinedField = [
    # experiment
    StructField('specimenID', StringType(), True),
    StructField('_experimentID', StringType(), True),
    StructField('_sequenceID', IntegerType(), True),
    StructField('_dateOfExperiment', StringType(), True),

    # line
    StructField('_colonyID', StringType(), True),

    # housing
    StructField('_fromLIMS', StringType(), True),
    StructField('_lastUpdated', StringType(), True),

    # experiment and line
    StructField('statusCode', ArrayType(StructType(statusCodeField)), True),

    # experiment line, and housing
    StructField('procedure', StructType(procedureField), True)
]

centreProcedureField = [
    StructField('_centreID', StringType(), True),
    StructField('_pipeline', StringType(), True),
    StructField('_project', StringType(), True),
    StructField('experiment', ArrayType(StructType(centreProcedureCombinedField)), True),
    StructField('line', ArrayType(StructType(centreProcedureCombinedField)), True),
    StructField('housing', ArrayType(StructType(centreProcedureCombinedField)), True)
]


def get_centre_procedure_schema():
    return StructType(centreProcedureField)


def flatten_procedure_df(centre_df: DataFrame,
                         source_file: str,
                         datasource_short_name: str) -> DataFrame:
    """
    Flattens out the procedure centreID, line and experiment data and adds type ('line' or 'experiment'),
    source file, and datasourceShortName (e.g. IMPC, 3i), returning the flattened DataFrame.

    :param centre_df: The hierarchical procedure DataFrame containing centreID and optional sub components 'mouse' and 'embryo'
    :param source_file: The xml source file name
    :param datasource_short_name: The data source (e.g. IMPC, 3i)
    :return: A dataframe containing, at the top level:
        _centreID
        _type ('line' or 'experiment')
        _sourceFile (the xml source file)
        _datasourceShortName (the data source (e.g. IMPC, 3i)
        the flattened contents of the 'line' and 'experiment' records

    """
    df: DataFrame

    if centre_df is None:
        return df

    experiment_df = centre_df.na.drop(subset=['experiment']).select(centre_df['_centreID'], centre_df['_pipeline'], centre_df['_project'], centre_df['experiment'])
    line_df = centre_df.na.drop(subset=['line']).select(centre_df['_centreID'], centre_df['_pipeline'], centre_df['_project'], centre_df['line'])

    exploded_experiments = experiment_df\
        .withColumn('tmp', explode_outer(experiment_df['experiment'])).select('tmp.*', '_centreID')\
        .withColumn('_type', lit('experiment'))

    exploded_lines = line_df\
        .withColumn('tmp', explode_outer(line_df['line'])).select('tmp.*', '_centreID')\
        .withColumn('_type', lit('line'))

    df = exploded_experiments.union(exploded_lines)\
        .withColumn('_sourceFile', lit(source_file)) \
        .withColumn('_datasourceShortName', lit(datasource_short_name))

    return df
