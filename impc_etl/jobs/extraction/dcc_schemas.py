from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, explode_outer, input_file_name, udf
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

specimenField = [
    StructField('_DOB',StringType(),True),
    StructField('_stage',StringType(),True),
    StructField('_stageUnit',StringType(),True),
    StructField('_colonyID',StringType(),True),
    StructField('_gender',StringType(),True),
    StructField('_isBaseline',BooleanType(),True),
    StructField('_litterId',StringType(),True),
    StructField('_phenotypingCentre',StringType(),True),
    StructField('_pipeline',StringType(),True),
    StructField('_productionCentre',StringType(),True),
    StructField('_project',StringType(),True),
    StructField('_specimenID',StringType(),True),
    StructField('_strainID',StringType(),True),
    StructField('_zygosity',StringType(),True),
    StructField('relatedSpecimen', StructType(relatedSpecimenField), True),
    StructField('genotype', StructType(genotypeField), True),
    StructField('chromosomalAlteration', StructType(chromosomalAlterationField), True),
    StructField('parentalStrain', StructType(parentalStrainField), True),
    StructField('statusCode', StructType(statusCodeField), True)]

centreField = [StructField('_centreID', StringType(), True),
               StructField('embryo', ArrayType(StructType(specimenField)), True),
               StructField('mouse', ArrayType(StructType(specimenField)), True)]

def get_specimen_centre_schema():
    return StructType(centreField)

def get_specimen_schema():
    return StructType(specimenField)

def flatten_specimen_df(spark_session: SparkSession,
                        centre_df: DataFrame,
                        source_file: str,
                        datasource_short_name: str) -> DataFrame:
    """
    Given a DataFrame with rowTag 'centre', tuple '_centreID, xxx', and zero or more 'mouse' or 'embryo' tags,
    returns a new DataFrame with each 'mouse' or 'embryo' ROW, a _type ('Mouse' or 'Embryo'), and the _centreID.

    :param centre_df:
    :return:
    """
    df: DataFrame

    if centre_df is None:
        return df

    mice_df = centre_df.select(centre_df['_centreID'], centre_df.mouse)
    embryos_df = centre_df.select(centre_df['_centreID'], centre_df.embryo)

    exploded_mice = mice_df\
        .withColumn('tmp', explode_outer(mice_df.mouse)).select('tmp.*', '_centreID')\
        .withColumn('_type', lit('mouse'))

    exploded_embryos = embryos_df\
        .withColumn('tmp', explode_outer(embryos_df.embryo)).select('tmp.*', '_centreID')\
        .withColumn('_type', lit('embryo'))

    df = exploded_mice.union(exploded_embryos)\
        .withColumn('_sourceFile', lit(source_file)) \
        .withColumn('_datasourceShortName', lit(datasource_short_name))

    return df
