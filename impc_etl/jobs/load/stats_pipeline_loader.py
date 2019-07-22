"""
Stats pipeline input loader

"""
from pyspark.sql import DataFrame

CSV_FIELDS = ['allele_accession_id', 'metadata', 'gene_accession_id', 'project_name',
              'strain_accession_id', 'litter_id', 'phenotyping_center', 'external_sample_id',
              'developmental_stage_name', 'developmental_stage_acc', 'datasource_name', 'age_in_days', 'date_of_birth', 'metadata_group', 'sequence_id', 'experiment_source_id']


def load(experiments_df: DataFrame, line_experiments_df: DataFrame, mice_df: DataFrame,
         embryo_df: DataFrame, imits_df: DataFrame, impress_df: DataFrame, allele2_df: DataFrame):
    imits_df.printSchema()
    mice_experiments_df: DataFrame = experiments_df.join(mice_df,
                                                         experiments_df['specimenID'] == mice_df[
                                                             '_specimenID'])
    mice_experiments_df = mice_experiments_df.join(allele2_df,
                                                   mice_experiments_df['Allele Symbol'] ==
                                                   allele2_df['allele_symbol'], 'left_outer')
    mice_experiments_df.show(10, truncate=False, vertical=True)
    mice_experiments_df = rename_columns(mice_experiments_df)
    mice_experiments_df.select(CSV_FIELDS).show(10, truncate=False, vertical=True)
    return


def rename_columns(experiments_df: DataFrame):
    experiments_df = experiments_df.withColumnRenamed('allele_mgi_accession_id',
                                                      'allele_accession_id')
    experiments_df = experiments_df.withColumnRenamed('marker_mgi_accession_id',
                                                      'gene_accession_id')
    experiments_df = experiments_df.withColumnRenamed('_project', 'project_name')
    experiments_df = experiments_df.withColumnRenamed('_strainID', 'strain_accession_id')
    experiments_df = experiments_df.withColumnRenamed('_litterId', 'litter_id')
    experiments_df = experiments_df.withColumnRenamed('_phenotypingCentre', 'phenotyping_center')
    experiments_df = experiments_df.withColumnRenamed('_specimenID', 'external_sample_id')
    experiments_df = experiments_df.withColumnRenamed('_dataSource', 'datasource_name')
    experiments_df = experiments_df.withColumnRenamed('ageInDays', 'age_in_days')
    experiments_df = experiments_df.withColumnRenamed('_dateOfExperiment', 'date_of_experiment')
    experiments_df = experiments_df.withColumnRenamed('_dateOfExperiment', 'date_of_experiment')
    experiments_df = experiments_df.withColumnRenamed('_DOB', 'date_of_birth')
    experiments_df = experiments_df.withColumnRenamed('metadataGroup', 'metadata_group')
    experiments_df = experiments_df.withColumnRenamed('_sequenceID', 'sequence_id')
    experiments_df = experiments_df.withColumnRenamed('_experimentID', 'experiment_source_id')



    return experiments_df
