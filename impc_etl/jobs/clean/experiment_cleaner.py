import sys
from impc_etl.jobs.normalize.dcc_transformations.experiments import *


def clean_experiments(spark_session: SparkSession, experiment_parquet_path: str) -> DataFrame:
    """
    DCC specimen level experiment cleaner

    :param SparkSession spark_session: PySpark session object
    :param str experiment_parquet_path: path to a parquet file with experiment raw data
    :return: a clean experiment parquet file
    :rtype: DataFrame
    """
    experiment_df = spark_session.read.parquet(experiment_parquet_path)
    experiment_df = experiment_df\
        .transform(map_centre_id) \
        .transform(map_project_id) \
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
    return experiment_df


def main(argv):
    input_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    experiment_clean_df = clean_experiments(spark, input_path)
    experiment_clean_df.write.mode('overwrite').parquet(output_path)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
