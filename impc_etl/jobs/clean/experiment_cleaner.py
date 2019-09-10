import sys
from impc_etl.shared.transformations.experiments import *


def clean_experiments(experiment_df: DataFrame) -> DataFrame:
    """
    DCC experiment level cleaner

    :param experiment_df: a raw experiment DataFrame
    :return: a clean experiment DataFrame
    :rtype: DataFrame
    """
    experiment_df = (
        experiment_df.transform(map_centre_id)
        .transform(map_project_id)
        .transform(standarize_europhenome_experiments)
        .transform(drop_skipped_experiments)
        .transform(drop_skipped_procedures)
        .transform(standarize_3i_experiments)
        .transform(drop_null_centre_id)
        .transform(drop_null_data_source)
        .transform(drop_null_date_of_experiment)
        .transform(drop_null_pipeline)
        .transform(drop_null_project)
        .transform(drop_null_specimen_id)
        .transform(generate_unique_id)
    )
    return experiment_df


def main(argv):
    input_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    experiment_df = spark.read.parquet(input_path)
    experiment_clean_df = clean_experiments(experiment_df)
    experiment_clean_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
