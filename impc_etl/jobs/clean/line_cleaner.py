import sys
from impc_etl.shared.transformations.experiments import *


def clean_lines(line_df: DataFrame):
    line_df = (
        line_df.transform(map_centre_id)
        .transform(map_project_id)
        .transform(drop_skipped_procedures)
        .transform(standarize_3i_experiments)
        .transform(drop_null_centre_id)
        .transform(drop_null_data_source)
        .transform(drop_null_pipeline)
        .transform(drop_null_project)
    )
    return line_df


def main(argv):
    input_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    line_df = spark.read.parquet(input_path)
    experiment_clean_df = clean_lines(line_df)

    experiment_clean_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
