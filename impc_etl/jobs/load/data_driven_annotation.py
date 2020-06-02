import sys
from pyspark.sql import DataFrame, SparkSession


def main(argv):
    """
    Data driven annotation generator
    :param list argv: the list elements should be:
                    [1]: Observations parquet
                    [2]: Output Path
    """
    observations_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    observations_df = spark.read.parquet(observations_parquet_path)
    # HERE YOUR TRANSFORMATIONS
    observations_df.write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
