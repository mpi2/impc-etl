"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
import sys


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    pipeline_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    pipeline_df = spark.read.parquet(pipeline_parquet_path)
    pipeline_df = pipeline_df.select("")
    pipeline_df.write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
