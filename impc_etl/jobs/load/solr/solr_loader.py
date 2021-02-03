"""
SOLR module
   Generates the required Solr cores
"""
import sys

from pyspark.sql import SparkSession


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    impc_parquet_path = argv[1]
    output_path = argv[2]
    impress_root_type = argv[3]

    spark = SparkSession.builder.getOrCreate()
    impc_df = spark.read.parquet(impc_parquet_path)
    impc_df.foreachPartition()


def index_partition(partition):
    return


if __name__ == "__main__":
    sys.exit(main(sys.argv))
