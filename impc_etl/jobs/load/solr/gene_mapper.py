"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode_outer, when, lit, least
import sys


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    mgi_gene_parquet_path = argv[1]
    imits_parquet_path = argv[2]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    mgi_gene_df = spark.read.parquet(mgi_gene_parquet_path)
    imits_df = spark.read.parquet(imits_parquet_path)

    mgi_gene_df.show()
    raise ValueError


if __name__ == "__main__":
    sys.exit(main(sys.argv))
