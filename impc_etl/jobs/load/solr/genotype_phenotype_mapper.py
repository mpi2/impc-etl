from pyspark.sql import DataFrame, SparkSession
import sys

from pyspark.sql.functions import col


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    stats_results_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    stats_results_df = spark.read.parquet(stats_results_parquet_path)

    genotype_phenotype_df = stats_results_df.where(col("significant")).select()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
