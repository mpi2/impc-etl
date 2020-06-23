from pyspark.sql import DataFrame, SparkSession
import sys

from pyspark.sql.functions import explode_outer, col

ONTOLOGY_STATS_MAP = {
    "mp_term_name": "term",
    "top_level_mp_term_id": "top_level_ids",
    "top_level_mp_term_name": "top_level_terms",
    "intermediate_mp_term_id": "intermediate_ids",
    "intermediate_mp_term_name": "intermediate_terms",
}


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: Open stats parquet file
                    [2]: Colony parquet
                    [3]: Ontology parquet
                    [4]: Ontology metadata parquet
                    [5]: Pipeline core parquet
                    [6]: Output Path
    """
    # print(argv)
    open_stats_parquet_path = argv[1]
    colony_parquet_path = argv[2]
    ontology_parquet_path = argv[3]
    ontology_metadata_parquet_path = argv[4]
    pipeline_core_parquet_path = argv[5]
    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    open_stats_df = spark.read.parquet(open_stats_parquet_path)
    colony_df = spark.read.parquet(colony_parquet_path)
    ontology_df = spark.read.parquet(ontology_parquet_path)
    ontology_df = ontology_df.alias("ontology")
    ontology_metadata_df = spark.read.parquet(ontology_metadata_parquet_path)
    open_stats_df = open_stats_df.withColumn("mp_term", explode_outer("mp_term"))
    open_stats_df = open_stats_df.withColumn("mp_term_id", col("mp_term.term_id"))
    stats_results_cols = open_stats_df.columns
    open_stats_df = open_stats_df.join(
        ontology_df, col("mp_term_id") == col("id"), "left_outer"
    )

    for column_name, ontology_column in ONTOLOGY_STATS_MAP.items():
        open_stats_df = open_stats_df.withColumn(column_name, col(ontology_column))
        stats_results_cols.append(column_name)
    open_stats_df.printSchema()
    open_stats_df.select(stats_results_cols).where(col("mp_term_id").isNotNull()).show(
        vertical=True
    )
    raise ValueError
    # open_stats_df.write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
