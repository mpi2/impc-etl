"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode_outer, when, lit, least
import sys


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
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    mgi_phenotype_parquet_path = argv[1]
    ontology_parquet_path = argv[2]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    mgi_phenotype_df = spark.read.parquet(mgi_phenotype_parquet_path)
    ontology_df = spark.read.parquet(ontology_parquet_path)

    mgi_phenotype_df = mgi_phenotype_df.join(
        ontology_df, col("mammalianPhenotypeID") == col("id"), "left_outer"
    )

    for column_name, ontology_column in ONTOLOGY_STATS_MAP.items():
        mgi_phenotype_df = mgi_phenotype_df.withColumn(
            f"{column_name}", col(ontology_column)
        )

    mgi_phenotype_df = mgi_phenotype_df.withColumn("assertion_type", lit("manual"))
    mgi_phenotype_df = mgi_phenotype_df.withColumn(
        "assertion_type_id", lit("ECO:0000218")
    )
    mgi_phenotype_df = mgi_phenotype_df.withColumn("life_stage_acc", lit("EFO:0002948"))
    mgi_phenotype_df = mgi_phenotype_df.withColumn("life_stage_name", lit("postnatal"))
    mgi_phenotype_df.show()
    raise ValueError


if __name__ == "__main__":
    sys.exit(main(sys.argv))
