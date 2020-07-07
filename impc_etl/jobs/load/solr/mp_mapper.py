"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    when,
    lit,
    split,
    collect_set,
    concat,
    concat_ws,
    flatten,
    monotonically_increasing_id,
)
import sys

from pyspark.sql.types import StringType

ONTOLOGY_MP_MAP = {
    "mp_id": "id",
    "mp_term": "term",
    "mp_definition": "definition",
    "mp_term_synonym": "synonyms",
    "alt_mp_id": "alt_ids",
    "child_mp_id": "child_ids",
    "child_mp_term": "child_terms",
    "parent_mp_id": "parent_ids",
    "parent_mp_term": "parent_terms",
    "intermediate_mp_id": "intermediate_ids",
    "intermediate_mp_term": "intermediate_terms",
    "top_level_mp_id": "top_level_ids",
    "top_level_mp_term": "top_level_terms",
    "top_level_mp_term_synonym": "top_level_synonyms",
}

ONTOLOGY_MA_MAP = {
    "inferred_ma_id": "id",
    "inferred_ma_term": "term",
    "inferred_intermediate_ma_id": "intermediate_ids",
    "inferred_intermediate_ma_term": "intermediate_terms",
    "inferred_selected_top_level_ma_id": "top_level_ids",
    "inferred_selected_top_level_ma_term": "top_level_terms",
}


MP_CORE_COLUMNS = [
    "doc_id",
    "mp_id",
    "mp_term",
    "mp_definition",
    "mp_term_synonym",
    "alt_mp_id",
    "child_mp_id",
    "child_mp_term",
    "parent_mp_id",
    "parent_mp_term",
    "intermediate_mp_id",
    "intermediate_mp_term",
    "top_level_mp_id",
    "top_level_mp_term",
    "top_level_mp_term_synonym",
    "top_level_mp_term_id",
    "inferred_ma_id",
    "inferred_ma_term",
    "inferred_intermediate_ma_id",
    "inferred_intermediate_ma_term",
    "inferred_selected_top_level_ma_id",
    "inferred_selected_top_level_ma_term",
    "hp_term",
]


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    ontology_parquet_path = argv[1]
    ontology_metadata_parquet_path = argv[2]
    pipeline_core_parquet_path = argv[3]
    impc_search_index_csv_path = argv[4]
    mp_relation_augmented_metadata_table_csv_path = argv[5]
    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    ontology_df = spark.read.parquet(ontology_parquet_path)
    ontology_metadata_df = spark.read.parquet(ontology_metadata_parquet_path)
    impc_search_index_df = spark.read.csv(impc_search_index_csv_path, header=True)
    mp_ext_df = spark.read.csv(
        mp_relation_augmented_metadata_table_csv_path, header=True
    )
    impc_search_index_df = (
        impc_search_index_df.groupBy("phenotype")
        .pivot("property")
        .agg(collect_set("value"))
    )
    mp_df = ontology_df.where(col("id").startswith("MP:"))
    for column_name, ontology_column in ONTOLOGY_MP_MAP.items():
        mp_df = mp_df.withColumnRenamed(ontology_column, column_name)
    mp_df = mp_df.select(list(ONTOLOGY_MP_MAP.keys()))

    mp_df = mp_df.join(
        impc_search_index_df, col("mp_id") == col("phenotype"), "left_outer"
    )
    mp_df = mp_df.withColumn(
        "hp_term",
        concat(
            "impc:childOneLabel",
            "impc:childTwoLabel",
            "impc:hpExactSynonym",
            "impc:hpLabel",
        ),
    )

    ma_df = ontology_df.where(
        (~col("id").startswith("MP:")) & (~col("id").startswith("MPATH:"))
    )
    for column_name, ontology_column in ONTOLOGY_MA_MAP.items():
        ma_df = ma_df.withColumnRenamed(ontology_column, column_name)
    ma_df = ma_df.select(list(ONTOLOGY_MA_MAP.keys()))
    mp_ma_df = (
        mp_ext_df.select(col("acc").alias("mp_id"), col("ma").alias("inferred_ma_id"))
        .where(col("inferred_ma_id").isNotNull())
        .distinct()
    )
    mp_ma_df = mp_ma_df.join(ma_df, "inferred_ma_id")
    mp_ma_df = mp_ma_df.groupBy("mp_id").agg(
        collect_set("inferred_ma_id").alias("inferred_ma_id"),
        collect_set("inferred_ma_term").alias("inferred_ma_term"),
        *[
            flatten(collect_set(col_name)).alias(col_name)
            for col_name in ma_df.columns
            if col_name not in ["mp_id", "inferred_ma_id", "inferred_ma_term"]
        ]
    )
    mp_df = mp_df.join(mp_ma_df, "mp_id", "left_outer")
    mp_df = mp_df.withColumn(
        "top_level_mp_term_id", concat_ws("___", "top_level_mp_id", "top_level_mp_term")
    )
    mp_df = mp_df.withColumn(
        "doc_id", monotonically_increasing_id().astype(StringType())
    )
    mp_df = mp_df.select(MP_CORE_COLUMNS)
    mp_df.write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
