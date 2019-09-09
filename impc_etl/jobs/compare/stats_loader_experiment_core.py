from typing import Dict, List
import json
from pysolr import Solr
from impc_etl import logger
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import sort_array, col, upper

CSV_FIELDS = [
    "allele_accession_id",
    "gene_accession_id",
    "project_name",
    "strain_accession_id",
    "litter_id",
    "phenotyping_center",
    "external_sample_id",
    "developmental_stage_name",
    "developmental_stage_acc",
    "datasource_name",
    "age_in_days",
    "date_of_birth",
    "metadata",
    "metadata_group",
    "experiment_source_id",
    "gene_symbol",
    "biological_sample_group",
    "sex",
    "allele_symbol",
    "production_center",
    "age_in_weeks",
    "weight",
    "weight_date",
    "weight_days_old",
    "weight_parameter_stable_id",
    "colony_id",
    "zygosity",
    "allelic_composition",
    "pipeline_name",
    "pipeline_stable_id",
    "procedure_name",
    "procedure_stable_id",
    "procedure_group",
    "parameter_name",
    "parameter_stable_id",
    "observation_type",
    "data_point",
    "text_value",
    "category",
    "strain_name",
    "genetic_background",
    "date_of_experiment",
]


def get_solr_core(solr_url: str, solr_query: str) -> List[Dict]:
    solr = Solr(solr_url)
    results = []
    cursor_mark = "*"
    logger.debug("start")
    done = False

    while not done:
        current_results = solr.search(
            solr_query, sort="id asc", rows=5000, cursorMark=cursor_mark
        )
        next_cursor_mark = current_results.nextCursorMark
        done = cursor_mark == next_cursor_mark
        cursor_mark = next_cursor_mark
        results.extend(current_results)
        logger.debug(f"Collected {len(results)}")
    return results


def export_to_json(experiments: List[Dict], output_path: str):
    i = 0
    for chunk in chunks(experiments, 1000):
        with open(
            f"{output_path}/experiment_core_{i}_{i + 1000}.json", "w", encoding="utf-8"
        ) as f:
            json.dump(chunk, f)
        i += 1000


def generate_parquet(json_path, output_path: str):
    conf = SparkConf().setAll(
        [
            ("spark.sql.sources.partitionColumnTypeInference.enabled", "false"),
            ("spark.driver.memory", "5g"),
        ]
    )
    spark = (
        SparkSession.builder.appName("IMPC_COMPARE_STATS_LOADER_EXP_CORE")
        .config(conf=conf)
        .getOrCreate()
    )
    experiment_core_df = spark.read.json(f"{json_path}/*.json")
    experiment_core_df.write.parquet(output_path)


def compare(experiment_core_parquet, stats_input_parquet):
    conf = SparkConf().setAll(
        [
            ("spark.sql.sources.partitionColumnTypeInference.enabled", "false"),
            ("spark.driver.memory", "5g"),
        ]
    )
    spark = (
        SparkSession.builder.appName("IMPC_COMPARE_STATS_LOADER_EXP_CORE")
        .config(conf=conf)
        .getOrCreate()
    )
    experiment_core_df = (
        spark.read.parquet(experiment_core_parquet)
        .select(CSV_FIELDS)
        .withColumn("metadata", sort_array(col("metadata")))
    )
    stats_input_df = (
        spark.read.parquet(stats_input_parquet)
        .select(CSV_FIELDS)
        .withColumn("datasource_name", upper(col("datasource_name")))
    )
    # experiment_core_df = experiment_core_df.where(
    #     (col("experiment_source_id") == "IMPC_BWT_001_2015-08-28")
    #     & (col("biological_sample_group") == "control")
    # )
    # stats_input_df = stats_input_df.where(
    #     (col("experiment_source_id") == "IMPC_BWT_001_2015-08-28")
    #     & (col("biological_sample_group") == "control")
    # )
    experiment_core_df = experiment_core_df.select(
        [
            c
            for c in CSV_FIELDS
            if c
            not in [
                "weight",
                "weight_date",
                "weight_days_old",
                "weight_parameter_stable_id",
            ]
        ]
    )
    stats_input_df = stats_input_df.select(
        [
            c
            for c in CSV_FIELDS
            if c
            not in [
                "weight",
                "weight_date",
                "weight_days_old",
                "weight_parameter_stable_id",
            ]
        ]
    )
    diff_df = experiment_core_df.exceptAll(stats_input_df)
    # experiment_core_df.sort(col("external_sample_id")).show(
    #     vertical=True, truncate=False
    # )
    # stats_input_df.sort(col("external_sample_id")).show(vertical=True, truncate=False)
    diff_df.sort(
        [
            c
            for c in CSV_FIELDS
            if c
            not in [
                "weight",
                "weight_date",
                "weight_days_old",
                "weight_parameter_stable_id",
            ]
        ]
    ).show(vertical=True, truncate=False)
    print(experiment_core_df.count())
    print(stats_input_df.count())
    print(diff_df.count())


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


if __name__ == "__main__":
    # rbrc_experiments = get_solr_core(
    #     "http://ves-ebi-d0.ebi.ac.uk:8986/solr/experiment",
    #     " AND ".join(
    #         [
    #             'phenotyping_center:"RBRC"',
    #             'production_center:"RBRC"',
    #             'observation_type:("unidimensional" OR "text" OR "categorical")',
    #             'datasource_name:"IMPC"',
    #         ]
    #     ),
    # )
    # json_path = "tests/data/json/experiment_core"
    # parquet_path = "tests/data/parquet/experiment_core"
    # export_to_json(rbrc_experiments, json_path)
    # generate_parquet(json_path, parquet_path)
    compare(
        "tests/data/parquet/experiment_core",
        "tests/data/parquet/impc_stats_input_parquet",
    )
