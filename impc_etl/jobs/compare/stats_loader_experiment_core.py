from typing import Dict, List
import json
from pysolr import Solr
from impc_etl import logger
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import sort_array, col, size, when, lit, count
from threading import Thread

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


def get_solr_core(
    solr_url: str, solr_query: str, solr_core_name: str, json_path: str
) -> List[Dict]:
    solr = Solr(solr_url)
    results = []
    cursor_mark = "*"
    logger.debug("start")
    done = False
    result_count = 0
    threads = []

    while not done:
        current_results = solr.search(
            solr_query,
            sort="id asc",
            rows=250000,
            cursorMark=cursor_mark,
            fl="id,phenotyping_center,procedure_group,parameter_stable_id,strain_accession_id,zygosity,metadata_group,colony_id,biological_sample_group",
        )
        next_cursor_mark = current_results.nextCursorMark
        done = cursor_mark == next_cursor_mark
        cursor_mark = next_cursor_mark
        thread = Thread(
            target=export_to_json,
            args=(list(current_results.docs), result_count, json_path),
        )
        result_count += 250000
        logger.debug(f"Collected {result_count}")
        logger.debug(f"Dumping {result_count - 250000} to {result_count} on thread")
        thread.start()
        threads.append(thread)
    for thread in threads:
        if thread.isAlive():
            thread.join()
    logger.debug("Finished dumping")


def export_to_json(experiments: List[Dict], offset: int, output_path: str):
    i = 0
    for chunk in chunks(experiments, 25000):
        with open(
            f"{output_path}/{offset + i}_{offset + i + 25000}.json",
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(chunk, f)
        i += 25000


def generate_parquet(json_path, output_path: str):
    conf = SparkConf().setAll(
        [
            ("spark.sql.sources.partitionColumnTypeInference.enabled", "false"),
            ("spark.driver.memory", "5g"),
        ]
    )
    spark = (
        SparkSession.builder.appName("IMPC_CORE_DUMP").config(conf=conf).getOrCreate()
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
        .withColumn(
            "metadata",
            when(size(col("metadata")) == 0, lit(None)).otherwise(
                sort_array(col("metadata"))
            ),
        )
        .withColumn(
            "allele_accession_id",
            when(col("allele_accession_id").like("%NULL%"), lit(None)).otherwise(
                col("allele_accession_id")
            ),
        )
    )
    stats_input_df = (
        spark.read.parquet(stats_input_parquet)
        .select(CSV_FIELDS)
        .withColumn(
            "litter_id",
            when(col("litter_id").isNull(), lit("")).otherwise(col("litter_id")),
        )
    )
    for col_name in [
        "weight",
        "weight_date",
        "weight_days_old",
        "weight_parameter_stable_id",
        "experiment_source_id",
        "data_point",
        "strain_name",
        "genetic_background",
        "strain_accession_id",
        "datasource_name",
        "project_name",
    ]:
        experiment_core_df = experiment_core_df.drop(col_name)
        stats_input_df = stats_input_df.drop(col_name)

    experiment_core_df = experiment_core_df.where(
        col("parameter_stable_id") != "IMPC_EYE_092_001"
    ).where(col("age_in_days") > 0)
    stats_input_df = stats_input_df.where(
        col("parameter_stable_id") != "IMPC_EYE_092_001"
    ).where(col("age_in_days") > 0)

    diff_df = experiment_core_df.exceptAll(stats_input_df)
    diff_df = diff_df.alias("experiment_core")
    stats_input_df = stats_input_df.alias("etl")
    compare_df = diff_df.join(
        stats_input_df,
        (stats_input_df.external_sample_id == diff_df.external_sample_id)
        & (stats_input_df.parameter_stable_id == diff_df.parameter_stable_id)
        & (stats_input_df.date_of_experiment == diff_df.date_of_experiment),
    )
    output_cols = []
    for column in experiment_core_df.columns:
        output_cols.append("experiment_core." + column)
        output_cols.append("etl." + column)
    # compare_df.select(output_cols).show(vertical=True, truncate=False)
    compare_df.where(
        col("experiment_core.metadata_group") != col("etl.metadata_group")
    ).select(output_cols).show(vertical=True, truncate=False)
    print(experiment_core_df.count())
    print(stats_input_df.count())
    print(diff_df.count())


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


if __name__ == "__main__":
    solr_query = "*:*"
    parquet_path = "/nfs/nobackup/spot/mouseinformatics/federico/snapshot/dr11.0/experiment_core_parquet"
    json_path = "/nfs/nobackup/spot/mouseinformatics/federico/snapshot/dr11.0/experiment_core_json"
    get_solr_core(
        "http://ves-ebi-d0.ebi.ac.uk:8986/solr/experiment/",
        solr_query,
        "experiment",
        json_path,
    )
    generate_parquet(json_path, parquet_path)
    # compare(
    #     "tests/data/parquet/experiment_core",
    #     "tests/data/parquet/impc_stats_input_parquet",
    # )
