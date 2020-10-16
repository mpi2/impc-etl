import sys

from pyspark.sql import DataFrame, SparkSession
import json
import requests
import os


def main(argv):
    pipeline_core_parquet_path = argv[1]
    http_proxy = argv[2]
    output_path = argv[3]
    spark = SparkSession.builder.getOrCreate()
    ontology_terms_list_url = "http://api.mousephenotype.org/impress/ontologyterm/list"
    ontology_term_dict = requests.get(
        ontology_terms_list_url, proxies={"http": http_proxy, "https": http_proxy}
    ).json()
    impress_df = spark.read.parquet(pipeline_core_parquet_path)
    mp_chooser = (
        impress_df.where(impress_df.parammpterm.isNotNull())
        .select(
            "pipelineKey",
            "procedure.procedureKey",
            "parameter.parameterKey",
            "parammpterm",
        )
        .collect()
    )
    output_dict = {}
    for row in mp_chooser:

        pipeline = row["pipelineKey"]
        if pipeline not in output_dict:
            output_dict[pipeline] = {}

        procedure = row["procedureKey"][: row["procedureKey"].rfind("_")]
        if procedure not in output_dict[pipeline]:
            output_dict[pipeline][procedure] = {}

        parameter = row["parameterKey"]
        if parameter not in output_dict[pipeline][procedure]:
            output_dict[pipeline][procedure][parameter] = {}

        mpterm = row["parammpterm"]

        sex = mpterm["sex"]
        if sex == "F":
            sex = "FEMALE"
        elif sex == "M":
            sex = "MALE"
        else:
            sex = "UNSPECIFIED"
        if sex not in output_dict[pipeline][procedure][parameter]:
            output_dict[pipeline][procedure][parameter][sex] = {}

        selection_outcome = mpterm["selectionOutcome"]
        if selection_outcome not in output_dict[pipeline][procedure][parameter][sex]:
            output_dict[pipeline][procedure][parameter][sex][selection_outcome] = {}

        category = (
            mpterm["optionText"] if mpterm["optionText"] is not None else "OVERALL"
        )
        ontology_term_id = mpterm["ontologyTermId"]
        if (
            category
            not in output_dict[pipeline][procedure][parameter][sex][selection_outcome]
        ):
            output_dict[pipeline][procedure][parameter][sex][selection_outcome][
                category
            ] = {"MPTERM": ontology_term_dict[ontology_term_id]}
    output_df = spark.createDataFrame([(json.dumps(output_dict, indent=2))], ["text"])
    output_df.coalesce(1).write.format("text").option("header", "false").mode(
        "append"
    ).save(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
