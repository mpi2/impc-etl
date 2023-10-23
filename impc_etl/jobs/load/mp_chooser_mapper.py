"""
    Luigi PySpark task that generates a JSON data structure that allows to select an ontology
    term based on the Pipeline, Procedure, Parameter, Outcome or Category.

    This data structure is called MP Chooser and is used by the Statistical Analysis pipeline.
"""
import json
from typing import Any

import luigi
import requests
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession

from impc_etl.jobs.load.solr.pipeline_mapper import ImpressToParameterMapper
from impc_etl.workflow import SmallPySparkTask
from impc_etl.workflow.config import ImpcConfig


class MPChooserGenerator(SmallPySparkTask):
    """
    PySpark task to generate the MP Chooser data structure.

    This task depends on:

    - `impc_etl.workflow.load.PipelineCoreLoader`
    """

    #: Name of the Spark task
    name: str = "IMPC_MP_Chooser_Generator"

    #: IMPReSS API endpoint URL
    impress_api_url: luigi.Parameter = luigi.Parameter()

    #: URL to the HTTP proxy server to be used on the HTTP requests.
    http_proxy: luigi.Parameter = luigi.Parameter(default="")

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return ImpressToParameterMapper()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}mp_chooser_json")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input().path,
            self.impress_api_url,
            self.http_proxy,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        pipeline_core_parquet_path = args[0]
        impress_api_url = args[1]
        http_proxy = args[2]
        output_path = args[3]
        spark = SparkSession(sc)
        ontology_terms_list_url = f"{impress_api_url}/ontologyterm/list"
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
            if (
                selection_outcome
                not in output_dict[pipeline][procedure][parameter][sex]
            ):
                output_dict[pipeline][procedure][parameter][sex][selection_outcome] = {}

            category = (
                mpterm["optionText"] if mpterm["optionText"] is not None else "OVERALL"
            )
            ontology_term_id = mpterm["ontologyTermId"]
            if (
                category
                not in output_dict[pipeline][procedure][parameter][sex][
                    selection_outcome
                ]
            ):
                output_dict[pipeline][procedure][parameter][sex][selection_outcome][
                    category
                ] = {"MPTERM": ontology_term_dict[ontology_term_id]}
        output_df = spark.createDataFrame(
            [{"text": (json.dumps(output_dict, indent=2))}]
        )
        output_df.coalesce(1).write.format("text").option("header", "false").mode(
            "append"
        ).save(output_path)
