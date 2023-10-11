import os

from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.webhdfs import WebHdfsClient

from impc_etl.shared.lsf_external_app_task import LSFExternalJobTask
from impc_etl.workflow.normalization import *
import glob


class Parquet2Solr(SparkSubmitTask):
    app = "lib/parquet2solr-0.0.1-SNAPSHOT.jar"
    name = "IMPC_Parquet2Solr"
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    parquet_name = ""
    solr_core_name = ""
    parquet_solr_map = {
        "observations_parquet": "experiment",
        "statistical_results_raw_data_include_parquet": "statistical-result",
        "statistical_results_raw_data_include_parquet_raw_data": "statistical-raw-data",
        "gene_data_include_parquet": "gene",
        "genotype_phenotype_parquet": "genotype-phenotype",
        "mp_parquet": "mp",
        "impress_parameter_parquet": "pipeline",
        "product_report_raw_parquet": "product",
        "mgi_phenotype_parquet": "mgi-phenotype",
        "impc_images_core_parquet": "impc_images",
    }

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        self.parquet_name = os.path.basename(os.path.normpath(self.input_path))
        self.solr_core_name = self.parquet_solr_map[self.parquet_name]
        return ImpcConfig().get_target(f"{self.output_path}{self.solr_core_name}_index")

    def app_options(self):
        return [
            self.app,
            self.input_path,
            self.solr_core_name,
            "false",
            "false",
            self.output().path,
        ]


class ImpcCopyIndexParts(luigi.Task):
    parquet_path = luigi.Parameter()
    solr_path = luigi.Parameter()
    local_path = luigi.Parameter()
    solr_core_name = ""

    def requires(self):
        return [
            Parquet2Solr(input_path=self.parquet_path, output_path=self.solr_path),
        ]

    def output(self):
        self.local_path = (
            self.local_path + "/"
            if not self.local_path.endswith("/")
            else self.local_path
        )
        self.solr_core_name = os.path.basename(os.path.normpath(self.input()[0].path))
        return luigi.LocalTarget(f"{self.local_path}{self.solr_core_name}")

    def run(self):
        client = WebHdfsClient()
        client.download(self.input()[0].path, self.output().path, n_threads=50)


class ImpcMergeIndex(ExternalProgramTask):
    parquet_path = luigi.Parameter()
    solr_path = luigi.Parameter()
    local_path = luigi.Parameter()
    big_task = luigi.Parameter(False)
    solr_core_name = ""
    cpus_per_task = 8
    memory_flag = 64
    runtime_flag = 60
    multiplier = 1

    def program_args(self):
        if self.big_task:
            self.multiplier = 4
        return [
            "srun",
            "--cpus-per-task",
            self.cpus_per_task * self.multiplier,
            "--mem",
            f"{self.memory_flag * self.multiplier}G",
            "-t",
            self.runtime_flag * self.multiplier,
            "java",
            "-jar",
            f"-Xmx{(self.memory_flag * 1024 * self.multiplier) - 1024}m",
            os.getcwd() + "/lib/impc-merge-index-1.0-SNAPSHOT.jar",
            self.output().path,
        ] + glob.glob(self.input()[0].path + "/*/data/index/")

    def requires(self):
        return [
            Parquet2Solr(input_path=self.parquet_path, output_path=self.solr_path),
        ]

    def output(self):
        self.local_path = (
            self.local_path + "/"
            if not self.local_path.endswith("/")
            else self.local_path
        )
        self.solr_core_name = os.path.basename(os.path.normpath(self.input()[0].path))
        return luigi.LocalTarget(f"{self.local_path}{self.solr_core_name}_merged")
