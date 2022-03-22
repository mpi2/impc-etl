import os

from luigi.contrib.webhdfs import WebHdfsClient

from impc_etl.shared.lsf_external_app_task import LSFExternalJobTask
from impc_etl.workflow.normalization import *


class Parquet2Solr(SparkSubmitTask):
    app = "lib/parquet2solr-03082021.jar"
    name = "IMPC_Parquet2Solr"
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    parquet_name = ""
    solr_core_name = ""
    parquet_solr_map = {
        "observations_parquet": "experiment",
        "statistical_results_raw_data_include_parquet": "statistical-result",
        "statistical_results_raw_data_include_parquet_raw_data": "statistical-raw-data",
        "gene_parquet": "gene",
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
    remote_host = luigi.Parameter()
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


class ImpcMergeIndex(LSFExternalJobTask):
    remote_host = luigi.Parameter()
    parquet_path = luigi.Parameter()
    solr_path = luigi.Parameter()
    local_path = luigi.Parameter()
    solr_core_name = ""
    n_cpu_flag = 56
    shared_tmp_dir = "/tmp"
    memory_flag = "210000"
    resource_flag = "mem=16000"
    extra_bsub_args = "-R span[ptile=14]"
    runtime_flag = 240

    def init_local(self):
        self.app = (
            "java -jar -Xmx209920m "
            + os.getcwd()
            + "/lib/impc-merge-index-1.0-SNAPSHOT.jar"
        )

    def requires(self):
        return [
            ImpcCopyIndexParts(
                remote_host=self.remote_host,
                parquet_path=self.parquet_path,
                solr_path=self.solr_path,
                local_path=self.local_path,
            ),
        ]

    def output(self):
        self.local_path = (
            self.local_path + "/"
            if not self.local_path.endswith("/")
            else self.local_path
        )
        self.solr_core_name = os.path.basename(os.path.normpath(self.input()[0].path))
        return luigi.LocalTarget(f"{self.local_path}{self.solr_core_name}_merged")

    def app_options(self):
        return [self.output().path, self.input()[0].path + "/*/data/index/"]
