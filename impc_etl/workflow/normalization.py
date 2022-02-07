import luigi
from luigi.contrib.spark import SparkSubmitTask

from impc_etl.jobs.clean import *
from impc_etl.jobs.clean.experiment_cleaner import (
    LineLevelExperimentCleaner,
)
from impc_etl.jobs.extract import ImpressExtractor
from impc_etl.workflow.config import ImpcConfig


class LineExperimentNormalizer(SparkSubmitTask):
    name = "IMPC_Line_Experiment_Normalizer"
    app = "impc_etl/jobs/transform/line_experiment_cross_ref.py"
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

    def requires(self):
        return [
            LineLevelExperimentCleaner(),
            ColonyCleaner(),
            ImpressExtractor(),
        ]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}{self.entity_type}_normalized_parquet"
        )

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.output().path,
        ]
