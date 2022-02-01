from impc_etl.jobs.clean.experiment_cleaner import IMPCExperimentCleaner
from impc_etl.workflow.extraction import *


class SpecimenExperimentCleaner(IMPCExperimentCleaner):
    experiment_type = "specimen_level"


class LineExperimentCleaner(IMPCExperimentCleaner):
    experiment_type = "line_level"


class MouseCleaner(SparkSubmitTask):
    name = "Mouse_Cleaner"
    app = "impc_etl/jobs/clean/specimen_cleaner.py"
    output_path = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

    def requires(self):
        return MouseSpecimenExtractor()

    def output(self):
        output_path = self.input().path.replace("_raw", "")
        return ImpcConfig().get_target(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]


class EmbryoCleaner(SparkSubmitTask):
    name = "Embryo_Cleaner"
    app = "impc_etl/jobs/clean/specimen_cleaner.py"
    output_path = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

    def requires(self):
        return EmbryoSpecimenExtractor()

    def output(self):
        output_path = self.input().path.replace("_raw", "")
        return ImpcConfig().get_target(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]
