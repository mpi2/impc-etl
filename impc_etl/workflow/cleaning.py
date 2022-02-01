from impc_etl.workflow.extraction import *


class ExperimentCleaner(SparkSubmitTask):
    name = "Experiment_Cleaner"
    app = "impc_etl/jobs/clean/experiment_cleaner.py"
    output_path = luigi.Parameter()
    entity_type = "experiment"
    dcc_xml_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

    def requires(self):
        return SpecimenExperimentExtractor()

    def output(self):
        output_path = self.input().path.replace("_raw", "")
        return ImpcConfig().get_target(output_path)

    def app_options(self):
        return [self.input().path, self.entity_type, self.output().path]


class LineExperimentCleaner(SparkSubmitTask):
    name = "Line_Experiment_Cleaner"
    app = "impc_etl/jobs/clean/experiment_cleaner.py"
    output_path = luigi.Parameter()
    entity_type = "line"
    dcc_xml_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

    def requires(self):
        return LineExperimentExtractor()

    def output(self):
        output_path = self.input().path.replace("_raw", "")
        return ImpcConfig().get_target(output_path)

    def app_options(self):
        return [self.input().path, self.entity_type, self.output().path]


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
