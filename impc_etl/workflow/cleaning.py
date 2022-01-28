from impc_etl.workflow.extraction import *
from impc_etl.workflow.config import ImpcConfig


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


class ColonyCleaner(SparkSubmitTask):
    name = "Colony_Cleaner"
    app = "impc_etl/jobs/clean/colony_cleaner.py"
    imits_colonies_tsv_path = luigi.Parameter()
    output_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

    def requires(self):
        return ColonyTrackingExtractor()

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}colony_parquet")

    def app_options(self):
        return [self.input().path, self.output().path]
