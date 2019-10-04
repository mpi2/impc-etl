from impc_etl.workflow.extraction import *
from impc_etl.workflow.config import ImpcConfig


class ExperimentCleaner(SparkSubmitTask):
    name = "Experiment_Cleaner"
    app = "impc_etl/jobs/clean/experiment_cleaner.py"
    output_path = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()

    def requires(self):
        return ExperimentExtractor(
            dcc_xml_path=self.dcc_xml_path, output_path=self.output_path
        )

    def output(self):
        output_path = self.input().path.replace("_raw", "")
        return ImpcConfig().get_target(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]


class LineCleaner(SparkSubmitTask):
    name = "Experiment_Cleaner"
    app = "impc_etl/jobs/clean/line_cleaner.py"
    output_path = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()

    def requires(self):
        return LineExtractor(
            dcc_xml_path=self.dcc_xml_path, output_path=self.output_path
        )

    def output(self):
        output_path = self.input().path.replace("_raw", "")
        return ImpcConfig().get_target(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]


class MouseCleaner(SparkSubmitTask):
    name = "Mouse_Cleaner"
    app = "impc_etl/jobs/clean/specimen_cleaner.py"
    output_path = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()

    def requires(self):
        return MouseExtractor(
            dcc_xml_path=self.dcc_xml_path, output_path=self.output_path
        )

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

    def requires(self):
        return EmbryoExtractor(
            dcc_xml_path=self.dcc_xml_path, output_path=self.output_path
        )

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

    def requires(self):
        return ColonyExtractor(
            imits_colonies_tsv_path=self.imits_colonies_tsv_path,
            output_path=self.output_path,
        )

    def output(self):
        output_path = self.input().path.replace("_raw", "")
        return ImpcConfig().get_target(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]
