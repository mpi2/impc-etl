from impc_etl.workflow.extraction import *


class ExperimentCleaner(SparkSubmitTask):
    name = 'Experiment_Cleaner'
    app = 'impc_etl/jobs/clean/experiment_cleaner.py'
    output_path = luigi.Parameter()
    xml_path = luigi.Parameter()

    def requires(self):
        return ExperimentExtractor(xml_path=self.xml_path, output_path=self.output_path)

    def output(self):
        output_path = self.input().path.replace('_raw', '')
        return luigi.LocalTarget(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]


class LineCleaner(SparkSubmitTask):
    name = 'Experiment_Cleaner'
    app = 'impc_etl/jobs/clean/line_cleaner.py'
    output_path = luigi.Parameter()
    xml_path = luigi.Parameter()

    def requires(self):
        return LineExtractor(xml_path=self.xml_path, output_path=self.output_path)

    def output(self):
        output_path = self.input().path.replace('_raw', '')
        return luigi.LocalTarget(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]


class MouseCleaner(SparkSubmitTask):
    name = 'Mouse_Cleaner'
    app = 'impc_etl/jobs/clean/specimen_cleaner.py'
    output_path = luigi.Parameter()
    xml_path = luigi.Parameter()

    def requires(self):
        return MouseExtractor(xml_path=self.xml_path, output_path=self.output_path)

    def output(self):
        output_path = self.input().path.replace('_raw', '')
        return luigi.LocalTarget(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]


class EmbryoCleaner(SparkSubmitTask):
    name = 'Embryo_Cleaner'
    app = 'impc_etl/jobs/clean/specimen_cleaner.py'
    output_path = luigi.Parameter()
    xml_path = luigi.Parameter()

    def requires(self):
        return EmbryoExtractor(xml_path=self.xml_path, output_path=self.output_path)

    def output(self):
        output_path = self.input().path.replace('_raw', '')
        return luigi.LocalTarget(output_path)

    def app_options(self):
        return [self.input().path, self.output().path]
