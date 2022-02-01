from impc_etl.jobs.clean import *
from impc_etl.workflow.cleaning import *
from impc_etl.workflow.config import ImpcConfig


class SpecimenNormalizer(SparkSubmitTask):
    name = "IMPC_Specimen_Normalizer"
    app = "impc_etl/jobs/transform/specimen_normalizer.py"
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

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
            self.entity_type,
            self.output().path,
        ]


class MouseNormalizer(SpecimenNormalizer):
    entity_type = "mouse"

    def requires(self):
        return [
            MouseCleaner(dcc_xml_path=self.dcc_xml_path, output_path=self.output_path),
            IMPCColonyCleaner(),
        ]


class EmbryoNormalizer(SpecimenNormalizer):
    entity_type = "embryo"

    def requires(self):
        return [
            EmbryoCleaner(dcc_xml_path=self.dcc_xml_path, output_path=self.output_path),
            IMPCColonyCleaner(),
        ]


class ExperimentNormalizer(SparkSubmitTask):
    name = "IMPC_Experiment_Normalizer"
    app = "impc_etl/jobs/transform/experiment_normalizer.py"
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            SpecimenExperimentCleaner(),
            MouseNormalizer(
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                dcc_xml_path=self.dcc_xml_path,
                output_path=self.output_path,
            ),
            EmbryoNormalizer(
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                dcc_xml_path=self.dcc_xml_path,
                output_path=self.output_path,
            ),
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
            self.input()[3].path,
            self.output().path,
        ]


class LineExperimentNormalizer(SparkSubmitTask):
    name = "IMPC_Line_Experiment_Normalizer"
    app = "impc_etl/jobs/transform/line_normalizer.py"
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()
    resources = {"overwrite_resource": 1}

    def requires(self):
        return [
            LineExperimentCleaner(),
            IMPCColonyCleaner(),
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
