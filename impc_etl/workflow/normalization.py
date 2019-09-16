from impc_etl.workflow.cleaning import *
from impc_etl.workflow.config import ImpcConfig


class SpecimenNormalizer(SparkSubmitTask):
    name = "IMPC_Specimen_Normalizer"
    app = "impc_etl/jobs/normalize/specimen_normalizer.py"
    xml_path = luigi.Parameter()
    tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()

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
            MouseCleaner(xml_path=self.xml_path, output_path=self.output_path),
            ColonyCleaner(tsv_path=self.tsv_path, output_path=self.output_path),
        ]


class EmbryoNormalizer(SpecimenNormalizer):
    entity_type = "embryo"

    def requires(self):
        return [
            EmbryoCleaner(xml_path=self.xml_path, output_path=self.output_path),
            ColonyCleaner(tsv_path=self.tsv_path, output_path=self.output_path),
        ]


class ExperimentNormalizer(SparkSubmitTask):
    name = "IMPC_Experiment_Normalizer"
    app = "impc_etl/jobs/normalize/experiment_normalizer.py"
    xml_path = luigi.Parameter()
    tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ExperimentCleaner(xml_path=self.xml_path, output_path=self.output_path),
            MouseNormalizer(
                tsv_path=self.tsv_path,
                xml_path=self.xml_path,
                output_path=self.output_path,
            ),
            EmbryoNormalizer(
                tsv_path=self.tsv_path,
                xml_path=self.xml_path,
                output_path=self.output_path,
            ),
            ImpressExtractor(output_path=self.output_path),
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
