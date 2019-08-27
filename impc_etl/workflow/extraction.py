import luigi
from luigi.contrib.spark import SparkSubmitTask


class DCCExtractor(SparkSubmitTask):
    name = "IMPC_DCC_Extractor"
    app = "impc_etl/jobs/extract/dcc_extractor.py"
    file_type = luigi.Parameter()
    xml_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return luigi.LocalTarget(f"{self.output_path}{self.entity_type}_raw_parquet")

    def app_options(self):
        return [self.xml_path, self.output().path, self.file_type, self.entity_type]


class MouseExtractor(DCCExtractor):
    file_type = "specimen"
    entity_type = "mouse"


class EmbryoExtractor(DCCExtractor):
    file_type = "specimen"
    entity_type = "embryo"


class ExperimentExtractor(DCCExtractor):
    file_type = "experiment"
    entity_type = "experiment"


class LineExtractor(DCCExtractor):
    file_type = "experiment"
    entity_type = "line"


class ImitsExtractor(SparkSubmitTask):
    name = "IMPC_IMITS_Extractor"
    app = "impc_etl/jobs/extract/imits_extractor.py"
    tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return luigi.LocalTarget(
            f"{self.output_path}{self.entity_type.lower()}_raw_parquet"
        )

    def app_options(self):
        return [self.tsv_path, self.output().path, self.entity_type]


class AlleleExtractor(ImitsExtractor):
    entity_type = "Allele"


class GeneExtractor(ImitsExtractor):
    entity_type = "Gene"


class ColonyExtractor(ImitsExtractor):
    entity_type = "Colony"


class ProductExtractor(ImitsExtractor):
    entity_type = "Product"


class ImpressExtractor(SparkSubmitTask):
    name = "IMPC_IMPRESS_Extractor"
    app = "impc_etl/jobs/extract/impress_extractor.py"
    impress_api_url = luigi.Parameter()
    output_path = luigi.Parameter()
    impress_root_type = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return luigi.LocalTarget(f"{self.output_path}{self.impress_root_type}_parquet")

    def app_options(self):
        return [self.impress_api_url, self.output().path, self.impress_root_type]


class MGIExtractor(SparkSubmitTask):
    name = "IMPC_IMPRESS_Extractor"
    app = "impc_etl/jobs/extract/mgi_extractor.py"
    strain_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return luigi.LocalTarget(f"{self.output_path}strain_parquet")

    def app_options(self):
        return [self.strain_input_path, self.output().path]
