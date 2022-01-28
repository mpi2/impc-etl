import luigi
from luigi.contrib.spark import SparkSubmitTask

from impc_etl.jobs.extract import *
from impc_etl.workflow.config import ImpcConfig


class MouseSpecimenExtractor(DCCSpecimenExtractor):
    specimen_type = "mouse"


class EmbryoSpecimenExtractor(DCCSpecimenExtractor):
    specimen_type = "embryo"


class SpecimenExperimentExtractor(DCCExperimentExtractor):
    experiment_type = "specimen_level"


class LineExperimentExtractor(DCCExperimentExtractor):
    experiment_type = "line_level"


class ImitsExtractor(SparkSubmitTask):
    name = "IMPC_IMITS_Extractor"
    app = "impc_etl/jobs/extract/imits_extractor.py"
    imits_tsv_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}imits_{self.entity_type.lower()}_raw_parquet"
        )

    def app_options(self):
        return [self.imits_tsv_path, self.output().path, self.entity_type]


class AlleleExtractor(ImitsExtractor):
    entity_type = "Allele"


class GeneExtractor(ImitsExtractor):
    entity_type = "Gene"


class Allele2Extractor(ImitsExtractor):
    entity_type = "allele2"


class ColonyExtractor(ImitsExtractor):
    entity_type = "Colony"


class ImpressExtractor(SparkSubmitTask):
    name = "IMPC_IMPRESS_Extractor"
    app = "impc_etl/jobs/extract/impress_extractor.py"
    impress_api_url = luigi.Parameter()
    output_path = luigi.Parameter()
    http_proxy = luigi.Parameter()
    impress_root_type = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}{self.impress_root_type}_parquet"
        )

    def app_options(self):
        return [
            self.impress_api_url,
            self.output().path,
            self.impress_root_type,
            self.http_proxy,
        ]


class MGIExtractor(SparkSubmitTask):
    name = "IMPC_MGI_Extractor"
    app = "impc_etl/jobs/extract/mgi_extractor.py"
    mgi_input_path = luigi.Parameter()
    entity_type = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}{self.entity_type.lower()}_parquet"
        )

    def app_options(self):
        return [self.mgi_input_path, self.entity_type, self.output().path]


class MGIAlleleExtractor(MGIExtractor):
    entity_type = "allele"


class MGIStrainExtractor(MGIExtractor):
    entity_type = "strain"


class MGIGenePhenoExtractor(MGIExtractor):
    entity_type = "gene_pheno"


class MGIHomoloGeneExtractor(MGIExtractor):
    entity_type = "cross_ref"


class MGIMrkListExtractor(MGIExtractor):
    entity_type = "mrk_list"


class OntologyExtractor(SparkSubmitTask):
    name = "IMPC_Ontology_Extractor"
    app = "impc_etl/jobs/extract/ontology_extractor.py"

    ontology_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}ontology_parquet")

    def app_options(self):
        return [self.ontology_input_path, self.output().path]


class OntologyMetadataExtractor(SparkSubmitTask):
    name = "IMPC_Ontology_Metadata_Extractor"
    app = "impc_etl/jobs/extract/ontology_metadata_extractor.py"

    ontology_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}ontology_metadata_parquet")

    def app_options(self):
        return [self.ontology_input_path, self.output().path]


class OpenStatsExtractor(SparkSubmitTask):
    name = "IMPC_OpenStats_Extractor"
    app = "impc_etl/jobs/extract/open_stats_extractor.py"

    openstats_jdbc_connection = luigi.Parameter()
    openstats_db_user = luigi.Parameter()
    openstats_db_password = luigi.Parameter()
    data_release_version = luigi.Parameter()
    use_cache = luigi.Parameter()
    raw_data_in_output = luigi.Parameter()
    extract_windowed_data = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        if self.extract_windowed_data == "true":
            return ImpcConfig().get_target(
                f"{self.output_path}open_stats_parquet_with_windowing_data"
            )
        elif self.raw_data_in_output == "include":
            return ImpcConfig().get_target(
                f"{self.output_path}open_stats_parquet_with_raw_data"
            )
        else:
            return ImpcConfig().get_target(f"{self.output_path}open_stats_parquet")

    def app_options(self):
        return [
            self.openstats_jdbc_connection,
            self.openstats_db_user,
            self.openstats_db_password,
            self.data_release_version,
            self.use_cache,
            self.raw_data_in_output,
            self.extract_windowed_data,
            self.output().path,
        ]
