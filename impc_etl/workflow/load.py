from impc_etl.workflow.normalization import *
from impc_etl.workflow.config import ImpcConfig


class ObservationsMapper(SparkSubmitTask):
    name = "IMPC_Observations_Mapper"
    app = "impc_etl/jobs/load/observation_mapper.py"
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ExperimentNormalizer(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                entity_type="experiment",
                output_path=self.output_path,
            ),
            LineExperimentNormalizer(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                entity_type="line",
                output_path=self.output_path,
            ),
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
            MGIAlleleExtractor(
                mgi_input_path=self.mgi_allele_input_path, output_path=self.output_path
            ),
            ColonyCleaner(
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                output_path=self.output_path,
            ),
            ImpressExtractor(output_path=self.output_path),
            MGIStrainExtractor(
                mgi_input_path=self.mgi_strain_input_path, output_path=self.output_path
            ),
            OntologyExtractor(
                ontology_input_path=self.ontology_input_path,
                output_path=self.output_path,
            ),
        ]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}observations_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.input()[4].path,
            self.input()[5].path,
            self.input()[6].path,
            self.input()[7].path,
            self.input()[8].path,
            self.output().path,
        ]


class Komp2Loader(SparkSubmitTask):
    name = "IMPC_KOMP2_Loader"
    komp2_table = luigi.Parameter()
    jdbc_connection = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}komp2_{self.komp2_table}_parquet"
        )


class Komp2AlleleLoader(Komp2Loader):
    app = "impc_etl/jobs/load/komp2/allele_loader.py"
    komp2_table = "allele"
    imits_allele2_tsv_file = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()

    def requires(self):
        return [
            AlleleExtractor(imits_tsv_path=self.imits_allele2_tsv_file),
            MGIAlleleExtractor(
                mgi_input_path=self.mgi_allele_input_path, output_path=self.output_path
            ),
        ]

    def app_options(self):
        return [self.input()[0].path, self.input()[1].path, self.output().path]
