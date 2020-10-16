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
            OntologyMetadataExtractor(
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


class PipelineCoreLoader(SparkSubmitTask):
    name = "IMPC_PipelineCore_Loader"
    app = "impc_etl/jobs/load/solr/pipeline_mapper.py"
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ImpressExtractor(output_path=self.output_path),
            ObservationsMapper(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
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
        return ImpcConfig().get_target(f"{self.output_path}pipeline_core_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.emap_emapa_csv_path,
            self.emapa_metadata_csv_path,
            self.ma_metadata_csv_path,
            self.output().path,
        ]


class StatsResultsCoreLoader(SparkSubmitTask):
    name = "IMPC_StatsResults_Loader"
    app = "impc_etl/jobs/load/solr/stats_results_mapper.py"

    openstats_jdbc_connection = luigi.Parameter()
    openstats_db_user = luigi.Parameter()
    openstats_db_password = luigi.Parameter()
    data_release_version = luigi.Parameter()
    use_cache = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    mpath_metadata_csv_path = luigi.Parameter()
    threei_stats_results_csv = luigi.Parameter()
    raw_data_in_output = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            OpenStatsExtractor(
                openstats_jdbc_connection=self.openstats_jdbc_connection,
                openstats_db_user=self.openstats_db_user,
                openstats_db_password=self.openstats_db_password,
                data_release_version=self.data_release_version,
                use_cache=self.use_cache,
                raw_data_in_output=self.raw_data_in_output,
                output_path=self.output_path,
            ),
            ObservationsMapper(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
            ),
            OntologyExtractor(
                ontology_input_path=self.ontology_input_path,
                output_path=self.output_path,
            ),
            ImpressExtractor(output_path=self.output_path),
            PipelineCoreLoader(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
            ),
            AlleleExtractor(
                imits_tsv_path=self.imits_alleles_tsv_path, output_path=self.output_path
            ),
            MPChooserLoader(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
            ),
        ]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}stats_results_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.input()[4].path,
            self.input()[5].path,
            self.input()[6].path,
            self.threei_stats_results_csv,
            self.mpath_metadata_csv_path,
            self.raw_data_in_output,
            self.output().path,
        ]


class GenotypePhenotypeCoreLoader(SparkSubmitTask):
    name = "IMPC_StatsResults_Loader"
    app = "impc_etl/jobs/load/solr/genotype_phenotype_mapper.py"
    openstats_jdbc_connection = luigi.Parameter()
    openstats_db_user = luigi.Parameter()
    openstats_db_password = luigi.Parameter()
    data_release_version = luigi.Parameter()
    use_cache = luigi.Parameter()
    raw_data_in_output = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    mpath_metadata_csv_path = luigi.Parameter()
    threei_stats_results_csv = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            StatsResultsCoreLoader(
                openstats_jdbc_connection=self.openstats_jdbc_connection,
                openstats_db_user=self.openstats_db_user,
                openstats_db_password=self.openstats_db_password,
                data_release_version=self.data_release_version,
                use_cache=self.use_cache,
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
                mpath_metadata_csv_path=self.mpath_metadata_csv_path,
                threei_stats_results_csv=self.threei_stats_results_csv,
                raw_data_in_output="exclude",
                output_path=self.output_path,
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
        return ImpcConfig().get_target(f"{self.output_path}genotype_phenotype_parquet")

    def app_options(self):
        return [self.input()[0].path, self.input()[1].path, self.output().path]


class MGIPhenotypeCoreLoader(SparkSubmitTask):
    name = "IMPC_MGI_Phenotype_Loader"
    app = "impc_etl/jobs/load/solr/mgi_phenotype_mapper.py"
    mgi_allele_input_path = luigi.Parameter()
    mgi_gene_pheno_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            MGIGenePhenoExtractor(
                mgi_input_path=self.mgi_gene_pheno_input_path,
                output_path=self.output_path,
            ),
            MGIAlleleExtractor(
                mgi_input_path=self.mgi_allele_input_path, output_path=self.output_path
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
        return ImpcConfig().get_target(f"{self.output_path}mgi_phenotype_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.output().path,
        ]


class MPChooserLoader(SparkSubmitTask):
    name = "IMPC_MGI_Phenotype_Loader"
    app = "impc_etl/jobs/load/solr/mp_chooser_mapper.py"
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            PipelineCoreLoader(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
            )
        ]


class MPCoreLoader(SparkSubmitTask):
    name = "IMPC_MGI_Phenotype_Loader"
    app = "impc_etl/jobs/load/solr/mp_mapper.py"

    ontology_input_path = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    impc_search_index_csv_path = luigi.Parameter()
    mp_relation_augmented_metadata_table_csv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            OntologyExtractor(
                ontology_input_path=self.ontology_input_path,
                output_path=self.output_path,
            ),
            OntologyMetadataExtractor(
                ontology_input_path=self.ontology_input_path,
                output_path=self.output_path,
            ),
            PipelineCoreLoader(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
            ),
        ]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}mp_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.impc_search_index_csv_path,
            self.mp_relation_augmented_metadata_table_csv_path,
            self.output().path,
        ]


class GeneCoreLoader(SparkSubmitTask):
    name = "IMPC_Gene_Core_Loader"
    app = "impc_etl/jobs/load/solr/gene_mapper.py"
    imits_tsv_path = luigi.Parameter()
    mgi_homologene_input_path = luigi.Parameter()
    mgi_mrk_list_input_path = luigi.Parameter()
    embryo_data_json_path = luigi.Parameter()
    output_path = luigi.Parameter()

    dcc_xml_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    openstats_jdbc_connection = luigi.Parameter()
    openstats_db_user = luigi.Parameter()
    openstats_db_password = luigi.Parameter()
    data_release_version = luigi.Parameter()
    use_cache = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    mpath_metadata_csv_path = luigi.Parameter()
    threei_stats_results_csv = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}gene_core_parquet")

    def requires(self):
        return [
            GeneExtractor(
                imits_tsv_path=self.imits_tsv_path, output_path=self.output_path
            ),
            AlleleExtractor(
                imits_tsv_path=self.imits_tsv_path, output_path=self.output_path
            ),
            MGIHomoloGeneExtractor(
                mgi_input_path=self.mgi_homologene_input_path,
                output_path=self.output_path,
            ),
            MGIMrkListExtractor(
                mgi_input_path=self.mgi_mrk_list_input_path,
                output_path=self.output_path,
            ),
            ObservationsMapper(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
            ),
            StatsResultsCoreLoader(
                openstats_jdbc_connection=self.openstats_jdbc_connection,
                openstats_db_user=self.openstats_db_user,
                openstats_db_password=self.openstats_db_password,
                data_release_version=self.data_release_version,
                use_cache=self.use_cache,
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
                mpath_metadata_csv_path=self.mpath_metadata_csv_path,
                threei_stats_results_csv=self.threei_stats_results_csv,
                raw_data_in_output="exclude",
                output_path=self.output_path,
            ),
            OntologyMetadataExtractor(
                ontology_input_path=self.ontology_input_path,
                output_path=self.output_path,
            ),
        ]

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.embryo_data_json_path,
            self.input()[4].path,
            self.input()[5].path,
            self.input()[6].path,
            self.output().path,
        ]


class ImpcImagesCoreLoader(SparkSubmitTask):
    name = "IMPC_Images_Core_Loader"
    app = "impc_etl/jobs/load/solr/impc_images_mapper.py"
    omero_ids_csv_path = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(f"{self.output_path}impc_images_core_parquet")

    def requires(self):
        return [
            ObservationsMapper(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
            ),
            PipelineCoreLoader(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
            ),
        ]

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.omero_ids_csv_path,
            self.output().path,
        ]
