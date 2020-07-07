from impc_etl.workflow.load import *


class ImpcEtl(luigi.Task):
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ObservationsMapper(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
            )
        ]


class ImpcSolrCores(luigi.Task):
    openstats_jdbc_connection = luigi.Parameter()
    openstats_db_user = luigi.Parameter()
    openstats_db_password = luigi.Parameter()
    data_release_version = luigi.Parameter()
    use_cache = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    imits_alleles_tsv_path = luigi.Parameter()
    imits_product_tsv_path = luigi.Parameter()
    mgi_allele_input_path = luigi.Parameter()
    mgi_strain_input_path = luigi.Parameter()
    mgi_gene_pheno_input_path = luigi.Parameter()
    mgi_homologene_input_path = luigi.Parameter()
    mgi_mrk_list_input_path = luigi.Parameter()
    ontology_input_path = luigi.Parameter()
    emap_emapa_csv_path = luigi.Parameter()
    emapa_metadata_csv_path = luigi.Parameter()
    ma_metadata_csv_path = luigi.Parameter()
    impc_search_index_csv_path = luigi.Parameter()
    mp_relation_augmented_metadata_table_csv_path = luigi.Parameter()
    threei_stats_results_csv = luigi.Parameter()
    embryo_data_json_path = luigi.Parameter()
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
            ),
            GenotypePhenotypeCoreLoader(
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
                threei_stats_results_csv=self.threei_stats_results_csv,
                output_path=self.output_path,
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
                threei_stats_results_csv=self.threei_stats_results_csv,
                output_path=self.output_path,
            ),
            MGIPhenotypeCoreLoader(
                mgi_allele_input_path=self.mgi_allele_input_path,
                mgi_gene_pheno_input_path=self.mgi_gene_pheno_input_path,
                ontology_input_path=self.ontology_input_path,
                output_path=self.output_path,
            ),
            MPCoreLoader(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                imits_alleles_tsv_path=self.imits_alleles_tsv_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
                emap_emapa_csv_path=self.emap_emapa_csv_path,
                emapa_metadata_csv_path=self.emapa_metadata_csv_path,
                ma_metadata_csv_path=self.ma_metadata_csv_path,
                impc_search_index_csv_path=self.impc_search_index_csv_path,
                mp_relation_augmented_metadata_table_csv_path=self.mp_relation_augmented_metadata_table_csv_path,
                output_path=self.output_path,
            ),
            GeneCoreLoader(
                imits_tsv_path=self.imits_alleles_tsv_path,
                embryo_data_json_path=self.embryo_data_json_path,
                mgi_homologene_input_path=self.mgi_homologene_input_path,
                mgi_mrk_list_input_path=self.mgi_mrk_list_input_path,
                output_path=self.output_path,
            ),
            Allele2Extractor(
                imits_tsv_path=self.imits_alleles_tsv_path, output_path=self.output_path
            ),
            ProductExtractor(
                imits_tsv_path=self.imits_product_tsv_path, output_path=self.output_path
            ),
        ]


class ImpcDataDrivenAnnotationLoader(SparkSubmitTask):
    app = "impc_etl/jobs/load/data_driven_annotation.py"
    name = "IMPC_Data_Driven_Annotation_Loader"
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ObservationsMapper(
                dcc_xml_path=self.dcc_xml_path,
                imits_colonies_tsv_path=self.imits_colonies_tsv_path,
                output_path=self.output_path,
                mgi_strain_input_path=self.mgi_strain_input_path,
                mgi_allele_input_path=self.mgi_allele_input_path,
                ontology_input_path=self.ontology_input_path,
            )
        ]

    def app_options(self):
        return [self.input()[0].path, self.output().path]

    def output(self):
        self.output_path = (
            self.output_path + "/"
            if not self.output_path.endswith("/")
            else self.output_path
        )
        return ImpcConfig().get_target(
            f"{self.output_path}annotated_observations_parquet"
        )
