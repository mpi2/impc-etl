from typing import Union

import luigi
from luigi.contrib.hdfs import HdfsTarget

from impc_etl.jobs.compare.dr_diff import ImpcDrDiffReportGeneration
from impc_etl.jobs.extract import ProductReportExtractor
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.impc_api.impc_api_mapper import (
    ImpcGeneSummaryMapper,
    ImpcGeneStatsResultsMapper,
    ImpcGenePhenotypeHitsMapper,
    ImpcLacZExpressionMapper,
    ImpcPublicationsMapper,
    ImpcProductsMapper,
    ImpcGeneImagesMapper,
    ImpcGeneDiseasesMapper,
    ImpcGeneHistopathologyMapper,
    ImpcDatasetsMapper,
    ImpcDatasetsMetadataMapper,
    ImpcPhenotypeSearchMapper,
    ImpcGeneSearchMapper,
    ImpcPhenotypeSummaryMapper,
    ImpcPhenotypeStatisticalResultsMapper,
    ImpcImagesMapper,
    ImpcBWTDatasetsMapper,
    ImpcExternalLinksMapper,
    ImpcPathologyDatasetsMapper,
    ImpcHistopathologyDatasetsMapper,
    ImpcReleaseMetadataMapper,
    ImpcLateAdultLandingPageMapper,
    ImpcHistopathologyLandingPageMapper,
    ImpcPhenotypePleiotropyMapper,
)
from impc_etl.jobs.load.impc_api.impc_bulk_api_mapper import ImpcBulkApiMapper
from impc_etl.jobs.load.impc_api.impc_bulk_api_mongo_load import ImpcBulkApiMongoLoader
from impc_etl.jobs.load.impc_images_mapper import ImagesPipelineInputGenerator
from impc_etl.jobs.load.impc_kg.impc_kg_mapper import (
    ImpcKgTimeSeriesObservationObservationMapper,
    ImpcKgTextObservationMapper,
    ImpcKgCategoricalObservationMapper,
    ImpcKgUnidimensionalObservationMapper,
    ImpcKgOntologicalObservationMapper,
    ImpcKgImageRecordObservationObservationMapper,
    ImpcKgSpecimenExperimentMapper,
    ImpcKgLineExperimentMapper,
    ImpcKgMouseSpecimenMapper,
    ImpcKgEmbryoSpecimenMapper,
    ImpcKgStatisticalResultMapper,
    ImpcKgGenePhenotypeAssociationMapper,
    ImpcKgParameterMapper,
    ImpcKgProcedureMapper,
    ImpcKgPipelineMapper,
    ImpcKgMouseGeneMapper,
    ImpcKgMouseAlleleMapper,
    ImpcKgPublicationsMapper,
    ImpcKgHumanGeneMapper,
)
from impc_etl.jobs.load.mp_chooser_mapper import MPChooserGenerator
from impc_etl.jobs.load.solr.gene_mapper import GeneLoader
from impc_etl.jobs.load.solr.genotype_phenotype_mapper import GenotypePhenotypeLoader
from impc_etl.jobs.load.solr.impc_images_mapper import ImpcImagesLoader
from impc_etl.jobs.load.solr.mgi_phenotype_mapper import MGIPhenotypeCoreLoader
from impc_etl.jobs.load.solr.mp_mapper import MpLoader
from impc_etl.jobs.load.solr.pipeline_mapper import ImpressToParameterMapper
from impc_etl.jobs.load.solr.stats_results_mapper import StatsResultsMapper
from impc_etl.jobs.load.stats_pipeline_input_mapper import StatsPipelineInputMapper
from impc_etl.workflow.load import ImpcMergeIndex


class ImpcPreStatisticalAnalysis(luigi.Task):
    def requires(self):
        return [
            StatsPipelineInputMapper(),
            ImagesPipelineInputGenerator(),
            MPChooserGenerator(),
            ImpcDrDiffReportGeneration(),
        ]


class ImpcPostStatisticalAnalysis(luigi.Task):
    name = "IMPC_Index_Data_Release"
    solr_path = luigi.Parameter()
    local_path = luigi.Parameter()

    def requires(self):
        return [
            ImpressToParameterMapper(),
            GenotypePhenotypeLoader(),
            GeneLoader(),
            MpLoader(),
            MGIPhenotypeCoreLoader(),
            ProductReportExtractor(),
            ImpcImagesLoader(),
            ExperimentToObservationMapper(),
            StatsResultsMapper(),
        ]

    def run(self):
        tasks = []
        for dependency in self.input():
            big_task = (
                "experiment" in dependency.path or "statistical" in dependency.path
            )

            tasks.append(
                ImpcMergeIndex(
                    parquet_path=dependency.path,
                    solr_path=self.solr_path,
                    local_path=self.local_path,
                    big_task=big_task,
                )
            )
            if "statistical" in dependency.path:
                tasks.append(
                    ImpcMergeIndex(
                        parquet_path=dependency.path + "_raw_data",
                        solr_path=self.solr_path,
                        local_path=self.local_path,
                        big_task=big_task,
                    )
                )
        yield tasks


class ImpcBulkApi(luigi.Task):
    def requires(self):
        return [ImpcBulkApiMapper()]


class ImpcIndexDaily(luigi.Task):
    name = "IMPC_Index_Daily"
    imits_product_tsv_path = luigi.Parameter()
    parquet_path = luigi.Parameter()
    solr_path = luigi.Parameter()
    local_path = luigi.Parameter()

    def requires(self):
        return [
            ProductReportExtractor(),
            GeneLoader(),
        ]

    def run(self):
        tasks = []
        for dependency in self.input():
            tasks.append(
                ImpcMergeIndex(
                    parquet_path=dependency.path,
                    solr_path=self.solr_path,
                    local_path=self.local_path,
                )
            )
        yield tasks


class ImpcCleanDaily(luigi.Task):
    name = "IMPC_Clean_Daily"
    imits_product_tsv_path = luigi.Parameter()
    parquet_path = luigi.Parameter()
    solr_path = luigi.Parameter()
    local_path = luigi.Parameter()

    def _delele_target_if_exists(
        self, target: Union[luigi.LocalTarget, HdfsTarget], hdfs=False
    ):
        if target.exists():
            print(target.path)
            if hdfs:
                target.remove(skip_trash=True)
            else:
                target.remove()

    def run(self):
        index_daily_task = ImpcIndexDaily(
            imits_product_tsv_path=self.imits_product_tsv_path,
            parquet_path=self.parquet_path,
            solr_path=self.solr_path,
            local_path=self.local_path,
        )
        for index_daily_dependency in index_daily_task.requires():
            impc_merge_index_task = ImpcMergeIndex(
                parquet_path=index_daily_dependency.output().path,
                solr_path=self.solr_path,
                local_path=self.local_path,
            )
            impc_copy_index_task = impc_merge_index_task.requires()[0]
            impc_parquet_to_solr_task = impc_copy_index_task.requires()[0]

            self._delele_target_if_exists(index_daily_dependency.output(), hdfs=True)
            self._delele_target_if_exists(impc_merge_index_task.output())
            self._delele_target_if_exists(impc_copy_index_task.output())
            self._delele_target_if_exists(impc_parquet_to_solr_task.output(), hdfs=True)


class ImpcWebApiMapper(luigi.Task):
    name = "IMPC_Web_Api_Mapper"

    def requires(self):
        return [
            ImpcGeneSummaryMapper(),
            ImpcGenePhenotypeHitsMapper(),
            ImpcGeneStatsResultsMapper(),
            ImpcLacZExpressionMapper(),
            ImpcGeneImagesMapper(),
            ImpcGeneHistopathologyMapper(),
            ImpcGeneDiseasesMapper(),
            ImpcPublicationsMapper(),
            ImpcProductsMapper(),
            ImpcDatasetsMetadataMapper(),
            ImpcDatasetsMapper(),
            ImpcPhenotypeSearchMapper(),
            ImpcPhenotypeSummaryMapper(),
            ImpcGeneSearchMapper(),
            ImpcPhenotypeStatisticalResultsMapper(),
            ImpcImagesMapper(),
            ImpcBWTDatasetsMapper(),
            ImpcExternalLinksMapper(),
            ImpcPathologyDatasetsMapper(),
            ImpcHistopathologyDatasetsMapper(),
            ImpcReleaseMetadataMapper(),
            ImpcLateAdultLandingPageMapper(),
            ImpcHistopathologyLandingPageMapper(),
            ImpcPhenotypePleiotropyMapper(),
        ]


class ImpcKgMapping(luigi.Task):
    name = "ImpcKgMapping"

    def requires(self):
        return [
            ImpcKgUnidimensionalObservationMapper(),
            ImpcKgCategoricalObservationMapper(),
            ImpcKgTextObservationMapper(),
            ImpcKgTimeSeriesObservationObservationMapper(),
            ImpcKgOntologicalObservationMapper(),
            ImpcKgImageRecordObservationObservationMapper(),
            ImpcKgSpecimenExperimentMapper(),
            ImpcKgLineExperimentMapper(),
            ImpcKgMouseSpecimenMapper(),
            ImpcKgEmbryoSpecimenMapper(),
            ImpcKgStatisticalResultMapper(),
            ImpcKgGenePhenotypeAssociationMapper(),
            ImpcKgParameterMapper(),
            ImpcKgProcedureMapper(),
            ImpcKgPipelineMapper(),
            ImpcKgMouseGeneMapper(),
            ImpcKgMouseAlleleMapper(),
            ImpcKgPublicationsMapper(),
            ImpcKgHumanGeneMapper(),
        ]
