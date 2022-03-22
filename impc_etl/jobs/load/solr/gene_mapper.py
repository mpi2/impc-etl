"""
SOLR module
   Generates the required Solr cores
"""
import base64
import gzip
import json
from typing import Any

import luigi
import requests
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import (
    count,
    col,
    collect_set,
    lit,
    when,
    array_except,
    array,
    first,
)
from pyspark.sql.types import StringType, DoubleType

from impc_etl.config.constants import Constants
from impc_etl.jobs.clean import ColonyCleaner
from impc_etl.jobs.extract import (
    MGIHomologyReportExtractor,
    MGIMarkerListReportExtractor,
    OntologyMetadataExtractor,
    GeneProductionStatusExtractor,
)
from impc_etl.jobs.extract.allele_ref_extractor import ExtractAlleleRef
from impc_etl.jobs.extract.gene_ref_extractor import ExtractGeneRef
from impc_etl.jobs.load import ExperimentToObservationMapper
from impc_etl.jobs.load.solr.stats_results_mapper import StatsResultsMapper
from impc_etl.workflow.config import ImpcConfig

GENE_REF_MAP = {
    "mgi_gene_acc_id": "mgi_accession_id",
    "symbol": "marker_symbol",
    "type": "marker_type",
    "synonyms": "marker_synonym",
}

GENE_CORE_COLUMNS = [
    "mgi_accession_id",
    "marker_symbol",
    "human_gene_symbol",
    "marker_name",
    "marker_synonym",
    "marker_type",
    "es_cell_production_status",
    "null_allele_production_status",
    "conditional_allele_production_status",
    "crispr_allele_production_status",
    "mouse_production_status",
    "assignment_status",
    "phenotyping_data_available",
    "allele_mgi_accession_id",
    "allele_name",
    "is_umass_gene",
    "is_idg_gene",
    "embryo_data_available",
    "embryo_analysis_view_url",
    "embryo_analysis_view_name",
    "embryo_modalities",
    "chr_name",
    "seq_region_id",
    "seq_region_start",
    "seq_region_end",
    "chr_strand",
    "ensembl_gene_id",
    "ccds_id",
    "ncbi_id",
    "production_centre",
    "phenotyping_centre",
]


class GeneMapper(PySparkTask):
    #: Name of the Spark task
    name = "IMPC_Gene_Mapper"

    embryo_data_json_path = luigi.Parameter()

    compress_data_sets = luigi.Parameter(default=True)

    raw_data_in_output = luigi.Parameter(default="include")

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def requires(self):
        return [
            ExtractGeneRef(),
            ExtractAlleleRef(),
            MGIHomologyReportExtractor(),
            MGIMarkerListReportExtractor(),
            ExperimentToObservationMapper(),
            StatsResultsMapper(raw_data_in_output=self.raw_data_in_output),
            OntologyMetadataExtractor(),
            GeneProductionStatusExtractor(),
            ColonyCleaner(),
        ]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/output/gene_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}gene_parquet")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
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
            self.embryo_data_json_path,
            self.compress_data_sets,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Solr Core loader
        :param list argv: the list elements should be:
                        [1]: source IMPC parquet file
                        [2]: Output Path
        """
        gene_ref_parquet_path = args[0]
        allele_ref_parquet_path = args[1]
        mgi_homologene_report_parquet_path = args[2]
        mgi_mrk_list_report_parquet_path = args[3]
        observations_parquet_path = args[4]
        stats_results_parquet_path = args[5]
        ontology_metadata_parquet_path = args[6]
        gene_production_status_path = args[7]
        colonies_report_parquet_path = args[8]
        embryo_data_json_path = args[9]
        compress_datasets = args[10]
        output_path = args[11]

        spark = SparkSession.builder.getOrCreate()
        gene_ref_df = spark.read.parquet(gene_ref_parquet_path)
        allele_ref_df = spark.read.parquet(allele_ref_parquet_path)
        mgi_homologene_df = spark.read.parquet(mgi_homologene_report_parquet_path)
        mgi_mrk_list_df = spark.read.parquet(mgi_mrk_list_report_parquet_path)
        mgi_mrk_list_df = mgi_mrk_list_df.select(
            [
                "mgi_accession_id",
                "chr",
                "genome_coordinate_start",
                "genome_coordinate_end",
                "strand",
            ]
        )
        embryo_data_df = spark.read.json(embryo_data_json_path, mode="FAILFAST")
        observations_df = spark.read.parquet(observations_parquet_path)
        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        ontology_metadata_df = spark.read.parquet(ontology_metadata_parquet_path)
        gene_production_status_df = spark.read.parquet(gene_production_status_path)
        colonies_report_df = spark.read.parquet(colonies_report_parquet_path)

        gene_df = self.get_gene_core_df(
            gene_ref_df,
            allele_ref_df,
            mgi_homologene_df,
            mgi_mrk_list_df,
            embryo_data_df,
            observations_df,
            stats_results_df,
            ontology_metadata_df,
            gene_production_status_df,
            colonies_report_df,
            compress_datasets,
        )
        gene_df.distinct().write.parquet(output_path)

    def get_gene_core_df(
        self,
        gene_ref_df,
        allele_ref_df,
        mgi_homologene_df,
        mgi_mrk_list_df,
        embryo_data_df,
        observations_df,
        stats_results_df,
        ontology_metadata_df,
        gene_production_status_df,
        colonies_report_df,
        compress_data_sets=True,
    ):
        phenotyping_data_availability_df = observations_df.groupBy(
            "gene_accession_id"
        ).agg(count("*").alias("data_points"))
        phenotyping_data_availability_df = phenotyping_data_availability_df.withColumn(
            "phenotype_status",
            when(col("data_points") > 0, lit("Phenotyping data available")).otherwise(
                lit("Phenotyping data not available")
            ),
        )
        phenotyping_data_availability_df = phenotyping_data_availability_df.drop(
            "data_points"
        )
        phenotyping_data_availability_df = (
            phenotyping_data_availability_df.withColumnRenamed(
                "gene_accession_id", "mgi_accession_id"
            )
        )

        ontology_metadata_df = ontology_metadata_df.select(
            functions.col("curie").alias("phenotype_term_id"),
            functions.col("name").alias("phenotype_term_name"),
        ).distinct()

        stats_results_df = stats_results_df.withColumnRenamed(
            "marker_accession_id", "gene_accession_id"
        )
        stats_results_df = stats_results_df.withColumnRenamed(
            "marker_symbol", "gene_symbol"
        )
        stats_results_df = stats_results_df.withColumn(
            "procedure_stable_id", functions.explode("procedure_stable_id")
        )
        stats_results_df = stats_results_df.withColumn(
            "procedure_name", functions.explode("procedure_name")
        )
        stats_results_df = stats_results_df.withColumn(
            "life_stage_name", functions.explode("life_stage_name")
        )
        stats_results_df = stats_results_df.withColumn(
            "top_level_mp_term_name",
            functions.when(
                (functions.size("top_level_mp_term_name") == 0)
                | functions.col("top_level_mp_term_name").isNull(),
                functions.array("mp_term_name"),
            ).otherwise(functions.col("top_level_mp_term_name")),
        )

        significant_mp_term = self._get_significance_fields_by_gene(stats_results_df)
        mgi_datasets_df = self._get_datasets_by_gene(
            stats_results_df, observations_df, ontology_metadata_df, compress_data_sets
        )

        gene_allele_info_df = allele_ref_df.groupBy("mgi_marker_acc_id").agg(
            *[
                collect_set(col_name).alias(col_name)
                for col_name in allele_ref_df.columns
                if col_name != "mgi_marker_acc_id"
            ]
        )
        gene_allele_info_df = gene_allele_info_df.drop("mgi_accession_id")
        for ref_col_name, gene_core_col_name in GENE_REF_MAP.items():
            gene_ref_df = gene_ref_df.withColumnRenamed(
                ref_col_name, gene_core_col_name
            )
        colonies_report_df = colonies_report_df.select(
            "mgi_accession_id", "production_centre", "phenotyping_centre", "colony_name"
        ).distinct()
        colonies_report_df = colonies_report_df.groupBy("mgi_accession_id").agg(
            collect_set("production_centre").alias("production_centre"),
            collect_set("phenotyping_centre").alias("phenotyping_centre"),
            first("colony_name").alias("colony_name"),
        )
        gene_ref_df = gene_ref_df.join(colonies_report_df, "mgi_accession_id", "left")
        gene_df = gene_ref_df.where(
            functions.col("subtype").isin(["protein coding gene", "miRNA gene"])
            | (col("colony_name").isNotNull())
        )
        gene_df = gene_df.select(
            gene_ref_df.columns + ["production_centre", "phenotyping_centre"]
        ).distinct()

        gene_df = gene_df.join(
            gene_allele_info_df,
            col("mgi_accession_id") == col("mgi_marker_acc_id"),
            "left_outer",
        )
        gene_df = gene_df.withColumnRenamed(
            "allele_mgi_accession_id", "allele_accession_id"
        )
        gene_df = gene_df.select(
            *[col_name for col_name in gene_df.columns if col_name in GENE_CORE_COLUMNS]
        )
        gene_df = gene_df.withColumn(
            "is_umass_gene", functions.col("marker_symbol").isin(Constants.UMASS_GENES)
        )
        gene_df = gene_df.withColumn(
            "is_idg_gene", functions.col("mgi_accession_id").isin(Constants.IDG_GENES)
        )

        embryo_data_df = embryo_data_df.withColumn(
            "colonies", functions.explode("colonies")
        )
        embryo_data_df = embryo_data_df.select("colonies.*")
        embryo_data_df = embryo_data_df.withColumn(
            "embryo_analysis_view_name",
            functions.when(
                functions.col("analysis_view_url").isNotNull(),
                functions.lit("volumetric analysis"),
            ).otherwise(functions.lit(None).cast(StringType())),
        )
        embryo_data_df = embryo_data_df.withColumnRenamed(
            "analysis_view_url", "embryo_analysis_view_url"
        )
        embryo_data_df = embryo_data_df.withColumn(
            "embryo_modalities", functions.col("procedures_parameters.modality")
        )
        embryo_data_df = embryo_data_df.alias("embryo")
        gene_df = gene_df.join(
            embryo_data_df,
            functions.col("mgi_accession_id") == functions.col("mgi"),
            "left_outer",
        )
        gene_df = gene_df.withColumn(
            "embryo_data_available", functions.col("embryo.mgi").isNotNull()
        )

        gene_df = gene_df.join(mgi_mrk_list_df, "mgi_accession_id", "left_outer")
        gene_df = gene_df.withColumn("seq_region_id", functions.col("chr"))
        gene_df = gene_df.withColumnRenamed("chr", "chr_name")
        gene_df = gene_df.withColumnRenamed(
            "genome_coordinate_start", "seq_region_start"
        )
        gene_df = gene_df.withColumnRenamed("genome_coordinate_end", "seq_region_end")
        gene_df = gene_df.withColumnRenamed("strand", "chr_strand")

        mgi_homologene_df = mgi_homologene_df.select(
            ["mgi_accession_id", "entrezgene_id", "ensembl_gene_id", "ccds_ids"]
        )
        gene_df = gene_df.join(mgi_homologene_df, "mgi_accession_id", "left_outer")
        gene_df = gene_df.withColumn(
            "ensembl_gene_id", functions.split(functions.col("ensembl_gene_id"), r"\|")
        )
        gene_df = gene_df.withColumn(
            "ccds_id", functions.split(functions.col("ccds_ids"), ",")
        )
        gene_df = gene_df.withColumn("ncbi_id", functions.col("entrezgene_id"))
        gene_df = gene_df.join(
            gene_production_status_df, "mgi_accession_id", "left_outer"
        )
        gene_df = gene_df.join(
            phenotyping_data_availability_df, "mgi_accession_id", "left_outer"
        )
        gene_df = gene_df.withColumn(
            "phenotype_status",
            when(
                col("phenotyping_status") == "Phenotyping finished",
                col("phenotyping_status"),
            )
            .when(
                col("phenotype_status") == "Phenotyping data available",
                col("phenotype_status"),
            )
            .otherwise(col("phenotyping_status")),
        )
        gene_df = gene_df.withColumn(
            "phenotyping_data_available",
            when(
                col("phenotyping_status").isin(
                    ["Phenotyping finished", "Phenotyping data available"]
                )
                | col("phenotype_status").isin(
                    ["Phenotyping finished", "Phenotyping data available"]
                ),
                lit(True),
            ).otherwise(lit(False)),
        )
        gene_df = gene_df.join(mgi_datasets_df, "mgi_accession_id", "left_outer")
        gene_df = gene_df.join(significant_mp_term, "mgi_accession_id", "left_outer")
        return gene_df

    def get_embryo_data(self, spark: SparkSession):
        r = requests.get("")
        df = spark.createDataFrame([json.loads(line) for line in r.iter_lines()])
        return df

    def _compress_and_encode(self, json_text):
        if json_text is None:
            return None
        else:
            return str(
                base64.b64encode(gzip.compress(bytes(json_text, "utf-8"))), "utf-8"
            )

    def _get_datasets_by_gene(
        self, stats_results_df, observations_df, ontology_metadata_df, compress=True
    ):
        significance_cols = [
            "female_ko_effect_p_value",
            "male_ko_effect_p_value",
            "genotype_effect_p_value",
            "male_pvalue_low_vs_normal_high",
            "male_pvalue_low_normal_vs_high",
            "female_pvalue_low_vs_normal_high",
            "female_pvalue_low_normal_vs_high",
            "genotype_pvalue_low_normal_vs_high",
            "genotype_pvalue_low_vs_normal_high",
            "male_ko_effect_p_value",
            "female_ko_effect_p_value",
            "p_value",
            "effect_size",
            "male_effect_size",
            "female_effect_size",
            "male_effect_size_low_vs_normal_high",
            "male_effect_size_low_normal_vs_high",
            "genotype_effect_size_low_vs_normal_high",
            "genotype_effect_size_low_normal_vs_high",
            "female_effect_size_low_vs_normal_high",
            "female_effect_size_low_normal_vs_high",
            "significant",
            "full_mp_term",
            "metadata_group",
            "male_mutant_count",
            "female_mutant_count",
            "statistical_method",
            "mp_term_id",
            "top_level_mp_term_id",
            "top_level_mp_term_name",
            "sex",
        ]

        data_set_cols = [
            "allele_accession_id",
            "allele_symbol",
            "gene_symbol",
            "gene_accession_id",
            "parameter_stable_id",
            "parameter_name",
            "procedure_stable_id",
            "procedure_name",
            "pipeline_name",
            "pipeline_stable_id",
            "zygosity",
            "phenotyping_center",
            "life_stage_name",
        ]

        stats_results_df = stats_results_df.select(*(data_set_cols + significance_cols))
        stats_results_df = stats_results_df.withColumn(
            "selected_p_value",
            functions.when(
                functions.col("statistical_method").isin(
                    ["Manual", "Supplied as data"]
                ),
                functions.col("p_value"),
            )
            .when(
                functions.col("statistical_method").contains("Reference Range Plus"),
                functions.when(
                    functions.col("sex") == "male",
                    functions.least(
                        functions.col("male_pvalue_low_vs_normal_high"),
                        functions.col("male_pvalue_low_normal_vs_high"),
                    ),
                )
                .when(
                    functions.col("sex") == "female",
                    functions.least(
                        functions.col("female_pvalue_low_vs_normal_high"),
                        functions.col("female_pvalue_low_normal_vs_high"),
                    ),
                )
                .otherwise(
                    functions.least(
                        functions.col("genotype_pvalue_low_normal_vs_high"),
                        functions.col("genotype_pvalue_low_vs_normal_high"),
                    )
                ),
            )
            .otherwise(
                functions.when(
                    functions.col("sex") == "male",
                    functions.col("male_ko_effect_p_value"),
                )
                .when(
                    functions.col("sex") == "female",
                    functions.col("female_ko_effect_p_value"),
                )
                .otherwise(
                    functions.when(
                        functions.col("statistical_method").contains(
                            "Fisher Exact Test framework"
                        ),
                        functions.col("p_value"),
                    ).otherwise(functions.col("genotype_effect_p_value"))
                )
            ),
        )
        stats_results_df = stats_results_df.withColumn(
            "selected_p_value", functions.col("selected_p_value").cast(DoubleType())
        )
        stats_results_df = stats_results_df.withColumn(
            "selected_effect_size",
            functions.when(
                functions.col("statistical_method").isin(
                    ["Manual", "Supplied as data"]
                ),
                functions.lit(1.0),
            )
            .when(
                ~functions.col("statistical_method").contains("Reference Range Plus"),
                functions.when(
                    functions.col("sex") == "male", functions.col("male_effect_size")
                )
                .when(
                    functions.col("sex") == "female",
                    functions.col("female_effect_size"),
                )
                .otherwise(functions.col("effect_size")),
            )
            .otherwise(
                functions.when(
                    functions.col("sex") == "male",
                    functions.when(
                        functions.col("male_effect_size_low_vs_normal_high")
                        <= functions.col("male_effect_size_low_normal_vs_high"),
                        functions.col("genotype_effect_size_low_vs_normal_high"),
                    ).otherwise(
                        functions.col("genotype_effect_size_low_normal_vs_high")
                    ),
                )
                .when(
                    functions.col("sex") == "female",
                    functions.when(
                        functions.col("female_effect_size_low_vs_normal_high")
                        <= functions.col("female_effect_size_low_normal_vs_high"),
                        functions.col("genotype_effect_size_low_vs_normal_high"),
                    ).otherwise(
                        functions.col("genotype_effect_size_low_normal_vs_high")
                    ),
                )
                .otherwise(functions.col("effect_size"))
            ),
        )
        stats_results_df = stats_results_df.withColumn(
            "selected_phenotype_term", functions.col("mp_term_id")
        )
        observations_df = observations_df.select(*data_set_cols).distinct()
        datasets_df = observations_df.join(
            stats_results_df, data_set_cols, "left_outer"
        )
        datasets_df = datasets_df.groupBy(data_set_cols).agg(
            functions.collect_set(
                functions.struct(
                    *[
                        "selected_p_value",
                        "selected_effect_size",
                        "selected_phenotype_term",
                        "metadata_group",
                        "male_mutant_count",
                        "female_mutant_count",
                        "significant",
                        "top_level_mp_term_id",
                        "top_level_mp_term_name",
                    ]
                )
            ).alias("stats_data")
        )
        datasets_df = datasets_df.withColumn(
            "successful_stats_data",
            functions.expr(
                "filter(stats_data, stat -> stat.selected_p_value IS NOT NULL)"
            ),
        )
        datasets_df = datasets_df.withColumn(
            "stats_data",
            functions.when(
                functions.size("successful_stats_data") > 0,
                functions.sort_array("successful_stats_data").getItem(0),
            ).otherwise(functions.sort_array("stats_data").getItem(0)),
        )
        datasets_df = datasets_df.select(*data_set_cols, "stats_data.*")
        datasets_df = datasets_df.withColumnRenamed("selected_p_value", "p_value")
        datasets_df = datasets_df.withColumnRenamed(
            "selected_effect_size", "effect_size"
        )
        datasets_df = datasets_df.withColumnRenamed(
            "selected_phenotype_term", "phenotype_term_id"
        )
        datasets_df = datasets_df.withColumnRenamed(
            "top_level_mp_term_id", "top_level_phenotype_term_id"
        )

        datasets_df = datasets_df.withColumn(
            "top_level_mp_term_name",
            array_except(
                col("top_level_mp_term_name"), array(lit(None).cast("string"))
            ),
        )

        datasets_df = datasets_df.withColumnRenamed(
            "top_level_mp_term_name", "top_level_phenotype_term_name"
        )
        datasets_df = datasets_df.join(
            ontology_metadata_df, "phenotype_term_id", "left_outer"
        )
        datasets_df = datasets_df.withColumn(
            "significance",
            functions.when(
                functions.col("significant") == True, functions.lit("Significant")
            )
            .when(
                functions.col("p_value").isNotNull(), functions.lit("Not significant")
            )
            .otherwise(functions.lit("N/A")),
        )
        mgi_datasets_df = datasets_df.groupBy("gene_accession_id").agg(
            functions.collect_set(
                functions.struct(
                    *(
                        data_set_cols
                        + [
                            "significance",
                            "p_value",
                            "effect_size",
                            "metadata_group",
                            "male_mutant_count",
                            "female_mutant_count",
                            "phenotype_term_id",
                            "phenotype_term_name",
                            "top_level_phenotype_term_id",
                            "top_level_phenotype_term_name",
                        ]
                    )
                )
            ).alias("datasets_raw_data")
        )

        mgi_datasets_df = mgi_datasets_df.withColumnRenamed(
            "gene_accession_id", "mgi_accession_id"
        )

        if compress:
            to_json_udf = functions.udf(
                lambda row: None
                if row is None
                else json.dumps(
                    [
                        {key: value for key, value in item.asDict().items()}
                        for item in row
                    ]
                ),
                StringType(),
            )
            mgi_datasets_df = mgi_datasets_df.withColumn(
                "datasets_raw_data", to_json_udf("datasets_raw_data")
            )
            compress_and_encode = functions.udf(self._compress_and_encode, StringType())
            mgi_datasets_df = mgi_datasets_df.withColumn(
                "datasets_raw_data", compress_and_encode("datasets_raw_data")
            )
        return mgi_datasets_df

    def _get_significance_fields_by_gene(self, stats_results_df):
        significant_mp_term = stats_results_df.select(
            "gene_accession_id", "top_level_mp_term_name", "significant"
        )

        significant_mp_term = significant_mp_term.withColumn(
            "top_level_mp_term_name", functions.explode("top_level_mp_term_name")
        )

        significant_mp_term = significant_mp_term.groupBy("gene_accession_id").agg(
            functions.collect_set(
                functions.when(
                    functions.col("significant") == True,
                    functions.col("top_level_mp_term_name"),
                ).otherwise(functions.lit(None))
            ).alias("significant_top_level_mp_terms"),
            functions.collect_set(
                functions.when(
                    functions.col("significant") == False,
                    functions.col("top_level_mp_term_name"),
                ).otherwise(functions.lit(None))
            ).alias("not_significant_top_level_mp_terms"),
        )
        significant_mp_term = significant_mp_term.withColumn(
            "not_significant_top_level_mp_terms",
            functions.array_except(
                "not_significant_top_level_mp_terms", "significant_top_level_mp_terms"
            ),
        )
        significant_mp_term = significant_mp_term.withColumnRenamed(
            "gene_accession_id", "mgi_accession_id"
        )
        return significant_mp_term
