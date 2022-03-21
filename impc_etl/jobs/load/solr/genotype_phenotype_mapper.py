"""
    Module to hold the jobs to map the Statistical Results parquet to the Genotype Phenotype parquet.

    It takes in the statistical results parquet file and the ontology hierarchy parquet file
    and returns the Genotype Phenotype parquet.
"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode_outer,
    when,
    lit,
    least,
    monotonically_increasing_id,
    regexp_replace,
)
from pyspark.sql.types import StringType

from impc_etl.jobs.extract.ontology_hierarchy_extractor import (
    OntologyTermHierarchyExtractor,
)
from impc_etl.jobs.load.solr.stats_results_mapper import StatsResultsMapper
from impc_etl.workflow.config import ImpcConfig

ONTOLOGY_STATS_MAP = {
    "mp_term_id": "id",
    "mp_term_name": "term",
    "top_level_mp_term_id": "top_level_ids",
    "top_level_mp_term_name": "top_level_terms",
    "intermediate_mp_term_id": "intermediate_ids",
    "intermediate_mp_term_name": "intermediate_terms",
}

BAD_MP_MAP = {
    '["MP:0000592","MP:0000592"]': "MP:0000592",
    '["MP:0003956","MP:0003956"]': "MP:0003956",
    '["MP:0000589","MP:0000589"]': "MP:0000589",
    '["MP:0010101","MP:0004649"]': "MP:0004649",
    '["MP:0004650","MP:0004647"]': "MP:0004650",
}

GENOTYPE_PHENOTYPE_COLUMNS = [
    "mpath_term_id",
    "mpath_term_name",
    "marker_symbol",
    "marker_accession_id",
    "colony_id",
    "allele_name",
    "allele_symbol",
    "allele_accession_id",
    "strain_name",
    "strain_accession_id",
    "phenotyping_center",
    "project_name",
    "resource_name",
    "zygosity",
    "pipeline_name",
    "pipeline_stable_id",
    "pipeline_stable_key",
    "procedure_name",
    "procedure_stable_id",
    "procedure_stable_key",
    "parameter_name",
    "parameter_stable_id",
    "parameter_stable_key",
    "percentage_change",
    "effect_size",
    "life_stage_acc",
    "life_stage_name",
    "sex",
    "p_value",
    "statistical_method",
]

STATS_RESULTS_COLUMNS = [
    "full_mp_term",
    "mpath_term_id",
    "mpath_term_name",
    "genotype_effect_p_value",
    "female_pvalue_low_vs_normal_high",
    "female_pvalue_low_normal_vs_high",
    "female_effect_size_low_vs_normal_high",
    "female_effect_size_low_normal_vs_high",
    "male_pvalue_low_vs_normal_high",
    "male_pvalue_low_normal_vs_high",
    "male_effect_size_low_vs_normal_high",
    "male_effect_size_low_normal_vs_high",
    "female_ko_effect_p_value",
    "male_ko_effect_p_value",
    "male_percentage_change",
    "female_percentage_change",
    "male_effect_size",
    "female_effect_size",
    "genotype_effect_size_low_vs_normal_high",
    "genotype_effect_size_low_normal_vs_high",
    "genotype_pvalue_low_vs_normal_high",
    "genotype_pvalue_low_normal_vs_high",
]


class GenotypePhenotypeMapper(PySparkTask):
    #: Name of the Spark task
    name = "IMPC_Genotype_Phenotype_Mapper"

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def requires(self):
        return [StatsResultsMapper(), OntologyTermHierarchyExtractor()]

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/output/genotype_phenotype_parquet)
        """
        return ImpcConfig().get_target(f"{self.output_path}genotype_phenotype_parquet")

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Takes in the stats results parquet, joins it with the ontology data and returns the significant genotype phenotype associations.
        """
        stats_results_parquet_path = args[0]
        ontology_parquet_path = args[1]
        output_path = args[2]

        spark = SparkSession.builder.getOrCreate()
        stats_results_df = spark.read.parquet(stats_results_parquet_path)
        ontology_df = spark.read.parquet(ontology_parquet_path)

        genotype_phenotype_df = stats_results_df.where(
            col("significant") == True
        ).select(GENOTYPE_PHENOTYPE_COLUMNS + STATS_RESULTS_COLUMNS)
        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "mp_term", explode_outer("full_mp_term")
        )
        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "sex", col("mp_term.sex")
        )
        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "mp_term_id", col("mp_term.term_id")
        )
        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "mp_term_id", regexp_replace("mp_term_id", " ", "")
        )
        for bad_mp in BAD_MP_MAP.keys():
            genotype_phenotype_df = genotype_phenotype_df.withColumn(
                "mp_term_id",
                when(col("mp_term_id") == bad_mp, lit(BAD_MP_MAP[bad_mp])).otherwise(
                    col("mp_term_id")
                ),
            )

        genotype_phenotype_df = genotype_phenotype_df.join(
            ontology_df, col("mp_term_id") == col("id"), "left_outer"
        )

        for column_name, ontology_column in ONTOLOGY_STATS_MAP.items():
            genotype_phenotype_df = genotype_phenotype_df.withColumn(
                f"{column_name}", col(ontology_column)
            )

        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "p_value",
            when(
                col("statistical_method").isin(["Manual", "Supplied as data"]),
                col("p_value"),
            )
            .when(
                col("statistical_method").contains("Reference Range Plus"),
                when(
                    col("sex") == "male",
                    least(
                        col("male_pvalue_low_vs_normal_high"),
                        col("male_pvalue_low_normal_vs_high"),
                    ),
                )
                .when(
                    col("sex") == "female",
                    least(
                        col("female_pvalue_low_vs_normal_high"),
                        col("female_pvalue_low_normal_vs_high"),
                    ),
                )
                .otherwise(
                    least(
                        "genotype_pvalue_low_normal_vs_high",
                        "genotype_pvalue_low_vs_normal_high",
                    )
                ),
            )
            .otherwise(
                when(col("sex") == "male", col("male_ko_effect_p_value"))
                .when(col("sex") == "female", col("female_ko_effect_p_value"))
                .otherwise(
                    when(
                        col("statistical_method").contains(
                            "Fisher Exact Test framework"
                        ),
                        col("p_value"),
                    ).otherwise(col("genotype_effect_p_value"))
                )
            ),
        )

        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "effect_size",
            when(
                col("statistical_method").isin(["Manual", "Supplied as data"]), lit(1.0)
            )
            .when(
                ~col("statistical_method").contains("Reference Range Plus"),
                when(col("sex") == "male", col("male_effect_size"))
                .when(col("sex") == "female", col("female_effect_size"))
                .otherwise(col("effect_size")),
            )
            .otherwise(
                when(
                    col("sex") == "male",
                    when(
                        col("male_effect_size_low_vs_normal_high")
                        <= col("male_effect_size_low_normal_vs_high"),
                        col("genotype_effect_size_low_vs_normal_high"),
                    ).otherwise(col("genotype_effect_size_low_normal_vs_high")),
                )
                .when(
                    col("sex") == "female",
                    when(
                        col("female_effect_size_low_vs_normal_high")
                        <= col("female_effect_size_low_normal_vs_high"),
                        col("genotype_effect_size_low_vs_normal_high"),
                    ).otherwise(col("genotype_effect_size_low_normal_vs_high")),
                )
                .otherwise(col("effect_size"))
            ),
        )

        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "percentage_change",
            when(col("sex") == "male", col("male_percentage_change"))
            .when(col("sex") == "female", col("female_percentage_change"))
            .otherwise(col("percentage_change")),
        )

        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "assertion_type_id",
            when(
                col("statistical_method").isin(["Manual", "Supplied as data"]),
                lit("ECO:0000218"),
            ).otherwise(lit("ECO:0000203")),
        )

        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "assertion_type",
            when(
                col("statistical_method").isin(["Manual", "Supplied as data"]),
                lit("manual"),
            ).otherwise(lit("automatic")),
        )

        genotype_phenotype_df = genotype_phenotype_df.select(
            GENOTYPE_PHENOTYPE_COLUMNS
            + list(ONTOLOGY_STATS_MAP.keys())
            + ["assertion_type_id", "assertion_type"]
        )
        genotype_phenotype_df = genotype_phenotype_df.withColumn(
            "doc_id", monotonically_increasing_id().astype(StringType())
        )
        ontology_field_prefixes = ["mpath_", "anatomy_"]
        for prefix in ontology_field_prefixes:
            for col_name in genotype_phenotype_df.columns:
                if prefix in col_name:
                    genotype_phenotype_df = genotype_phenotype_df.withColumn(
                        col_name,
                        when(
                            col(col_name).isNotNull(),
                            col(col_name.replace(prefix, "mp_")),
                        ).otherwise(col(col_name)),
                    )
        genotype_phenotype_df.distinct().write.parquet(output_path)
