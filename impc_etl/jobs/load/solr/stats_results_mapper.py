import base64
import gzip
import json
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
import sys

from pyspark.sql.functions import (
    array_contains,
    col,
    lit,
    split,
    collect_set,
    when,
    udf,
    expr,
    struct,
    lower,
    regexp_replace,
    size,
    array,
    regexp_extract,
    count,
    flatten,
    array_distinct,
    first,
    monotonically_increasing_id,
    from_json,
    explode,
    explode_outer,
    md5,
    arrays_zip,
    array_repeat,
    to_json,
    coalesce,
    concat,
    max,
    min,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
    Row,
)

from impc_etl.config import Constants
from impc_etl.shared.utils import convert_to_row

ONTOLOGY_STATS_MAP = {
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

PIPELINE_STATS_MAP = {
    "mp_term_id_options": "mp_id",
    "mp_term_name_options": "mp_term",
    "top_level_mp_id_options": "top_level_mp_id",
    "top_level_mp_term_options": "top_level_mp_term",
    "parameter_stable_key": "parameter_stable_key",
    "procedure_name": "procedure_name",
    "pipeline_stable_key": "pipeline_stable_key",
    "procedure_stable_key": "procedure_stable_key",
}

THREEI_STATS_MAP = {
    "colony_id": "Colony.Prefixes",
    "parameter_name": "Parameter.Name",
    "marker_symbol": "Gene",
    "procedure_name": "Procedure.Name",
    "procedure_stable_id": "Procedure.Id",
    "parameter_stable_id": "Parameter.Id",
    "classification_tag": "Call.Type",
    "mp_id": "Annotation.Calls",
    "allele_name": "Construct",
    "zygosity": "Genotype",
    "combine_sex_call": "Combine.Gender.Call",
    "sex": "Gender",
}


OBSERVATIONS_STATS_MAP = {
    "genetic_background": "genetic_background",
    "production_center": "production_center",
    "project_name": "project_name",
    "project_fullname": "project_name",
    "strain_name": "strain_name",
    "life_stage_name": "life_stage_name",
    "resource_name": "datasource_name",
    "resource_fullname": "datasource_name",
    "life_stage_acc": "life_stage_acc",
    "experiment_sex": "sex",
    "metadata": "metadata",
}

STATS_OBSERVATIONS_JOIN = [
    "procedure_group",
    "procedure_name",
    "parameter_stable_id",
    "phenotyping_center",
    "pipeline_stable_id",
    "colony_id",
    "metadata_group",
    "zygosity",
]

RAW_DATA_COLUMNS = [
    "observations_body_weight",
    "observations_date_of_experiment",
    "observations_external_sample_id",
    "observations_response",
    "observations_sex",
    "observations_data_points",
    "observations_categories",
    "observations_time_point",
    "observations_discrete_point",
]


ALLELE_STATS_MAP = {"allele_name": "allele_name"}

STATS_RESULTS_COLUMNS = [
    "doc_id",
    "additional_information",
    "allele_accession_id",
    "allele_name",
    "allele_symbol",
    "batch_significant",
    "both_mutant_count",
    "both_mutant_diversity_in_response",
    "both_mutant_mean",
    "both_mutant_sd",
    "both_mutant_unique_n",
    "classification_tag",
    "colony_id",
    "data_type",
    "effect_size",
    "female_control_count",
    "female_control_diversity_in_response",
    "female_control_mean",
    "female_control_sd",
    "female_control_unique_n",
    "female_ko_effect_p_value",
    "female_ko_effect_stderr_estimate",
    "female_ko_parameter_estimate",
    "female_mutant_count",
    "female_mutant_diversity_in_response",
    "female_mutant_mean",
    "female_mutant_sd",
    "female_mutant_unique_n",
    "female_percentage_change",
    "female_pvalue_low_normal_vs_high",
    "female_pvalue_low_vs_normal_high",
    "female_effect_size_low_normal_vs_high",
    "female_effect_size_low_vs_normal_high",
    "genotype_effect_p_value",
    "genotype_effect_parameter_estimate",
    "genotype_effect_size_low_normal_vs_high",
    "genotype_effect_size_low_vs_normal_high",
    "genotype_effect_stderr_estimate",
    "genotype_pvalue_low_normal_vs_high",
    "genotype_pvalue_low_vs_normal_high",
    "weight_effect_p_value",
    "weight_effect_stderr_estimate",
    "weight_effect_parameter_estimate",
    "group_1_genotype",
    "group_1_residuals_normality_test",
    "group_2_genotype",
    "group_2_residuals_normality_test",
    "interaction_effect_p_value",
    "interaction_significant",
    "intercept_estimate",
    "intercept_estimate_stderr_estimate",
    "male_control_count",
    "male_control_diversity_in_response",
    "male_control_mean",
    "male_control_sd",
    "male_control_unique_n",
    "male_ko_effect_p_value",
    "male_ko_effect_stderr_estimate",
    "male_ko_parameter_estimate",
    "male_mutant_count",
    "male_mutant_diversity_in_response",
    "male_mutant_mean",
    "male_mutant_sd",
    "male_mutant_unique_n",
    "male_percentage_change",
    "male_pvalue_low_normal_vs_high",
    "male_pvalue_low_vs_normal_high",
    "male_effect_size_low_normal_vs_high",
    "male_effect_size_low_vs_normal_high",
    "marker_accession_id",
    "marker_symbol",
    "metadata",
    "metadata_group",
    "percentage_change",
    "no_data_control_count",
    "no_data_control_diversity_in_response",
    "no_data_control_unique_n",
    "no_data_mutant_count",
    "no_data_mutant_diversity_in_response",
    "no_data_mutant_unique_n",
    "p_value",
    "parameter_name",
    "parameter_stable_id",
    "phenotype_sex",
    "phenotyping_center",
    "pipeline_name",
    "pipeline_stable_id",
    "procedure_group",
    "procedure_name",
    "procedure_stable_id",
    "procedure_stable_key",
    "sex_effect_p_value",
    "sex_effect_parameter_estimate",
    "sex_effect_stderr_estimate",
    "statistical_method",
    "status",
    "strain_accession_id",
    "variance_significant",
    "zygosity",
    "mpath_term_id",
    "mpath_term_name",
    "mp_term_id",
    "mp_term_name",
    "mp_term_event",
    "mp_term_sex",
    "top_level_mp_term_id",
    "top_level_mp_term_name",
    "intermediate_mp_term_id",
    "intermediate_mp_term_name",
    "mp_term_id_options",
    "mp_term_name_options",
    "anatomy_term_id",
    "anatomy_term_name",
    "anatomy_term_event",
    "anatomy_term_sex",
    "top_level_anatomy_term_id",
    "top_level_anatomy_term_name",
    "intermediate_anatomy_term_id",
    "intermediate_anatomy_term_name",
    "anatomy_term_id_options",
    "anatomy_term_name_options",
    "parameter_stable_key",
    "pipeline_stable_key",
    "genetic_background",
    "production_center",
    "project_name",
    "project_fullname",
    "resource_name",
    "resource_fullname",
    "strain_name",
    "life_stage_name",
    "life_stage_acc",
    "sex",
    "significant",
    "full_mp_term",
    "male_effect_size",
    "female_effect_size",
]


##TODO missing strain name and genetic background


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: Open stats parquet file
                    [2]: Observations parquet
                    [3]: Ontology parquet
                    [4]: Threei stats results file
                    [5]: Pipeline core parquet
                    [6]: Allele parquet
                    [7]: Output Path
    """
    open_stats_parquet_path = argv[1]
    observations_parquet_path = argv[2]
    ontology_parquet_path = argv[3]
    pipeline_parquet_path = argv[4]
    pipeline_core_parquet_path = argv[5]
    allele_parquet_path = argv[6]
    mp_chooser_path = argv[7]
    threei_parquet_path = argv[8]
    mpath_metadata_path = argv[9]
    raw_data_in_output = argv[10]
    output_path = argv[11]
    spark = SparkSession.builder.getOrCreate()
    open_stats_df = spark.read.parquet(open_stats_parquet_path)
    ontology_df = spark.read.parquet(ontology_parquet_path)
    allele_df = spark.read.parquet(allele_parquet_path)
    pipeline_df = spark.read.parquet(pipeline_parquet_path)
    pipeline_core_df = spark.read.parquet(pipeline_core_parquet_path)
    observations_df = spark.read.parquet(observations_parquet_path)
    threei_df = spark.read.csv(threei_parquet_path, header=True)
    threei_df = standardize_threei_schema(threei_df)
    mpath_metadata_df = spark.read.csv(mpath_metadata_path, header=True)
    mp_chooser_txt = spark.sparkContext.wholeTextFiles(mp_chooser_path).collect()[0][1]
    mp_chooser = json.loads(mp_chooser_txt)
    embryo_stat_packets = open_stats_df.where(
        (
            (col("procedure_stable_id").contains("IMPC_GPL"))
            | (col("procedure_stable_id").contains("IMPC_GEL"))
            | (col("procedure_stable_id").contains("IMPC_GPM"))
            | (col("procedure_stable_id").contains("IMPC_GEM"))
            | (col("procedure_stable_id").contains("IMPC_GPO"))
            | (col("procedure_stable_id").contains("IMPC_GEO"))
            | (col("procedure_stable_id").contains("IMPC_GPP"))
            | (col("procedure_stable_id").contains("IMPC_GEP"))
        )
    )

    open_stats_df = open_stats_df.where(
        ~(
            col("procedure_stable_id").contains("IMPC_FER_001")
            | (col("procedure_stable_id").contains("IMPC_VIA_001"))
            | (col("procedure_stable_id").contains("IMPC_VIA_002"))
            | (col("procedure_stable_id").contains("_PAT"))
            | (col("procedure_stable_id").contains("_EVL"))
            | (col("procedure_stable_id").contains("_EVM"))
            | (col("procedure_stable_id").contains("_EVO"))
            | (col("procedure_stable_id").contains("_EVP"))
            | (col("procedure_stable_id").contains("_ELZ"))
            | (col("procedure_name").startswith("Histopathology"))
            | (col("procedure_stable_id").contains("IMPC_GPL"))
            | (col("procedure_stable_id").contains("IMPC_GEL"))
            | (col("procedure_stable_id").contains("IMPC_GPM"))
            | (col("procedure_stable_id").contains("IMPC_GEM"))
            | (col("procedure_stable_id").contains("IMPC_GPO"))
            | (col("procedure_stable_id").contains("IMPC_GEO"))
            | (col("procedure_stable_id").contains("IMPC_GPP"))
            | (col("procedure_stable_id").contains("IMPC_GEP"))
        )
    )

    fertility_stats = _fertility_stats_results(observations_df, pipeline_df)

    for col_name in open_stats_df.columns:
        if col_name not in fertility_stats.columns:
            fertility_stats = fertility_stats.withColumn(col_name, lit(None))
    fertility_stats = fertility_stats.select(open_stats_df.columns)

    open_stats_df = open_stats_df.union(fertility_stats)

    viability_stats = _viability_stats_results(observations_df, pipeline_df)
    for col_name in open_stats_df.columns:
        if col_name not in viability_stats.columns:
            viability_stats = viability_stats.withColumn(col_name, lit(None))
    viability_stats = viability_stats.select(open_stats_df.columns)
    open_stats_df = open_stats_df.union(viability_stats)

    gross_pathology_stats = _gross_pathology_stats_results(observations_df)
    for col_name in open_stats_df.columns:
        if col_name not in gross_pathology_stats.columns:
            gross_pathology_stats = gross_pathology_stats.withColumn(
                col_name, lit(None)
            )
    gross_pathology_stats = gross_pathology_stats.select(open_stats_df.columns)
    open_stats_df = open_stats_df.union(gross_pathology_stats)

    histopathology_stats = _histopathology_stats_results(observations_df)
    for col_name in open_stats_df.columns:
        if col_name not in histopathology_stats.columns:
            histopathology_stats = histopathology_stats.withColumn(col_name, lit(None))
    histopathology_stats = histopathology_stats.select(open_stats_df.columns)
    open_stats_df = open_stats_df.union(histopathology_stats)

    embryo_viability_stats = _embryo_viability_stats_results(
        observations_df, pipeline_df
    )
    for col_name in open_stats_df.columns:
        if col_name not in embryo_viability_stats.columns:
            embryo_viability_stats = embryo_viability_stats.withColumn(
                col_name, lit(None)
            )
    embryo_viability_stats = embryo_viability_stats.select(open_stats_df.columns)
    open_stats_df = open_stats_df.union(embryo_viability_stats)

    embryo_stats = _embryo_stats_results(
        observations_df, pipeline_df, embryo_stat_packets
    )
    for col_name in open_stats_df.columns:
        if col_name not in embryo_stats.columns:
            embryo_stats = embryo_stats.withColumn(col_name, lit(None))
    embryo_stats = embryo_stats.select(open_stats_df.columns)
    open_stats_df = open_stats_df.union(embryo_stats)

    observations_metadata_df = observations_df.select(
        STATS_OBSERVATIONS_JOIN + list(set(OBSERVATIONS_STATS_MAP.values()))
    ).dropDuplicates()
    observations_metadata_df = observations_metadata_df.groupBy(
        *[
            col_name
            for col_name in observations_metadata_df.columns
            if col_name != "sex"
        ]
    ).agg(collect_set("sex").alias("sex"))

    aggregation_expresion = []

    for col_name in list(set(OBSERVATIONS_STATS_MAP.values())):
        if col_name not in ["datasource_name", "production_center"]:
            if col_name == "sex":
                aggregation_expresion.append(
                    array_distinct(flatten(collect_set(col_name))).alias(col_name)
                )
            elif col_name in ["strain_name", "genetic_background"]:
                aggregation_expresion.append(first(col(col_name)).alias(col_name))
            else:
                aggregation_expresion.append(collect_set(col_name).alias(col_name))

    observations_metadata_df = observations_metadata_df.groupBy(
        STATS_OBSERVATIONS_JOIN + ["datasource_name", "production_center"]
    ).agg(*aggregation_expresion)
    open_stats_df = map_to_stats(
        open_stats_df,
        observations_metadata_df,
        STATS_OBSERVATIONS_JOIN,
        OBSERVATIONS_STATS_MAP,
        "observation",
    )
    open_stats_df = open_stats_df.withColumn(
        "pipeline_stable_id",
        when(col("procedure_stable_id") == "ESLIM_022_001", lit("ESLIM_001")).otherwise(
            col("pipeline_stable_id")
        ),
    )
    open_stats_df = open_stats_df.withColumn(
        "procedure_stable_id",
        when(
            col("procedure_stable_id").contains("~"),
            split(col("procedure_stable_id"), "~"),
        ).otherwise(array(col("procedure_stable_id"))),
    )
    open_stats_df = open_stats_df.alias("stats")

    mp_ancestors_df = ontology_df.select(
        "id",
        struct("parent_ids", "intermediate_ids", "top_level_ids").alias("ancestors"),
    )
    mp_ancestors_df_1 = mp_ancestors_df.alias("mp_term_1")
    mp_ancestors_df_2 = mp_ancestors_df.alias("mp_term_2")
    open_stats_df = open_stats_df.join(
        mp_ancestors_df_1, (expr("mp_term[0].term_id") == "mp_term_1.id"), "left_outer"
    )

    open_stats_df = open_stats_df.join(
        mp_ancestors_df_2, (expr("mp_term[1].term_id") == "mp_term_2.id"), "left_outer"
    )

    mp_term_schema = ArrayType(
        StructType(
            [
                StructField("event", StringType(), True),
                StructField("otherPossibilities", StringType(), True),
                StructField("sex", StringType(), True),
                StructField("term_id", StringType(), True),
            ]
        )
    )
    select_collapsed_mp_term_udf = udf(
        lambda mp_term_array, pipeline, procedure_group, parameter, data_type, first_term_ancestors, second_term_ancestors: _select_collapsed_mp_term(
            mp_term_array,
            pipeline,
            procedure_group,
            parameter,
            mp_chooser,
            data_type,
            first_term_ancestors,
            second_term_ancestors,
        ),
        mp_term_schema,
    )
    open_stats_df = open_stats_df.withColumn(
        "collapsed_mp_term",
        when(
            expr(
                "exists(mp_term.sex, sex -> sex = 'male') AND exists(mp_term.sex, sex -> sex = 'female')"
            ),
            select_collapsed_mp_term_udf(
                "mp_term",
                "pipeline_stable_id",
                "procedure_group",
                "parameter_stable_id",
                "data_type",
                "mp_term_1.ancestors",
                "mp_term_2.ancestors",
            ),
        ).otherwise(col("mp_term")),
    )
    open_stats_df = open_stats_df.drop("mp_term_1.*", "mp_term_2.*")
    open_stats_df = open_stats_df.withColumn(
        "collapsed_mp_term", expr("collapsed_mp_term[0]")
    )
    open_stats_df = open_stats_df.withColumn("significant", lit(False))

    open_stats_df = open_stats_df.join(
        threei_df,
        [
            "resource_name",
            "colony_id",
            "marker_symbol",
            "procedure_stable_id",
            "parameter_stable_id",
            "zygosity",
        ],
        "left_outer",
    )
    open_stats_df = map_three_i(open_stats_df)
    open_stats_df = open_stats_df.withColumn(
        "collapsed_mp_term",
        when(
            col("threei_collapsed_mp_term").isNotNull(), col("threei_collapsed_mp_term")
        ).otherwise(col("collapsed_mp_term")),
    )

    open_stats_df = open_stats_df.drop("threei_collapsed_mp_term")

    open_stats_df = open_stats_df.withColumn(
        "mp_term_id", regexp_replace("collapsed_mp_term.term_id", " ", "")
    )
    for bad_mp in BAD_MP_MAP.keys():
        open_stats_df = open_stats_df.withColumn(
            "mp_term_id",
            when(col("mp_term_id") == bad_mp, lit(BAD_MP_MAP[bad_mp])).otherwise(
                col("mp_term_id")
            ),
        )
    open_stats_df = open_stats_df.withColumn(
        "mp_term_event", col("collapsed_mp_term.event")
    )
    open_stats_df = open_stats_df.withColumn(
        "mp_term_sex", col("collapsed_mp_term.sex")
    )
    open_stats_df = open_stats_df.withColumnRenamed("mp_term", "full_mp_term")
    open_stats_df = open_stats_df.withColumn(
        "full_mp_term",
        when(col("full_mp_term").isNull(), array(col("collapsed_mp_term"))).otherwise(
            col("full_mp_term")
        ),
    )

    stats_results_column_list = (
        STATS_RESULTS_COLUMNS
        if raw_data_in_output == "exclude"
        else STATS_RESULTS_COLUMNS + RAW_DATA_COLUMNS
    )

    for col_name in stats_results_column_list:
        if col_name not in open_stats_df.columns:
            open_stats_df = open_stats_df.withColumn(col_name, lit(None))

    ontology_df = ontology_df.withColumnRenamed("id", "mp_term_id")
    open_stats_df = map_to_stats(
        open_stats_df, ontology_df, ["mp_term_id"], ONTOLOGY_STATS_MAP, "ontology"
    )

    pipeline_core_join = [
        "parameter_stable_id",
        "pipeline_stable_id",
        "procedure_stable_id",
    ]
    pipeline_core_df = (
        pipeline_core_df.select(
            [
                col_name
                for col_name in pipeline_core_df.columns
                if col_name in pipeline_core_join
                or col_name in PIPELINE_STATS_MAP.values()
            ]
        )
        .groupBy(
            [
                "parameter_stable_id",
                "pipeline_stable_id",
                "procedure_stable_id",
                "pipeline_stable_key",
            ]
        )
        .agg(
            *[
                array_distinct(flatten(collect_set(col_name))).alias(col_name)
                if col_name
                in ["mp_id", "mp_term", "top_level_mp_id", "top_level_mp_term"]
                else collect_set(col_name).alias(col_name)
                for col_name in list(set(PIPELINE_STATS_MAP.values()))
                if col_name != "pipeline_stable_key"
            ]
        )
        .dropDuplicates()
    )
    pipeline_core_df = pipeline_core_df.withColumnRenamed(
        "procedure_stable_id", "proc_id"
    )
    pipeline_core_df = pipeline_core_df.withColumn(
        "procedure_stable_id", array(col("proc_id"))
    )
    open_stats_df = map_to_stats(
        open_stats_df,
        pipeline_core_df,
        pipeline_core_join,
        PIPELINE_STATS_MAP,
        "impress",
    )

    open_stats_df = open_stats_df.withColumn(
        "top_level_mp_term_id",
        when(
            col("top_level_mp_term_id").isNull(), col("top_level_mp_id_options")
        ).otherwise(col("top_level_mp_term_id")),
    )
    open_stats_df = open_stats_df.withColumn(
        "top_level_mp_term_name",
        when(
            col("top_level_mp_term_name").isNull(), col("top_level_mp_term_options")
        ).otherwise(col("top_level_mp_term_name")),
    )

    allele_df = allele_df.select(
        ["allele_symbol"] + list(ALLELE_STATS_MAP.values())
    ).dropDuplicates()

    open_stats_df = map_to_stats(
        open_stats_df, allele_df, ["allele_symbol"], ALLELE_STATS_MAP, "allele"
    )

    open_stats_df = open_stats_df.withColumn("sex", col("mp_term_sex"))
    open_stats_df = open_stats_df.withColumn(
        "phenotype_sex",
        when(col("phenotype_sex").isNull(), lit(None))
        .when(
            col("phenotype_sex").contains("Both sexes included"),
            array(lit("male"), lit("female")),
        )
        .otherwise(
            array(
                lower(
                    regexp_extract(
                        col("phenotype_sex"),
                        r"Only one sex included in the analysis; (.*)\[.*\]",
                        1,
                    )
                )
            )
        ),
    )
    open_stats_df = open_stats_df.withColumn(
        "phenotype_sex",
        when(
            col("phenotype_sex").isNull() & col("mp_term_sex").isNotNull(),
            when(
                col("mp_term_sex") == "not_considered",
                array(lit("male"), lit("female")),
            ).otherwise(array(col("mp_term_sex"))),
        ).otherwise(col("phenotype_sex")),
    )
    open_stats_df = open_stats_df.withColumn(
        "doc_id", monotonically_increasing_id().astype(StringType())
    )
    open_stats_df = open_stats_df.withColumn(
        "zygosity",
        when(col("zygosity") == "homozygous", lit("homozygote")).otherwise(
            col("zygosity")
        ),
    )

    open_stats_df = map_ontology_prefix(open_stats_df, "MA:", "anatomy_")
    open_stats_df = map_ontology_prefix(open_stats_df, "EMAP:", "anatomy_")
    open_stats_df = map_ontology_prefix(open_stats_df, "EMAPA:", "anatomy_")
    open_stats_df = open_stats_df.withColumn(
        "significant",
        when(col("mp_term_id").isNotNull(), lit(True)).otherwise(lit(False)),
    )
    open_stats_df = map_ontology_prefix(open_stats_df, "MPATH:", "mpath_")
    mpath_metadata_df = mpath_metadata_df.select(
        col("acc").alias("mpath_term_id"), col("name").alias("mpath_metadata_term_name")
    ).distinct()
    open_stats_df = open_stats_df.join(mpath_metadata_df, "mpath_term_id", "left_outer")
    open_stats_df = open_stats_df.withColumn(
        "mpath_term_name", col("mpath_metadata_term_name")
    )
    open_stats_df = open_stats_df.withColumn(
        "metadata",
        expr("transform(metadata, metadata_values -> concat_ws('|', metadata_values))"),
    )
    open_stats_df = open_stats_df.withColumn(
        "significant",
        when(col("data_type") == "time_series", lit(False)).otherwise(
            col("significant")
        ),
    )
    open_stats_df = open_stats_df.withColumn(
        "status",
        when(col("data_type") == "time_series", lit("NotProcessed")).otherwise(
            col("status")
        ),
    )
    if raw_data_in_output == "include":
        open_stats_df = _parse_raw_data(open_stats_df)
    open_stats_df = open_stats_df.withColumn(
        "data_type",
        when(
            col("procedure_group").rlike(
                "|".join(
                    [
                        "IMPC_GPL",
                        "IMPC_GEL",
                        "IMPC_GPM",
                        "IMPC_GEM",
                        "IMPC_GPO",
                        "IMPC_GEO",
                        "IMPC_GPP",
                        "IMPC_GEP",
                    ]
                )
            )
            & (col("data_type") == "categorical"),
            lit("embryo"),
        ).otherwise(col("data_type")),
    )
    stats_results_df = open_stats_df.select(*STATS_RESULTS_COLUMNS)
    stats_results_df.distinct().write.parquet(output_path)
    if raw_data_in_output == "include":
        raw_data_df = open_stats_df.select("doc_id", "raw_data")
        raw_data_df.write.parquet(output_path + "_raw_data")


def _compress_and_encode(json_text):
    if json_text is None:
        return None
    else:
        return str(base64.b64encode(gzip.compress(bytes(json_text, "utf-8"))), "utf-8")


def _parse_raw_data(open_stats_df):
    compress_and_encode = udf(_compress_and_encode, StringType())
    open_stats_df = open_stats_df.withColumnRenamed(
        "observations_biological_sample_group", "biological_sample_group"
    )
    open_stats_df = open_stats_df.withColumnRenamed(
        "observations_external_sample_id", "external_sample_id"
    )
    open_stats_df = open_stats_df.withColumnRenamed(
        "observations_date_of_experiment", "date_of_experiment"
    )
    open_stats_df = open_stats_df.withColumnRenamed("observations_sex", "specimen_sex")
    open_stats_df = open_stats_df.withColumnRenamed(
        "observations_body_weight", "body_weight"
    )
    open_stats_df = open_stats_df.withColumnRenamed(
        "observations_time_point", "time_point"
    )
    open_stats_df = open_stats_df.withColumnRenamed(
        "observations_discrete_point", "discrete_point"
    )
    for col_name in [
        "biological_sample_group",
        "date_of_experiment",
        "external_sample_id",
        "specimen_sex",
    ]:
        open_stats_df = open_stats_df.withColumn(
            col_name,
            when(
                (
                    col("data_type").isin(
                        ["unidimensional", "time_series", "categorical"]
                    )
                ),
                from_json(col(col_name), ArrayType(StringType(), True)),
            ).otherwise(lit(None)),
        )
    open_stats_df = open_stats_df.withColumn(
        "body_weight",
        when(
            (col("data_type").isin(["unidimensional", "time_series", "categorical"])),
            from_json(col("body_weight"), ArrayType(DoubleType(), True)),
        ).otherwise(expr("transform(external_sample_id, sample_id -> NULL)")),
    )
    open_stats_df = open_stats_df.withColumn(
        "data_point",
        when(
            (col("data_type").isin(["unidimensional", "time_series"]))
            & (col("observations_response").isNotNull()),
            from_json(col("observations_response"), ArrayType(DoubleType(), True)),
        ).otherwise(expr("transform(external_sample_id, sample_id -> NULL)")),
    )
    open_stats_df = open_stats_df.withColumn(
        "category",
        when(
            (col("data_type") == "categorical")
            & (col("observations_response").isNotNull()),
            from_json(col("observations_response"), ArrayType(StringType(), True)),
        ).otherwise(expr("transform(external_sample_id, sample_id -> NULL)")),
    )
    open_stats_df = open_stats_df.withColumn(
        "time_point",
        when(
            (col("data_type") == "time_series") & (col("time_point").isNotNull()),
            from_json(col("time_point"), ArrayType(StringType(), True)),
        ).otherwise(expr("transform(external_sample_id, sample_id -> NULL)")),
    )
    open_stats_df = open_stats_df.withColumn(
        "discrete_point",
        when(
            (col("data_type") == "time_series") & (col("discrete_point").isNotNull()),
            from_json(col("discrete_point"), ArrayType(DoubleType(), True)),
        ).otherwise(expr("transform(external_sample_id, sample_id -> NULL)")),
    )
    raw_data_cols = [
        "biological_sample_group",
        "date_of_experiment",
        "external_sample_id",
        "specimen_sex",
        "body_weight",
        "data_point",
        "category",
        "time_point",
        "discrete_point",
    ]
    open_stats_df = open_stats_df.withColumn("raw_data", arrays_zip(*raw_data_cols))

    to_json_udf = udf(
        lambda row: None
        if row is None
        else json.dumps(
            [
                {raw_data_cols[int(key)]: value for key, value in item.asDict().items()}
                for item in row
            ]
        ),
        StringType(),
    )
    open_stats_df = open_stats_df.withColumn("raw_data", to_json_udf("raw_data"))
    open_stats_df = open_stats_df.withColumn(
        "raw_data", compress_and_encode("raw_data")
    )
    return open_stats_df


def map_ontology_prefix(open_stats_df, term_prefix, field_prefix):
    mapped_columns = [
        col_name for col_name in STATS_RESULTS_COLUMNS if field_prefix in col_name
    ]
    for col_name in mapped_columns:
        mp_col_name = col_name.replace(field_prefix, "mp_")
        open_stats_df = open_stats_df.withColumn(
            col_name,
            when(
                col(col_name).isNull(),
                when(
                    col("mp_term_id").startswith(term_prefix), col(mp_col_name)
                ).otherwise(lit(None)),
            ).otherwise(col(col_name)),
        )
    mapped_id = field_prefix + "term_id"
    for col_name in mapped_columns:
        mp_col_name = col_name.replace(field_prefix, "mp_")
        open_stats_df = open_stats_df.withColumn(
            mp_col_name,
            when(col(mapped_id).isNotNull(), lit(None)).otherwise(col(mp_col_name)),
        )
    return open_stats_df


def map_to_stats(
    open_stats_df, metadata_df, join_columns, source_stats_map, source_name
):
    for col_name in metadata_df.columns:
        if col_name not in join_columns:
            metadata_df = metadata_df.withColumnRenamed(
                col_name, f"{source_name}_{col_name}"
            )
    if source_name == "observations":
        open_stats_df = open_stats_df.join(metadata_df, join_columns)
    else:
        open_stats_df = open_stats_df.join(metadata_df, join_columns, "left_outer")
    for column_name, source_column in source_stats_map.items():
        open_stats_df = open_stats_df.withColumn(
            column_name, col(f"{source_name}_{source_column}")
        )
    for source_column in source_stats_map.values():
        open_stats_df = open_stats_df.drop(f"{source_name}_{source_column}")
    return open_stats_df


def standardize_threei_schema(threei_df: DataFrame):
    for col_name, threei_column in THREEI_STATS_MAP.items():
        threei_df = threei_df.withColumnRenamed(threei_column, col_name)
    threei_df = threei_df.withColumn("resource_name", lit("3i"))
    threei_df = threei_df.withColumn(
        "procedure_stable_id", array(col("procedure_stable_id"))
    )
    threei_df = threei_df.withColumn(
        "sex",
        when(col("sex") == "both", lit("not_considered")).otherwise(lower(col("sex"))),
    )
    threei_df = threei_df.withColumn(
        "zygosity",
        when(col("zygosity") == "Hom", lit("homozygote"))
        .when(col("zygosity") == "Hemi", lit("hemizygote"))
        .otherwise(lit("heterozygote")),
    )
    threei_df = threei_df.withColumn("term_id", regexp_replace("mp_id", r"\[", ""))

    threei_df = threei_df.withColumn(
        "threei_collapsed_mp_term",
        when(
            (col("mp_id") != "NA") & (col("mp_id").isNotNull()),
            struct(
                lit(None).cast(StringType()).alias("event"),
                lit(None).cast(StringType()).alias("otherPossibilities"),
                "sex",
                col("term_id").alias("term_id"),
            ),
        ).otherwise(lit(None)),
    )
    threei_df = threei_df.withColumn(
        "threei_p_value",
        when(col("classification_tag") == "Significant", lit(0.0)).otherwise(lit(1.0)),
    )
    threei_df = threei_df.withColumn(
        "threei_genotype_effect_p_value",
        when(col("classification_tag") == "Significant", lit(0.0)).otherwise(lit(1.0)),
    )
    threei_df = threei_df.withColumn(
        "threei_genotype_effect_parameter_estimate",
        when(col("classification_tag") == "Significant", lit(1.0)).otherwise(lit(0.0)),
    )
    threei_df = threei_df.withColumn(
        "threei_significant",
        when(col("classification_tag") == "Significant", lit(True)).otherwise(
            lit(False)
        ),
    )
    threei_df = threei_df.withColumn(
        "threei_status",
        when(
            col("classification_tag").isin(["Significant", "Not Significant"]),
            lit("Successful"),
        ).otherwise(lit("NotProcessed")),
    )
    threei_df = threei_df.withColumn(
        "threei_statistical_method", lit("Supplied as data")
    )
    threei_df = threei_df.drop(
        "sex",
        "term_id",
        "mp_id",
        "parameter_name",
        "procedure_name",
        "combine_sex_call",
        "samples",
        "allele_name",
        "classification_tag",
    )
    return threei_df


def map_three_i(open_stats_df):
    open_stats_df = open_stats_df.withColumn(
        "genotype_effect_parameter_estimate",
        when(
            col("threei_genotype_effect_parameter_estimate").isNotNull(),
            col("threei_genotype_effect_parameter_estimate"),
        ).otherwise(col("genotype_effect_parameter_estimate")),
    )
    open_stats_df = open_stats_df.drop("threei_genotype_effect_parameter_estimate")

    open_stats_df = open_stats_df.withColumn(
        "significant",
        when(
            col("threei_significant").isNotNull(), col("threei_significant")
        ).otherwise(col("significant")),
    )
    open_stats_df = open_stats_df.drop("threei_significant")

    open_stats_df = open_stats_df.withColumn(
        "status",
        when(col("threei_status").isNotNull(), col("threei_status")).otherwise(
            col("status")
        ),
    )
    open_stats_df = open_stats_df.drop("threei_status")
    open_stats_df = open_stats_df.withColumn(
        "statistical_method",
        when(
            col("threei_statistical_method").isNotNull(),
            col("threei_statistical_method"),
        ).otherwise(col("statistical_method")),
    )
    open_stats_df = open_stats_df.drop("threei_statistical_method")

    open_stats_df = open_stats_df.withColumn(
        "p_value",
        when(col("threei_p_value").isNotNull(), col("threei_p_value")).otherwise(
            col("p_value")
        ),
    )
    open_stats_df = open_stats_df.drop("threei_p_value")

    open_stats_df = open_stats_df.withColumn(
        "genotype_effect_p_value",
        when(
            col("threei_genotype_effect_p_value").isNotNull(),
            col("threei_genotype_effect_p_value"),
        ).otherwise(col("genotype_effect_p_value")),
    )
    open_stats_df = open_stats_df.drop("threei_genotype_effect_p_value")
    return open_stats_df


def map_manual_annotations(observations_df: DataFrame):

    return observations_df


def _fertility_stats_results(observations_df: DataFrame, pipeline_df: DataFrame):
    fertility_condition = col("parameter_stable_id").isin(
        ["IMPC_FER_001_001", "IMPC_FER_019_001"]
    )

    mp_chooser = (
        pipeline_df.select(
            col("pipelineKey").alias("pipeline_stable_id"),
            col("procedure.procedureKey").alias("procedure_stable_id"),
            col("parameter.parameterKey").alias("parameter_stable_id"),
            col("parammpterm.optionText").alias("category"),
            col("termAcc"),
        )
        .withColumn("category", lower(col("category")))
        .distinct()
    )

    required_stats_columns = STATS_OBSERVATIONS_JOIN + [
        "sex",
        "procedure_stable_id",
        "pipeline_name",
        "category",
        "allele_accession_id",
        "parameter_name",
        "allele_symbol",
        "marker_accession_id",
        "marker_symbol",
        "strain_accession_id",
    ]
    fertility_stats_results = (
        observations_df.where(fertility_condition)
        .withColumnRenamed("gene_accession_id", "marker_accession_id")
        .withColumnRenamed("gene_symbol", "marker_symbol")
        .select(required_stats_columns)
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "category", lower(col("category"))
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "data_type", lit("line")
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "effect_size", lit(1.0)
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "statistical_method", lit("Supplied as data")
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "status", lit("Successful")
    )
    fertility_stats_results = fertility_stats_results.withColumn("p_value", lit(0.0))

    fertility_stats_results = fertility_stats_results.join(
        mp_chooser,
        [
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "category",
        ],
        "left_outer",
    )

    fertility_stats_results = fertility_stats_results.groupBy(
        required_stats_columns
        + ["data_type", "status", "effect_size", "statistical_method", "p_value"]
    ).agg(
        collect_set("category").alias("categories"),
        collect_set(
            struct(
                lit("ABNORMAL").cast(StringType()).alias("event"),
                lit(None).cast(StringType()).alias("otherPossibilities"),
                "sex",
                col("termAcc").alias("term_id"),
            )
        ).alias("mp_term"),
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "mp_term", expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)")
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "mp_term",
        when(size(col("mp_term.term_id")) == 0, lit(None)).otherwise(col("mp_term")),
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "p_value", when(col("mp_term").isNull(), lit(1.0)).otherwise(col("p_value"))
    )
    fertility_stats_results = fertility_stats_results.withColumn(
        "effect_size",
        when(col("mp_term").isNull(), lit(0.0)).otherwise(col("effect_size")),
    )
    return fertility_stats_results


def _embryo_stats_results(
    observations_df: DataFrame, pipeline_df: DataFrame, embryo_stats_packets: DataFrame
):

    mp_chooser = pipeline_df.select(
        "pipelineKey",
        "procedure.procedureKey",
        "parameter.parameterKey",
        "parammpterm.optionText",
        "parammpterm.selectionOutcome",
        "termAcc",
    ).distinct()

    mp_chooser = (
        mp_chooser.withColumnRenamed("pipelineKey", "pipeline_stable_id")
        .withColumnRenamed("procedureKey", "procedure_stable_id")
        .withColumnRenamed("parameterKey", "parameter_stable_id")
    )

    mp_chooser = mp_chooser.withColumn(
        "category",
        when(col("optionText").isNull(), col("selectionOutcome")).otherwise(
            col("optionText")
        ),
    )

    mp_chooser = mp_chooser.withColumn("category", lower(col("category")))
    mp_chooser = mp_chooser.drop("optionText", "selectionOutcome")

    required_stats_columns = STATS_OBSERVATIONS_JOIN + [
        "sex",
        "procedure_stable_id",
        "pipeline_name",
        "category",
        "allele_accession_id",
        "parameter_name",
        "allele_symbol",
        "marker_accession_id",
        "marker_symbol",
        "strain_accession_id",
        "text_value",
    ]
    embryo_stats_results = (
        observations_df.where(
            col("procedure_group").rlike(
                "|".join(
                    [
                        "IMPC_GPL",
                        "IMPC_GEL",
                        "IMPC_GPM",
                        "IMPC_GEM",
                        "IMPC_GPO",
                        "IMPC_GEO",
                        "IMPC_GPP",
                        "IMPC_GEP",
                    ]
                )
            )
            & (col("biological_sample_group") == "experimental")
            & (col("observation_type") == "categorical")
        )
        .withColumnRenamed("gene_accession_id", "marker_accession_id")
        .withColumnRenamed("gene_symbol", "marker_symbol")
        .select(required_stats_columns)
    )
    embryo_stats_results = embryo_stats_results.withColumn(
        "category", lower(col("category"))
    )

    embryo_stats_results = embryo_stats_results.withColumn(
        "data_type", lit("categorical")
    )
    embryo_stats_results = embryo_stats_results.withColumn("status", lit("Successful"))
    embryo_stats_results = embryo_stats_results.withColumn(
        "statistical_method", lit("Supplied as data")
    )

    embryo_stats_results = embryo_stats_results.join(
        mp_chooser,
        [
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "category",
        ],
        "left_outer",
    )

    group_by_cols = [
        col_name
        for col_name in required_stats_columns
        if col_name not in ["category", "sex"]
    ]
    group_by_cols += ["data_type", "status", "statistical_method"]

    embryo_stats_results = embryo_stats_results.groupBy(group_by_cols).agg(
        collect_set("sex").alias("sex"),
        collect_set("category").alias("categories"),
        collect_set(
            struct(
                lit("ABNORMAL").cast(StringType()).alias("event"),
                lit(None).cast(StringType()).alias("otherPossibilities"),
                col("sex"),
                col("termAcc").alias("term_id"),
            )
        ).alias("mp_term"),
    )

    embryo_stats_results = embryo_stats_results.withColumn(
        "mp_term", expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)")
    )
    embryo_stats_results = embryo_stats_results.withColumn(
        "mp_term",
        when(size(col("mp_term.term_id")) == 0, lit(None)).otherwise(col("mp_term")),
    )
    embryo_stats_results = embryo_stats_results.withColumn(
        "p_value", when(col("mp_term").isNull(), lit(1.0)).otherwise(lit(0.0))
    )
    embryo_stats_results = embryo_stats_results.withColumn(
        "effect_size", when(col("mp_term").isNull(), lit(0.0)).otherwise(lit(1.0))
    )
    embryo_stats_results = embryo_stats_results.groupBy(
        *[
            col_name
            for col_name in embryo_stats_results.columns
            if col_name not in ["mp_term", "p_value", "effect_size"]
        ]
    ).agg(
        flatten(collect_set("mp_term")).alias("mp_term"),
        min("p_value").alias("p_value"),
        max("effect_size").alias("effect_size"),
    )
    for col_name in embryo_stats_packets.columns:
        if (
            col_name in embryo_stats_results.columns
            and col_name not in STATS_OBSERVATIONS_JOIN
        ):
            embryo_stats_packets = embryo_stats_packets.drop(col_name)
    embryo_stats_results = embryo_stats_results.join(
        embryo_stats_packets, STATS_OBSERVATIONS_JOIN, "left_outer"
    )
    return embryo_stats_results


def _embryo_viability_stats_results(observations_df: DataFrame, pipeline_df: DataFrame):

    mp_chooser = (
        pipeline_df.select(
            "pipelineKey",
            "procedure.procedureKey",
            "parameter.parameterKey",
            "parammpterm.optionText",
            "termAcc",
        )
        .distinct()
        .withColumnRenamed("pipelineKey", "pipeline_stable_id")
        .withColumnRenamed("procedureKey", "procedure_stable_id")
        .withColumnRenamed("parameterKey", "parameter_stable_id")
        .withColumnRenamed("optionText", "category")
    ).withColumn("category", lower(col("category")))

    required_stats_columns = STATS_OBSERVATIONS_JOIN + [
        "sex",
        "procedure_stable_id",
        "pipeline_name",
        "category",
        "allele_accession_id",
        "parameter_name",
        "allele_symbol",
        "marker_accession_id",
        "marker_symbol",
        "strain_accession_id",
        "text_value",
    ]
    embryo_viability_stats_results = (
        observations_df.where(
            col("parameter_stable_id").isin(
                [
                    "IMPC_EVL_001_001",
                    "IMPC_EVM_001_001",
                    "IMPC_EVO_001_001",
                    "IMPC_EVP_001_001",
                ]
            )
        )
        .withColumnRenamed("gene_accession_id", "marker_accession_id")
        .withColumnRenamed("gene_symbol", "marker_symbol")
        .select(required_stats_columns)
    )
    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "category", lower(col("category"))
    )

    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "data_type", lit("embryo")
    )
    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "status", lit("Successful")
    )
    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "statistical_method", lit("Supplied as data")
    )

    embryo_viability_stats_results = embryo_viability_stats_results.join(
        mp_chooser,
        [
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "category",
        ],
        "left_outer",
    )

    embryo_viability_stats_results = embryo_viability_stats_results.groupBy(
        required_stats_columns + ["data_type", "status", "statistical_method"]
    ).agg(
        collect_set("category").alias("categories"),
        collect_set(
            struct(
                lit("ABNORMAL").cast(StringType()).alias("event"),
                lit(None).cast(StringType()).alias("otherPossibilities"),
                lit("not_considered").cast(StringType()).alias("sex"),
                col("termAcc").alias("term_id"),
            )
        ).alias("mp_term"),
    )

    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "mp_term", expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)")
    )
    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "mp_term",
        when(size(col("mp_term.term_id")) == 0, lit(None)).otherwise(col("mp_term")),
    )
    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "p_value", when(col("mp_term").isNull(), lit(1.0)).otherwise(lit(0.0))
    )
    embryo_viability_stats_results = embryo_viability_stats_results.withColumn(
        "effect_size", when(col("mp_term").isNull(), lit(0.0)).otherwise(lit(1.0))
    )

    return embryo_viability_stats_results


def _viability_stats_results(observations_df: DataFrame, pipeline_df: DataFrame):
    mp_chooser = (
        pipeline_df.select(
            "pipelineKey",
            "procedure.procedureKey",
            "parameter.parameterKey",
            "parammpterm.optionText",
            "termAcc",
        )
        .distinct()
        .withColumnRenamed("pipelineKey", "pipeline_stable_id")
        .withColumnRenamed("procedureKey", "procedure_stable_id")
        .withColumnRenamed("parameterKey", "parameter_stable_id")
        .withColumnRenamed("optionText", "category")
    ).withColumn("category", lower(col("category")))

    required_stats_columns = STATS_OBSERVATIONS_JOIN + [
        "sex",
        "procedure_stable_id",
        "pipeline_name",
        "category",
        "allele_accession_id",
        "parameter_name",
        "allele_symbol",
        "marker_accession_id",
        "marker_symbol",
        "strain_accession_id",
        "text_value",
    ]

    viability_stats_results = (
        observations_df.where(
            (
                (col("parameter_stable_id") == "IMPC_VIA_001_001")
                & (col("procedure_stable_id") == "IMPC_VIA_001")
            )
            | (
                (
                    col("parameter_stable_id").isin(
                        [
                            "IMPC_VIA_063_001",
                            "IMPC_VIA_064_001",
                            "IMPC_VIA_065_001",
                            "IMPC_VIA_066_001",
                            "IMPC_VIA_067_001",
                        ]
                    )
                )
                & (col("procedure_stable_id") == "IMPC_VIA_002")
            )
        )
        .withColumnRenamed("gene_accession_id", "marker_accession_id")
        .withColumnRenamed("gene_symbol", "marker_symbol")
        .select(required_stats_columns)
    )

    viability_stats_results = viability_stats_results.withColumn(
        "category", lower(col("category"))
    )

    json_outcome_schema = StructType(
        [
            StructField("outcome", StringType()),
            StructField("n", IntegerType()),
            StructField("P", DoubleType()),
        ]
    )

    viability_stats_results = viability_stats_results.withColumn(
        "viability_outcome",
        when(
            col("procedure_stable_id") == "IMPC_VIA_002",
            from_json(col("text_value"), json_outcome_schema),
        ).otherwise(lit(None)),
    )

    viability_p_values = observations_df.where(
        col("parameter_stable_id") == "IMPC_VIA_032_001"
    ).select("procedure_stable_id", "colony_id", col("data_point").alias("p_value"))

    viability_male_mutants = observations_df.where(
        col("parameter_stable_id") == "IMPC_VIA_010_001"
    ).select(
        "procedure_stable_id", "colony_id", col("data_point").alias("male_mutants")
    )

    viability_female_mutants = observations_df.where(
        col("parameter_stable_id") == "IMPC_VIA_014_001"
    ).select(
        "procedure_stable_id", "colony_id", col("data_point").alias("female_mutants")
    )

    viability_stats_results = viability_stats_results.withColumn(
        "data_type", lit("line")
    )
    viability_stats_results = viability_stats_results.withColumn(
        "effect_size", lit(1.0)
    )
    viability_stats_results = viability_stats_results.join(
        viability_p_values, ["colony_id", "procedure_stable_id"], "left_outer"
    )
    viability_stats_results = viability_stats_results.withColumn(
        "p_value",
        when(
            col("procedure_stable_id") == "IMPC_VIA_002", col("viability_outcome.P")
        ).otherwise(col("p_value")),
    )
    viability_stats_results = viability_stats_results.withColumn(
        "p_value",
        when(
            col("p_value").isNull() & ~col("category").contains("Viable"), lit(0.0)
        ).otherwise(col("p_value")),
    )
    viability_stats_results = viability_stats_results.withColumn(
        "male_controls", lit(None)
    )
    viability_stats_results = viability_stats_results.join(
        viability_male_mutants, ["colony_id", "procedure_stable_id"], "left_outer"
    )
    viability_stats_results = viability_stats_results.withColumn(
        "male_mutants",
        when(
            (col("procedure_stable_id") == "IMPC_VIA_002")
            & (col("parameter_name").contains(" males ")),
            col("viability_outcome.n"),
        ).otherwise(col("male_mutants")),
    )

    viability_stats_results = viability_stats_results.withColumn(
        "female_controls", lit(None)
    )
    viability_stats_results = viability_stats_results.join(
        viability_female_mutants, ["colony_id", "procedure_stable_id"], "left_outer"
    )

    viability_stats_results = viability_stats_results.withColumn(
        "female_mutants",
        when(
            (col("procedure_stable_id") == "IMPC_VIA_002")
            & (col("parameter_name").contains(" females ")),
            col("viability_outcome.n"),
        ).otherwise(col("female_mutants")),
    )

    viability_stats_results = viability_stats_results.withColumn(
        "statistical_method", lit("Supplied as data")
    )

    viability_stats_results = viability_stats_results.withColumn(
        "status", lit("Successful")
    )

    viability_stats_results = viability_stats_results.join(
        mp_chooser,
        [
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "category",
        ],
        "left_outer",
    )

    viability_stats_results = viability_stats_results.groupBy(
        required_stats_columns
        + [
            "data_type",
            "status",
            "effect_size",
            "statistical_method",
            "p_value",
            "male_mutants",
            "female_mutants",
            "viability_outcome",
        ]
    ).agg(
        collect_set("category").alias("categories"),
        collect_set(
            struct(
                lit("ABNORMAL").cast(StringType()).alias("event"),
                lit(None).cast(StringType()).alias("otherPossibilities"),
                lit("not_considered").cast(StringType()).alias("sex"),
                col("termAcc").alias("term_id"),
            )
        ).alias("mp_term"),
    )

    viability_stats_results = viability_stats_results.withColumn(
        "mp_term",
        when(
            (col("procedure_stable_id") == "IMPC_VIA_002"),
            array(
                struct(
                    lit("ABNORMAL").cast(StringType()).alias("event"),
                    lit(None).cast(StringType()).alias("otherPossibilities"),
                    when(col("parameter_name").contains(" males "), lit("male"))
                    .when(col("parameter_name").contains(" females "), lit("female"))
                    .otherwise(lit("not_considered"))
                    .cast(StringType())
                    .alias("sex"),
                    when(
                        col("viability_outcome.outcome").contains("subviable"),
                        lit("MP:0011110"),
                    )
                    .when(
                        col("viability_outcome.outcome").contains("lethal"),
                        lit("MP:0011100"),
                    )
                    .otherwise(lit(None).cast(StringType()))
                    .cast(StringType())
                    .alias("term_id"),
                )
            ),
        ).otherwise(col("mp_term")),
    )

    viability_stats_results = viability_stats_results.withColumn(
        "mp_term", expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)")
    )
    viability_stats_results = viability_stats_results.withColumn(
        "mp_term",
        when(size(col("mp_term.term_id")) == 0, lit(None)).otherwise(col("mp_term")),
    )
    viability_stats_results = viability_stats_results.withColumn(
        "p_value", when(col("mp_term").isNull(), lit(1.0)).otherwise(col("p_value"))
    )
    viability_stats_results = viability_stats_results.withColumn(
        "effect_size",
        when(col("mp_term").isNull(), lit(0.0)).otherwise(col("effect_size")),
    )
    return viability_stats_results


def _histopathology_stats_results(observations_df: DataFrame):
    histopathology_significance_scores = observations_df.where(
        col("parameter_name").endswith("Significance score")
    ).where(col("category") == "1")

    histopathology_significance_scores = histopathology_significance_scores.withColumn(
        "tissue_name", regexp_extract("parameter_name", "(.*)( - .*)", 1)
    )

    histopathology_stats_results = observations_df.where(
        expr("exists(sub_term_id, term -> term LIKE 'MPATH:%')")
        & ~expr("exists(sub_term_name, term -> term = 'normal')")
    )
    histopathology_stats_results = histopathology_stats_results.withColumn(
        "tissue_name", regexp_extract("parameter_name", "(.*)( - .*)", 1)
    )
    significance_stats_join = [
        "pipeline_stable_id",
        "procedure_stable_id",
        "specimen_id",
        "experiment_id",
        "tissue_name",
    ]
    histopathology_significance_scores = histopathology_significance_scores.select(
        significance_stats_join
    )
    histopathology_stats_results = histopathology_stats_results.join(
        histopathology_significance_scores, significance_stats_join
    )

    required_stats_columns = STATS_OBSERVATIONS_JOIN + [
        "sex",
        "procedure_stable_id",
        "pipeline_name",
        "allele_accession_id",
        "parameter_name",
        "allele_symbol",
        "marker_accession_id",
        "marker_symbol",
        "strain_accession_id",
        "sub_term_id",
        "sub_term_name",
        "specimen_id",
    ]

    histopathology_stats_results = (
        histopathology_stats_results.withColumnRenamed(
            "gene_accession_id", "marker_accession_id"
        )
        .withColumnRenamed("gene_symbol", "marker_symbol")
        .select(required_stats_columns)
    )

    histopathology_stats_results = histopathology_stats_results.withColumn(
        "sub_term_id", expr("filter(sub_term_id, mp -> mp LIKE 'MPATH:%')")
    )
    histopathology_stats_results = histopathology_stats_results.withColumn(
        "term_id", explode_outer("sub_term_id")
    )

    histopathology_stats_results = histopathology_stats_results.groupBy(
        *[
            col_name
            for col_name in required_stats_columns
            if col_name not in ["sex", "term_id"]
        ]
    ).agg(
        collect_set(
            struct(
                lit("ABNORMAL").cast(StringType()).alias("event"),
                lit(None).cast(StringType()).alias("otherPossibilities"),
                col("sex"),
                col("term_id"),
            )
        ).alias("mp_term")
    )
    histopathology_stats_results = histopathology_stats_results.withColumn(
        "mp_term", expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)")
    )
    histopathology_stats_results = histopathology_stats_results.withColumn(
        "mp_term",
        when(size(col("mp_term.term_id")) == 0, lit(None)).otherwise(col("mp_term")),
    )

    histopathology_stats_results = histopathology_stats_results.withColumn(
        "statistical_method", lit("Supplied as data")
    )
    histopathology_stats_results = histopathology_stats_results.withColumn(
        "status", lit("Successful")
    )
    histopathology_stats_results = histopathology_stats_results.withColumn(
        "p_value", when(col("mp_term").isNull(), lit(1.0)).otherwise(lit(0.0))
    )
    histopathology_stats_results = histopathology_stats_results.withColumn(
        "effect_size", when(col("mp_term").isNull(), lit(0.0)).otherwise(lit(1.0))
    )

    histopathology_stats_results = histopathology_stats_results.withColumn(
        "data_type", lit("histopathology")
    )

    return histopathology_stats_results


def _gross_pathology_stats_results(observations_df: DataFrame):
    gross_pathology_stats_results = observations_df.where(
        (col("biological_sample_group") != "control")
        & col("parameter_stable_id").like("%PAT%")
        & (expr("exists(sub_term_id, term -> term LIKE 'MP:%')"))
    )
    required_stats_columns = STATS_OBSERVATIONS_JOIN + [
        "sex",
        "procedure_stable_id",
        "pipeline_name",
        "category",
        "allele_accession_id",
        "parameter_name",
        "allele_symbol",
        "marker_accession_id",
        "marker_symbol",
        "strain_accession_id",
        "sub_term_id",
        "sub_term_name",
        "specimen_id",
    ]
    gross_pathology_stats_results = (
        gross_pathology_stats_results.withColumnRenamed(
            "gene_accession_id", "marker_accession_id"
        )
        .withColumnRenamed("gene_symbol", "marker_symbol")
        .select(required_stats_columns)
    )

    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "sub_term_id", expr("filter(sub_term_id, mp -> mp LIKE 'MP:%')")
    )
    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "term_id", explode_outer("sub_term_id")
    )

    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "term_id",
        when(
            expr(
                "exists(sub_term_name, term -> term = 'no abnormal phenotype detected')"
            )
            | expr("exists(sub_term_name, term -> term = 'normal')"),
            lit(None),
        ).otherwise(col("term_id")),
    )

    gross_pathology_stats_results = gross_pathology_stats_results.groupBy(
        *[
            col_name
            for col_name in required_stats_columns
            if col_name not in ["sex", "term_id"]
        ]
    ).agg(
        collect_set(
            struct(
                lit("ABNORMAL").cast(StringType()).alias("event"),
                lit(None).cast(StringType()).alias("otherPossibilities"),
                col("sex"),
                col("term_id"),
            )
        ).alias("mp_term")
    )
    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "mp_term", expr("filter(mp_term, mp -> mp.term_id IS NOT NULL)")
    )
    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "mp_term",
        when(size(col("mp_term.term_id")) == 0, lit(None)).otherwise(col("mp_term")),
    )

    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "statistical_method", lit("Supplied as data")
    )
    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "status", lit("Successful")
    )
    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "p_value", when(col("mp_term").isNull(), lit(1.0)).otherwise(lit(0.0))
    )
    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "effect_size", when(col("mp_term").isNull(), lit(0.0)).otherwise(lit(1.0))
    )
    gross_pathology_stats_results = gross_pathology_stats_results.withColumn(
        "data_type", lit("adult-gross-path")
    )
    return gross_pathology_stats_results


def _select_collapsed_mp_term(
    mp_term_array: List[Row],
    pipeline,
    procedure_group,
    parameter,
    mp_chooser,
    data_type,
    first_term_ancestors,
    second_term_ancestors,
):
    if mp_term_array is None or data_type == "time_series":
        return None
    mp_term = mp_term_array[0].asDict()
    mp_term["sex"] = "not_considered"
    mp_terms = [mp["term_id"] for mp in mp_term_array]
    try:
        if len(set(mp_terms)) > 1:
            print(mp_term_array)
            print(f"{pipeline} {procedure_group} {parameter}")
            mp_term["term_id"] = mp_chooser[pipeline][procedure_group][parameter][
                "UNSPECIFIED"
            ]["ABNORMAL"]["OVERALL"]["MPTERM"]
        else:
            mp_term["term_id"] = mp_term_array[0]["term_id"]
    except KeyError:
        ancestor_types = ["parent", "intermediate", "top_level"]
        closest_common_ancestor = None
        for ancestor_type in ancestor_types:
            ancestor_intersect = set(first_term_ancestors[ancestor_type]) & set(
                second_term_ancestors[ancestor_type]
            )
            if len(ancestor_intersect) > 0:
                closest_common_ancestor = ancestor_intersect[0]
                break
        if closest_common_ancestor is None:
            print(mp_term_array)
            print(f"{pipeline} {procedure_group} {parameter}")
            print("Unexpected error:", sys.exc_info()[0])
            raise Exception(
                str(mp_term_array)
                + f" | {pipeline} {procedure_group} {parameter} | "
                + f"[{str(first_term_ancestors)}] [{str(second_term_ancestors)}]"
                + str(sys.exc_info()[0])
            )
        else:
            mp_term["term_id"] = closest_common_ancestor
    return [convert_to_row(mp_term)]


def stop_and_count(df):
    print(df.count())
    raise ValueError


if __name__ == "__main__":
    sys.exit(main(sys.argv))
