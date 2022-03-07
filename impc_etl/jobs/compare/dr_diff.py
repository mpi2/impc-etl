from pyspark.sql.functions import concat_ws, col, lit, when, explode, collect_set, size

observations_df = spark.read.parquet("impc/dr16.0/output/observations_parquet")
observations_dr15_df = spark.read.parquet("impc/dr15.1/parquet/observations_parquet")
exp_df = spark.read.parquet(
    "impc/dr16.0/output/specimen_level_experiment_with_bw_age_parquet"
)
experiment_list = exp_df.select(
    col("_centreID").alias("phenotyping_center"),
    col("_pipeline").alias("pipeline_stable_id"),
    col("_procedureID").alias("procedure_stable_id"),
    col("_sequenceID").alias("procedure_sequence_id"),
    col("specimenID").alias("external_sample_id"),
    explode(col("procedureMetadata.parameterStatus")).alias("parameter_status"),
)
experiment_list = experiment_list.groupBy(
    [col_name for col_name in experiment_list.columns if col_name != "parameter_status"]
).agg(collect_set("parameter_status").alias("parameter_status"))
experiment_list = experiment_list.withColumn("has_status", size("parameter_status") > 0)
experiment_with_status_list = experiment_list.where(
    col("has_status") == True
).distinct()
experiments = (
    observations_df.where(col("datasource_name") == "IMPC")
    .select(
        "phenotyping_center",
        "pipeline_stable_id",
        "procedure_stable_id",
        "procedure_sequence_id",
        "external_sample_id",
    )
    .distinct()
)
experiments_dr15 = (
    observations_dr15_df.where(col("datasource_name") == "IMPC")
    .select(
        "phenotyping_center",
        "pipeline_stable_id",
        "procedure_stable_id",
        "procedure_sequence_id",
        "external_sample_id",
    )
    .distinct()
)
dr15_dr16_diff = experiments_dr15.subtract(experiments)
dr15_dr16_diff.count()
experiment_with_status_list = experiment_with_status_list.alias("exp")
dr15_dr16_diff = dr15_dr16_diff.alias("obs")
dr14_dr15_diff_out = (
    dr15_dr16_diff.join(
        experiment_with_status_list,
        (
            dr15_dr16_diff["phenotyping_center"]
            == experiment_with_status_list["phenotyping_center"]
        )
        & (
            dr15_dr16_diff["procedure_stable_id"]
            == experiment_with_status_list["procedure_stable_id"]
        )
        & (
            when(
                dr15_dr16_diff["procedure_sequence_id"].isNull()
                & experiment_with_status_list["procedure_sequence_id"].isNull(),
                lit(True),
            )
            .when(
                (
                    dr15_dr16_diff["procedure_sequence_id"].isNotNull()
                    & experiment_with_status_list["procedure_sequence_id"].isNull()
                )
                | (
                    dr15_dr16_diff["procedure_sequence_id"].isNull()
                    & experiment_with_status_list["procedure_sequence_id"].isNotNull()
                ),
                lit(False),
            )
            .otherwise(
                dr15_dr16_diff["procedure_sequence_id"]
                == experiment_with_status_list["procedure_sequence_id"]
            )
        )
        & (
            dr15_dr16_diff["external_sample_id"]
            == experiment_with_status_list["external_sample_id"]
        ),
        "left_outer",
    )
    .select(
        "obs.*",
        concat_ws(";", "exp.parameter_status").alias("parameter_statuses"),
        "exp.has_status",
    )
    .repartition(1)
    .write.csv("impc/dr16.0/output/dr15_dr16_round_2_diff", header=True)
)


# diff_vs_bora = dr15_dr16_diff.join(
#     bora_report,
#     (dr15_dr16_diff["phenotyping_center"] == bora_report["phenotyping_center"])
#     & (dr15_dr16_diff["procedure_stable_id"] == bora_report["procedure_stable_id"])
#     & (
#         when(
#             dr15_dr16_diff["procedure_sequence_id"].isNull()
#             & bora_report["procedure_sequence_id"].isNull(),
#             lit(True),
#         )
#         .when(
#             (
#                 dr15_dr16_diff["procedure_sequence_id"].isNotNull()
#                 & bora_report["procedure_sequence_id"].isNull()
#             )
#             | (
#                 dr15_dr16_diff["procedure_sequence_id"].isNull()
#                 & bora_report["procedure_sequence_id"].isNotNull()
#             ),
#             lit(False),
#         )
#         .otherwise(
#             dr15_dr16_diff["procedure_sequence_id"]
#             == bora_report["procedure_sequence_id"]
#         )
#     )
#     & (dr15_dr16_diff["external_sample_id"] == bora_report["external_sample_id"]),
#     "left_outer",
# )
#
# bora_vs_mice_vs_colony = bora_vs_mice_vs_colony.alias("bora")
#
# bora_vs_mice_vs_colony_vs_status = bora_vs_mice_vs_colony.join(
#     experiment_with_status_list,
#     (
#         experiment_with_status_list["phenotyping_center"]
#         == bora_vs_mice_vs_colony["phenotyping_center"]
#     )
#     & (
#         experiment_with_status_list["procedure_stable_id"]
#         == bora_vs_mice_vs_colony["procedure_stable_id"]
#     )
#     & (
#         when(
#             experiment_with_status_list["procedure_sequence_id"].isNull()
#             & bora_vs_mice_vs_colony["procedure_sequence_id"].isNull(),
#             lit(True),
#         )
#         .when(
#             (
#                 experiment_with_status_list["procedure_sequence_id"].isNotNull()
#                 & bora_vs_mice_vs_colony["procedure_sequence_id"].isNull()
#             )
#             | (
#                 experiment_with_status_list["procedure_sequence_id"].isNull()
#                 & bora_vs_mice_vs_colony["procedure_sequence_id"].isNotNull()
#             ),
#             lit(False),
#         )
#         .otherwise(
#             experiment_with_status_list["procedure_sequence_id"]
#             == bora_vs_mice_vs_colony["procedure_sequence_id"]
#         )
#     )
#     & (
#         experiment_with_status_list["external_sample_id"]
#         == bora_vs_mice_vs_colony["external_sample_id"]
#     ),
#     "left_outer",
# )
#
#
# exp_df.where(col("_procedureID") == "IMPC_INS_003").where(
#     col("specimenID") == "TCPR0357C-9"
# )
