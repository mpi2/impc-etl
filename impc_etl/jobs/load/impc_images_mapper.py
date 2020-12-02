observations_df.where(
    (col("observation_type") == "image_record")
    & (col("download_file_path").contains("mousephenotype.org"))
    & (~col("download_file_path").like("%.mov"))
    & (~col("download_file_path").like("%.bz2"))
).select(
    "observation_id",
    "increment_value",
    "download_file_path",
    "phenotyping_center",
    "pipeline_stable_id",
    "procedure_stable_id",
    "datasource_name",
    "parameter_stable_id",
).repartition(
    1
).write.csv(
    "data/dr13.0/csv/impc_images_input/", header=True
)
