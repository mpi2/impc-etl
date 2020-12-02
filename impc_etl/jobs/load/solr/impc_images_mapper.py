"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql.functions import (
    col,
    explode,
    concat_ws,
    collect_set,
    when,
    flatten,
    explode_outer,
    concat,
    lit,
)
from pyspark.sql import DataFrame, SparkSession
import sys


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    observations_parquet_path = argv[1]
    pipeline_core_parquet_path = argv[2]
    omero_ids_csv_path = argv[3]
    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    observations_df = spark.read.parquet(observations_parquet_path)
    pipeline_core_df = spark.read.parquet(pipeline_core_parquet_path)
    pipeline_core_df = pipeline_core_df.select(
        "fully_qualified_name",
        "mouse_anatomy_id",
        "mouse_anatomy_term",
        "embryo_anatomy_id",
        "embryo_anatomy_term",
        col("mp_id").alias("impress_mp_id"),
        col("mp_term").alias("impress_mp_term"),
        "top_level_mouse_anatomy_id",
        "top_level_mouse_anatomy_term",
        "top_level_embryo_anatomy_id",
        "top_level_embryo_anatomy_term",
        col("top_level_mp_id").alias("impress_top_level_mp_id"),
        col("top_level_mp_term").alias("impress_top_level_mp_term"),
        col("intermediate_mp_id").alias("impress_intermediate_mp_id"),
        col("intermediate_mp_term").alias("impress_intermediate_mp_term"),
    )
    omero_ids_df = spark.read.csv(omero_ids_csv_path, header=True)
    image_observations_df = observations_df.where(
        col("observation_type") == "image_record"
    )
    image_observations_df = image_observations_df.join(
        omero_ids_df,
        [
            "observation_id",
            "download_file_path",
            "phenotyping_center",
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_stable_id",
            "datasource_name",
        ],
    )
    parameter_association_fields = [
        "parameter_association_stable_id",
        "parameter_association_sequence_id",
        "parameter_association_name",
        "parameter_association_value",
    ]
    for parameter_association_field in parameter_association_fields:
        image_observations_df = image_observations_df.withColumn(
            f"{parameter_association_field}_exp",
            explode_outer(parameter_association_field),
        )
    image_observations_df = image_observations_df.withColumn(
        "fully_qualified_name",
        concat_ws(
            "_",
            "pipeline_stable_id",
            "procedure_stable_id",
            "parameter_association_stable_id_exp",
        ),
    )
    image_observations_df = image_observations_df.join(
        pipeline_core_df, "fully_qualified_name", "left_outer"
    )
    group_by_expressions = [
        collect_set(
            when(
                col("mouse_anatomy_id").isNotNull(), col("mouse_anatomy_id")
            ).otherwise(col("embryo_anatomy_id"))
        ).alias("anatomy_id"),
        collect_set(
            when(
                col("mouse_anatomy_term").isNotNull(), col("mouse_anatomy_term")
            ).otherwise(col("embryo_anatomy_term"))
        ).alias("anatomy_term"),
        flatten(
            collect_set(
                when(
                    col("mouse_anatomy_id").isNotNull(),
                    col("top_level_mouse_anatomy_id"),
                ).otherwise(col("top_level_embryo_anatomy_id"))
            )
        ).alias("selected_top_level_anatomy_id"),
        flatten(
            collect_set(
                when(
                    col("mouse_anatomy_id").isNotNull(),
                    col("top_level_mouse_anatomy_term"),
                ).otherwise(col("top_level_embryo_anatomy_term"))
            )
        ).alias("selected_top_level_anatomy_term"),
        collect_set("impress_mp_id").alias("mp_id"),
        collect_set("impress_mp_term").alias("mp_term"),
        flatten(collect_set("impress_top_level_mp_id")).alias("top_level_mp_id_set"),
        flatten(collect_set("impress_top_level_mp_term")).alias(
            "top_level_mp_term_set"
        ),
        flatten(collect_set("impress_intermediate_mp_id")).alias(
            "intermediate_mp_id_set"
        ),
        flatten(collect_set("impress_intermediate_mp_term")).alias(
            "intermediate_mp_term_set"
        ),
    ]
    group_by_expressions += [
        collect_set(f"{parameter_association_field}_exp").alias(
            parameter_association_field
        )
        for parameter_association_field in parameter_association_fields
    ]
    image_observations_df = image_observations_df.groupBy(
        [
            col_name
            for col_name in image_observations_df.columns
            if "parameter_association_" not in col_name
            and col_name != "fully_qualified_name"
        ]
    ).agg(*group_by_expressions)

    image_observations_df = image_observations_df.withColumn(
        "download_url",
        concat(
            lit("//www.ebi.ac.uk/mi/media/omero/webgateway/archived_files/download/"),
            col("omero_id"),
        ),
    )
    image_observations_df = image_observations_df.withColumn(
        "jpeg_url",
        concat(
            lit("//www.ebi.ac.uk/mi/media/omero/webgateway/render_image/"),
            col("omero_id"),
        ),
    )
    image_observations_df = image_observations_df.withColumn(
        "thumbnail_url",
        concat(
            lit("//www.ebi.ac.uk/mi/media/omero/webgateway/render_birds_eye_view/"),
            col("omero_id"),
        ),
    )
    image_observations_df.write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
