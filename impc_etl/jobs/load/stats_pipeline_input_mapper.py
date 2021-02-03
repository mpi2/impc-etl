from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit, when


def main(argv):
    observations_parquet_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    observations_df = spark.read.parquet(observations_parquet_path)
    flatten_observations_df = observations_df
    for d_type in observations_df.dtypes:
        if d_type[1] == "array<string>":
            flatten_observations_df = flatten_observations_df.withColumn(
                d_type[0], concat_ws("::", d_type[0])
            )
            flatten_observations_df = flatten_observations_df.withColumn(
                d_type[0],
                when(col(d_type[0]).isNotNull(), col(d_type[0])).otherwise(lit(None)),
            )
    flatten_observations_df.write.parquet(f"{output_path}_temp/")
    flatten_observations__temp = spark.read.parquet(f"{output_path}_temp/")
    flatten_observations__temp.write.parquet(output_path)
