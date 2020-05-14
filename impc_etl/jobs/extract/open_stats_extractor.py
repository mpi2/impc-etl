import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    jdbc_connection_str = argv[1]
    db_user = argv[2]
    db_password = argv[3]
    data_release_version = argv[4]
    output_path = argv[5]

    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }

    spark = SparkSession.builder.getOrCreate()
    stats_db_df = spark.read.jdbc(
        jdbc_connection_str, table=f'"{data_release_version}"', properties=properties
    )
    stats_db_df = stats_db_df.withColumnRenamed("statpacket", "json")
    stats_db_df.write.mode("overwrite").parquet(output_path)

    stats_df = spark.read.parquet(output_path)
    json_schema = spark.read.json(stats_df.rdd.map(lambda row: row.json)).schema
    stats_df = stats_df.withColumn("statpacket", from_json(col("json"), json_schema))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
