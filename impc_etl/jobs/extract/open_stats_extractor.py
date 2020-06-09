import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col
import json
import re


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
    stats_df = spark.read.jdbc(
        jdbc_connection_str,
        table=f"""(
                    SELECT *, row_number() OVER () as rnum
                    FROM public."DR11WithMPTERMsV2") AS tmp""",
        properties=properties,
        numPartitions=5000,
        column="rnum",
        lowerBound=1,
        upperBound=2646323,
    )
    stats_df = stats_df.withColumnRenamed("statpacket", "json")
    json_df = spark.read.json(
        stats_df.rdd.map(
            lambda row: json.dumps(
                json.loads(row.json, object_pairs_hook=object_pairs_hook)
                # normalized_phenstat_fields(
                #     json.loads(row.json, object_pairs_hook=object_pairs_hook)
                # )
            )
        )
    )
    stats_df.write.mode("overwrite").parquet(output_path)


def object_pairs_hook(lit):
    return dict(
        [
            (re.sub(r"\{|\}|\(|\)", "|", re.sub(r"\s|,|;|\n\|\t|=", "_", key)), value)
            for (key, value) in lit
        ]
    )


# def normalized_phenstat_fields(statpacket):
#     if "phenlist_data_summary_statistics" in statpacket:
#         statpacket["phenlist_data_summary_statistics"] = [
#             {"key": key, "value": value}
#             for key, value in statpacket["phenlist_data_summary_statistics"].items()
#         ]
#     if "raw_data_summary_statistics" in statpacket:
#         statpacket["raw_data_summary_statistics"] = [
#             {"key": key, "value": value}
#             for key, value in statpacket["raw_data_summary_statistics"].items()
#         ]
#     return statpacket


if __name__ == "__main__":
    sys.exit(main(sys.argv))
