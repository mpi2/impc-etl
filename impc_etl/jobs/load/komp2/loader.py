import sys
from typing import Dict
from impc_etl.jobs.load.komp2.mappers import MAPPERS
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    monotonically_increasing_id,
    when,
    concat,
    lit,
    substring,
    md5,
)


def main(argv):
    print(argv)
    allele2_parquet_path = argv[1]
    mgi_allele_parquet_path = argv[2]
    jdbc_connection_str = argv[3]
    db_user = argv[4]
    db_password = argv[5]
    output_path = argv[6]

    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "com.mysql.jdbc.Driver",
    }
    spark = SparkSession.builder.getOrCreate()
    allele2_df = spark.read.parquet(allele2_parquet_path).alias("allele2")
    allele2_df.printSchema()
    mgi_allele_df = spark.read.parquet(mgi_allele_parquet_path)
    komp2_allele_df = spark.read.jdbc(
        jdbc_connection_str, table="allele", properties=properties
    )
    komp2_ontology_term = (
        spark.read.jdbc(
            jdbc_connection_str, table="ontology_term", properties=properties
        )
        .withColumnRenamed("acc", "uri")
        .withColumnRenamed("name", "term")
        .alias("ontology")
    )
    komp2_allele_cols = komp2_allele_df.columns
    komp2_allele_df = allele2_df.join(
        komp2_allele_df,
        allele2_df["allele_symbol"] == komp2_allele_df["allele_symbol"],
        "left_outer",
    )
    komp2_allele_df = komp2_allele_df.join(
        komp2_ontology_term,
        col("allele2.mutation_type") == col("ontology.term"),
        "left_outer",
    )
    komp2_allele_df = komp2_allele_df.withColumn("biotype_acc", col("ontology.uri"))
    komp2_allele_df = komp2_allele_df.withColumn("biotype_db_id", col("ontology.db_id"))
    komp2_allele_df = komp2_allele_df.withColumn("symbol", col("allele2.allele_symbol"))
    komp2_allele_df = komp2_allele_df.withColumn("name", col("allele2.allele_name"))
    komp2_allele_df = komp2_allele_df.withColumn("db_id", monotonically_increasing_id())
    komp2_allele_df = komp2_allele_df.withColumn(
        "gf_acc", col("allele2.marker_mgi_accession_id")
    )
    komp2_allele_df.show()
    for column in komp2_ontology_term.columns + allele2_df.columns:
        komp2_allele_df = komp2_allele_df.drop(column)
    komp2_allele_df.show()
    komp2_allele_df.write.mode("overwrite").jdbc(
        jdbc_connection_str, table="allele", properties=properties
    )


def new_main(argv):
    task_name = argv.pop(0)
    table_name = argv.pop(0)
    jdbc_connection_str = argv.pop(0)
    db_user = argv.pop(0)
    db_password = argv.pop(0)

    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "com.mysql.jdbc.Driver",
    }

    spark = SparkSession.builder.getOrCreate()

    table_df = spark.read.jdbc(
        jdbc_connection_str, table=table_name, properties=properties
    )

    mapper = MAPPERS[table_name]
    input_columns = []
    renamed_columns = {}
    for i, data_source in enumerate(mapper["data_sources"]):
        if data_source["type"] == "parquet":
            data_source_df = spark.read.parquet(argv.pop(0))
        else:
            data_source_df = spark.read.jdbc(
                jdbc_connection_str, table=data_source["name"], properties=properties
            )

        for column_name in data_source_df.columns:
            if column_name in table_df.columns:
                data_source_df = data_source_df.withColumnRenamed(
                    column_name, f"{column_name}_{i}"
                )
                renamed_columns[data_source["name"] + "." + column_name] = (
                    data_source["name"] + "." + f"{column_name}_{i}"
                )

        data_source_df = data_source_df.alias(data_source["name"])
        input_columns += data_source_df.columns
        join_desc = data_source["join"]
        join_column_a = join_desc["column_a"]
        join_column_a = (
            renamed_columns[join_column_a]
            if join_column_a in renamed_columns.keys()
            else join_column_a
        )
        join_column_b = join_desc["column_b"]
        join_column_b = (
            renamed_columns[join_column_b]
            if join_column_b in renamed_columns.keys()
            else join_column_b
        )
        table_df = table_df.join(
            data_source_df,
            table_df[join_column_a] == data_source_df[join_column_b],
            join_desc["how"],
        )
    for mapping in mapper["mappings"]:
        column_in = mapping["input_column"]
        column_in = (
            renamed_columns[column_in]
            if column_in in renamed_columns.keys()
            else column_in
        )
        column_out = mapping["output_column"]
        table_df = table_df.withColumn(column_out, col(column_in))
    for column in input_columns:
        table_df = table_df.drop(column)
    table_df.show()


if __name__ == "__main__":
    sys.exit(new_main(sys.argv))
