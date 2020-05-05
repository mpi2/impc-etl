import sys
from typing import Dict

from pyspark.sql import SparkSession

allele_mapper = {
    "acc": "allele_accession_id",
    "db_id": "",
    "gf_id": "",
    "gf_db_id": "",
    "biotype_acc": "",
    "symbol": "allele_symbol",
    "name": ""
}


def main(argv):
    allele2_parquet_path = argv[1]
    mgi_allele_parquet_path = argv[2]
    jdbc_connection_str = argv[3]
    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    allele2_df = spark.read.parquet(allele2_parquet_path)
    mgi_allele_df = spark.read.parquet(mgi_allele_parquet_path)
    komp2_allele_df = spark.read.jdbc(jdbc_connection_str, table="allele")
    komp2_allele_df.printSchema()
    # komp2_allele_df.write.mode("overwrite").parquet(output_path)


def load_table(table_name: str, mapper: Dict):


if __name__ == "__main__":
    sys.exit(main(sys.argv))
