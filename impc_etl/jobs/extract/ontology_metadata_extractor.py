import os
import sys
from impc_etl.workflow.config import ImpcConfig
from io import BytesIO
from pyspark.sql import DataFrame, SparkSession
from impc_etl.config import OntologySchema, Constants
from typing import List, Dict, Iterable
from impc_etl.shared.utils import convert_to_row
from luigi.contrib.hdfs.hadoopcli_clients import HdfsClient
from luigi.contrib.hdfs.target import HdfsTarget


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    input_path = argv[1]
    output_path = argv[2]
    spark = SparkSession.builder.getOrCreate()
    ontology_df = spark.read.csv(f"{input_path}/*_metadata_table.csv", header=True)
    ontology_df.distinct().write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
