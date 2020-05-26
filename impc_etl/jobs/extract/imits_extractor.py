"""
IMITS extractor module
    This module takes care of extracting data from CSV IMITS files to  Spark DataFrames.
    There are three kind of IMITS files: colonies, products and alleles files representing 4 different data
    entities, genes and alleles in the alleles report files; products in the product report files
    and colonies in the colony report files.
"""
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    when,
    col,
    concat,
    lit,
    substring,
    md5,
    monotonically_increasing_id,
)
from pyspark.sql.types import StringType

from impc_etl.shared import utils
from impc_etl.shared.exceptions import UnsupportedEntityError


def main(argv):
    """
    IMITS Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input TSV Path
                    [2]: Output Path
                    [3]: Entity type
    """
    input_path = argv[1]
    output_path = argv[2]
    entity_type = argv[3]
    spark = SparkSession.builder.getOrCreate()

    if entity_type in ["Gene", "Allele"]:
        imits_df = extract_imits_tsv_by_entity_type(spark, input_path, entity_type)
    elif entity_type in ["Product", "Colony"]:
        imits_df = extract_imits_tsv(spark, input_path)
    else:
        raise UnsupportedEntityError

    imits_df.write.mode("overwrite").parquet(output_path)


def extract_imits_tsv(spark_session: SparkSession, file_path) -> DataFrame:
    """
    Uses a Spark session to generate a DataFrame from a TSV file. Can extract a Colonies file or a Products file.
    :param spark_session: spark SQL session to be used in the extraction
    :param file_path: path to the TSV file
    :return: Spark DataFrame with the extracted data
    """
    imits_df = utils.extract_tsv(spark_session, file_path)
    imits_df = imits_df.toDF(
        *[column_name.replace(" ", "_").lower() for column_name in imits_df.columns]
    )
    return imits_df


def extract_imits_tsv_by_entity_type(
    spark_session: SparkSession, file_path, entity_type
) -> DataFrame:
    """
    Uses a Spark Session to generate a DataFrame from a TSV file and a specific entity type.
    Can extract Genes or Alleles from a Alleles report file produced by IMITS.
    :param spark_session: spark SQL session to be used in the extraction
    :param file_path: path to the TSV file
    :param entity_type: 'Allele' or 'Gene'
    :return: Spark DataFrame with the extracted data
    """
    imits_df = utils.extract_tsv(spark_session, file_path)
    imtis_entity_df = imits_df.where(imits_df.type == entity_type)
    if entity_type == "Allele":
        imtis_entity_df = imtis_entity_df.withColumn(
            "acc",
            when(
                col("allele_mgi_accession_id").isNull(),
                concat(
                    lit("NOT-RELEASED-"), substring(md5(col("allele_symbol")), 0, 10)
                ),
            ).otherwise(col("allele_mgi_accession_id")),
        )
        imtis_entity_df = imtis_entity_df.withColumn(
            "allele2_id", monotonically_increasing_id().astype(StringType())
        )
    return imtis_entity_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
