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
    split,
    array,
)
from pyspark.sql.types import StringType

from impc_etl.shared import utils
from impc_etl.shared.exceptions import UnsupportedEntityError


ALLELE2_MULTIVALUED = [
    "allele_features",
    "without_allele_features",
    "production_centres",
    "phenotyping_centres",
    "ikmc_project",
    "pipeline",
    "latest_production_centre",
    "latest_phenotyping_centre",
    "genetic_map_links",
    "sequence_map_links",
    "gene_model_ids",
    "links",
]


PRODUCT_MULTIVALUED = [
    "genetic_info",
    "production_info",
    "qc_data",
    "order_names",
    "order_links",
    "contact_names",
    "contact_links",
    "other_links",
    "loa_assays",
    "allele_symbol",
]


def main(argv):
    """
    IMITS Extractor job runner
    :param list argv: the list elements should be:

    - [1]: Input TSV Path
    - [2]: Output Path
    - [3]: Entity type
    """
    input_path = argv[1]
    output_path = argv[2]
    entity_type = argv[3]
    spark = SparkSession.builder.getOrCreate()

    if entity_type in ["Gene", "Allele"]:
        imits_df = extract_imits_tsv_by_entity_type(spark, input_path, entity_type)
    elif entity_type == "allele2":
        imits_df = extract_imits_tsv_allele_2(spark, input_path)
    elif entity_type in ["Product", "Colony"]:
        imits_df = extract_imits_tsv(spark, input_path, entity_type)
    else:
        raise UnsupportedEntityError

    imits_df.write.mode("overwrite").parquet(output_path)


def extract_imits_tsv(spark_session: SparkSession, file_path, entity_type) -> DataFrame:
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
    if entity_type == "Product":
        for col_name in PRODUCT_MULTIVALUED:
            imits_df = imits_df.withColumn(
                col_name,
                when(
                    col(col_name).contains("|"),
                    split(col_name, r"\|"),
                )
                .when(col(col_name).isNull(), lit(None))
                .otherwise(array(col_name)),
            )
    return imits_df


def extract_imits_tsv_by_entity_type(
    spark_session: SparkSession, file_path: str, entity_type: str
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
    for col_name in ALLELE2_MULTIVALUED:
        imits_df = imits_df.withColumn(
            col_name,
            when(
                col(col_name).contains("|"),
                split(col_name, r"\|"),
            ).otherwise(array(col_name)),
        )
    return imtis_entity_df


def extract_imits_tsv_allele_2(
    spark_session: SparkSession, file_path: str
) -> DataFrame:
    imits_df = utils.extract_tsv(spark_session, file_path)
    imits_df = imits_df.withColumn(
        "allele_mgi_accession_id",
        when(
            (col("allele_mgi_accession_id").isNull()) & (col("type") == "Allele"),
            concat(lit("NOT-RELEASED-"), substring(md5(col("allele_symbol")), 0, 10)),
        ).otherwise(col("allele_mgi_accession_id")),
    )
    imits_df = imits_df.withColumn(
        "marker_mgi_accession_id",
        when(
            (col("marker_mgi_accession_id").isNull()) & (col("type") == "Gene"),
            concat(lit("NOT-RELEASED-"), substring(md5(col("marker_symbol")), 0, 10)),
        ).otherwise(col("marker_mgi_accession_id")),
    )
    imits_df = imits_df.withColumn(
        "allele2_id", monotonically_increasing_id().astype(StringType())
    )
    for col_name in ALLELE2_MULTIVALUED:
        imits_df = imits_df.withColumn(
            col_name,
            when(
                col(col_name).contains("|"),
                split(col_name, r"\|"),
            ).otherwise(array(col_name)),
        )
    return imits_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
