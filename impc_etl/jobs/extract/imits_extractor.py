"""
IMITS extractor module
    extract_imits_tsv
    extract_imits_tsv_by_entity_type
"""
from pyspark.sql import DataFrame, SparkSession
from impc_etl.shared import utils
import sys


def extract_imits_tsv(spark_session: SparkSession, file_path) -> DataFrame:
    imits_df = utils.extract_tsv(spark_session, file_path)
    imits_df = imits_df.toDF(
        *[column_name.replace(" ", "_").lower() for column_name in imits_df.columns]
    )
    return imits_df


def extract_imits_tsv_by_entity_type(
    spark_session: SparkSession, file_path, entity_type
) -> DataFrame:
    """

    :param spark_session:
    :param file_path:
    :param entity_type:
    :return:
    """
    imits_df = utils.extract_tsv(spark_session, file_path)
    imtis_entity_df = imits_df.where(imits_df.type == entity_type)
    return imtis_entity_df


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
        raise Exception

    imits_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
