"""
IMITS loader module
    extract_imits: extracts the IMITS data into a set of Spark Dataframes
    extract_alleles: extracts the alleles in the Allele2 TSV file into a Spark Dataframe
    extract_genes: extracts the genes in the Allele2 TSV file into a Spark Dataframe
    extract_products: extracts the Product TSV file into a Spark Dataframe
"""
from pyspark.sql import DataFrame, SparkSession
from impc_etl.shared import utils


def extract_imits(spark_session: SparkSession) -> DataFrame:
    """
    Load IMITS data into a given Spark Context.
    :param spark_session: Spark Context to load the IMITS data
    :return spark_dataframe: Dataframe containing IMITS data
    """
    print(spark_session)


def extract_alleles(spark_session: SparkSession, file_path='.') -> DataFrame:
    """

    :param spark_session:
    :param file_path:
    :return allele_df:
    """
    allele2_df = utils.extract_tsv(spark_session, file_path)
    allele_df = allele2_df.where(allele2_df.type == 'Allele')
    return allele_df


def extract_products(spark_session: SparkSession, file_path='.') -> DataFrame:
    """

    :param spark_session:
    :param file_path:
    :return product_df:
    """
    product_df = utils.extract_tsv(spark_session, file_path).withColumn()
    return product_df


def extract_genes(spark_session: SparkSession, file_path='.') -> DataFrame:
    """

    :param spark_session:
    :param file_path:
    :return genes_df:
    """
    allele2_df = utils.extract_tsv(spark_session, file_path)
    genes_df = allele2_df.where(allele2_df.type == 'Gene')
    return genes_df
