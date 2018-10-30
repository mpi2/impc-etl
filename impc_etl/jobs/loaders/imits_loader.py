"""
IMITS loader module
    load: loads the IMITS data into a set of Spark Dataframes
    load_allele2_file: loads the Allele2 TSV file into a Spark Dataframe
    load_product_file: loads the Product TSV file into a Spark Dataframe
"""
from pyspark.sql import DataFrame, SparkSession


def load(spark_session: SparkSession):
    """
    Load IMITS data into a given Spark Context.
    :param spark_session: Spark Context to load the IMITS data
    :return spark_dataframe: Dataframe containing IMITS data
    """
    print(spark_session)


def load_alleles(spark_session: SparkSession, file_path='.'):
    """

    :param spark_session:
    :param file_path:
    :return alleleDataframe:
    """
    allele2_df = _load_tsv(spark_session, file_path)
    allele_df = allele2_df.where(allele2_df.type == 'Allele')
    return allele_df


def load_products(spark_session: SparkSession, file_path='.'):
    """

    :param spark_session:
    :param file_path:
    :return productDataframe:
    """
    product_df = _load_tsv(spark_session, file_path).withColumn()
    return product_df


def load_genes(spark_session: SparkSession, file_path='.'):
    """

    :param spark_session:
    :param file_path:
    :return:
    """
    allele2_df = _load_tsv(spark_session, file_path)
    allele_df = allele2_df.where(allele2_df.type == 'Gene')
    return allele_df


def _load_tsv(spark_session, file_path) -> DataFrame:
    """

    :param spark_session:
    :param file_path:
    :return:
    """
    return spark_session.read.csv(file_path,
                                  header=True,
                                  mode='DROPMALFORMED',
                                  sep='\t').load(file_path)
