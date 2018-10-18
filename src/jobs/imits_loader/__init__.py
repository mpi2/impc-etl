"""
IMITS loade module
    load: loads the IMITS data into a Spark Dataframe
"""


def load(spark_context):
    """
    Load IMITS data into a given Spark Context.
    :param spark_context: Spark Context to load the IMITS data
    :return spark_dataframe: Dataframe containing IMITS data
    """
    print(spark_context)
