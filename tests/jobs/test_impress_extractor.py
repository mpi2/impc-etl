import pytest
from impc_etl.jobs.extraction.impress_extractor import extract_impress
from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.usefixtures("spark_session")


#def test_extract_impress(spark_session: SparkSession):
#    extract_impress(spark_session, 'http://sandbox.mousephenotype.org/impress/', 'pipeline')


def test_jeremy_test(spark_session: SparkSession):
    experiment_file = "/Users/federico/data/*experiment*"
    experiments_df = spark_session.read.format("com.databricks.spark.xml") \
        .options(rowTag="centre").load(experiment_file, )
    experiments_df.show()
    experiments_df.printSchema()
    print(experiments_df.take(1)[0]['experiment'])