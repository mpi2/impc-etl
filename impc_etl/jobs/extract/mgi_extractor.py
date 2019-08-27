from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from impc_etl.shared.utils import extract_tsv
import sys


def extract_mgi_strain_report(
    spark: SparkSession, strain_report_path: str
) -> DataFrame:
    schema = StructType(
        [
            StructField("mgiStrainID", StringType(), True),
            StructField("strainName", StringType(), True),
            StructField("strainType", StringType(), True),
        ]
    )
    strain_df = extract_tsv(spark, strain_report_path, schema=schema, header=False)
    return strain_df


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    strain_report_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    strain_df = extract_mgi_strain_report(spark, strain_report_path)
    strain_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
