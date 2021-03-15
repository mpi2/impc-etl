import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, when, regexp_replace, col, lit, md5, concat
from impc_etl.shared import utils
from impc_etl import logger


def main(argv):
    input_path = argv[1]
    output_path = argv[2]
    spark_session = SparkSession.builder.getOrCreate()
    specimen_df = spark_session.read.parquet(input_path)
    specimen_clean_df = clean_specimens(specimen_df)
    specimen_clean_df = specimen_clean_df.dropDuplicates(["_specimenID", "_centreID"])
    specimen_clean_df.write.mode("overwrite").parquet(output_path)


def clean_specimens(specimen_df: DataFrame) -> DataFrame:
    """
    DCC specimen cleaner
    :return: a clean specimen parquet file
    :rtype: DataFrame
    """
    specimen_df: DataFrame = (
        specimen_df.transform(map_centre_ids)
        .transform(map_project_ids)
        .transform(map_production_centre_ids)
        .transform(map_phenotyping_centre_ids)
    )
    specimen_df = specimen_df.transform(truncate_europhenome_specimen_ids)
    specimen_df = specimen_df.transform(truncate_europhenome_colony_ids)
    specimen_df = specimen_df.transform(parse_europhenome_colony_xml_entities)
    specimen_df = specimen_df.transform(standardize_strain_ids)
    specimen_df = specimen_df.transform(override_3i_specimen_data)
    specimen_df = specimen_df.transform(generate_unique_id)
    return specimen_df.drop_duplicates(
        [col_name for col_name in specimen_df.columns if col_name != "_sourceFile"]
    )


def map_centre_ids(dcc_df: DataFrame) -> DataFrame:
    """
    Maps the center ids  found in the XML files to a standard list of ids e.g:

    :param dcc_df: DataFrame
    """
    dcc_df = dcc_df.withColumn(
        "_centreID", udf(utils.map_centre_id, StringType())("_centreID")
    )
    return dcc_df


def map_project_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "_project", udf(utils.map_project_id, StringType())("_project")
    )
    return dcc_df


def map_production_centre_ids(dcc_experiment_df: DataFrame):
    if "_productionCentre" not in dcc_experiment_df.columns:
        dcc_experiment_df = dcc_experiment_df.withColumn("_productionCentre", lit(None))
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "_productionCentre", udf(utils.map_centre_id, StringType())("_productionCentre")
    )
    return dcc_experiment_df


def map_phenotyping_centre_ids(dcc_experiment_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "_phenotypingCentre",
        udf(utils.map_centre_id, StringType())("_phenotypingCentre"),
    )
    return dcc_experiment_df


def truncate_europhenome_specimen_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "_specimenID",
        when(
            dcc_df["_dataSource"].isin(["europhenome", "MGP"]),
            udf(utils.truncate_specimen_id, StringType())(dcc_df["_specimenID"]),
        ).otherwise(dcc_df["_specimenID"]),
    )
    return dcc_df


def truncate_europhenome_colony_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "_colonyID",
        when(
            dcc_df["_dataSource"] == "europhenome",
            udf(utils.truncate_colony_id, StringType())(dcc_df["_colonyID"]),
        ).otherwise(dcc_df["_colonyID"]),
    )
    return dcc_df


def parse_europhenome_colony_xml_entities(dcc_df: DataFrame) -> DataFrame:
    """
    Some EuroPhenome Colony Ids have &lt; &gt; values that have to be replaced
    :param dcc_df:
    :return:
    """
    dcc_df = dcc_df.withColumn(
        "_colonyID",
        when(
            (dcc_df["_dataSource"] == "europhenome"),
            regexp_replace("_colonyID", "&lt;", "<"),
        ).otherwise(dcc_df["_colonyID"]),
    )

    dcc_df = dcc_df.withColumn(
        "_colonyID",
        when(
            (dcc_df["_dataSource"] == "europhenome"),
            regexp_replace("_colonyID", "&gt;", ">"),
        ).otherwise(dcc_df["_colonyID"]),
    )
    return dcc_df


def standardize_strain_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "_strainID", regexp_replace(dcc_df["_strainID"], "MGI:", "")
    )
    return dcc_df


def override_3i_specimen_data(dcc_specimen_df: DataFrame) -> DataFrame:
    dcc_specimen_df_a = dcc_specimen_df.alias("a")
    dcc_specimen_df_b = dcc_specimen_df.alias("b")
    dcc_specimen_df = dcc_specimen_df_a.join(
        dcc_specimen_df_b,
        (dcc_specimen_df_a["_specimenID"] == dcc_specimen_df_b["_specimenID"])
        & (dcc_specimen_df_a["_centreID"] == dcc_specimen_df_b["_centreID"])
        & (dcc_specimen_df_a["_pipeline"] == dcc_specimen_df_b["_pipeline"])
        & (dcc_specimen_df_a["_dataSource"] != dcc_specimen_df_b["_dataSource"]),
        "left_outer",
    )
    dcc_specimen_df = dcc_specimen_df.where(
        col("b._specimenID").isNull()
        | ((col("b._specimenID").isNotNull()) & (col("a._dataSource") != "3i"))
    )
    return dcc_specimen_df.select("a.*")


def generate_unique_id(dcc_specimen_df: DataFrame) -> DataFrame:
    unique_columns = ["_productionCentre", "_specimenID", "_pipeline"]
    unique_columns = [
        col_name for col_name in dcc_specimen_df.columns if col_name in unique_columns
    ]
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "unique_id", md5(concat(*unique_columns))
    )
    return dcc_specimen_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
