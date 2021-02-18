import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    udf,
    when,
    lit,
    md5,
    concat,
    col,
    regexp_replace,
    regexp_extract,
)
from pyspark.sql.types import BooleanType, StringType
from impc_etl.config.constants import Constants
from impc_etl.shared import utils


def main(argv):
    """
    Takes in an parquet file from DCC experiment XML files and generates a clean version of it
    :param argv: [1] input_path, [2] entity_type,  [3] output_path
    """
    input_path = argv[1]
    entity_type = argv[2]
    output_path = argv[3]
    spark = SparkSession.builder.getOrCreate()
    dcc_df = spark.read.parquet(input_path)

    if entity_type == "experiment":
        dcc_clean_df = clean_experiments(dcc_df)
    else:
        dcc_clean_df = clean_lines(dcc_df)

    dcc_clean_df.write.mode("overwrite").parquet(output_path)


def clean_experiments(experiment_df: DataFrame) -> DataFrame:
    """
    DCC experiment level cleaner

    :param experiment_df: a raw experiment DataFrame
    :return: a clean experiment DataFrame
    :rtype: DataFrame
    """
    experiment_df = (
        experiment_df.transform(map_centre_ids)
        .transform(map_project_ids)
        .transform(truncate_europhenome_specimen_ids)
        .transform(drop_skipped_experiments)
        .transform(drop_skipped_procedures)
        .transform(map_3i_project_ids)
        .transform(prefix_3i_experiment_ids)
        .transform(drop_null_centre_id)
        .transform(drop_null_data_source)
        .transform(drop_null_date_of_experiment)
        .transform(drop_null_pipeline)
        .transform(drop_null_project)
        .transform(drop_null_specimen_id)
        .transform(generate_unique_id)
    )
    return experiment_df


def clean_lines(line_df: DataFrame):
    """
    DCC experiment level cleaner

    :param line_df: a raw line DataFrame
    :return: a clean line DataFrame
    :rtype: DataFrame
    """
    line_df = (
        line_df.transform(generate_line_experiment_id)
        .transform(map_centre_ids)
        .transform(map_project_ids)
        .transform(drop_skipped_procedures)
        .transform(truncate_europhenome_colony_ids)
        .transform(parse_europhenome_colony_xml_entities)
        .transform(map_3i_project_ids)
        .transform(prefix_3i_experiment_ids)
        .transform(drop_null_centre_id)
        .transform(drop_null_data_source)
        .transform(drop_null_pipeline)
        .transform(drop_null_project)
        .transform(generate_unique_id)
    )
    return line_df


def generate_line_experiment_id(line_df: DataFrame):
    line_df = line_df.withColumn(
        "_experimentID", concat(col("_procedureID"), lit("-"), col("_colonyID"))
    )
    return line_df


def map_centre_ids(dcc_df: DataFrame) -> DataFrame:
    """
    Maps the center ids  found in the XML files to a standard list of ids e.g:
        - gmc -> HMGU
        - h -> MRC Harwell
    The full list of mappings can be found at impc_etl.config.Constans.CENTRE_ID_MAP
    :param dcc_df: DataFrame
    """
    dcc_df = dcc_df.withColumn(
        "_centreID", udf(utils.map_centre_id, StringType())("_centreID")
    )
    return dcc_df


def map_project_ids(dcc_df: DataFrame) -> DataFrame:
    """
    Maps the center ids  found in the XML files to a standard list of ids e.g:
        - dtcc -> DTCC
        - riken brc -> RBRC
    The full list of mappings can be found at impc_etl.config.Constans.PROJECT_ID_MAP
    :param dcc_df: DataFrame
    """
    dcc_df = dcc_df.withColumn(
        "_project", udf(utils.map_project_id, StringType())("_project")
    )
    return dcc_df


def truncate_europhenome_specimen_ids(dcc_df: DataFrame) -> DataFrame:
    """
    Some EuroPhenome Specimen Ids have a suffix that should be truncated
    :param dcc_df:
    :return:
    """
    dcc_df = dcc_df.withColumn(
        "specimenID",
        when(
            (dcc_df["_dataSource"] == "europhenome"),
            udf(utils.truncate_specimen_id, StringType())(dcc_df["specimenID"]),
        ).otherwise(dcc_df["specimenID"]),
    )
    return dcc_df


def truncate_europhenome_colony_ids(dcc_df: DataFrame) -> DataFrame:
    """
    Some EuroPhenome Colony Ids have a suffix that should be truncated
    :param dcc_df:
    :return:
    """
    dcc_df = dcc_df.withColumn(
        "_colonyID",
        when(
            (dcc_df["_dataSource"] == "europhenome"),
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


def drop_skipped_experiments(dcc_df: DataFrame) -> DataFrame:
    """

    :param dcc_df:
    :return:
    """
    return dcc_df.where(
        ~(
            (dcc_df["_centreID"] == "Ucd")
            & (
                dcc_df["_experimentID"].isin(
                    ["GRS_2013-10-09_4326", "GRS_2014-07-16_8800"]
                )
            )
        )
    )


def drop_skipped_procedures(dcc_df: DataFrame) -> DataFrame:
    """

    :param dcc_df:
    :return:
    """
    return dcc_df.where(
        (
            ~(
                regexp_extract(col("_procedureID"), "(.+_.+)_.+", 1).isin(
                    Constants.SKIPPED_PROCEDURES
                )
            )
        )
        | (dcc_df["_dataSource"] == "3i")
    )


def map_3i_project_ids(dcc_df: DataFrame) -> DataFrame:
    """

    :param dcc_df:
    :return:
    """
    return dcc_df.withColumn(
        "_project",
        when(
            (dcc_df["_dataSource"] == "3i")
            & (~dcc_df["_project"].isin(Constants.VALID_PROJECT_IDS)),
            lit("MGP"),
        ).otherwise(dcc_df["_project"]),
    )


def prefix_3i_experiment_ids(dcc_df: DataFrame) -> DataFrame:
    return dcc_df.withColumn(
        "_experimentID",
        when(
            (dcc_df["_dataSource"] == "3i"), concat(lit("3i_"), dcc_df["_experimentID"])
        ).otherwise(dcc_df["_experimentID"]),
    )


def drop_null_procedure_id(dcc_df: DataFrame):
    """

    :param dcc_df:
    :return:
    """
    return drop_if_null(dcc_df, "_procedureID")


def drop_null_centre_id(dcc_df: DataFrame):
    """

    :param dcc_df:
    :return:
    """
    return drop_if_null(dcc_df, "_centreID")


def drop_null_data_source(dcc_df: DataFrame):
    """

    :param dcc_df:
    :return:
    """
    return drop_if_null(dcc_df, "_dataSource")


def drop_null_date_of_experiment(dcc_df: DataFrame):
    """

    :param dcc_df:
    :return:
    """
    return drop_if_null(dcc_df, "_dateOfExperiment")


def drop_null_pipeline(dcc_df: DataFrame):
    """

    :param dcc_df:
    :return:
    """
    return drop_if_null(dcc_df, "_pipeline")


def drop_null_project(dcc_df: DataFrame):
    """

    :param dcc_df:
    :return:
    """
    return drop_if_null(dcc_df, "_project")


def drop_null_specimen_id(dcc_df: DataFrame):
    """

    :param dcc_df:
    :return:
    """
    return drop_if_null(dcc_df, "specimenID")


def drop_if_null(dcc_df: DataFrame, column: str) -> DataFrame:
    """

    :param dcc_df:
    :param column:
    :return:
    """
    return dcc_df.where(dcc_df[column].isNotNull())


def generate_unique_id(dcc_experiment_df: DataFrame):
    """
    Generates an unique_id column using as an input every column
    except from those that have non unique values and
    the ones that correspond to parameter values.
    Given that _sequenceID could be null, the function transforms it the to the string NA
    when its null to avoid the nullifying the concat.
    It concatenates the unique set of values and then applies
    an MD5 hash function to the resulting string.
    :param dcc_experiment_df:
    :return: DataFrame
    """
    non_unique_columns = [
        "_type",
        "_sourceFile",
        "_VALUE",
        "procedureMetadata",
        "statusCode",
        "_sequenceID",
        "_project",
    ]
    if "_sequenceID" in dcc_experiment_df.columns:
        dcc_experiment_df = dcc_experiment_df.withColumn(
            "_sequenceIDStr",
            when(col("_sequenceID").isNull(), lit("NA")).otherwise(col("_sequenceID")),
        )
    unique_columns = [
        col_name
        for col_name in dcc_experiment_df.columns
        if col_name not in non_unique_columns and not col_name.endswith("Parameter")
    ]
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "unique_id", md5(concat(*unique_columns))
    )
    return dcc_experiment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
