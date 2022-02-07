"""
    Module to hold helper functions for cross-reference tasks.
"""
import hashlib
from typing import List

from pyspark.sql import DataFrame, Column, Window, Row
from pyspark.sql.functions import (
    lit,
    when,
    lower,
    explode,
    col,
    concat,
    concat_ws,
    md5,
    sort_array,
    collect_set,
    udf,
    array_union,
    array,
    max,
)
from pyspark.sql.types import StringType, ArrayType

from impc_etl.config.constants import Constants


def generate_allelic_composition(
    zigosity: str,
    allele_symbol: str,
    gene_symbol: str,
    is_baseline: bool,
    colony_id: str,
):
    """
    Takes in a zigosity, allele symbol, gene symbol,
    baseline flag and a colony id and returns a description of the allelic composition.
    """
    if is_baseline or colony_id == "baseline":
        return ""

    if zigosity in ["homozygous", "homozygote"]:
        if allele_symbol is not None and allele_symbol is not "baseline":
            if allele_symbol is not "" and " " not in allele_symbol:
                return f"{allele_symbol}/{allele_symbol}"
            else:
                return f"{gene_symbol}<?>/{gene_symbol}<?>"
        else:
            return f"{gene_symbol}<+>/{gene_symbol}<+>"

    if zigosity in ["heterozygous", "heterozygote"]:
        if allele_symbol is not "baseline":
            if (
                allele_symbol is not None
                and allele_symbol is not ""
                and " " not in allele_symbol
            ):
                return f"{allele_symbol}/{gene_symbol}<+>"
            else:
                return f"{gene_symbol}<?>/{gene_symbol}<+>"
        else:
            return None

    if zigosity in ["hemizygous", "hemizygote"]:
        if allele_symbol is not "baseline":
            if (
                allele_symbol is not None
                and allele_symbol is not ""
                and " " not in allele_symbol
            ):
                return f"{allele_symbol}/0"
            else:
                return f"{gene_symbol}<?>/0"
        else:
            return None


def override_europhenome_datasource(dcc_df: DataFrame) -> DataFrame:
    """
    Takes in a specimen dataframe and replaces the project with 'MGP' and data sources information,
    for specimens with colonies marked as MGP and MGP Legacy.
    """
    legacy_entity_cond: Column = (
        (dcc_df["_dataSource"] == "europhenome")
        & (~lower(dcc_df["_colonyID"]).startswith("baseline"))
        & (dcc_df["_colonyID"].isNotNull())
        & (
            (dcc_df["phenotyping_consortium"] == "MGP")
            | (dcc_df["phenotyping_consortium"] == "MGP Legacy")
        )
    )

    dcc_df = dcc_df.withColumn(
        "_project",
        when(legacy_entity_cond, lit("MGP")).otherwise(dcc_df["_project"]),
    )

    dcc_df = dcc_df.withColumn(
        "_dataSource",
        when(legacy_entity_cond, lit("MGP")).otherwise(dcc_df["_dataSource"]),
    )
    return dcc_df


def generate_metadata_group(
    experiment_specimen_df: DataFrame,
    impress_df: DataFrame,
    exp_type="experiment",
) -> DataFrame:
    """
    Takes in an Experiment-Specimen DataFrame and the IMPReSS dataframe,
    and generates a hash value with the parameters marked as 'isImportant' on IMPReSS.
    This hash is used to identify experiments that are comparable (i.e. share the same experimental conditions).
    """

    # Explode the experiments by procedureMetadata so each row contains data for each procedureMetadata value
    experiment_metadata = experiment_specimen_df.withColumn(
        "procedureMetadata", explode("procedureMetadata")
    )

    # Filter the IMPReSS to leave only those that generate a metadata split: isImportant = True
    impress_df_required = impress_df.where(
        (col("parameter.isImportant") == True)
        & (col("parameter.type") == "procedureMetadata")
    )

    # Join the experiment DF with he IMPReSS DF
    experiment_metadata = experiment_metadata.join(
        impress_df_required,
        (
            (experiment_metadata["_pipeline"] == impress_df_required["pipelineKey"])
            & (
                experiment_metadata["_procedureID"]
                == impress_df_required["procedure.procedureKey"]
            )
            & (
                experiment_metadata["procedureMetadata._parameterID"]
                == impress_df_required["parameter.parameterKey"]
            )
        ),
    )

    # Create a new column by concatenating the parameter name and the parameter value
    experiment_metadata = experiment_metadata.withColumn(
        "metadataItem",
        when(
            col("procedureMetadata.value").isNotNull(),
            concat(col("parameter.name"), lit(" = "), col("procedureMetadata.value")),
        ).otherwise(concat(col("parameter.name"), lit(" = "), lit("null"))),
    )

    # Select the right column name for production and phenotyping centre depending on experiment type
    if exp_type == "experiment":
        production_centre_col = "_productionCentre"
        phenotyping_centre_col = "_phenotypingCentre"
    else:
        production_centre_col = "production_centre"
        phenotyping_centre_col = "phenotyping_centre"

    # Create a window for the DataFrame over experiment id, production and phenotyping centre
    window = Window.partitionBy(
        "unique_id", production_centre_col, phenotyping_centre_col
    ).orderBy("parameter.name")

    # Use the window to create for every experiment an array containing the set of "parameter =  value" pairs.
    experiment_metadata_input = experiment_metadata.withColumn(
        "metadataItems", collect_set(col("metadataItem")).over(window)
    )

    # Add the production centre to the metadata group when this is different form the phenotyping centre.
    # This is because in that given case we would like to generate a metadata split among specimens
    # That have been produced and phenotyped on the same centre
    experiment_metadata_input = experiment_metadata_input.withColumn(
        "metadataItems",
        when(
            (col(production_centre_col).isNotNull())
            & (col(production_centre_col) != col(phenotyping_centre_col)),
            array_union(
                col("metadataItems"),
                array(concat(lit("ProductionCenter = "), col(production_centre_col))),
            ),
        ).otherwise(col("metadataItems")),
    )

    # Create a string with the concatenation of the metadata items "parameter = value" separated by '::'.
    experiment_metadata = experiment_metadata_input.groupBy(
        "unique_id", production_centre_col, phenotyping_centre_col
    ).agg(
        concat_ws("::", sort_array(max(col("metadataItems")))).alias(
            "metadataGroupList"
        )
    )

    # Hash the list to generate a medata group identifier.
    experiment_metadata = experiment_metadata.withColumn(
        "metadataGroup", md5(col("metadataGroupList"))
    )

    # Select the experiment IDs and the metadata group IDs
    experiment_metadata = experiment_metadata.select("unique_id", "metadataGroup")

    # Join the original experiment DataFrame with the result of the metadata group generation
    experiment_specimen_df = experiment_specimen_df.join(
        experiment_metadata, "unique_id", "left_outer"
    )

    # Add the hashed version of an empty string to those rows without a metadata group.
    experiment_specimen_df = experiment_specimen_df.withColumn(
        "metadataGroup",
        when(experiment_specimen_df["metadataGroup"].isNull(), md5(lit(""))).otherwise(
            experiment_specimen_df["metadataGroup"]
        ),
    )
    return experiment_specimen_df


def generate_metadata(
    experiment_specimen_df: DataFrame,
    impress_df: DataFrame,
    exp_type="experiment",
) -> DataFrame:
    """
    Takes in a DataFrame with experiment and specimen data, and the IMPReSS information DataFrame.
    For every experiment generates a list of "parameter = value" pairs containing
    every metadata parameter for a that experiment.
    """

    # Explodes the experiment DF so every row represents a procedureMetadata value
    experiment_metadata = experiment_specimen_df.withColumn(
        "procedureMetadata", explode("procedureMetadata")
    )

    # Filters the IMPReSS DF so it only contains metadata parameters
    impress_df_required = impress_df.where(
        (col("parameter.type") == "procedureMetadata")
    )

    # Joins the experiment metadata values with the IMPReSS DF.
    experiment_metadata = experiment_metadata.join(
        impress_df_required,
        (
            (experiment_metadata["_pipeline"] == impress_df_required["pipelineKey"])
            & (
                experiment_metadata["_procedureID"]
                == impress_df_required["procedure.procedureKey"]
            )
            & (
                experiment_metadata["procedureMetadata._parameterID"]
                == impress_df_required["parameter.parameterKey"]
            )
        ),
    )

    # Some experimenter IDs need to be replaced on the metadata values
    process_experimenter_id_udf = udf(_process_experimenter_id, StringType())
    experiment_metadata = experiment_metadata.withColumn(
        "experimenterIdMetadata",
        when(
            lower(col("parameter.name")).contains("experimenter"),
            process_experimenter_id_udf("procedureMetadata"),
        ).otherwise(lit(None)),
    )
    experiment_metadata = experiment_metadata.withColumn(
        "procedureMetadata.value",
        when(
            col("experimenterIdMetadata").isNotNull(), col("experimenterIdMetadata")
        ).otherwise("procedureMetadata.value"),
    )

    # Create the "parameter = value" pairs, if there is not value: "parameter = null"
    experiment_metadata = experiment_metadata.withColumn(
        "metadataItem",
        concat(
            col("parameter.name"),
            lit(" = "),
            when(
                col("procedureMetadata.value").isNotNull(),
                col("procedureMetadata.value"),
            ).otherwise(lit("null")),
        ),
    )

    # Select the right column name for production and phenotyping centre depending on experiment type
    if exp_type == "experiment":
        production_centre_col = "_productionCentre"
        phenotyping_centre_col = "_phenotypingCentre"
    else:
        production_centre_col = "production_centre"
        phenotyping_centre_col = "phenotyping_centre"

    # Group by the experiment unique_id, phenotyping centre and production centre
    # And create an array containing all the metadata pairs
    experiment_metadata = experiment_metadata.groupBy(
        "unique_id", production_centre_col, phenotyping_centre_col
    ).agg(sort_array(collect_set(col("metadataItem"))).alias("metadata"))

    # Append the phenotyping centre value to all the
    # experiments where the phenotyping centre !=  the production centre
    experiment_metadata = experiment_metadata.withColumn(
        "metadata",
        when(
            (col(production_centre_col).isNotNull())
            & (col(production_centre_col) != col(phenotyping_centre_col)),
            udf(_append_phenotyping_centre_to_metadata, ArrayType(StringType()))(
                col("metadata"), col(production_centre_col)
            ),
        ).otherwise(col("metadata")),
    )

    # Join the original experiment DF with the resulting experiment metadata DF
    experiment_specimen_df = experiment_specimen_df.join(
        experiment_metadata, "unique_id", "left_outer"
    )
    return experiment_specimen_df


def _append_phenotyping_centre_to_metadata(metadata: List, prod_centre: str):
    """
    Takes in a metadata list and a production centre and appends the production centre as a metadata value.
    """
    if prod_centre is not None:
        metadata.append("ProductionCenter = " + prod_centre)
    return metadata


def _process_experimenter_id(experimenter_metadata: Row):
    """
    Some experimenter IDs need to be replaced on the metadata values.
    """
    experimenter_metadata = experimenter_metadata.asDict()
    if experimenter_metadata["value"] in Constants.EXPERIMENTER_IDS:
        experimenter_metadata["value"] = Constants.EXPERIMENTER_IDS[
            experimenter_metadata["value"]
        ]
    if experimenter_metadata["value"] is not None:
        experimenter_metadata["value"] = (
            hashlib.md5(experimenter_metadata["value"].encode()).hexdigest()[:5].upper()
        )
    return experimenter_metadata["value"]
