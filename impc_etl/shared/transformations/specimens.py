from pyspark.sql.functions import col, concat

from impc_etl.shared.transformations.commons import *

from impc_etl.config import Constants


def map_production_centre_id(dcc_experiment_df: DataFrame):
    if "_productionCentre" not in dcc_experiment_df.columns:
        dcc_experiment_df = dcc_experiment_df.withColumn("_productionCentre", lit(None))
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "_productionCentre", udf(map_centre_ids, StringType())("_productionCentre")
    )
    return dcc_experiment_df


def map_phenotyping_centre_id(dcc_experiment_df: DataFrame):
    dcc_experiment_df = dcc_experiment_df.withColumn(
        "_phenotypingCentre", udf(map_centre_ids, StringType())("_phenotypingCentre")
    )
    return dcc_experiment_df


def standarize_europhenome_colony_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "_colonyID",
        when(
            dcc_df["_dataSource"] == "EuroPhenome",
            udf(_truncate_colony_id, StringType())(dcc_df["_colonyID"]),
        ).otherwise(dcc_df["_colonyID"]),
    )
    return dcc_df


def standarize_strain_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "_strainID",
        when(dcc_df["_strainID"].startswith("MGI:"), dcc_df["_strainID"]).otherwise(
            concat(lit("MGI:"), dcc_df["_strainID"])
        ),
    )
    return dcc_df


def standarize_europhenome_specimen_ids(dcc_df: DataFrame) -> DataFrame:
    dcc_df = dcc_df.withColumn(
        "_specimenID",
        when(
            (dcc_df["_dataSource"] == "EuroPhenome") | (dcc_df["_dataSource"] == "MGP"),
            udf(truncate_specimen_id, StringType())(dcc_df["_specimenID"]),
        ).otherwise(dcc_df["_specimenID"]),
    )
    return dcc_df


def generate_allelic_composition(dcc_specimen_df: DataFrame) -> DataFrame:
    generate_allelic_composition_udf = udf(_generate_allelic_composition, StringType())
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "allelicComposition",
        generate_allelic_composition_udf(
            "specimen._zygosity",
            "colony.allele_symbol",
            "colony.marker_symbol",
            "specimen._isBaseline",
        ),
    )
    return dcc_specimen_df


def _generate_allelic_composition(
    zigosity: str, allele_symbol: str, gene_symbol: str, is_baseline: bool
):
    if is_baseline:
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


def _truncate_colony_id(colony_id: str) -> str:
    if colony_id in Constants.EUROPHENOME_VALID_COLONIES or colony_id is None:
        return colony_id
    else:
        return colony_id[: colony_id.rfind("_")].strip()


def override_3i_specimen_data(dcc_specimen_df: DataFrame):
    dcc_specimen_df_a = dcc_specimen_df.alias("a")
    dcc_specimen_df_b = dcc_specimen_df.alias("b")
    dcc_specimen_df = dcc_specimen_df_a.join(
        dcc_specimen_df_b,
        (dcc_specimen_df_a["_specimenID"] == dcc_specimen_df_b["_specimenID"])
        & (dcc_specimen_df_a["_centreID"] == dcc_specimen_df_b["_centreID"])
        & (dcc_specimen_df_a["_dataSource"] != dcc_specimen_df_b["_dataSource"]),
        "left_outer",
    )
    dcc_specimen_df = dcc_specimen_df.where(
        col("b._specimenID").isNull()
        | ((col("b._specimenID").isNotNull()) & (col("a._dataSource") != "3i"))
    )
    return dcc_specimen_df.select("a.*")


def override_3i_specimen_project(dcc_specimen_df: DataFrame):
    dcc_specimen_df.withColumn(
        "_project",
        when(
            dcc_specimen_df["_dataSource"] == "3i", col("phenotyping_consortium")
        ).otherwise("_project"),
    )
    return dcc_specimen_df


def add_mouse_life_stage_acc(dcc_specimen_df: DataFrame):
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_acc", lit("EFO:0002948")
    )
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_name", lit("postnatal")
    )
    return dcc_specimen_df


def add_embryo_life_stage_acc(dcc_specimen_df: DataFrame):
    efo_acc_udf = udf(
        lambda x: Constants.EFO_EMBRYONIC_STAGES[str(x).replace("E", "")], StringType()
    )
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_acc", efo_acc_udf("_stage")
    )
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_name", concat(lit("embryonic day "), col("_stage"))
    )
    return dcc_specimen_df
