import sys
from pyspark.sql.functions import col, concat, lit, udf, when
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame, SparkSession
from impc_etl.config import Constants
from impc_etl.jobs.normalize.experiment_normalizer import (
    override_europhenome_datasource,
)


def main(argv):
    specimen_parquet_path = argv[1]
    colonies_parquet_path = argv[2]
    entity_type = argv[3]
    output_path = argv[4]
    spark = SparkSession.builder.getOrCreate()
    specimen_df = spark.read.parquet(specimen_parquet_path)
    colonies_df = spark.read.parquet(colonies_parquet_path)
    specimen_normalized_df = normalize_specimens(specimen_df, colonies_df, entity_type)
    specimen_normalized_df.write.mode("overwrite").parquet(output_path)


def normalize_specimens(
    specimen_df: DataFrame, colonies_df: DataFrame, entity_type: str
) -> DataFrame:
    """
    DCC specimen normalizer

    :param colonies_df:
    :param specimen_df:
    :param entity_type:
    :return: a normalized specimen parquet file
    :rtype: DataFrame
    """
    specimen_df = specimen_df.alias("specimen")
    colonies_df = colonies_df.alias("colony")

    specimen_df = specimen_df.join(
        colonies_df,
        (specimen_df["_colonyID"] == colonies_df["colony_name"]),
        ##        "left_outer", only specimen with colonies in imits should appear in the normalized output
    )

    specimen_df = (
        specimen_df.transform(generate_allelic_composition)
        .transform(override_europhenome_datasource)
        .transform(override_3i_specimen_project)
    )
    specimen_df = specimen_df.withColumn(
        "_productionCentre",
        when(
            col("_productionCentre").isNull(), col("colony.production_centre")
        ).otherwise(col("_productionCentre")),
    )
    specimen_df = specimen_df.select(
        "specimen.*",
        "_productionCentre",
        "allelicComposition",
        "colony.phenotyping_consortium",
    )

    if entity_type == "embryo":
        specimen_df = specimen_df.transform(add_embryo_life_stage_acc)
    if entity_type == "mouse":
        specimen_df = specimen_df.transform(add_mouse_life_stage_acc)
    return specimen_df


def generate_allelic_composition(dcc_specimen_df: DataFrame) -> DataFrame:
    generate_allelic_composition_udf = udf(_generate_allelic_composition, StringType())
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "allelicComposition",
        generate_allelic_composition_udf(
            "specimen._zygosity",
            "colony.allele_symbol",
            "colony.marker_symbol",
            "specimen._isBaseline",
            "specimen._colonyID",
        ),
    )
    return dcc_specimen_df


def override_3i_specimen_project(dcc_specimen_df: DataFrame):
    dcc_specimen_df.withColumn(
        "_project",
        when(
            dcc_specimen_df["_dataSource"] == "3i", col("phenotyping_consortium")
        ).otherwise("_project"),
    )
    return dcc_specimen_df


def _generate_allelic_composition(
    zigosity: str,
    allele_symbol: str,
    gene_symbol: str,
    is_baseline: bool,
    colony_id: str,
):
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


def add_mouse_life_stage_acc(dcc_specimen_df: DataFrame):
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_acc", lit("EFO:0002948")
    )
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_name", lit("postnatal")
    )
    return dcc_specimen_df


def add_embryo_life_stage_acc(dcc_specimen_df: DataFrame):
    efo_acc_udf = udf(resolve_embryo_life_stage, StringType())
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_acc", efo_acc_udf("_stage")
    )
    dcc_specimen_df = dcc_specimen_df.withColumn(
        "developmental_stage_name", concat(lit("embryonic day "), col("_stage"))
    )
    return dcc_specimen_df


def resolve_embryo_life_stage(embryo_stage):
    embryo_stage = str(embryo_stage).replace("E", "")
    return (
        Constants.EFO_EMBRYONIC_STAGES[embryo_stage]
        if embryo_stage in Constants.EFO_EMBRYONIC_STAGES
        else "EFO:" + embryo_stage + "NOT_FOUND"
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
