"""
    Luigi tasks to take care of cross-reference the Specimen data with the tracking system reports.
    It also applies some sanitation functions to the data after dong the cross-reference operations.
"""

from luigi.contrib.spark import PySparkTask
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, lit, udf, when
from pyspark.sql.types import StringType

from impc_etl.config.constants import Constants
from impc_etl.jobs.transform.cross_ref_helper import generate_allelic_composition
from impc_etl.jobs.transform.experiment_cross_ref import (
    override_europhenome_datasource,
)


class IMPCSpecimenCrossRef(PySparkTask):
    """
    PysPark task to cross-reference Specimen data with tracking systems reports.
    This task depends on `impc_etl.jobs.clean.specimen_cleaner.IMPCSpecimenCleaner`
    (`impc_etl.workflow.cleaning.IMPCMouseCleaner` or `impc_etl.workflow.cleaning.IMPCEmbryoCleaner`)
    and `impc_etl.jobs.clean.colony_cleaner.IMPCColonyCleaner`.
    """

    def main(self, argv):
        specimen_parquet_path = argv[1]
        colonies_parquet_path = argv[2]
        entity_type = argv[3]
        output_path = argv[4]
        spark = SparkSession.builder.getOrCreate()
        specimen_df = spark.read.parquet(specimen_parquet_path)
        colonies_df = spark.read.parquet(colonies_parquet_path)
        specimen_normalized_df = self.cross_reference_specimens(
            specimen_df, colonies_df, entity_type
        )
        specimen_normalized_df.write.mode("overwrite").parquet(output_path)

    def cross_reference_specimens(
        self, specimen_df: DataFrame, colonies_df: DataFrame, entity_type: str
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
            "left_outer",
        )

        specimen_df = (
            specimen_df.transform(self.generate_specimen_allelic_composition)
            .transform(override_europhenome_datasource)
            .transform(self.override_3i_specimen_project)
        )
        specimen_df = specimen_df.withColumn(
            "_productionCentre",
            when(
                col("_productionCentre").isNull(),
                when(
                    col("_phenotypingCentre").isNotNull(), col("_phenotypingCentre")
                ).otherwise(col("colony.production_centre")),
            ).otherwise(col("_productionCentre")),
        )
        specimen_df = specimen_df.select(
            "specimen.*",
            "_productionCentre",
            "allelicComposition",
            "colony.phenotyping_consortium",
        )

        if entity_type == "embryo":
            specimen_df = specimen_df.transform(self.add_embryo_life_stage_acc)
        if entity_type == "mouse":
            specimen_df = specimen_df.transform(self.add_mouse_life_stage_acc)
        return specimen_df

    def generate_specimen_allelic_composition(
        self, dcc_specimen_df: DataFrame
    ) -> DataFrame:
        generate_allelic_composition_udf = udf(
            generate_allelic_composition, StringType()
        )
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

    def override_3i_specimen_project(self, dcc_specimen_df: DataFrame):
        dcc_specimen_df.withColumn(
            "_project",
            when(
                dcc_specimen_df["_dataSource"] == "3i", col("phenotyping_consortium")
            ).otherwise("_project"),
        )
        return dcc_specimen_df

    def add_mouse_life_stage_acc(self, dcc_specimen_df: DataFrame):
        dcc_specimen_df = dcc_specimen_df.withColumn(
            "developmental_stage_acc", lit("EFO:0002948")
        )
        dcc_specimen_df = dcc_specimen_df.withColumn(
            "developmental_stage_name", lit("postnatal")
        )
        return dcc_specimen_df

    def add_embryo_life_stage_acc(self, dcc_specimen_df: DataFrame):
        efo_acc_udf = udf(self.resolve_embryo_life_stage, StringType())
        dcc_specimen_df = dcc_specimen_df.withColumn(
            "developmental_stage_acc", efo_acc_udf("_stage")
        )
        dcc_specimen_df = dcc_specimen_df.withColumn(
            "developmental_stage_name", concat(lit("embryonic day "), col("_stage"))
        )
        return dcc_specimen_df

    def resolve_embryo_life_stage(self, embryo_stage):
        embryo_stage = str(embryo_stage).replace("E", "")
        return (
            Constants.EFO_EMBRYONIC_STAGES[embryo_stage]
            if embryo_stage in Constants.EFO_EMBRYONIC_STAGES
            else "EFO:" + embryo_stage + "NOT_FOUND"
        )
