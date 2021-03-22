from typing import Type
from pyspark.sql.functions import col, lit, when
from impc_etl.shared import utils
from impc_etl.workflow.config import ImpcConfig
from pyspark.sql.session import SparkSession
import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql.types import StringType


class ColonyTrackingExtractor(PySparkTask):
    name = "IMPC_Colony_Tracking_Extractor"
    imits_colonies_tsv_path = luigi.Parameter()
    gentar_colonies_tsv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}all_colonies_parquet")

    def app_options(self):
        return [
            self.imits_colonies_tsv_path,
            self.gentar_colonies_tsv_path,
            self.output().path,
        ]

    def main(self, sc, *args):
        spark = SparkSession(sc)
        imits_tsv_path = args[0]
        gentar_tsv_path = args[1]
        output_path = args[2]
        imits_df = utils.extract_tsv(spark, imits_tsv_path)
        gentar_df = utils.extract_tsv(spark, gentar_tsv_path)
        gentar_col_mapping = {
            "Phenotyping External Reference": "colony_name",
            "Background Strain": "colony_background_strain",
            "Mutation Symbol": "allele_symbol",
            "Gene Symbol": "marker_symbol",
            "MGI Gene Accession ID": "mgi_accession_id",
            "MGI Strain Accession ID": "mgi_strain_accession_id",
            "Phenotyping Work Unit": "phenotyping_centre",
            "Phenotyping Work Group": "phenotyping_consortium",
        }
        new_col_names = []
        for col_name in gentar_df.columns:
            if col_name in gentar_col_mapping:
                new_col_names.append(gentar_col_mapping[col_name])
            else:
                new_col_names.append(col_name.replace(" ", "_").lower())
        gentar_df = gentar_df.toDF(*new_col_names)
        imits_df = imits_df.toDF(
            *[column_name.replace(" ", "_").lower() for column_name in imits_df.columns]
        )

        imits_df = imits_df.alias("imits")
        gentar_tmp_df = gentar_df.alias("gentar")
        imits_df = imits_df.join(gentar_tmp_df, "colony_name", "left_outer")
        imits_df = imits_df.where(col("gentar.marker_symbol").isNull())
        imits_df = imits_df.select("imits.*")
        for col_name in imits_df.columns:
            if col_name not in gentar_df.columns:
                gentar_df = gentar_df.withColumn(col_name, lit(None).cast(StringType()))
        for col_name in gentar_df.columns:
            if col_name not in imits_df.columns:
                imits_df = imits_df.withColumn(col_name, lit(None).cast(StringType()))
        colonies_df = imits_df.union(gentar_df.select(*imits_df.columns))
        colonies_df.write.parquet(output_path)
