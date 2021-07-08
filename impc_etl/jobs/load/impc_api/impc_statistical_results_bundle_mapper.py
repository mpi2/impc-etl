import luigi
import json
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from impc_etl.jobs.load.solr.stats_results_mapper import (
    get_stats_results_core,
    STATS_RESULTS_COLUMNS,
)
from impc_etl.workflow.config import ImpcConfig
from impc_etl.workflow.extraction import (
    AlleleExtractor,
    ImpressExtractor,
    OntologyExtractor,
    OpenStatsExtractor,
)
from impc_etl.workflow.load import (
    ObservationsMapper,
    PipelineCoreLoader,
    MPChooserLoader,
)


class ImpcStatsBundleMapper(PySparkTask):
    name = "IMPC_Stats_Bundle_Mapper"
    embryo_data_json_path = luigi.Parameter()
    mongodb_database = luigi.Parameter()
    output_path = luigi.Parameter()
    mongodb_connection_uri = luigi.Parameter()
    mongodb_stats_collection = luigi.Parameter()
    mongodb_replica_set = luigi.Parameter()
    threei_stats_results_csv = luigi.Parameter()
    mpath_metadata_csv_path = luigi.Parameter()

    @property
    def packages(self):
        return super().packages + super(PySparkTask, self).packages

    def requires(self):
        return [
            OpenStatsExtractor(),
            ObservationsMapper(),
            OntologyExtractor(),
            ImpressExtractor(),
            PipelineCoreLoader(),
            AlleleExtractor(),
            MPChooserLoader(),
        ]

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}stats_bundle_parquet")

    def app_options(self):
        return [
            self.input()[0].path,
            self.input()[1].path,
            self.input()[2].path,
            self.input()[3].path,
            self.input()[4].path,
            self.input()[5].path,
            self.input()[6].path,
            self.threei_stats_results_csv,
            self.mpath_metadata_csv_path,
            self.output().path,
        ]

    def main(self, sc, *argv):
        open_stats_parquet_path = argv[0]
        observations_parquet_path = argv[1]
        ontology_parquet_path = argv[2]
        pipeline_parquet_path = argv[3]
        pipeline_core_parquet_path = argv[4]
        allele_parquet_path = argv[5]
        mp_chooser_path = argv[6]
        threei_parquet_path = argv[7]
        mpath_metadata_path = argv[8]
        output_path = argv[9]

        spark = SparkSession(sc)
        open_stats_complete_df = spark.read.parquet(open_stats_parquet_path)
        ontology_df = spark.read.parquet(ontology_parquet_path)
        allele_df = spark.read.parquet(allele_parquet_path)
        pipeline_df = spark.read.parquet(pipeline_parquet_path)
        pipeline_core_df = spark.read.parquet(pipeline_core_parquet_path)
        observations_df = spark.read.parquet(observations_parquet_path)
        threei_df = spark.read.csv(threei_parquet_path, header=True)
        mpath_metadata_df = spark.read.csv(mpath_metadata_path, header=True)

        mp_chooser_txt = spark.sparkContext.wholeTextFiles(mp_chooser_path).collect()[
            0
        ][1]
        mp_chooser = json.loads(mp_chooser_txt)

        open_stats_df = get_stats_results_core(
            open_stats_complete_df,
            ontology_df,
            allele_df,
            pipeline_df,
            pipeline_core_df,
            observations_df,
            threei_df,
            mpath_metadata_df,
            mp_chooser,
            False,
            "include",
        )

        stats_results_column_list = STATS_RESULTS_COLUMNS
        stats_results_column_list.remove("observation_ids")
        stats_results_df = open_stats_df.select(*stats_results_column_list)
        for col_name in stats_results_df.columns:
            if dict(stats_results_df.dtypes)[col_name] == "null":
                stats_results_df = stats_results_df.withColumn(
                    col_name, lit(None).astype(StringType())
                )
        stats_results_df.distinct().write.parquet(output_path)
        raw_data_df = open_stats_df.select("doc_id", "raw_data").distinct()
        raw_data_df.write.parquet(output_path + "_raw_data")
        stats_results_df = spark.read.parquet(output_path)
        raw_data_df = spark.read.parquet(output_path + "_raw_data")
        stats_results_df = stats_results_df.join(raw_data_df, "doc_id", "left_outer")
        stats_results_df.write.format("mongo").mode("append").option(
            "spark.mongodb.output.uri",
            f"{self.mongodb_connection_uri}/admin?replicaSet={self.mongodb_replica_set}",
        ).option("database", str(self.mongodb_database)).option(
            "collection", str(self.mongodb_stats_collection)
        ).save()
