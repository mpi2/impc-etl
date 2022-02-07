import luigi
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from impc_etl.jobs.load.impc_api.impc_api_mapper import (
    ApiSpecimenMapper,
    ApiExperimentMapper,
    ApiObservationMapper,
)
from impc_etl.jobs.load.impc_api.impc_api_tables import (
    SPECIMEN_COLUMN_TYPES,
    EXPERIMENT_COLUMN_TYPES,
    OBSERVATION_COLUMN_TYPES,
)
from impc_etl.workflow.config import ImpcConfig


class ApiPostgreSQLLoader(PySparkTask):
    name = "IMPC_Api_Loader"
    api_db_jdbc_connection_str = luigi.Parameter()
    api_db_user = luigi.Parameter()
    api_db_password = luigi.Parameter()
    database_tables = [
        {
            "name": "specimen",
            "id_col": "specimen_id",
            "col_defs": SPECIMEN_COLUMN_TYPES,
        },
        {
            "name": "experiment",
            "id_col": "experiment_id",
            "col_defs": EXPERIMENT_COLUMN_TYPES,
        },
        {
            "name": "observation",
            "id_col": "observation_id",
            "col_defs": OBSERVATION_COLUMN_TYPES,
        },
    ]

    def output(self):
        return ImpcConfig().get_target("done!")

    def requires(self):
        return [ApiSpecimenMapper(), ApiExperimentMapper(), ApiObservationMapper()]

    def app_options(self):
        return [i.path for i in self.input()]

    def main(self, sc, *args):
        spark = SparkSession(sc)
        input_parquet_paths = args
        api_dataframes = [
            spark.read.parquet(input_path) for input_path in input_parquet_paths
        ]
        properties = {
            "user": self.api_db_user,
            "password": self.api_db_password,
            "driver": "org.postgresql.Driver",
        }

        for i, api_df in enumerate(api_dataframes):
            api_table_props = properties.copy()
            table_df = self.database_tables[i]
            if table_df["col_defs"] is not "":
                api_table_props["createTableColumnTypes"] = self.database_tables[i][
                    "col_defs"
                ]
            api_df = api_df.dropDuplicates([table_df["id_col"]])
            for table in self.database_tables:
                if table["id_col"] in api_df.columns:
                    api_df = api_df.where(col(table["id_col"]).isNotNull())
            api_df.write.mode("overwrite").jdbc(
                self.api_db_jdbc_connection_str,
                table_df["name"],
                properties=properties,
            )
