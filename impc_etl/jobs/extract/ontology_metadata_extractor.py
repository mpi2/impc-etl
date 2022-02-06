"""
    Ontology metadata extractor Task.
    It takes the output of the [IMPC Ontology Pipeline](https://github.com/mpi2/impc-ontology-pipeline)
    and transforms it  to a parquet files.

"""
from typing import Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession

from impc_etl.workflow.config import ImpcConfig


class OntologyMetadataExtractor(PySparkTask):
    """
    PySpark task to generate the ontology metadata parquet.
    Takes in a  directory   containing a set of ontology  metadata CSV files and returns a parquet version of all of them.

    It expects to contains metadata files for  eco, efo, emap, emapa, ma, mp, mpath, pato and uberon.
    """

    #: Name of the Spark task
    name = "IMPC_Ontology_Metadata_Extractor"

    #: Path to the directory containing Ontology metadata CSV files
    ontology_metadata_input_path: luigi.Parameter = luigi.Parameter()

    #: Path of the output directory where ethe new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/impc_ontology_metadata_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}impc_ontology_metadata_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.ontology_metadata_input_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Ontology metadata extraction runner.
        """
        input_path = args[0]
        output_path = args[1]
        spark = SparkSession.builder.getOrCreate()
        ontology_df = spark.read.csv(
            input_path
            + "/{eco,efo,emap,emapa,ma,mp,mpath,pato,uberon}_metadata_table.csv",
            header=True,
        )
        ontology_df.distinct().write.mode("overwrite").parquet(output_path)
