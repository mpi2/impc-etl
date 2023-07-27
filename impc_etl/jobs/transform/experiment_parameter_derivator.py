"""
    Module to hold Luigi task that calculates the derived parameters on experimental data.

    The general process is:

    - Takes in a set of experiments and the information coming from IMPReSS.
    - Gets the derived parameter list from IMPReSS for IMPC parameters
    and some EuroPhenome derivations from a constant list.
    - Checks for each experiment that all the input values for the derivation formula are present
    - Generates a string value containing the derivation formula and the input values
    - Applies the derivation using the parameter derivation JAR application provided by the DCC.
    - Adds the resulting derived parameter values to the original experiments as new parameter values.
"""
from typing import List, Dict, Any

import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    concat,
    explode,
    concat_ws,
    collect_set,
    udf,
)

from impc_etl.jobs.transform.specimen_experiment_cross_ref import (
    SpecimenLevelExperimentCrossRef,
)
from impc_etl.workflow.config import ImpcConfig


class ParameterDerivator(PySparkTask):
    """
    PySpark task that takes in a set of experiments and computes all the derived parameters.

    This tasks depends on:

    - `impc_etl.jobs.transform.specimen_experiment_cross_ref.SpecimenLevelExperimentCrossRef` for
    specimen level experiments or
    `impc_etl.jobs.transform.line_experiment_cross_ref.LineLevelExperimentCrossRef` for line level
     experiments
     - `impc_etl.jobs.extract.impress_extractor.ImpressExtractor`
    """

    #: Name of the Spark task
    name = "IMPC_Experiment_Parameter_Derivator"

    #: Experimental level of the data (can be 'specimen_level' or 'line_level')
    experiment_level = luigi.Parameter()

    europhenome_pwg_derived_values_parquet_path = luigi.Parameter()

    #: Path of the output directory where the new parquet file will be generated.
    output_path = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/specimen_level_experiment_derived_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}{self.experiment_level}_experiment_derived_parquet"
        )

    def requires(self):
        """
        Defines the luigi  task dependencies
        """
        return SpecimenLevelExperimentCrossRef()

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.input().path,
            self.europhenome_pwg_derived_values_parquet_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        Takes in a set of experiments and the information coming from IMPReSS.
        Gets the derived parameter list from IMPReSS for IMPC parameters and
        some EuroPhenome derivations from a constant list.
        Applies the derivations to all the experiments and adds the derived parameter values to each experiment.
        """
        spark = SparkSession(sc)
        experiment_parquet_path = args[0]
        europhenome_pwg_derived_values_parquet_path = args[1]
        output_path = args[2]
        experiment_df = spark.read.parquet(experiment_parquet_path)
        europhenome_pwg_derived_values_df = spark.read.parquet(
            europhenome_pwg_derived_values_parquet_path
        )

        experiment_df = experiment_df.join(
            europhenome_pwg_derived_values_df,
            ["unique_id", "_pipeline", "_procedureID"],
            "left_outer",
        )

        simple_parameter_type = None

        for c_type in experiment_df.dtypes:
            if c_type[0] == "simpleParameter":
                simple_parameter_type = c_type[1]
                break
        merge_simple_parameters = udf(_merge_simple_parameters, simple_parameter_type)
        experiment_df = experiment_df.withColumn(
            "simpleParameter",
            when(
                (col("results").isNotNull() & col("simpleParameter").isNotNull()),
                merge_simple_parameters(col("simpleParameter"), col("results")),
            )
            .when(col("simpleParameter").isNull(), col("results"))
            .otherwise(col("simpleParameter")),
        )
        experiment_df = experiment_df.drop(
            "results",
        )
        experiment_df.write.parquet(output_path)


def _merge_simple_parameters(simple_parameters: List[Dict], results: [Dict]):
    merged_array = []
    if results is None or simple_parameters is None:
        return simple_parameters
    result_parameter_keys = {result["_parameterID"]: result for result in results}
    for simple_parameter in simple_parameters:
        parameter_id = simple_parameter["_parameterID"]
        if parameter_id in result_parameter_keys:
            merged_array.append(result_parameter_keys[parameter_id])
        else:
            merged_array.append(simple_parameter)
    simple_parameter_keys = {
        simple_parameter["_parameterID"]: simple_parameter
        for simple_parameter in simple_parameters
    }
    for result in results:
        parameter_id = result["_parameterID"]
        if parameter_id not in simple_parameter_keys:
            merged_array.append(result)
    return merged_array


class SpecimenLevelLegacyPWGExperimentParameterDerivator(ParameterDerivator):
    name = "IMPC_Specimen_Level_Experiment_Parameter_Derivator"
    experiment_level = "specimen_level"
