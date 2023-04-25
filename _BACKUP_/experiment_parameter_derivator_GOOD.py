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
    collect_list,
    collect_set,
    struct,
    udf,
    sum,
    size,
    expr,
)
from pyspark.sql.types import (
    ArrayType,
    StringType,
    LongType,
)

from impc_etl.config.constants import Constants
from impc_etl.jobs.extract import ImpressExtractor
from impc_etl.jobs.transform.line_experiment_cross_ref import (
    LineLevelExperimentCrossRef,
)
from impc_etl.jobs.transform.specimen_experiment_cross_ref import (
    SpecimenLevelExperimentCrossRef,
)
from impc_etl.shared.utils import (
    extract_parameters_from_derivation,
    has_column,
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
        experiment_dependency = (
            SpecimenLevelExperimentCrossRef()
            if self.experiment_level == "specimen_level"
            else LineLevelExperimentCrossRef()
        )
        return [experiment_dependency, ImpressExtractor()]

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [self.input()[0].path, self.input()[1].path, self.output().path]

    def main(self, sc: SparkContext, *args: Any):
        """
        Takes in a set of experiments and the information coming from IMPReSS.
        Gets the derived parameter list from IMPReSS for IMPC parameters and
        some EuroPhenome derivations from a constant list.
        Applies the derivations to all the experiments and adds the derived parameter values to each experiment.
        """
        spark = SparkSession(sc)
        experiment_parquet_path = args[0]
        impress_parquet_path = args[1]
        output_path = args[2]
        experiment_df = spark.read.parquet(experiment_parquet_path)
        impress_df = spark.read.parquet(impress_parquet_path)

        # Some EuroPhenome derivations haven't been migrated to the new syntax
        # so we load them from a Constant
        europhenome_derivations_json = spark.sparkContext.parallelize(
            Constants.EUROPHENOME_DERIVATIONS
        )
        europhenome_derivations_df = spark.read.json(europhenome_derivations_json)
        europhenome_derivations_df = europhenome_derivations_df.alias("europhenome")

        europhenome_parameters = [
            derivation["europhenomeParameter"]
            for derivation in Constants.EUROPHENOME_DERIVATIONS
        ]

        # Filter impress DataFrame to get only the derived parameters, filtering out archive and unimplemented
        derived_parameters: DataFrame = (
            impress_df.where(
                (
                    (impress_df["parameter.isDerived"] == True)
                    & (impress_df["parameter.isDeprecated"] == False)
                    & (~impress_df["parameter.derivation"].contains("archived"))
                    & (~impress_df["parameter.derivation"].contains("unimplemented"))
                )
                | (impress_df["parameter.parameterKey"].isin(europhenome_parameters))
            )
            .where(
                ~impress_df["parameter.parameterKey"].isin(
                    Constants.DERIVED_PARAMETER_BANLIST
                )
            )
            .select(
                "pipelineKey",
                "procedure.procedureKey",
                "parameter.parameterKey",
                "parameter.derivation",
                "parameter.type",
                "unitName",
            )
            .dropDuplicates()
        )

        derived_parameters = derived_parameters.join(
            europhenome_derivations_df,
            col("parameterKey") == europhenome_derivations_df["europhenomeParameter"],
            "left_outer",
        )
        derived_parameters = derived_parameters.withColumn(
            "derivation",
            when(
                col("europhenomeDerivation").isNotNull(), col("europhenomeDerivation")
            ).otherwise(col("derivation")),
        )
        derived_parameters = derived_parameters.drop("europhenome.*")

        # Use a Python UDF to extract the keys of the parameters involved in the derivations as a list
        extract_parameters_from_derivation_udf = udf(
            extract_parameters_from_derivation, ArrayType(StringType())
        )

        derived_parameters = derived_parameters.withColumn(
            "derivationInputs", extract_parameters_from_derivation_udf("derivation")
        )

        # Explode the derivation inputs
        derived_parameters_ex = derived_parameters.withColumn(
            "derivationInput", explode("derivationInputs")
        ).select(
            "pipelineKey",
            "procedureKey",
            "parameterKey",
            "derivation",
            "derivationInput",
        )

        # Compute the derivation inputs for simple, procedure and series parameters
        # Each input is has the form <PARAMETER_KEY>$<PARAMETER_VALUE>
        # If the parameter has increments the inputs will have the form
        # <PARAMETER_KEY>$INCREMENT_1$<PARAMETER_VALUE>|INCREMENT_1$<PARAMETER_VALUE>
        experiments_simple = _get_inputs_by_parameter_type(
            experiment_df, derived_parameters_ex, "simpleParameter"
        )
        experiments_metadata = _get_inputs_by_parameter_type(
            experiment_df, derived_parameters_ex, "procedureMetadata"
        )
        experiments_vs_derivations = experiments_simple.union(experiments_metadata)

        if has_column(experiment_df, "seriesParameter"):
            experiments_series = _get_inputs_by_parameter_type(
                experiment_df, derived_parameters_ex, "seriesParameter"
            )
            experiments_vs_derivations = experiments_vs_derivations.union(
                experiments_series
            )
        # Collect the derivation inputs in a comma separated list
        experiments_vs_derivations = experiments_vs_derivations.groupby(
            "unique_id", "pipelineKey", "procedureKey", "parameterKey", "derivation"
        ).agg(
            concat_ws(
                ",", collect_list(experiments_vs_derivations["derivationInput"])
            ).alias("derivationInputStr")
        )

        experiments_vs_derivations = experiments_vs_derivations.join(
            derived_parameters.drop("derivation"),
            ["pipelineKey", "procedureKey", "parameterKey"],
        )

        # Check if the experiment contains all the parameter values to perform the derivation
        experiments_vs_derivations = experiments_vs_derivations.withColumn(
            "derivationInput", explode("derivationInputs")
        )
        experiments_vs_derivations = experiments_vs_derivations.withColumn(
            "isPresent",
            when(
                col("derivationInputStr").contains(col("derivationInput")), 1
            ).otherwise(0),
        )
        experiments_vs_derivations = experiments_vs_derivations.groupBy(
            [
                "unique_id",
                "pipelineKey",
                "procedureKey",
                "parameterKey",
                "derivationInputStr",
                "derivationInputs",
                "derivation",
            ]
        ).agg(sum("isPresent").alias("presentColumns"))

        experiments_vs_derivations = experiments_vs_derivations.withColumn(
            "isComplete",
            when((size(col("derivationInputs")) == col("presentColumns")), lit(True))
            .when(
                (
                    (col("derivation").contains("retinaCombined"))
                    | (col("derivation").contains("ifElse"))
                )
                & (size(col("derivationInputs")) > 0),
                lit(True),
            )
            .otherwise(lit(False)),
        )
        experiments_vs_derivations = experiments_vs_derivations.withColumn(
            "derivationInputStr", concat("derivation", lit(";"), "derivationInputStr")
        )
        spark.udf.registerJavaFunction(
            "phenodcc_derivator",
            "org.mousephenotype.dcc.derived.parameters.SparkDerivator",
            StringType(),
        )

        results_df = experiments_vs_derivations.select(
            "unique_id",
            "pipelineKey",
            "procedureKey",
            "parameterKey",
            "presentColumns",
            "isComplete",
            "derivationInputStr",
        ).dropDuplicates()
        results_df = results_df.repartition(10000)
        results_df = results_df.withColumn(
            "result",
            when(
                (col("isComplete") == True),
                expr("phenodcc_derivator(derivationInputStr)"),
            ).otherwise(lit(None)),
        )

        # Filtering not valid numeric values
        results_df = results_df.withColumn(
            "result",
            when(
                (col("result") == "NaN")
                | (col("result") == "Infinity")
                | (col("result") == "-Infinity"),
                lit(None),
            ).otherwise(col("result")),
        )
        results_df = results_df.where(col("result").isNotNull())
        results_df = results_df.join(
            derived_parameters, ["pipelineKey", "procedureKey", "parameterKey"]
        )

        result_schema_fields = [
            results_df["parameterKey"].alias("_parameterID"),
            results_df["unitName"].alias("_unit"),
            lit(None).cast(StringType()).alias("parameterStatus"),
            results_df["result"].alias("value"),
        ]
        if self.experiment_level == "specimen_level":
            result_schema_fields.insert(
                1, lit(None).cast(LongType()).alias("_sequenceID")
            )
        elif has_column(experiment_df, "simpleParameter._VALUE"):
            result_schema_fields.insert(0, lit(None).cast(StringType()).alias("_VALUE"))
        result_struct = struct(*result_schema_fields)
        results_df = results_df.groupBy("unique_id", "pipelineKey", "procedureKey").agg(
            collect_list(result_struct).alias("results")
        )
        results_df = results_df.withColumnRenamed("unique_id", "unique_id_result")

        experiment_df = experiment_df.join(
            results_df,
            (experiment_df["unique_id"] == results_df["unique_id_result"])
            & (experiment_df["_procedureID"] == results_df["procedureKey"])
            & (experiment_df["_pipeline"] == results_df["pipelineKey"]),
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
            "complete_derivations.unique_id",
            "unique_id_result",
            "pipelineKey",
            "procedureKey",
            "parameterKey",
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


def _get_inputs_by_parameter_type(
    dcc_experiment_df, derived_parameters_ex, parameter_type
):
    experiments_by_type = dcc_experiment_df.select(
        "unique_id",
        "_pipeline",
        "_procedureID",
        explode(parameter_type).alias(parameter_type),
    )
    if parameter_type == "seriesParameter":
        experiments_by_type = experiments_by_type.select(
            "unique_id",
            "_pipeline",
            "_procedureID",
            col(parameter_type + "._parameterID").alias("_parameterID"),
            explode(parameter_type + ".value").alias("value"),
        )
        experiments_by_type = experiments_by_type.withColumn(
            "value", concat(col("value._incrementValue"), lit("|"), col("value._VALUE"))
        )
        experiments_by_type = experiments_by_type.groupBy(
            "unique_id", "_pipeline", "_parameterID", "_procedureID"
        ).agg(concat_ws("$", collect_set("value")).alias("value"))

    parameter_id_column = (
        parameter_type + "._parameterID"
        if parameter_type != "seriesParameter"
        else "_parameterID"
    )
    parameter_value_column = (
        parameter_type + ".value" if parameter_type != "seriesParameter" else "value"
    )
    experiments_vs_derivations = derived_parameters_ex.join(
        experiments_by_type,
        (
            (
                experiments_by_type[parameter_id_column]
                == derived_parameters_ex["derivationInput"]
            )
            & (
                experiments_by_type["_procedureID"]
                == derived_parameters_ex.procedureKey
            )
            & (experiments_by_type["_pipeline"] == derived_parameters_ex.pipelineKey)
        ),
    )
    experiments_vs_derivations: DataFrame = experiments_vs_derivations.withColumn(
        "derivationInput",
        concat(col("derivationInput"), lit("$"), col(parameter_value_column)),
    )
    return (
        experiments_vs_derivations.drop(parameter_type, "_procedureID", "_pipeline")
        if parameter_type != "seriesParameter"
        else experiments_vs_derivations.drop(
            "value", "_parameterID", "_procedureID", "_pipeline"
        )
    )


class SpecimenLevelExperimentParameterDerivator(ParameterDerivator):
    name = "IMPC_Specimen_Level_Experiment_Parameter_Derivator"
    experiment_level = "specimen_level"


class LineLevelExperimentParameterDerivator(ParameterDerivator):
    name = "IMPC_Line_Level_Experiment_Parameter_Derivator"
    experiment_level = "line_level"
