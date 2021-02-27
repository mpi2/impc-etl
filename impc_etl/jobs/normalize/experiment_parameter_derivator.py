from typing import List, Dict

import luigi
from luigi.contrib.spark import PySparkTask
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
from impc_etl.shared.utils import (
    extract_parameters_from_derivation,
    has_column,
)
from impc_etl.workflow.config import ImpcConfig
from impc_etl.workflow.extraction import ImpressExtractor
from impc_etl.workflow.normalization import (
    ExperimentNormalizer,
    LineExperimentNormalizer,
)


class ParameterDerivator(PySparkTask):
    experiment_level = luigi.Parameter()
    dcc_xml_path = luigi.Parameter()
    imits_colonies_tsv_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return ImpcConfig().get_target(
            f"{self.output_path}{self.experiment_level}_derived_parquet"
        )

    def app_options(self):
        return [self.input()[0].path, self.input()[1].path, self.output().path]

    def main(self, sc, *args):
        spark = SparkSession(sc)
        experiment_parquet_path = args[0]
        pipeline_parquet_path = args[1]
        output_path = args[2]
        dcc_experiment_df = spark.read.parquet(experiment_parquet_path)
        impress_df = spark.read.parquet(pipeline_parquet_path)
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
            dcc_experiment_df, derived_parameters_ex, "simpleParameter"
        )
        experiments_metadata = _get_inputs_by_parameter_type(
            dcc_experiment_df, derived_parameters_ex, "procedureMetadata"
        )
        experiments_vs_derivations = experiments_simple.union(experiments_metadata)

        if has_column(dcc_experiment_df, "seriesParameter"):
            experiments_series = _get_inputs_by_parameter_type(
                dcc_experiment_df, derived_parameters_ex, "seriesParameter"
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
        if self.experiment_level == "experiment":
            result_schema_fields.insert(
                1, lit(None).cast(LongType()).alias("_sequenceID")
            )
        elif has_column("simpleParameter._VALUE"):
            result_schema_fields.insert(0, lit(None).cast(StringType()).alias("_VALUE"))
        result_struct = struct(*result_schema_fields)
        results_df = results_df.groupBy("unique_id", "pipelineKey", "procedureKey").agg(
            collect_list(result_struct).alias("results")
        )
        results_df = results_df.withColumnRenamed("unique_id", "unique_id_result")

        dcc_experiment_df = dcc_experiment_df.join(
            results_df,
            (dcc_experiment_df["unique_id"] == results_df["unique_id_result"])
            & (dcc_experiment_df["_procedureID"] == results_df["procedureKey"])
            & (dcc_experiment_df["_pipeline"] == results_df["pipelineKey"]),
            "left_outer",
        )

        simple_parameter_type = None

        for c_type in dcc_experiment_df.dtypes:
            if c_type[0] == "simpleParameter":
                simple_parameter_type = c_type[1]
                break
        merge_simple_parameters = udf(_merge_simple_parameters, simple_parameter_type)
        dcc_experiment_df = dcc_experiment_df.withColumn(
            "simpleParameter",
            when(
                (col("results").isNotNull() & col("simpleParameter").isNotNull()),
                merge_simple_parameters(col("simpleParameter"), col("results")),
            )
            .when(col("simpleParameter").isNull(), col("results"))
            .otherwise(col("simpleParameter")),
        )
        dcc_experiment_df = dcc_experiment_df.drop(
            "complete_derivations.unique_id",
            "unique_id_result",
            "pipelineKey",
            "procedureKey",
            "parameterKey",
            "results",
        )
        dcc_experiment_df.write.parquet(output_path)


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


class ExperimentParameterDerivator(ParameterDerivator):
    name = "IMPC_Experiment_Parameter_Derivator"
    experiment_level = "experiment"

    def requires(self):
        return [ExperimentNormalizer(entity_type="experiment"), ImpressExtractor()]


class LineParameterDerivator(ParameterDerivator):
    name = "IMPC_Line_Parameter_Derivator"
    experiment_level = "line"

    def requires(self):
        return [LineExperimentNormalizer(entity_type="line"), ImpressExtractor()]
