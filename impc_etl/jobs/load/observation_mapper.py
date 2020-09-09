import sys
from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import (
    concat,
    col,
    when,
    lit,
    explode,
    lower,
    regexp_replace,
    regexp_extract,
    collect_list,
    collect_set,
    md5,
    unix_timestamp,
    from_unixtime,
    udf,
    array,
    substring,
    upper,
)
from pyspark.sql.types import StringType, IntegerType, LongType, ArrayType
from impc_etl.config import Constants
import datetime

from impc_etl.jobs.clean.colony_cleaner import map_strain_name
from impc_etl.shared.utils import has_column


def main(argv):
    experiment_parquet_path = argv[1]
    line_experiment_parquet_path = argv[2]
    mouse_parquet_path = argv[3]
    embryo_parquet_path = argv[4]
    allele_parquet_path = argv[5]
    colony_parquet_path = argv[6]
    pipeline_parquet_path = argv[7]
    strain_parquet_path = argv[8]
    ontology_parquet_path = argv[9]
    output_path = argv[10]
    spark = SparkSession.builder.getOrCreate()
    experiment_df = spark.read.parquet(experiment_parquet_path)
    line_experiment_df = spark.read.parquet(line_experiment_parquet_path)
    mouse_df = spark.read.parquet(mouse_parquet_path)
    embryo_df = spark.read.parquet(embryo_parquet_path)
    allele_df = spark.read.parquet(allele_parquet_path)
    colony_df = spark.read.parquet(colony_parquet_path)
    pipeline_df = spark.read.parquet(pipeline_parquet_path)
    strain_df = spark.read.parquet(strain_parquet_path)
    ontology_df = spark.read.parquet(ontology_parquet_path)

    observations_df = map_experiments_to_observations(
        experiment_df,
        line_experiment_df,
        mouse_df,
        embryo_df,
        allele_df,
        colony_df,
        pipeline_df,
        strain_df,
        ontology_df,
    )
    observations_df = observations_df.where(
        ~(
            (col("datasource_name") == "EuroPhenome")
            & (col("parameter_stable_id") == "ESLIM_001_001_125")
            & (col("sex") == "male")
            & (col("category") == "present")
            & (col("phenotyping_center") == "ICS")
        )
    )
    observations_df = observations_df.where(
        ~(
            col("procedure_stable_id").contains("_EYE_002")
            & col("parameter_stable_id").contains("EYE_092_001")
        )
    )
    observations_df = observations_df.where(
        col("category").isNull() | (col("category") != "INCOMPLETE_INPUT_STR")
    )
    observations_df = observations_df.where(
        col("strain_name").isNotNull() | (col("biological_sample_group") == "control")
    )
    observations_df = observations_df.where(
        (~col("text_value").like('%outcome": null%')) | col("text_value").isNull()
    )
    observations_df.write.mode("overwrite").parquet(output_path)


def map_line_columns(line_df: DataFrame):
    for field, value in Constants.LINE_TO_OBSERVATION_MAP.items():
        if value is not None:
            line_df = line_df.withColumn(field, col(value))
        else:
            line_df = line_df.withColumn(field, lit(None))
    line_df = line_df.withColumn("biological_sample_group", lit("experimental"))
    line_df = line_df.withColumn(
        "datasource_name",
        when(col("_dataSource") == "impc", lit("IMPC")).otherwise(
            when(col("_dataSource") == "europhenome", lit("EuroPhenome")).otherwise(
                col("_dataSource")
            )
        ),
    )
    line_df = line_df.withColumn(
        "allele_accession_id",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            when(
                col("allele.mgiAlleleID").isNull(),
                concat(
                    lit("NOT-RELEASED-"),
                    substring(md5(line_df["allele_symbol"]), 0, 10),
                ),
            ).otherwise(col("allele.mgiAlleleID"))
        ),
    )
    line_df = line_df.withColumn(
        "strain_accession_id",
        when(
            col("strain_accession_id").isNull(),
            concat(
                lit("IMPC-CURATE-"), upper(substring(md5(line_df["strain_name"]), 0, 5))
            ),
        ).otherwise(col("strain_accession_id")),
    )
    return line_df


def map_experiment_columns(exp_df: DataFrame):
    for field in Constants.EXPERIMENT_TO_OBSERVATION_MAP:
        exp_df = exp_df.withColumn(
            field, col(Constants.EXPERIMENT_TO_OBSERVATION_MAP[field])
        )
    exp_df = exp_df.withColumnRenamed("weight", "weightStruct")
    exp_df = exp_df.withColumn("weight", col("weightStruct.weightValue"))
    exp_df = exp_df.withColumn("weight_date", col("weightStruct.weightDate"))
    exp_df = exp_df.withColumn("weight_days_old", col("weightStruct.weightDaysOld"))
    exp_df = exp_df.withColumn(
        "weight_parameter_stable_id", col("weightStruct.weightParameterID")
    )

    exp_df = exp_df.withColumn(
        "biological_sample_group",
        when(col("_isBaseline") == True, lit("control")).otherwise("experimental"),
    )

    exp_df = exp_df.withColumn(
        "allele_symbol",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            when(
                exp_df["allele.alleleSymbol"].isNull(), exp_df["colony.allele_symbol"]
            ).otherwise(exp_df["allele.alleleSymbol"])
        ),
    )

    exp_df = exp_df.withColumn(
        "allele_accession_id",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            when(
                col("allele.mgiAlleleID").isNull(),
                concat(
                    lit("NOT-RELEASED-"), substring(md5(exp_df["allele_symbol"]), 0, 10)
                ),
            ).otherwise(col("allele.mgiAlleleID"))
        ),
    )

    exp_df = exp_df.withColumn(
        "mgiMarkerAccessionID",
        when(col("mgiMarkerAccessionID").isNull(), col("mgi_accession_id")).otherwise(
            col("mgiMarkerAccessionID")
        ),
    )

    exp_df = exp_df.withColumn(
        "gene_accession_id",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            col("mgiMarkerAccessionID")
        ),
    )

    exp_df = exp_df.withColumn(
        "gene_symbol",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            when(
                col("allele.markerSymbol").isNull(), exp_df["colony.marker_symbol"]
            ).otherwise(col("allele.markerSymbol"))
        ),
    )

    exp_df = exp_df.withColumn("zygosity", col("specimen._zygosity"))

    exp_df = exp_df.withColumn(
        "zygosity",
        when(col("zygosity") == "heterozygous", lit("heterozygote")).otherwise(
            col("zygosity")
        ),
    )

    exp_df = exp_df.withColumn(
        "zygosity",
        when(col("zygosity") == "homozygous", lit("homozygote")).otherwise(
            col("zygosity")
        ),
    )

    exp_df = exp_df.withColumn(
        "zygosity",
        when(col("zygosity") == "hemizygous", lit("hemizygote")).otherwise(
            col("zygosity")
        ),
    )

    exp_df = exp_df.withColumn(
        "zygosity",
        when(col("zygosity") == "wild type", lit("homozygote")).otherwise(
            col("zygosity")
        ),
    )

    exp_df = exp_df.withColumn(
        "datasource_name",
        when(col("experiment._dataSource") == "impc", lit("IMPC")).otherwise(
            when(
                col("experiment._dataSource") == "europhenome", lit("EuroPhenome")
            ).otherwise(col("experiment._dataSource"))
        ),
    )

    exp_df = exp_df.withColumn(
        "colony_id",
        when(lower(col("specimen._colonyID")) == "baseline", lit("baseline")).otherwise(
            when(col("specimen._colonyID").isNull(), "unknown").otherwise(
                col("specimen._colonyID")
            )
        ),
    )

    exp_df = exp_df.withColumn(
        "strain_name",
        when(
            (col("colony_id") == "baseline") | (col("specimen._isBaseline") == True),
            when(
                col("strain.strainName").isNotNull(), col("strain.strainName")
            ).otherwise(col("specimen._strainID")),
        ).otherwise(
            when(
                col("strain.strainName").isNotNull(), col("strain.strainName")
            ).otherwise(col("specimen._strainID"))
        ),
    )

    exp_df = exp_df.withColumn(
        "genetic_background",
        when(
            (col("colony_id") == "baseline") | (col("specimen._isBaseline") == True),
            concat(lit("involves: "), col("strain.strainName")),
        ).otherwise(col("colony.genetic_background")),
    )

    exp_df = exp_df.withColumn(
        "strain_accession_id",
        when(
            col("strain_accession_id").isNull(),
            concat(
                lit("IMPC-CURATE-"), upper(substring(md5(exp_df["strain_name"]), 0, 5))
            ),
        ).otherwise(col("strain_accession_id")),
    )

    return exp_df


def unify_schema(obs_df: DataFrame):
    for column in Constants.OBSERVATION_COLUMNS:
        if column not in obs_df.columns:
            col_schema = Constants.PARAMETER_SPECIFIC_FIELDS[column]
            obs_df = obs_df.withColumn(column, lit(None).cast(col_schema))
    return obs_df


def add_impress_info(
    experiments_df, pipeline_df, parameter_type, exp_type="experiment"
):
    pipeline_columns = [
        "pipeline.parameter",
        "pipeline.procedure",
        "pipeline.name",
        "pipeline.pipelineKey",
    ]
    pipeline_df = (
        pipeline_df.drop("weight")
        .alias("pipeline")
        .select(pipeline_columns)
        .drop_duplicates()
    )
    experiments_df = experiments_df.join(
        pipeline_df,
        (
            col(parameter_type + "._parameterID")
            == col("pipeline.parameter.parameterKey")
        )
        & (col("_procedureID") == col("pipeline.procedure.procedureKey"))
        & (col("experiment._pipeline") == col("pipeline.pipelineKey")),
        "left_outer",
    )
    experiments_df = experiments_df.withColumn("pipeline_name", col("pipeline.name"))
    experiments_df = experiments_df.withColumn(
        "pipeline_stable_id", col("pipeline.pipelineKey")
    )

    experiments_df = experiments_df.withColumn(
        "procedure_name", col("pipeline.procedure.name")
    )
    experiments_df = experiments_df.withColumn(
        "procedure_stable_id", col("pipeline.procedure.procedureKey")
    )
    experiments_df = experiments_df.withColumn(
        "procedure_group", regexp_extract(col("procedure_stable_id"), "(.+_.+)_.+", 1)
    )

    experiments_df = experiments_df.withColumn(
        "parameter_name", col("pipeline.parameter.name")
    )
    experiments_df = experiments_df.withColumn(
        "parameter_stable_id", col("pipeline.parameter.parameterKey")
    )
    if exp_type == "experiment":
        experiments_df = experiments_df.withColumn(
            "sex", when(col("sex").isNull(), lit("no_data")).otherwise(col("sex"))
        )
    else:
        experiments_df = experiments_df.withColumn(
            "sex",
            when(
                col("sex").isNull(),
                when(
                    col("parameter_stable_id").isin(Constants.FEMALE_LINE_PARAMETERS),
                    lit("female"),
                )
                .when(
                    col("parameter_stable_id").isin(Constants.MALE_LINE_PARAMETERS),
                    lit("male"),
                )
                .otherwise(lit("both")),
            ).otherwise(col("sex")),
        )
        experiments_df = experiments_df.withColumn(
            "zygosity",
            when(
                col("parameter_stable_id").isin(Constants.HET_LINE_PARAMETERS),
                lit("heterozygote"),
            )
            .when(
                col("parameter_stable_id").isin(Constants.HEM_LINE_PARAMETERS),
                lit("hemizygote"),
            )
            .when(
                col("parameter_stable_id").isin(Constants.ANZ_LINE_PARAMETERS),
                lit("anzygote"),
            )
            .when(
                col("parameter_stable_id").isin(Constants.ZYG_NA_LINE_PARAMETERS),
                lit("not_applicable"),
            )
            .otherwise(lit("homozygote")),
        )
    return experiments_df


def add_observation_type(experiments_df):
    experiments_df = experiments_df.withColumn(
        "observation_type",
        when(
            (col("pipeline.parameter.isOption") == True)
            | (col("pipeline.parameter.parameterKey").contains("EYE_092_001")),
            lit("categorical"),
        ).otherwise(
            when(
                (col("pipeline.parameter.valueType") != "TEXT")
                | (
                    col("parameter_stable_id").isin(
                        [
                            "ESLIM_006_001_035",
                            "M-G-P_022_001_001_001",
                            "M-G-P_022_001_001",
                        ]
                    )
                ),
                lit("unidimensional"),
            ).otherwise(lit("text"))
        ),
    )
    return experiments_df


def resolve_simple_value(exp_df, pipeline_df):
    options_df = pipeline_df.select("option.*").distinct().alias("options")
    if has_column(exp_df, "simpleParameter._sequenceID"):
        exp_df = exp_df.withColumn("sequence_id", col("simpleParameter._sequenceID"))
    exp_df = exp_df.join(
        options_df,
        (
            col("pipeline.parameter.optionCollection").getItem(
                regexp_replace("simpleParameter.value", ".0", "").cast(IntegerType())
            )
            == col("options.optionId")
        ),
        "left_outer",
    )
    exp_df = exp_df.withColumn(
        "category",
        when(
            col("observation_type") == "categorical",
            when(
                (col("pipeline.parameter.valueType") == "TEXT")
                | (~col("simpleParameter.value").rlike("(^\d+.\d+$)|(^\d+$)")),
                col("simpleParameter.value"),
            ).otherwise(
                when(
                    (
                        col("options.name").rlike("^\d+$")
                        & col("options.description").isNotNull()
                    ),
                    col("options.description"),
                ).otherwise(col("options.name"))
            ),
        ).otherwise(lit(None)),
    )

    exp_df = exp_df.withColumn(
        "data_point",
        when(
            col("observation_type") == "unidimensional",
            when(
                col("simpleParameter.value").like("%.%"), col("simpleParameter.value")
            ).otherwise(concat(col("simpleParameter.value"), lit(".0"))),
        ).otherwise(lit(None)),
    )

    exp_df = exp_df.withColumn(
        "text_value",
        when(col("observation_type") == "text", col("simpleParameter.value")).otherwise(
            lit(None)
        ),
    )
    return exp_df


def resolve_ontology_value(ontological_observation_df, ontology_df):
    ontology_df = ontology_df.distinct().alias("onto")
    if has_column(ontology_df, "ontologyParameter._sequenceID"):
        ontology_df = ontology_df.withColumn(
            "sequence_id", col("ontologyParameter._sequenceID")
        )
    id_vs_terms_df = (
        ontological_observation_df.withColumn("term", explode("ontologyParameter.term"))
        .withColumnRenamed("pos", "ontologyPos")
        .select(
            "observation_id",
            "ontologyParameter._parameterID",
            "ontologyParameter._sequenceID",
            "term",
        )
        .alias("temp")
    )
    id_vs_terms_df = id_vs_terms_df.join(
        ontology_df,
        (regexp_extract(col("temp.term"), "([A-Z]+:\d+)[\s:]*", 1) == col("onto.acc")),
    )
    id_vs_terms_df = id_vs_terms_df.withColumn("sub_term_id", col("onto.acc"))
    id_vs_terms_df = id_vs_terms_df.withColumn("sub_term_name", col("onto.name"))
    id_vs_terms_df = id_vs_terms_df.withColumn(
        "sub_term_description", col("onto.description")
    ).dropDuplicates()
    id_vs_terms_df = id_vs_terms_df.groupBy(
        col("observation_id"), col("temp._parameterID"), col("temp._sequenceID")
    ).agg(
        collect_list("sub_term_id").alias("sub_term_id"),
        collect_list("sub_term_name").alias("sub_term_name"),
        collect_list("sub_term_description").alias("sub_term_description"),
    )
    ontological_observation_df = ontological_observation_df.join(
        id_vs_terms_df, "observation_id", "left_outer"
    )
    ontological_observation_df = ontological_observation_df.withColumn(
        "observation_type", lit("ontological")
    )
    return ontological_observation_df


def resolve_time_series_value(time_series_observation_df: DataFrame):
    time_series_observation_df = time_series_observation_df.selectExpr(
        "*",
        "posexplode(seriesParameter.value) as (seriesParameterPos, seriesParameterValue)",
    )

    time_series_observation_df = time_series_observation_df.withColumn(
        "observation_id",
        md5(
            concat(
                col("observation_id"),
                lit("_seriesParameterValue_"),
                col("seriesParameterPos"),
            )
        ),
    )
    time_series_observation_df = time_series_observation_df.withColumn(
        "data_point", col("seriesParameterValue._VALUE")
    ).where(col("data_point").isNotNull())
    time_point_expr = None

    for index, format_str in enumerate(Constants.DATE_FORMATS):
        unix_timestamp_column = unix_timestamp(
            col("seriesParameterValue._incrementValue"), format_str
        )
        if time_point_expr is None:
            time_point_expr = when(
                unix_timestamp_column.isNotNull(), unix_timestamp_column
            )
        else:
            time_point_expr = time_point_expr.when(
                unix_timestamp_column.isNotNull(), unix_timestamp_column
            )
        if index == len(Constants.DATE_FORMATS) - 1:
            time_point_expr = time_point_expr.otherwise(lit(None))
    time_series_observation_df = time_series_observation_df.withColumn(
        "measured_at", time_point_expr
    )

    time_series_observation_df = time_series_observation_df.withColumn(
        "time_point", from_unixtime(time_point_expr)
    )

    time_series_observation_df = time_series_observation_df.withColumn(
        "date_of_experiment_seconds",
        unix_timestamp(col("date_of_experiment"), "yyyy-MM-dd"),
    )

    resolve_lights_out_udf = udf(_resolve_lights_out, LongType())

    lights_out_expr = (
        when(
            col("_dataSource") == "impc",
            when(
                col("procedure_stable_id").like("%IMPC_CAL%"),
                resolve_lights_out_udf(
                    "procedureMetadata", "date_of_experiment_seconds"
                ),
            ).otherwise(unix_timestamp(col("date_of_experiment"))),
        )
        .when(
            col("_dataSource") == "europhenome",
            when(
                col("phenotyping_center") == "HMGU",
                col("date_of_experiment_seconds") + lit(18 * 60 * 60),
            )
            .when(
                col("phenotyping_center").isin(["MRC", "WTSI", "ICS"]),
                col("date_of_experiment_seconds") + lit(19 * 60 * 60),
            )
            .otherwise(col("date_of_experiment_seconds")),
        )
        .otherwise(lit(0.0))
    )

    time_series_observation_df = time_series_observation_df.withColumn(
        "lights_out", lights_out_expr
    )
    time_series_observation_df = time_series_observation_df.withColumn(
        "discrete_point",
        when(
            col("time_point").isNull(), col("seriesParameterValue._incrementValue")
        ).otherwise((col("measured_at") - col("lights_out")) / 3600),
    )
    if has_column(time_series_observation_df, "_dateOfExperiment"):
        time_series_observation_df = time_series_observation_df.withColumn(
            "time_point",
            when(col("time_point").isNull(), col("_dateOfExperiment")).otherwise(
                col("time_point")
            ),
        )
    time_series_observation_df = time_series_observation_df.withColumn(
        "observation_type", lit("time_series")
    )
    return time_series_observation_df


def _resolve_lights_out(metadata_values, date_of_experiment_seconds):
    if metadata_values is None:
        return None
    lights_out = None
    lights_out_parameters = {
        "IMPC_CAL_010_001": "%H:%M %p",
        "IMPC_CAL_010_002": "%H:%M:%S",
        "IMPC_CAL_010_003": "%Y-%m-%dT%H:%M:%S%z",
    }
    for metadata_value in metadata_values:
        if metadata_value["_parameterID"] in lights_out_parameters.keys():
            if metadata_value["_parameterID"] == "IMPC_CAL_010_003":
                date_str = metadata_value["value"]
                pattern_str = lights_out_parameters[metadata_value["_parameterID"]]
                if date_str.count(":") > 2:
                    date_str = date_str.rsplit(":", 1)
                    date_str = "".join(date_str)
                if date_str.count(":") == 1:
                    pattern_str = "%Y-%m-%dT%H:%M"
                if date_str.endswith("Z"):
                    pattern_str = "%Y-%m-%dT%H:%M:%SZ"
                lights_out = datetime.datetime.strptime(
                    date_str, pattern_str
                ).timestamp()
            else:
                lights_out = datetime.datetime.strptime(
                    metadata_value["value"],
                    lights_out_parameters[metadata_value["_parameterID"]],
                ).time()
                lights_out = datetime.timedelta(
                    hours=lights_out.hour,
                    minutes=lights_out.minute,
                    seconds=lights_out.second,
                ).total_seconds()
                lights_out += date_of_experiment_seconds
            break
    return int(lights_out) if lights_out is not None else None


def resolve_image_record_value(image_record_observation_df: DataFrame):
    image_record_observation_df = image_record_observation_df.selectExpr(
        "*",
        "posexplode(seriesMediaParameter.value) as (seriesMediaParameterPos, seriesMediaParameterValue)",
    )
    image_record_observation_df = image_record_observation_df.withColumn(
        "observation_id",
        md5(
            concat(
                col("observation_id"),
                lit("_seriesMediaParameterValue_"),
                col("seriesMediaParameterPos"),
            )
        ),
    )
    image_record_observation_df = image_record_observation_df.withColumn(
        "download_file_path", col("seriesMediaParameterValue._URI")
    )
    image_record_observation_df = image_record_observation_df.withColumn(
        "file_type", col("seriesMediaParameterValue._fileType")
    )
    image_record_observation_df = image_record_observation_df.withColumn(
        "observation_type", lit("image_record")
    )
    return image_record_observation_df


def resolve_image_record_parameter_association(
    image_record_observation_df: DataFrame, simple_observations_df: DataFrame
):
    simple_df = simple_observations_df.alias("simple")
    image_df = image_record_observation_df.alias("image").withColumn(
        "parameterAsc", explode("image.seriesMediaParameterValue.parameterAssociation")
    )
    image_vs_simple_parameters_df = image_df.join(
        simple_df,
        (col("simple.experiment_id") == col("image.experiment_id"))
        & (col("simple.parameter_stable_id") == col("parameterAsc._parameterID")),
    )
    image_vs_simple_parameters_df = image_vs_simple_parameters_df.withColumn(
        "paramName", col("simple.parameter_name")
    )
    image_vs_simple_parameters_df = image_vs_simple_parameters_df.withColumn(
        "paramSeq", lit("0")
    )
    image_vs_simple_parameters_df = image_vs_simple_parameters_df.withColumn(
        "paramValue",
        when(col("data_point").isNotNull(), col("data_point")).otherwise(
            when(col("category").isNotNull(), col("category")).otherwise(
                col("text_value")
            )
        ),
    )
    image_vs_simple_parameters_df = image_vs_simple_parameters_df.groupBy(
        col("image.observation_id"), col("image.parameter_stable_id")
    ).agg(
        collect_set("parameterAsc._parameterID").alias("paramIDs"),
        collect_set("paramName").alias("paramNames"),
        collect_set("paramSeq").alias("paramSeqs"),
        collect_set("paramValue").alias("paramValues"),
    )
    image_vs_simple_parameters_df = image_vs_simple_parameters_df.withColumnRenamed(
        "observation_id", "img_observation_id"
    ).withColumnRenamed("parameter_stable_id", "img_parameter_stable_id")
    image_vs_simple_parameters_df = image_vs_simple_parameters_df.select(
        "img_observation_id",
        "img_parameter_stable_id",
        "paramIDs",
        "paramNames",
        "paramSeqs",
        "paramValues",
    )
    image_record_observation_df = image_record_observation_df.join(
        image_vs_simple_parameters_df,
        (
            image_record_observation_df["observation_id"]
            == image_vs_simple_parameters_df["img_observation_id"]
        )
        & (
            image_record_observation_df["parameter_stable_id"]
            == image_vs_simple_parameters_df["img_parameter_stable_id"]
        ),
        "left_outer",
    )
    image_record_observation_df = (
        image_record_observation_df.withColumnRenamed(
            "paramIDs", "parameter_association_stable_id"
        )
        .withColumnRenamed("paramNames", "parameter_association_name")
        .withColumnRenamed("paramSeqs", "parameter_association_sequence_id")
        .withColumnRenamed("paramValues", "parameter_association_value")
    )
    return image_record_observation_df


def resolve_simple_media_value(media_parameter_df):
    media_parameter_df = media_parameter_df.withColumn(
        "download_file_path", col("mediaParameter._URI")
    )
    media_parameter_df = media_parameter_df.withColumn(
        "file_type", col("mediaParameter._fileType")
    )
    media_parameter_df = media_parameter_df.withColumn(
        "observation_type", lit("image_record")
    )
    return media_parameter_df


def format_columns(experiments_df):
    experiments_df = experiments_df.withColumn(
        "weight",
        when(col("weight").like("%.%"), col("weight")).otherwise(
            concat(col("weight"), lit(".0"))
        ),
    )

    date_columns = ["date_of_birth", "weight_date", "date_of_experiment", "time_point"]

    for column in date_columns:
        experiments_df = experiments_df.withColumn(
            column,
            when(
                col(column).rlike(
                    "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
                ),
                col(column),
            ).otherwise(concat(col(column), lit("T00:00:00Z"))),
        )
    return experiments_df


def process_parameter_values(
    exp_df, pipeline_df, parameter_column, exp_type="experiment"
):
    parameter_cols = [
        "simpleParameter",
        "mediaParameter",
        "ontologyParameter",
        "seriesMediaParameter",
        "seriesParameter",
    ]
    if parameter_column not in exp_df.columns:
        return None
    parameter_observation_df = exp_df
    for column in parameter_cols:
        if column is not parameter_column:
            parameter_observation_df = parameter_observation_df.drop(column)
    parameter_observation_df = (
        parameter_observation_df.selectExpr(
            "*",
            "posexplode("
            + parameter_column
            + ") as (experimentPos, "
            + parameter_column
            + "Exploded )",
        )
        .withColumn(parameter_column, col(parameter_column + "Exploded"))
        .drop(parameter_column + "Exploded")
    )
    parameter_observation_df = parameter_observation_df.withColumn(
        "observation_id",
        md5(
            concat(
                col("experiment_id"),
                lit("_" + parameter_column + "_"),
                col("experimentPos"),
            )
        ),
    )
    if exp_type == "experiment":
        parameter_observation_df = map_experiment_columns(parameter_observation_df)
    else:
        parameter_observation_df = map_line_columns(parameter_observation_df)

    parameter_observation_df = add_impress_info(
        parameter_observation_df, pipeline_df, parameter_column, exp_type=exp_type
    )
    if has_column(parameter_observation_df, parameter_column + ".parameterStatus"):
        parameter_observation_df = parameter_observation_df.withColumn(
            "parameter_status", col(parameter_column + ".parameterStatus")
        )
    else:
        parameter_observation_df = parameter_observation_df.withColumn(
            "parameter_status", lit(None)
        )
    return parameter_observation_df


def get_body_weight_curve_observations(unidimensional_observations_df: DataFrame):
    body_weight_curve_df = None
    for (
        parameter_stable_id,
        parameter_data,
    ) in Constants.BODY_WEIGHT_CURVE_PARAMETERS.items():
        bwt_observations = unidimensional_observations_df.where(
            col("parameter_stable_id").isin(parameter_data["parameters"])
        )
        bwt_observations = bwt_observations.withColumn(
            "procedure_stable_id", lit(parameter_data["procedure_stable_id"])
        )
        bwt_observations = bwt_observations.withColumn(
            "parameter_stable_id", lit(parameter_stable_id)
        )
        bwt_observations = bwt_observations.withColumn(
            "procedure_group", lit(parameter_data["procedure_group"])
        )
        bwt_observations = bwt_observations.withColumn(
            "procedure_name", lit(parameter_data["procedure_name"])
        )
        bwt_observations = bwt_observations.withColumn(
            "parameter_name", lit(parameter_data["parameter_name"])
        )
        bwt_observations = bwt_observations.withColumn(
            "experiment_id",
            md5(concat(lit(parameter_stable_id + "_"), col("experiment_id"))),
        )
        bwt_observations = bwt_observations.withColumn(
            "observation_id",
            md5(concat(lit(parameter_stable_id + "_"), col("observation_id"))),
        )
        bwt_observations = bwt_observations.withColumn(
            "experiment_source_id",
            concat(lit(parameter_stable_id + "_"), col("experiment_source_id")),
        )
        bwt_observations = bwt_observations.withColumn("metadata_group", md5(lit("")))
        bwt_observations = bwt_observations.withColumn(
            "metadata",
            array(concat(lit("Source experiment id: "), col("experiment_id"))),
        )

        bwt_observations = bwt_observations.withColumn(
            "observation_type", lit("time_series")
        )
        bwt_observations = bwt_observations.withColumn(
            "discrete_point", col("age_in_weeks")
        )
        bwt_observations = bwt_observations.withColumn(
            "time_point", col("date_of_experiment")
        )
        if body_weight_curve_df is None:
            body_weight_curve_df = bwt_observations
        else:
            body_weight_curve_df = body_weight_curve_df.union(bwt_observations)
    return body_weight_curve_df.drop_duplicates()


def map_experiments_to_observations(
    experiment_df: DataFrame,
    line_df: DataFrame,
    mouse_df: DataFrame,
    embryo_df,
    allele_df: DataFrame,
    colony_df: DataFrame,
    pipeline_df: DataFrame,
    strain_df: DataFrame,
    ontology_df: DataFrame,
):
    experiment_df = experiment_df.withColumnRenamed(
        "_sourceFile", "experiment_source_file"
    )
    experiment_df = experiment_df.withColumnRenamed("unique_id", "experiment_id")
    experiment_df = experiment_df.alias("experiment")

    colony_df = colony_df.alias("colony")
    embryo_df = embryo_df.withColumn("_DOB", lit(None).cast(StringType()))
    embryo_df = embryo_df.withColumn("_VALUE", lit(None).cast(StringType()))
    mouse_df = mouse_df.withColumn("_stage", lit(None).cast(StringType()))
    mouse_df = mouse_df.withColumn("_stageUnit", lit(None).cast(StringType()))
    specimen_df = mouse_df.union(embryo_df.select(mouse_df.columns))
    map_strain_name_udf = udf(map_strain_name, StringType())
    specimen_df = specimen_df.withColumn(
        "_strainID",
        when(
            ((lower(col("_colonyID")) == "baseline") | (col("_isBaseline") == True)),
            map_strain_name_udf("_strainID"),
        ).otherwise(col("_strainID")),
    )
    specimen_df = specimen_df.withColumnRenamed("_sourceFile", "specimen_source_file")
    specimen_df = specimen_df.withColumnRenamed("unique_id", "specimen_id")
    specimen_df = specimen_df.alias("specimen")

    allele_df = allele_df.alias("allele")
    strain_df = strain_df.alias("strain")

    observation_df: DataFrame = experiment_df.join(
        specimen_df,
        (experiment_df["experiment._centreID"] == specimen_df["specimen._centreID"])
        & (
            experiment_df["experiment.specimenID"]
            == specimen_df["specimen._specimenID"]
        ),
        "left_outer",
    )
    observation_df = observation_df.join(
        colony_df,
        (observation_df["specimen._colonyID"] == colony_df["colony.colony_name"]),
        "left_outer",
    )
    observation_df = observation_df.where(
        col("colony.colony_name").isNotNull()
        | (
            (lower(col("specimen._colonyID")) == "baseline")
            | (col("specimen._isBaseline") == True)
        )
    )
    observation_df = observation_df.join(
        allele_df,
        observation_df["colony.allele_symbol"] == allele_df["allele.alleleSymbol"],
        "left_outer",
    )

    experimental_observation_df = observation_df.where(
        (lower(col("specimen._colonyID")) != "baseline")
        & (col("specimen._isBaseline") != True)
    ).join(
        strain_df,
        (col("colony.colony_background_strain") == col("strain.strainName"))
        | (concat(lit("MGI:"), col("specimen._strainID")) == col("strain.mgiStrainID"))
        | (col("specimen._strainID") == col("strain.strainName")),
        "left_outer",
    )

    baseline_observation_df = observation_df.where(
        (lower(col("specimen._colonyID")) == "baseline")
        | (col("specimen._isBaseline") == True)
    ).join(
        strain_df,
        (concat(lit("MGI:"), col("specimen._strainID")) == col("strain.mgiStrainID"))
        | (col("specimen._strainID") == col("strain.strainName")),
        "left_outer",
    )

    ## TODO fallback to imits when its missing and do the join again

    observation_df = baseline_observation_df.union(experimental_observation_df)

    simple_observation_df = process_parameter_values(
        observation_df, pipeline_df, "simpleParameter"
    )
    simple_observation_df = add_observation_type(simple_observation_df)
    simple_observation_df = resolve_simple_value(simple_observation_df, pipeline_df)
    simple_observation_df = unify_schema(simple_observation_df).select(
        Constants.OBSERVATION_COLUMNS
    )

    line_df = (
        line_df.withColumnRenamed("_sourceFile", "experiment_source_file")
        .withColumnRenamed("unique_id", "experiment_id")
        .withColumn("specimen_source_file", lit(None))
        .alias("experiment")
    )

    line_observation_df = line_df.join(
        colony_df, line_df["_colonyID"] == colony_df["colony.colony_name"]
    )

    line_observation_df = line_observation_df.join(
        strain_df,
        col("colony.colony_background_strain") == col("strain.strainName"),
        "left_outer",
    )

    line_observation_df = line_observation_df.join(
        allele_df,
        observation_df["colony.allele_symbol"] == allele_df["allele.alleleSymbol"],
        "left_outer",
    )
    line_simple_observation_df = process_parameter_values(
        line_observation_df, pipeline_df, "simpleParameter", exp_type="line"
    )
    line_simple_observation_df = add_observation_type(line_simple_observation_df)
    line_simple_observation_df = resolve_simple_value(
        line_simple_observation_df, pipeline_df
    )
    line_simple_observation_df = line_simple_observation_df.withColumn(
        "specimen_id", lit(None)
    )
    line_simple_observation_df = unify_schema(line_simple_observation_df).select(
        Constants.OBSERVATION_COLUMNS
    )

    simple_observation_df = simple_observation_df.union(line_simple_observation_df)

    body_weight_curve_observation_df = get_body_weight_curve_observations(
        simple_observation_df.where(col("observation_type") == "unidimensional")
    )

    simple_media_observation_df = process_parameter_values(
        observation_df, pipeline_df, "mediaParameter"
    )
    if simple_media_observation_df is not None:
        simple_media_observation_df = resolve_simple_media_value(
            simple_media_observation_df
        )
        simple_media_observation_df = unify_schema(simple_media_observation_df).select(
            Constants.OBSERVATION_COLUMNS
        )

    ontological_observation_df = process_parameter_values(
        observation_df, pipeline_df, "ontologyParameter"
    )
    ontological_observation_df = resolve_ontology_value(
        ontological_observation_df, ontology_df
    )
    ontological_observation_df = unify_schema(ontological_observation_df).select(
        Constants.OBSERVATION_COLUMNS
    )

    time_series_observation_df = process_parameter_values(
        observation_df, pipeline_df, "seriesParameter"
    )
    time_series_observation_df = resolve_time_series_value(time_series_observation_df)
    time_series_observation_df = unify_schema(time_series_observation_df).select(
        Constants.OBSERVATION_COLUMNS
    )

    line_time_series_observation_df = process_parameter_values(
        line_observation_df, pipeline_df, "seriesParameter", exp_type="line"
    )
    line_time_series_observation_df = resolve_time_series_value(
        line_time_series_observation_df
    )
    line_time_series_observation_df = line_time_series_observation_df.withColumn(
        "specimen_id", lit(None)
    )
    line_time_series_observation_df = unify_schema(
        line_time_series_observation_df
    ).select(Constants.OBSERVATION_COLUMNS)
    time_series_observation_df = time_series_observation_df.union(
        line_time_series_observation_df
    )

    image_record_observation_df = process_parameter_values(
        observation_df, pipeline_df, "seriesMediaParameter"
    )
    image_record_observation_df = resolve_image_record_value(
        image_record_observation_df
    )
    image_record_observation_df = resolve_image_record_parameter_association(
        image_record_observation_df, simple_observation_df
    )
    image_record_observation_df = unify_schema(image_record_observation_df).select(
        Constants.OBSERVATION_COLUMNS
    )

    observation_df = (
        simple_observation_df.union(ontological_observation_df)
        .union(image_record_observation_df)
        .union(time_series_observation_df)
        .union(body_weight_curve_observation_df)
    )
    if simple_media_observation_df is not None:
        observation_df = observation_df.union(simple_media_observation_df)
    observation_df = observation_df.where(col("parameter_status").isNull())
    observation_df = format_columns(observation_df).drop_duplicates()
    observation_df = observation_df.withColumn(
        "experiment_source_file",
        regexp_extract(col("experiment_source_file"), "(.*\/)(.*\/.*\.xml)", idx=2),
    )
    observation_df = observation_df.withColumn(
        "specimen_source_file",
        regexp_extract(col("specimen_source_file"), "(.*\/)(.*\/.*\.xml)", idx=2),
    )
    observation_df = observation_df.withColumn("life_stage_name", lit(None))
    observation_df = observation_df.withColumn("life_stage_acc", lit(None))
    for life_stage in Constants.PROCEDURE_LIFE_STAGE_MAPPER:
        life_stage_name = life_stage["lifeStage"]
        observation_df = observation_df.withColumn(
            "life_stage_name",
            when(
                col("life_stage_name").isNull(),
                when(
                    (
                        col("procedure_stable_id").rlike(
                            "|".join(
                                [f"({ proc })" for proc in life_stage["procedures"]]
                            )
                        )
                        | (col("developmental_stage_name") == life_stage_name)
                    ),
                    lit(life_stage_name),
                ).otherwise(lit(None)),
            ).otherwise(col("life_stage_name")),
        )
        observation_df = observation_df.withColumn(
            "life_stage_acc",
            when(
                col("life_stage_acc").isNull(),
                when(
                    (
                        col("procedure_stable_id").rlike(
                            "|".join(
                                [f"({ proc })" for proc in life_stage["procedures"]]
                            )
                        )
                        | (col("developmental_stage_name") == life_stage_name)
                    ),
                    lit(life_stage["lifeStageAcc"]),
                ).otherwise(lit(None)),
            ).otherwise(col("life_stage_acc")),
        )
    observation_df = observation_df.withColumn(
        "life_stage_name",
        when((col("life_stage_name").isNull()), lit("Early adult")).otherwise(
            col("life_stage_name")
        ),
    )
    observation_df = observation_df.withColumn(
        "life_stage_acc",
        when((col("life_stage_acc").isNull()), lit("IMPCLS:0005")).otherwise(
            col("life_stage_acc")
        ),
    )
    return observation_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
