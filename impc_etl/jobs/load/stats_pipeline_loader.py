"""
Stats pipeline input loader

"""
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    concat,
    col,
    when,
    lit,
    explode,
    regexp_extract,
    size,
    upper,
    lower,
)

CSV_FIELDS = [
    "allele_accession_id",
    "gene_accession_id",
    "project_name",
    "strain_accession_id",
    "litter_id",
    "phenotyping_center",
    "external_sample_id",
    "developmental_stage_name",
    "developmental_stage_acc",
    "datasource_name",
    "age_in_days",
    "date_of_birth",
    "metadata",
    "metadata_group",
    "sequence_id",
    "experiment_source_id",
    "gene_symbol",
    "biological_sample_group",
    "sex",
    "allele_symbol",
    "production_center",
    "age_in_weeks",
    "weight",
    "weight_date",
    "weight_days_old",
    "weight_parameter_stable_id",
    "colony_id",
    "zygosity",
    "allelic_composition",
    "pipeline_name",
    "pipeline_stable_id",
    "procedure_name",
    "procedure_stable_id",
    "procedure_group",
    "parameter_name",
    "parameter_stable_id",
    "observation_type",
    "data_point",
    "text_value",
    "category",
    "strain_name",
    "genetic_background",
    "date_of_experiment",
]


def load(
    experiment_df: DataFrame,
    mouse_df: DataFrame,
    allele_df: DataFrame,
    colony_df: DataFrame,
    pipeline_df: DataFrame,
    strain_df: DataFrame,
) -> DataFrame:

    experiment_df = experiment_df.alias("experiment")
    colony_df = colony_df.alias("colony")
    mouse_df = mouse_df.alias("specimen")
    allele_df = allele_df.alias("allele")
    strain_df = strain_df.alias("strain")

    mouse_df = mouse_df.join(
        colony_df,
        mouse_df["specimen._colonyID"] == colony_df["colony.colony_name"],
        "left_outer",
    )
    mice_experiments_df: DataFrame = experiment_df.join(
        mouse_df,
        (experiment_df["experiment._centreID"] == mouse_df["specimen._centreID"])
        & (experiment_df["experiment.specimenID"] == mouse_df["specimen._specimenID"]),
        "left_outer",
    )
    mice_experiments_df = mice_experiments_df.join(
        allele_df,
        mice_experiments_df["colony.allele_symbol"]
        == allele_df["allele.allele_symbol"],
        "left_outer",
    )

    mice_experiments_df_exp = mice_experiments_df.where(
        (lower(col("specimen._colonyID")) != "baseline")
        & (col("specimen._isBaseline") != True)
    ).join(
        strain_df,
        col("colony.colony_background_strain") == col("strain.strainName"),
        "left_outer",
    )

    mice_experiments_df_baseline = mice_experiments_df.where(
        (lower(col("specimen._colonyID")) == "baseline")
        | (col("specimen._isBaseline") == True)
    ).join(
        strain_df, col("specimen._strainID") == col("strain.mgiStrainID"), "left_outer"
    )

    mice_experiments_df = mice_experiments_df_baseline.unionAll(mice_experiments_df_exp)

    mice_experiments_df = mice_experiments_df.drop(
        "ontologyParameter",
        "procedureMetadata",
        "seriesMediaParameter",
        "seriesParameter",
    )
    mice_experiments_df = rename_columns(mice_experiments_df)
    mice_experiments_df = mice_experiments_df.withColumn(
        "simpleParameter", explode("simpleParameter")
    )
    mice_experiments_df = add_impress_info(mice_experiments_df, pipeline_df)
    mice_experiments_df = mice_experiments_df.drop("pipeline.weight")
    mice_experiments_df = format_date_columns(
        ["date_of_birth", "weight_date", "date_of_experiment"], mice_experiments_df
    )
    return mice_experiments_df.select(CSV_FIELDS)


def rename_columns(experiments_df: DataFrame):
    experiments_df = experiments_df.withColumn(
        "biological_sample_group",
        when(col("_isBaseline") == True, lit("control")).otherwise("experimental"),
    )
    experiments_df = experiments_df.withColumn(
        "allele_accession_id",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            col("allele.allele_mgi_accession_id")
        ),
    )
    experiments_df = experiments_df.withColumn(
        "gene_accession_id",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            col("allele.marker_mgi_accession_id")
        ),
    )
    experiments_df = experiments_df.withColumn(
        "gene_symbol",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            col("colony.marker_symbol")
        ),
    )
    experiments_df = experiments_df.drop(col("colony.allele_symbol"))
    experiments_df = experiments_df.withColumn(
        "allele_symbol",
        when(col("biological_sample_group") == "control", lit(None)).otherwise(
            col("allele.allele_symbol")
        ),
    )
    experiments_df = experiments_df.withColumn("zygosity", col("specimen._zygosity"))

    experiments_df = experiments_df.withColumn(
        "zygosity",
        when(col("zygosity") == "heterozygous", lit("heterozygote")).otherwise(
            col("zygosity")
        ),
    )

    experiments_df = experiments_df.withColumn(
        "zygosity",
        when(col("zygosity") == "homozygous", lit("homozygote")).otherwise(
            col("zygosity")
        ),
    )

    experiments_df = experiments_df.withColumn(
        "zygosity",
        when(col("zygosity") == "hemizygous", lit("hemizygote")).otherwise(
            col("zygosity")
        ),
    )

    experiments_df = experiments_df.withColumn(
        "zygosity",
        when(col("zygosity") == "wild type", lit("homozygote")).otherwise(
            col("zygosity")
        ),
    )

    experiments_df = experiments_df.withColumn(
        "project_name", col("experiment._project")
    )
    experiments_df = experiments_df.withColumn(
        "strain_accession_id", col("strain.mgiStrainID")
    )
    experiments_df = experiments_df.withColumn("litter_id", col("specimen._litterId"))
    experiments_df = experiments_df.withColumn(
        "phenotyping_center", col("specimen._phenotypingCentre")
    )
    experiments_df = experiments_df.withColumn(
        "external_sample_id", col("specimen._specimenID")
    )
    experiments_df = experiments_df.withColumn(
        "datasource_name", upper(col("experiment._dataSource"))
    )
    experiments_df = experiments_df.withColumn(
        "age_in_days", col("experiment.ageInDays")
    )
    experiments_df = experiments_df.withColumn(
        "date_of_experiment", col("experiment._dateOfExperiment")
    )
    experiments_df = experiments_df.withColumn("date_of_birth", col("specimen._DOB"))
    experiments_df = experiments_df.withColumn(
        "metadata_group", col("experiment.metadataGroup")
    )
    experiments_df = experiments_df.withColumn(
        "sequence_id", col("experiment._sequenceID")
    )
    experiments_df = experiments_df.withColumn(
        "experiment_source_id", col("experiment._experimentID")
    )

    experiments_df = experiments_df.withColumn("sex", col("specimen._gender"))

    experiments_df = experiments_df.withColumn(
        "allelic_composition", col("specimen.allelicComposition")
    )
    experiments_df = experiments_df.withColumn(
        "colony_id",
        when(lower(col("specimen._colonyID")) == "baseline", lit("baseline")).otherwise(
            when(col("specimen._colonyID").isNull(), "unknown").otherwise(
                col("specimen._colonyID")
            )
        ),
    )

    experiments_df = experiments_df.drop(col("allele.production_centre"))
    experiments_df = experiments_df.withColumn(
        "production_center", col("specimen._productionCentre")
    )

    experiments_df = experiments_df.withColumn(
        "age_in_weeks", col("experiment.ageInWeeks")
    )

    # experiments_df = experiments_df.withColumn(
    #     "metadataStr", concat_ws(", ", experiments_df["metadata"])
    # )
    # experiments_df = experiments_df.drop("metadata")
    # experiments_df = experiments_df.withColumnRenamed("metadataStr", "metadata")
    experiments_df = experiments_df.withColumnRenamed("weight", "weightStruct")
    experiments_df = experiments_df.withColumn(
        "weight", col("weightStruct.weightValue")
    )
    experiments_df = experiments_df.withColumn(
        "weight_date", col("weightStruct.weightDate")
    )
    experiments_df = experiments_df.withColumn(
        "weight_days_old", col("weightStruct.weightDaysOld")
    )
    experiments_df = experiments_df.withColumn(
        "weight_parameter_stable_id", col("weightStruct.weightParameterID")
    )
    experiments_df = experiments_df.withColumn(
        "strain_name",
        when(
            (col("colony_id") == "baseline") | (col("specimen._isBaseline") == True),
            col("strain.strainName"),
        ).otherwise(col("colony.colony_background_strain")),
    )
    experiments_df = experiments_df.withColumn(
        "genetic_background",
        when(
            (col("colony_id") == "baseline") | (col("specimen._isBaseline") == True),
            concat(lit("involves: "), col("strain.strainName")),
        ).otherwise(col("colony.genetic_background")),
    )
    return experiments_df


def format_date_columns(date_columns, stats_input_df: DataFrame):
    for column in date_columns:
        stats_input_df = stats_input_df.withColumn(
            column, concat(col(column), lit("T00:00:00Z"))
        )
    return stats_input_df


def add_impress_info(experiments_df, pipeline_df):
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
        (col("simpleParameter._parameterID") == col("pipeline.parameter.parameterKey"))
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
    experiments_df = experiments_df.withColumn(
        "observation_type",
        when(
            col("pipeline.parameter.valueType") != "TEXT", lit("unidimensional")
        ).otherwise(
            when(
                (size(col("pipeline.parameter.optionCollection")) > 0)
                | (col("pipeline.parameter.parameterKey") == "IMPC_EYE_092_001"),
                lit("categorical"),
            ).otherwise(lit("text"))
        ),
    )

    experiments_df = experiments_df.withColumn(
        "weight",
        when(col("weight").like("%.%"), col("weight")).otherwise(
            concat(col("weight"), lit(".0"))
        ),
    )

    experiments_df = experiments_df.withColumn(
        "data_point",
        when(
            col("observation_type") == "unidimensional",
            when(
                col("simpleParameter.value").like("%.%"), col("simpleParameter.value")
            ).otherwise(concat(col("simpleParameter.value"), lit(".0"))),
        ).otherwise(lit(None)),
    )

    experiments_df = experiments_df.withColumn(
        "text_value",
        when(col("observation_type") == "text", col("simpleParameter.value")).otherwise(
            lit(None)
        ),
    )

    experiments_df = experiments_df.withColumn(
        "category",
        when(
            col("observation_type") == "categorical", col("simpleParameter.value")
        ).otherwise(lit(None)),
    )

    return experiments_df


def main(argv):
    experiment_parquet_path = argv[1]
    mouse_parquet_path = argv[2]
    allele_parquet_path = argv[3]
    colony_parquet_path = argv[4]
    pipeline_parquet_path = argv[5]
    strain_parquet_path = argv[6]
    output_path = argv[7]
    spark = SparkSession.builder.getOrCreate()
    experiment_df = spark.read.parquet(experiment_parquet_path)
    mouse_df = spark.read.parquet(mouse_parquet_path)
    allele_df = spark.read.parquet(allele_parquet_path)
    colony_df = spark.read.parquet(colony_parquet_path)
    pipeline_df = spark.read.parquet(pipeline_parquet_path)
    strain_df = spark.read.parquet(strain_parquet_path)

    experiment_clean_df = load(
        experiment_df, mouse_df, allele_df, colony_df, pipeline_df, strain_df
    )
    experiment_clean_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
