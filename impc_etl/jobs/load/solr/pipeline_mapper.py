"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
import sys


COLUMN_MAPPER = {
    "schedule_key": "scheduleId",
    "stage": "schedule.stage",
    "stage_label": "schedule.timeLabel",
    "pipeline_id": "pipelineId",
    "pipeline_stable_key": "pipelineId",
    "pipeline_stable_id": "pipelineKey",
    "pipeline_name": "name",
    "procedure_id": "procedureId",
    "procedure_stable_key": "procedureId",
    "procedure_stable_id": "procedure.procedureKey",
    "procedure_name": "procedure.name",
    "parameter_id": "parameterId",
    "parameter_stable_key": "parameter.parameterId",
    "parameter_stable_id": "parameter.parameterKey",
    "parameter_name": "parameter.name",
    "description": "parameter.description",
    "data_type": "parameter.valueType",
    "required": "parameter.isRequired",
    "annotate": "parameter.isAnnotation",
    "media": "parameter.isMedia",
    "has_options": "parameter.isOption",
    "increment": "parameter.isIncrement",
}

MISSING = {
    "unit_y": "if increment != null parameter.unit else null",
    "unit_x": "if increment != null increment.unit else parameter.unit",
    "metadata": "parameter.type == 'procedureMetadata'",
    "observation_type": "first observations.observation_type where fully_qualified_name",
    "fully_qualified_name": "concat(pipeline_key, procedure_key, parameter_key)",
    "categories": "",
    "mp_id": "",
    "mp_term": "",
    "top_level_mp_id": "",
    "top_level_mp_term": "",
    "top_level_mp_term_synonym": "",
    "intermediate_mp_id": "",
    "intermediate_mp_term": "",
    "abnormal_mp_id": "",
    "abnormal_mp_term": "",
    "decreased_mp_id": "",
    "decreased_mp_term": "",
    "increased_mp_id": "",
    "increased_mp_term": "",
    "anatomy_id": "",
    "anatomy_term": "",
    "emap_id": "",
    "ma_id": "",
    "ma_term": "",
}


def main(argv):
    """
    Solr Core loader
    :param list argv: the list elements should be:
                    [1]: source IMPC parquet file
                    [2]: Output Path
    """
    pipeline_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    pipeline_df = spark.read.parquet(pipeline_parquet_path)
    pipeline_df = pipeline_df.select("")
    pipeline_df.write.parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
