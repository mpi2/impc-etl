"""
SOLR module
   Generates the required Solr cores
"""
from pyspark.sql import DataFrame, SparkSession
import sys


COLUMN_MAPPER = {
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
    "metadata": "",
    "increment": "parameter.isIncrement",
    "unit_x": "parameter.unit",
    "observation_type": "",
    "fully_qualified_name": "",
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
