"""
Module to group extraction jobs for input data. Most of these jobs ara simple read write operation that take some format
in and generate a Parquet file with a slightly different schema.
"""

from impc_etl.jobs.extract.product_report_extractor import ProductReportExtractor
from impc_etl.jobs.extract.dcc_specimen_extractor import DCCSpecimenExtractor
from impc_etl.jobs.extract.dcc_experiment_extractor import DCCExperimentExtractor
from impc_etl.jobs.extract.colony_tracking_extractor import ColonyTrackingExtractor
from impc_etl.jobs.extract.gene_production_status_extractor import (
    GeneProductionStatusExtractor,
)
