"""
Module to group extraction jobs for input data. Most of these jobs ara simple read write operation that take some format
in and generate a Parquet file with a slightly different schema.
"""

from impc_etl.jobs.extract.colony_tracking_extractor import ColonyTrackingExtractor
from impc_etl.jobs.extract.experiment_extractor import (
    SpecimenLevelExperimentExtractor,
    LineLevelExperimentExtractor,
)
from impc_etl.jobs.extract.gene_production_status_extractor import (
    GeneProductionStatusExtractor,
)
from impc_etl.jobs.extract.impress_extractor import ImpressExtractor
from impc_etl.jobs.extract.mgi_gene_pheno_extractor import MGIGenePhenoReportExtractor
from impc_etl.jobs.extract.mgi_homology_extractor import MGIHomologyReportExtractor
from impc_etl.jobs.extract.mgi_mrk_list_extractor import MGIMarkerListReportExtractor
from impc_etl.jobs.extract.mgi_phenotypic_allele import MGIPhenotypicAlleleExtractor
from impc_etl.jobs.extract.mgi_strain_extractor import MGIStrainReportExtractor
from impc_etl.jobs.extract.ontology_metadata_extractor import (
    OntologyMetadataExtractor,
)
from impc_etl.jobs.extract.product_report_extractor import ProductReportExtractor
from impc_etl.jobs.extract.specimen_extractor import (
    MouseSpecimenExtractor,
    EmbryoSpecimenExtractor,
)
