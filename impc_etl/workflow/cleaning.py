from impc_etl.jobs.clean import IMPCSpecimenCleaner
from impc_etl.jobs.clean.experiment_cleaner import IMPCExperimentCleaner


class SpecimenExperimentCleaner(IMPCExperimentCleaner):
    experiment_type = "specimen_level"


class LineExperimentCleaner(IMPCExperimentCleaner):
    experiment_type = "line_level"


class MouseCleaner(IMPCSpecimenCleaner):
    name = "IMPC_Mouse_Specimen_Cleaner"
    specimen_type = "mouse"


class EmbryoCleaner(IMPCSpecimenCleaner):
    name = "IMPC_Embryo_Specimen_Cleaner"
    specimen_type = "embryo"
