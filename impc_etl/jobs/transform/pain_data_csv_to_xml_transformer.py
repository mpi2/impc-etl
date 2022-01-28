"""
Python task to transform the Pain Working Group data (Formalin, VonFrey and Hargreaves procedures)
from the CSV format provided by them to a standard experimental data XML.
"""
import csv
import os
import sys
from xml.etree.ElementTree import Element, ElementTree

import luigi
from lxml import etree

from impc_etl.workflow.config import ImpcConfig

CENTRE_ID_MAP = {"JAX": "J", "HAR": "H", "BCM": "Bcm", "UCD": "Ucd", "TCP": "Tcp"}
CENTRE_PROJECT_MAP = {
    "JAX": "PWG",
    "HAR": "PWG",
    "BCM": "PWG",
    "UCD": "PWG",
    "TCP": "PWG",
}
CENTRE_PIPELINE_MAP = {
    "JAX": "JAX_001",
    "HAR": "HRWL_001",
    "BCM": "BCM_001",
    "UCD": "UCD_001",
    "TCP": "TCP_001",
}
CENTRE_PROCEDURE_MAP = {
    "JAX": {"VFR": "JAX_VFR_001", "HRG": "JAX_HRG_001", "FOR": "JAX_FOR_001"},
    "HAR": {"VFR": "HRWL_VFR_001", "FOR": "HRWL_FOR_001"},
    "BCM": {"FOR": "BCM_FOR_001"},
    "UCD": {"HRG": "UCD_HRG_001", "VFR": "UCD_VFR_001"},
    "TCP": {"HRG": "TCP_HRG_001", "VFR": "TCP_VFR_001"},
}
SIMPLE_PARAMETERS = [
    "JAX_VFR_001_001",
    "JAX_VFR_002_001",
    "JAX_VFR_003_001",
    "JAX_VFR_004_001",
    "JAX_VFR_005_001",
    "JAX_VFR_006_001",
    "JAX_VFR_007_001",
    "JAX_VFR_008_001",
    "JAX_VFR_009_001",
    "JAX_VFR_010_001",
    "JAX_VFR_011_001",
    "JAX_VFR_012_001",
    "JAX_HRG_002_001",
    "JAX_HRG_004_001",
    "JAX_HRG_006_001",
    "JAX_FOR_002_001",
    "HRWL_VFR_005_001",
    "HRWL_VFR_006_001",
    "HRWL_VFR_011_001",
    "HRWL_VFR_012_001",
    "HRWL_VFR_017_001",
    "HRWL_VFR_018_001",
    "HRWL_FOR_002_001",
    "HRWL_FOR_024_001",
    "HRWL_FOR_026_001",
    "HRWL_FOR_027_001",
    "BCM_FOR_002_001",
    "BCM_FOR_024_001",
    "BCM_FOR_026_001",
    "BCM_FOR_027_001",
    "BCM_FOR_029_001",
    "BCM_FOR_030_001",
    "UCD_HRG_002_001",
    "UCD_HRG_004_001",
    "UCD_HRG_006_001",
    "UCD_VFR_003_001",
    "UCD_VFR_005_001",
    "UCD_VFR_008_001",
    "UCD_VFR_010_001",
    "UCD_VFR_013_001",
    "UCD_VFR_015_001",
    "TCP_HRG_002_001",
    "TCP_HRG_004_001",
    "TCP_HRG_006_001",
    "TCP_VFR_003_001",
    "TCP_VFR_005_001",
    "TCP_VFR_008_001",
    "TCP_VFR_010_001",
    "TCP_VFR_013_001",
    "TCP_VFR_015_001",
]
SERIES_PARAMETERS = [
    "JAX_HRG_001_001",
    "JAX_HRG_003_001",
    "JAX_HRG_005_001",
    "JAX_FOR_001_001",
    "HRWL_VFR_001_001",
    "HRWL_VFR_002_001",
    "HRWL_VFR_003_001",
    "HRWL_VFR_004_001",
    "HRWL_VFR_007_001",
    "HRWL_VFR_008_001",
    "HRWL_VFR_009_001",
    "HRWL_VFR_010_001",
    "HRWL_VFR_013_001",
    "HRWL_VFR_014_001",
    "HRWL_VFR_015_001",
    "HRWL_VFR_016_001",
    "HRWL_FOR_001_001",
    "HRWL_FOR_025_001",
    "BCM_FOR_001_001",
    "BCM_FOR_025_001",
    "BCM_FOR_028_001",
    "UCD_HRG_001_001",
    "UCD_HRG_003_001",
    "UCD_HRG_005_001",
    "UCD_VFR_001_001",
    "UCD_VFR_002_001",
    "UCD_VFR_004_001",
    "UCD_VFR_006_001",
    "UCD_VFR_007_001",
    "UCD_VFR_009_001",
    "UCD_VFR_011_001",
    "UCD_VFR_012_001",
    "UCD_VFR_014_001",
    "TCP_HRG_001_001",
    "TCP_HRG_003_001",
    "TCP_HRG_005_001",
    "TCP_VFR_001_001",
    "TCP_VFR_002_001",
    "TCP_VFR_004_001",
    "TCP_VFR_006_001",
    "TCP_VFR_007_001",
    "TCP_VFR_009_001",
    "TCP_VFR_011_001",
    "TCP_VFR_012_001",
    "TCP_VFR_014_001",
]
METADATA_PARAMETERS = [
    "JAX_VFR_013_001",
    "JAX_VFR_014_001",
    "JAX_VFR_015_001",
    "JAX_VFR_016_001",
    "JAX_VFR_017_001",
    "JAX_VFR_018_001",
    "JAX_VFR_019_001",
    "JAX_VFR_020_001",
    "JAX_VFR_021_001",
    "JAX_VFR_022_001",
    "JAX_VFR_023_001",
    "JAX_VFR_024_001",
    "JAX_VFR_025_001",
    "JAX_VFR_026_001",
    "JAX_VFR_027_001",
    "JAX_VFR_028_001",
    "JAX_VFR_029_001",
    "JAX_VFR_030_001",
    "JAX_VFR_031_001",
    "JAX_VFR_032_001",
    "JAX_VFR_033_001",
    "JAX_VFR_034_001",
    "JAX_VFR_035_001",
    "JAX_VFR_036_001",
    "JAX_VFR_037_001",
    "JAX_VFR_038_001",
    "JAX_VFR_039_001",
    "JAX_VFR_040_001",
    "JAX_HRG_007_001",
    "JAX_HRG_008_001",
    "JAX_HRG_009_001",
    "JAX_HRG_010_001",
    "JAX_HRG_011_001",
    "JAX_HRG_012_001",
    "JAX_HRG_013_001",
    "JAX_HRG_014_001",
    "JAX_HRG_015_001",
    "JAX_HRG_016_001",
    "JAX_HRG_017_001",
    "JAX_HRG_018_001",
    "JAX_HRG_019_001",
    "JAX_HRG_020_001",
    "JAX_HRG_021_001",
    "JAX_HRG_022_001",
    "JAX_HRG_023_001",
    "JAX_HRG_024_001",
    "JAX_HRG_025_001",
    "JAX_HRG_026_001",
    "JAX_HRG_027_001",
    "JAX_HRG_028_001",
    "JAX_HRG_029_001",
    "JAX_HRG_030_001",
    "JAX_HRG_031_001",
    "JAX_HRG_032_001",
    "JAX_HRG_033_001",
    "JAX_HRG_034_001",
    "JAX_HRG_035_001",
    "JAX_FOR_008_001",
    "JAX_FOR_009_001",
    "JAX_FOR_011_001",
    "JAX_FOR_007_001",
    "JAX_FOR_017_001",
    "JAX_FOR_020_001",
    "JAX_FOR_021_001",
    "JAX_FOR_013_001",
    "JAX_FOR_022_001",
    "JAX_FOR_019_001",
    "JAX_FOR_023_001",
    "HRWL_VFR_019_001",
    "HRWL_VFR_020_001",
    "HRWL_VFR_021_001",
    "HRWL_VFR_022_001",
    "HRWL_VFR_023_001",
    "HRWL_VFR_024_001",
    "HRWL_VFR_025_001",
    "HRWL_VFR_026_001",
    "HRWL_VFR_027_001",
    "HRWL_VFR_028_001",
    "HRWL_VFR_029_001",
    "HRWL_VFR_030_001",
    "HRWL_VFR_031_001",
    "HRWL_VFR_032_001",
    "HRWL_VFR_033_001",
    "HRWL_VFR_034_001",
    "HRWL_VFR_035_001",
    "HRWL_VFR_036_001",
    "HRWL_VFR_037_001",
    "HRWL_VFR_038_001",
    "HRWL_VFR_039_001",
    "HRWL_VFR_040_001",
    "HRWL_VFR_041_001",
    "HRWL_VFR_042_001",
    "HRWL_FOR_008_001",
    "HRWL_FOR_009_001",
    "HRWL_FOR_011_001",
    "HRWL_FOR_007_001",
    "HRWL_FOR_017_001",
    "HRWL_FOR_020_001",
    "HRWL_FOR_021_001",
    "HRWL_FOR_013_001",
    "HRWL_FOR_022_001",
    "HRWL_FOR_019_001",
    "HRWL_FOR_023_001",
    "HRWL_FOR_024_001",
    "HRWL_FOR_025_001",
    "HRWL_FOR_026_001",
    "HRWL_FOR_027_001",
    "BCM_FOR_008_001",
    "BCM_FOR_009_001",
    "BCM_FOR_011_001",
    "BCM_FOR_007_001",
    "BCM_FOR_017_001",
    "BCM_FOR_020_001",
    "BCM_FOR_021_001",
    "BCM_FOR_013_001",
    "BCM_FOR_022_001",
    "BCM_FOR_019_001",
    "BCM_FOR_023_001",
    "UCD_HRG_007_001",
    "UCD_HRG_008_001",
    "UCD_HRG_009_001",
    "UCD_HRG_010_001",
    "UCD_HRG_011_001",
    "UCD_HRG_012_001",
    "UCD_HRG_013_001",
    "UCD_HRG_014_001",
    "UCD_HRG_015_001",
    "UCD_HRG_016_001",
    "UCD_HRG_017_001",
    "UCD_HRG_018_001",
    "UCD_HRG_019_001",
    "UCD_HRG_020_001",
    "UCD_HRG_021_001",
    "UCD_HRG_022_001",
    "UCD_HRG_023_001",
    "UCD_HRG_024_001",
    "UCD_HRG_025_001",
    "UCD_HRG_026_001",
    "UCD_HRG_027_001",
    "UCD_HRG_028_001",
    "UCD_HRG_029_001",
    "UCD_HRG_030_001",
    "UCD_HRG_031_001",
    "UCD_HRG_032_001",
    "UCD_HRG_033_001",
    "UCD_HRG_034_001",
    "UCD_HRG_035_001",
    "UCD_VFR_016_001",
    "UCD_VFR_017_001",
    "UCD_VFR_018_001",
    "UCD_VFR_019_001",
    "UCD_VFR_020_001",
    "UCD_VFR_021_001",
    "UCD_VFR_022_001",
    "UCD_VFR_023_001",
    "UCD_VFR_024_001",
    "UCD_VFR_025_001",
    "UCD_VFR_026_001",
    "UCD_VFR_027_001",
    "UCD_VFR_028_001",
    "UCD_VFR_029_001",
    "UCD_VFR_030_001",
    "UCD_VFR_031_001",
    "UCD_VFR_032_001",
    "UCD_VFR_033_001",
    "UCD_VFR_034_001",
    "UCD_VFR_035_001",
    "UCD_VFR_036_001",
    "UCD_VFR_037_001",
    "UCD_VFR_038_001",
    "UCD_VFR_039_001",
    "UCD_VFR_040_001",
    "UCD_VFR_041_001",
    "UCD_VFR_042_001",
    "UCD_VFR_043_001",
    "UCD_VFR_044_001",
    "UCD_VFR_045_001",
    "UCD_VFR_046_001",
    "UCD_VFR_047_001",
    "TCP_HRG_007_001",
    "TCP_HRG_008_001",
    "TCP_HRG_009_001",
    "TCP_HRG_010_001",
    "TCP_HRG_011_001",
    "TCP_HRG_012_001",
    "TCP_HRG_013_001",
    "TCP_HRG_014_001",
    "TCP_HRG_015_001",
    "TCP_HRG_016_001",
    "TCP_HRG_017_001",
    "TCP_HRG_018_001",
    "TCP_HRG_019_001",
    "TCP_HRG_020_001",
    "TCP_HRG_021_001",
    "TCP_HRG_022_001",
    "TCP_HRG_023_001",
    "TCP_HRG_024_001",
    "TCP_HRG_025_001",
    "TCP_HRG_026_001",
    "TCP_HRG_027_001",
    "TCP_HRG_028_001",
    "TCP_HRG_029_001",
    "TCP_HRG_030_001",
    "TCP_HRG_031_001",
    "TCP_HRG_032_001",
    "TCP_HRG_033_001",
    "TCP_HRG_034_001",
    "TCP_HRG_035_001",
    "TCP_VFR_016_001",
    "TCP_VFR_017_001",
    "TCP_VFR_018_001",
    "TCP_VFR_019_001",
    "TCP_VFR_020_001",
    "TCP_VFR_021_001",
    "TCP_VFR_022_001",
    "TCP_VFR_023_001",
    "TCP_VFR_024_001",
    "TCP_VFR_025_001",
    "TCP_VFR_026_001",
    "TCP_VFR_027_001",
    "TCP_VFR_028_001",
    "TCP_VFR_029_001",
    "TCP_VFR_030_001",
    "TCP_VFR_031_001",
    "TCP_VFR_032_001",
    "TCP_VFR_033_001",
    "TCP_VFR_034_001",
    "TCP_VFR_035_001",
    "TCP_VFR_036_001",
    "TCP_VFR_037_001",
    "TCP_VFR_038_001",
    "TCP_VFR_039_001",
    "TCP_VFR_040_001",
    "TCP_VFR_041_001",
    "TCP_VFR_042_001",
    "TCP_VFR_043_001",
    "TCP_VFR_044_001",
    "TCP_VFR_045_001",
    "TCP_VFR_046_001",
    "TCP_VFR_047_001",
]

PARAMETERS_UNITS = {
    "JAX_VFR_002_001": "g",
    "JAX_VFR_003_001": "g",
    "JAX_VFR_006_001": "g",
    "JAX_VFR_007_001": "g",
    "JAX_VFR_010_001": "g",
    "JAX_VFR_011_001": "g",
    "JAX_VFR_018_001": "min",
    "JAX_VFR_020_001": "Hours",
    "JAX_VFR_022_001": "Hours",
    "JAX_VFR_023_001": "Hours",
    "JAX_VFR_024_001": "Hours",
    "JAX_VFR_026_001": "cm",
    "JAX_VFR_032_001": "mm",
    "JAX_VFR_036_001": "g",
    "JAX_VFR_037_001": "g",
    "JAX_HRG_001_001": "s",
    "JAX_HRG_002_001": "s",
    "JAX_HRG_003_001": "s",
    "JAX_HRG_004_001": "s",
    "JAX_HRG_005_001": "s",
    "JAX_HRG_006_001": "s",
    "JAX_HRG_007_001": "min",
    "JAX_HRG_009_001": "s",
    "JAX_HRG_011_001": "min",
    "JAX_HRG_015_001": "Hours",
    "JAX_HRG_016_001": "Hours",
    "JAX_HRG_017_001": "Hours",
    "JAX_HRG_018_001": "C",
    "JAX_HRG_020_001": "%",
    "JAX_HRG_021_001": "%",
    "JAX_HRG_029_001": "cm",
    "JAX_FOR_001_001": "s",
    "JAX_FOR_002_001": "s",
    "JAX_FOR_009_001": "ul",
    "JAX_FOR_013_001": "mm",
    "JAX_FOR_017_001": "min",
    "HRWL_VFR_002_001": "g",
    "HRWL_VFR_003_001": "g",
    "HRWL_VFR_005_001": "g",
    "HRWL_VFR_008_001": "g",
    "HRWL_VFR_009_001": "g",
    "HRWL_VFR_011_001": "g",
    "HRWL_VFR_014_001": "g",
    "HRWL_VFR_015_001": "g",
    "HRWL_VFR_017_001": "g",
    "HRWL_VFR_021_001": "min",
    "HRWL_VFR_023_001": "Hours",
    "HRWL_VFR_025_001": "Hours",
    "HRWL_VFR_026_001": "Hours",
    "HRWL_VFR_028_001": "cm",
    "HRWL_VFR_034_001": "mm",
    "HRWL_VFR_038_001": "g",
    "HRWL_VFR_039_001": "g",
    "HRWL_FOR_001_001": "s",
    "HRWL_FOR_002_001": "s",
    "HRWL_FOR_009_001": "ul",
    "HRWL_FOR_013_001": "mm",
    "HRWL_FOR_017_001": "min",
    "HRWL_FOR_025_001": "s",
    "HRWL_FOR_026_001": "s",
    "HRWL_FOR_027_001": "s",
    "BCM_FOR_001_001": "s",
    "BCM_FOR_002_001": "s",
    "BCM_FOR_009_001": "ul",
    "BCM_FOR_013_001": "mm",
    "BCM_FOR_017_001": "min",
    "BCM_FOR_025_001": "s",
    "BCM_FOR_026_001": "s",
    "BCM_FOR_027_001": "s",
    "BCM_FOR_028_001": "s",
    "BCM_FOR_029_001": "s",
    "UCD_HRG_001_001": "s",
    "UCD_HRG_002_001": "s",
    "UCD_HRG_003_001": "s",
    "UCD_HRG_004_001": "s",
    "UCD_HRG_005_001": "s",
    "UCD_HRG_006_001": "s",
    "UCD_HRG_007_001": "min",
    "UCD_HRG_009_001": "s",
    "UCD_HRG_011_001": "min",
    "UCD_HRG_015_001": "Hours",
    "UCD_HRG_016_001": "Hours",
    "UCD_HRG_017_001": "Hours",
    "UCD_HRG_018_001": "C",
    "UCD_HRG_020_001": "%",
    "UCD_HRG_021_001": "%",
    "UCD_HRG_029_001": "cm",
    "UCD_VFR_004_001": "g",
    "UCD_VFR_009_001": "g",
    "UCD_VFR_014_001": "g",
    "UCD_VFR_005_001": "g",
    "UCD_VFR_010_001": "g",
    "UCD_VFR_015_001": "g",
    "UCD_VFR_021_001": "min",
    "UCD_VFR_023_001": "Hours",
    "UCD_VFR_025_001": "Hours",
    "UCD_VFR_026_001": "Hours",
    "UCD_VFR_027_001": "Hours",
    "UCD_VFR_029_001": "cm",
    "UCD_VFR_035_001": "mm",
    "UCD_VFR_039_001": "g",
    "UCD_VFR_040_001": "g",
    "TCP_HRG_001_001": "s",
    "TCP_HRG_002_001": "s",
    "TCP_HRG_003_001": "s",
    "TCP_HRG_004_001": "s",
    "TCP_HRG_005_001": "s",
    "TCP_HRG_006_001": "s",
    "TCP_HRG_007_001": "min",
    "TCP_HRG_009_001": "s",
    "TCP_HRG_011_001": "min",
    "TCP_HRG_015_001": "Hours",
    "TCP_HRG_016_001": "Hours",
    "TCP_HRG_017_001": "Hours",
    "TCP_HRG_018_001": "C",
    "TCP_HRG_020_001": "%",
    "TCP_HRG_021_001": "%",
    "TCP_HRG_029_001": "cm",
    "TCP_VFR_004_001": "g",
    "TCP_VFR_009_001": "g",
    "TCP_VFR_014_001": "g",
    "TCP_VFR_005_001": "g",
    "TCP_VFR_010_001": "g",
    "TCP_VFR_015_001": "g",
    "TCP_VFR_021_001": "min",
    "TCP_VFR_023_001": "Hours",
    "TCP_VFR_025_001": "Hours",
    "TCP_VFR_026_001": "Hours",
    "TCP_VFR_027_001": "Hours",
    "TCP_VFR_029_001": "cm",
    "TCP_VFR_035_001": "mm",
    "TCP_VFR_039_001": "g",
    "TCP_VFR_040_001": "g",
}


class PainDataCsvToXml(luigi.Task):
    name = "IMPC_Pain_Data_CSV_to_XML_Transformer"
    pain_csv_raw_data_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return ImpcConfig().get_target(f"{self.output_path}/pain-xml/")

    def run(self):
        file_name = os.path.basename(self.pain_csv_raw_data_path)
        centre = str(file_name).split("_")[0]
        centre_id = CENTRE_ID_MAP[centre]
        pipeline_id = CENTRE_PIPELINE_MAP[centre]
        project_id = CENTRE_PROJECT_MAP[centre]
        procedure = str(file_name).split("_")[1]
        procedure_stable_id = CENTRE_PROCEDURE_MAP[centre][procedure]
        exp_id_prefix = f"{centre}_{procedure_stable_id}"
        with open(self.pain_csv_raw_data_path) as csv_file:
            csv_dict_reader = csv.DictReader(csv_file)
            centre_procedure_set_tag: Element = etree.Element(
                "centreProcedureSet",
                xmlns="http://www.mousephenotype.org/dcc/exportlibrary/datastructure/core/procedure",
            )
            centre_tag = etree.Element(
                "centre", centreID=centre_id, pipeline=pipeline_id, project=project_id
            )
            centre_procedure_set_tag.append(centre_tag)
            row_index = 1
            for row in csv_dict_reader:
                specimen_id = row["Animal name"]
                if specimen_id is None or specimen_id == "":
                    continue
                experiment_id = f"{exp_id_prefix}_{specimen_id}_{row_index}"
                # TODO parse different date formats
                date_of_experiment = row["Date of experiment"].replace("/", "-")
                row_index += 1

                experiment_tag: Element = etree.Element(
                    "experiment",
                    experimentID=experiment_id,
                    dateOfExperiment=date_of_experiment,
                )
                centre_tag.append(experiment_tag)

                specimen_tag: Element = etree.Element("specimenID")
                specimen_tag.text = specimen_id
                experiment_tag.append(specimen_tag)
                procedure_tag: Element = etree.Element(
                    "procedure", procedureID=procedure_stable_id
                )
                experiment_tag.append(procedure_tag)
                parameter_columns = list(row.keys())[12:]
                parameter_tags = {}
                for parameter_col_name in parameter_columns:
                    if "-" in parameter_col_name:
                        col_name_parts = parameter_col_name.split("-INCREMENT:")
                        parameter_stable_id = col_name_parts[0]
                        increment_value = col_name_parts[1]
                    else:
                        parameter_stable_id = parameter_col_name
                        increment_value = 0
                    parameter_value = row[parameter_col_name]
                    if parameter_value is None or parameter_value == "":
                        continue
                    parameter_unit = (
                        PARAMETERS_UNITS[parameter_stable_id]
                        if parameter_stable_id in PARAMETERS_UNITS
                        else None
                    )
                    if parameter_stable_id not in parameter_tags:
                        parameter_tag: Element = etree.Element("dummmy")
                        if parameter_stable_id in SIMPLE_PARAMETERS:
                            parameter_tag = etree.Element(
                                "simpleParameter", parameterID=parameter_stable_id
                            )
                        elif parameter_stable_id in SERIES_PARAMETERS:
                            parameter_tag = etree.Element(
                                "seriesParameter", parameterID=parameter_stable_id
                            )
                        elif parameter_stable_id in METADATA_PARAMETERS:
                            parameter_tag = etree.Element(
                                "procedureMetadata", parameterID=parameter_stable_id
                            )
                        else:
                            print(parameter_stable_id)
                            raise Exception
                        parameter_tags[parameter_stable_id] = parameter_tag
                        procedure_tag.append(parameter_tag)
                    else:
                        parameter_tag = parameter_tags[parameter_stable_id]
                    if parameter_unit:
                        parameter_tag.attrib["unit"] = parameter_unit
                    value_tag: Element = etree.Element("value")
                    if parameter_stable_id in SERIES_PARAMETERS:
                        value_tag.attrib["incrementValue"] = str(increment_value)
                    value_tag.text = parameter_value
                    parameter_tag.append(value_tag)
        full_tree: ElementTree = etree.ElementTree(centre_procedure_set_tag)
        f = open("test.xml", "ab")
        f.write(etree.tostring(centre_procedure_set_tag, pretty_print=True))
        f.close()
