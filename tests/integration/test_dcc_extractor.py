"""
DCC extractor test suite
"""
from impc_etl.jobs.extract.dcc_extractor_helper import *
import pytest
from impc_etl.shared.exceptions import *
from pathlib import Path
import os
import bs4

INPUT_PATH = (
    os.environ["INPUT_PATH"] if "INPUT_PATH" in os.environ else "tests/data/xml/"
)


def tag_count(file_type, entity_type):
    count = 0
    for f in Path(INPUT_PATH).glob(f"**/*{file_type}*"):
        with open(str(f), "r") as file:
            data = file.read()
            soup = bs4.BeautifulSoup(data, "lxml")
            entities = soup.find_all(entity_type)
            count += len(entities) if entities is not None else 0
    return count


class TestDCCExtractXMLFiles:
    """
    If the file_type is not supported should raise an UnsupportedFileTypeException
    If the provided path is empty should raise an FileNotFoundException
    If there is a valid Input the output DataFrame should contain
        as many rows as files in the input path
    """

    def test_file_type_not_supported(self, spark_session: SparkSession):
        with pytest.raises(UnsupportedFileTypeError):
            extract_dcc_xml_files(
                spark_session, "SOME XML PATH", "NON EXISTENT FILE TYPE"
            )

    @pytest.mark.parametrize("file_type", ["experiment", "specimen"])
    def test_file_not_found(self, spark_session: SparkSession, file_type):
        with pytest.raises(FileNotFoundError):
            extract_dcc_xml_files(spark_session, "NON EXISTENT XML FILE", file_type)

    @pytest.mark.parametrize("file_type", ["experiment", "specimen"])
    def test_number_of_center_rows(self, spark_session: SparkSession, file_type):
        number_of_files = len([f for f in Path(INPUT_PATH).glob(f"**/*{file_type}*")])
        dcc_df = extract_dcc_xml_files(spark_session, INPUT_PATH, file_type)
        assert dcc_df.count() == number_of_files


class TestDCCGetByType:
    """
    If the entity_type is not supported should raise an UnsupportedEntityError
    If there is a valid Input the output DataFrame should contain as many rows
        as tags that match the target entity in the XML path
    """

    def test_entity_type_not_supported(self):
        with pytest.raises(UnsupportedEntityError):
            get_experiments_by_type(None, "UNKNOWN TYPE")
        with pytest.raises(UnsupportedEntityError):
            get_specimens_by_type(None, "UNKNOWN TYPE")

    @pytest.mark.parametrize("entity_type", ["experiment", "line"])
    def test_number_of_experiments(self, dcc_experiment_df, entity_type):
        expected = tag_count("experiment", entity_type)
        assert (
            get_experiments_by_type(dcc_experiment_df, entity_type).count() == expected
        )

    @pytest.mark.parametrize("entity_type", ["mouse", "embryo"])
    def test_number_of_specimens(self, dcc_specimen_df, entity_type):
        expected = tag_count("specimen", entity_type)
        assert get_specimens_by_type(dcc_specimen_df, entity_type).count() == expected
