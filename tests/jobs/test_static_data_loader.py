"""
STATIC DATA LOADER TEST SUITE
"""
from mock import MagicMock
from impc_etl.jobs.loaders.static_data_loader import load_phenotyping_centres


def test_load_phenotyping_centres_info():
    """
    Dummy test
    """
    session_mock = MagicMock()
    load_phenotyping_centres(session_mock, 'some/path')