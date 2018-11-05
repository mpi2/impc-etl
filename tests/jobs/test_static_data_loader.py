"""
STATIC DATA LOADER TEST SUITE
"""
from mock import MagicMock
from impc_etl.jobs.loaders.static_data_loader import load_phenotyping_centres, _parse_ontology
from owlready2 import *


def test_load_phenotyping_centres_info():
    """
    Dummy test
    """
    session_mock = MagicMock()
    load_phenotyping_centres(session_mock, 'some/path')
    assert 5 == 5


def test_parse_ontology():
    """

    :return:
    """
    onto_path.append('/Users/federico/git/spark/impc-etl/tests/data/ontologies/efo.owl')
    ontology = get_ontology('http://www.ebi.ac.uk/efo/efo.owl').load()
    ontology_terms = _parse_ontology(ontology)
    print(ontology_terms)
