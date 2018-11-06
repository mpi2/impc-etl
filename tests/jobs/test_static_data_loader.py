"""
STATIC DATA LOADER TEST SUITE
"""
from mock import MagicMock
from impc_etl.jobs.loaders.static_data_loader import load_phenotyping_centres, _parse_ontology
from owlready2 import *


def test_load_phenotyping_centres_info():
    """
    If the
    """
    session_mock = MagicMock()
    load_phenotyping_centres(session_mock, 'some/path')
    session_mock.read.csv.assert_called_with('some/path', header=True, mode='DROPMALFORMED', schema=None, sep='\t')


def test_parse_ontology():
    """
    If the ontology does not contains any class the parse_ontology function should return []
    If the ontology contains any class the parse_ontology function should return a list with the ontology terms
    """
    ontology = get_ontology('http://purl.obolibrary.org/obo/pizza.owl')
    go = get_ontology('http://www.geneontology.org/formats/oboInOwl#')
    obo = get_ontology('http://purl.obolibrary.org/obo/')
    ontology_terms = _parse_ontology(ontology)
    assert ontology_terms == []

    with ontology:
        class Pizza(Thing):
            pass

        class HawaiianPizza(Pizza):
            pass

    with obo:
        class IAO_0000115(AnnotationProperty):
            pass

    with go:
        class hasExactSynonym(AnnotationProperty):
            pass

    Pizza.label = 'Pizza'
    HawaiianPizza.label = 'Hawaiian Pizza'
    Pizza.IAO_0000115.append('Italian dish')
    HawaiianPizza.IAO_0000115.append('Controversial Italian dish with pineapple')
    HawaiianPizza.hasExactSynonym.append('Ham and pineapple pizza')
    ontology_terms = _parse_ontology(ontology)
    assert ontology_terms == [
        {
            'ontologyId': 'pizza',
            'ontologyTermId': 'Pizza',
            'label': ['Pizza'],
            'description': ['Italian dish'],
            'synonyms': [],
            'parents': ['Thing'],
            'children': ['HawaiianPizza']
        },
        {
            'ontologyId': 'pizza',
            'ontologyTermId': 'HawaiianPizza',
            'label': ['Hawaiian Pizza'],
            'description': ['Controversial Italian dish with pineapple'],
            'synonyms': ['Ham and pineapple pizza'],
            'parents': ['Pizza'],
            'children': []
        }
    ]
