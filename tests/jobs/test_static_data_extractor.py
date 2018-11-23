"""
STATIC DATA EXTRACTOR TEST SUITE
"""
from mock import MagicMock
from owlready2 import get_ontology, Thing, AnnotationProperty
from impc_etl.jobs.extraction.static_data_extractor import extract_phenotyping_centres, \
    parse_ontology
# pylint:disable=C0111,W0612,C0103


def test_extract_phenotyping_centres_info():
    """
    If the
    """
    session_mock = MagicMock()
    extract_phenotyping_centres(session_mock, 'some/path')
    session_mock.read.csv.assert_called_with('some/path', header=True, mode='DROPMALFORMED',
                                             schema=None, sep='\t')


def test_parse_ontology():
    """
    If the ontology does not contains any class the parse_ontology function should return []
    If the ontology contains any class the parse_ontology function should return
    a list with the ontology terms
    """
    ontology = get_ontology('http://purl.obolibrary.org/obo/pizza.owl')
    gene_ontology = get_ontology('http://www.geneontology.org/formats/oboInOwl#')
    obo = get_ontology('http://purl.obolibrary.org/obo/')
    ontology_terms = parse_ontology(ontology)
    assert ontology_terms == []

    with ontology:
        class Pizza(Thing):
            pass

        class HawaiianPizza(Pizza):
            pass

    with obo:
        class IAO_0000115(AnnotationProperty):
            pass

    with gene_ontology:
        class hasExactSynonym(AnnotationProperty):
            pass

    Pizza.label = 'Pizza'
    HawaiianPizza.label = 'Hawaiian Pizza'
    Pizza.IAO_0000115.append('Italian dish')
    HawaiianPizza.IAO_0000115.append('Controversial Italian dish with pineapple')
    HawaiianPizza.hasExactSynonym.append('Ham and pineapple pizza')
    ontology_terms = parse_ontology(ontology)
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
