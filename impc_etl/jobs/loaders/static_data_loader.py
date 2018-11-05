"""
Static Data loader
    load_human_gene_orthologues:
    load_phenotyping_centres:
    load_ontology_terms:
"""
from pyspark.sql import DataFrame, SparkSession
from impc_etl.shared import utils
import os
from owlready2 import *
from impc_etl.shared.utils import convert_to_row
from typing import List
from impc_etl.config import OntologySchema


def load_phenotyping_centres(spark_session: SparkSession, file_path: str) -> DataFrame:
    """
    :param spark_session:
    :param file_path:
    :return:
    """
    phenotyping_centres_df = utils.load_tsv(spark_session, file_path)
    return phenotyping_centres_df


def load_ontology_terms(spark_session: SparkSession, ontologies_path: str) -> DataFrame:
    """

    :param spark_session:
    :param ontologies_path:
    :return:
    """
    directory = os.fsencode(ontologies_path)
    ontology_terms = []
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".owl"):
            onto_path.append(os.path.join(directory, filename))
            ontology = get_ontology().load()
            ontology_terms.extend(_parse_ontology(ontology))
    ontology_terms_df = spark_session.createDataFrame(convert_to_row(term) for term in ontology_terms)
    return ontology_terms_df


def _parse_ontology(ontology: Ontology) -> List[dict]:
    """
    Parse an ontology from owlready2.Ontology to a list of dicts with the domain fields for OntologyTerm
    :param ontology:
    :return:
    """
    ontology_terms = []
    for ontology_class in ontology.classes():
        ontology_id = ontology.name
        ontology_term_id = ontology_class.name
        term_label = ontology_class.label
        term_definition = _collect_annotations(ontology_class, [OntologySchema.DEFINITION_ANNOTATION])
        synonyms = _collect_annotations(ontology_class, OntologySchema.SYNONYM_ANNOTATIONS)
        parents = [str(parent.name) for parent in ontology_class.is_a if isinstance(parent, ThingClass)]
        children = [str(child.name) for child in ontology_class.subclasses() if isinstance(child, ThingClass)]
        ontology_term = {
            'ontologyId': ontology_id,
            'ontologyTermId': ontology_term_id,
            'label': term_label,
            'definition': term_definition,
            'synonyms': synonyms,
            'parents': parents,
            'children': children
        }
        ontology_terms.append(ontology_term)
    return ontology_terms


def _collect_annotations(ontology_class, annotation_iris):
    annotation_values = []
    for annotation_iri in annotation_iris:
        annotation_values.extend(IRIS[annotation_iri][ontology_class])
    return annotation_values
