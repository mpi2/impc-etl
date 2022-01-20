"""
Static Data extractor
    extract_human_gene_orthologues:
    extract_phenotyping_centres:
    extract_ontology_terms:
"""
import os
from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from owlready2 import (
    get_ontology,
    Ontology,
    onto_path,
    ThingClass,
    Nothing,
    Thing,
    IRIS,
)
from impc_etl.shared import utils
from impc_etl.shared.utils import convert_to_row
from impc_etl.config.ontology_schema import OntologySchema


def extract_human_gene_orthologues(
    spark_session: SparkSession, file_path: str
) -> DataFrame:
    """

    :param spark_session:
    :param file_path:
    :return human_gene_orthologues_df: Dataframe with the human gene to mouse gene mapping
    """
    file_string_fields = [
        "Human Marker Symbol",
        "Human Entrez Gene ID",
        "HomoloGene ID",
        "Mouse Marker Symbol",
        "MGI Marker Accession ID",
    ]
    file_array_fields = ["High-level Mammalian Phenotype ID"]
    schema_fields = [
        StructField(field_name, StringType(), True) for field_name in file_string_fields
    ]
    schema_fields.extend(
        [
            StructField(field_name, ArrayType(StringType), True)
            for field_name in file_array_fields
        ]
    )
    hmd_file_schema = StructType(schema_fields)
    human_gene_orthologues_df = utils.extract_tsv(
        spark_session, file_path, hmd_file_schema
    )
    return human_gene_orthologues_df


def extract_phenotyping_centres(
    spark_session: SparkSession, file_path: str
) -> DataFrame:
    """
    :param spark_session:
    :param file_path:
    :return:
    """
    phenotyping_centres_df = utils.extract_tsv(spark_session, file_path)
    return phenotyping_centres_df


def extract_ontology_terms(
    spark_session: SparkSession, ontologies_path: str
) -> DataFrame:
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
            ontology = get_ontology(None).load()
            ontology_terms.extend(parse_ontology(ontology))
    ontology_terms_df = spark_session.createDataFrame(
        convert_to_row(term) for term in ontology_terms
    )
    return ontology_terms_df


def parse_ontology(ontology: Ontology, schema=OntologySchema) -> List[dict]:
    """
    Parse an ontology from owlready2.Ontology to a list of dicts with
    the domain fields forOntologyTerm
    By default it uses the OBO Schema for the definition and synonyms annotations.
    :param ontology: owlready2.Ontology to parse
    :param schema: schema class extending OntologySchema
    :return ontology_terms: list of dicts containing ontology terms
    """
    ontology_terms = []
    for ontology_class in ontology.classes():
        ontology_id = ontology.name
        ontology_term_id = ontology_class.name
        term_label = ontology_class.label
        term_definition = _collect_annotations(
            ontology_class, [schema.DEFINITION_ANNOTATION]
        )
        synonyms = _collect_annotations(ontology_class, schema.SYNONYM_ANNOTATIONS)
        parents = [
            str(parent.name)
            for parent in ontology_class.is_a
            if isinstance(parent, ThingClass)
        ]
        children = [
            str(child.name)
            for child in ontology_class.subclasses()
            if isinstance(child, ThingClass)
        ]
        ontology_term = {
            "ontologyId": ontology_id,
            "ontologyTermId": ontology_term_id,
            "label": term_label,
            "description": term_definition,
            "synonyms": synonyms,
            "parents": parents,
            "children": children,
        }
        ontology_terms.append(ontology_term)
    return ontology_terms


def _collect_annotations(ontology_class: ThingClass, annotation_iris: List[str]):
    """
    Collects the values for one or several annotations for one specific class
    :param ontology_class: owlready2.ThingClass
    :param annotation_iris: list of annotation iris
    :return annotations_values: list of values for the input annotations
    """
    annotation_values = []
    for annotation_iri in annotation_iris:
        if IRIS[annotation_iri] is None or ontology_class in (Nothing, Thing):
            continue
        annotation_values.extend(IRIS[annotation_iri][ontology_class])
    return annotation_values
