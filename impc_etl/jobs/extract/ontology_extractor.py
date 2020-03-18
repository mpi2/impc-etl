from zipfile import ZipFile
import os
import sys

with ZipFile("dist/libs.zip", "r") as zip_file:
    zip_file.extractall("dist/libs/")
    sys.path.insert(0,'dist/libs/')
from io import BytesIO
from pyspark.sql import DataFrame, SparkSession
from owlready2 import (
    get_ontology,
    Ontology,
    onto_path,
    ThingClass,
    Nothing,
    Thing,
    IRIS,
)
from impc_etl.config import OntologySchema
from typing import List
from impc_etl.shared.utils import convert_to_row
from impc_etl.workflow.config import ImpcConfig
from luigi.contrib.hdfs.hadoopcli_clients import HdfsClient
from luigi.contrib.hdfs.target import HdfsTarget


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    ontology_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    ontology_df = extract_ontology_terms(spark, ontology_path)
    ontology_df.write.mode("overwrite").parquet(output_path)


def extract_ontology_terms(
    spark_session: SparkSession, ontology_path: str
) -> DataFrame:
    """

    :param spark_session:
    :param ontologies_path:
    :return:
    """
    ontology_terms = []
    print(ImpcConfig().deploy_mode)
    if ImpcConfig().deploy_mode in ["local", "client"]:
        for file in os.listdir(ontology_path):
            filename = os.fsdecode(file)
            if filename.endswith(".owl"):
                onto_path.append(os.path.join(ontology_path, filename))
                ontology = get_ontology(os.path.join(ontology_path, filename)).load()
                ontology_terms.extend(parse_ontology(ontology))
    else:
        hdfs_client = HdfsClient()
        for file in hdfs_client.listdir(ontology_path):
            # for file in files:
            hdfs_target = HdfsTarget(file)
            print(ontology_path + file)
            with hdfs_target.open() as reader:
                text_content = reader.read()
                file_like = BytesIO(text_content.encode())
                onto_path.append(file)
                ontology = get_ontology(file).load(fileobj=file_like)
                ontology_terms.extend(parse_ontology(ontology))
    ontology_terms_df = spark_session.createDataFrame(
        convert_to_row(term) for term in ontology_terms
    )
    return ontology_terms_df


def parse_ontology(ontology: Ontology, schema=OntologySchema) -> List[dict]:
    """
    Parse an ontology from owlready2.Ontology to a list of dicts with
    the domain fields forOntologyTerm
    By default it use the OBO Schema for the definition and synonyms annotations.
    :param ontology: owlready2.Ontology to parse
    :param schema: schema class extending OntologySchema
    :return ontology_terms: list of dicts containing ontology terms
    """
    ontology_terms = []
    for ontology_class in ontology.classes():
        ontology_id = ontology.name
        ontology_term_id = ontology_class.name
        term_label = ontology_class.label

        if isinstance(term_label, list):
            term_label = str(term_label[0]) if len(term_label) > 0 else ""
        else:
            term_label = str(term_label)

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
            "ontologyId": ontology_id.upper(),
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
        annotation_values.extend(
            [
                str(annotation_value)
                for annotation_value in IRIS[annotation_iri][ontology_class]
            ]
        )
    return annotation_values


if __name__ == "__main__":
    sys.exit(main(sys.argv))
