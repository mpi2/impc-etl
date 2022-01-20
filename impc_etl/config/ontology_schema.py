"""
Module to hold ontology schemas, it only contains the OBO in OWL ontology format schema.
All the ontologies we consume now are available as OBO so we don't need any other schema,
but if needed it should be added here.
"""


class OntologySchema:
    """
    Class to represent an OBO ontology schemas to be used by owlready2.
    """

    LABEL_ANNOTATION = ""
    ALT_ID = ""
    X_REF = ""
    REPLACEMENT = ""
    CONSIDER = ""
    TERM_REPLACED_BY = ""
    IS_OBSOLETE = ""
    SYNONYM_ANNOTATIONS = [
        "http://www.geneontology.org/formats/oboInOwl#hasExactSynonym",
        "http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym",
        "http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym",
        "http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym",
    ]
    """
    List of  SYNONYM ANNOTATIONS properties URIs
    """
    DEFINITION_ANNOTATION = "http://purl.obolibrary.org/obo/IAO_0000115"
    """
    DEFINITION ANNOTATION property URI.
    """
    PART_OF = ""
