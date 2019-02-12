"""
Config file
"""


class SparkConfig:
    SPARK_JAR_PACKAGES = ['com.databricks:spark-xml_2.11:0.5.0']


class OntologySchema:
    LABEL_ANNOTATION = ''
    ALT_ID = ''
    X_REF = ''
    REPLACEMENT = ''
    CONSIDER = ''
    TERM_REPLACED_BY = ''
    IS_OBSOLETE = ''
    SYNONYM_ANNOTATIONS = [
        'http://www.geneontology.org/formats/oboInOwl#hasExactSynonym',
        'http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym',
        'http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym',
        'http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym'
    ]
    DEFINITION_ANNOTATION = 'http://purl.obolibrary.org/obo/IAO_0000115'
    PART_OF = ''
