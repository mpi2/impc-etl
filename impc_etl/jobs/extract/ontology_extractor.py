import os
import sys

from pronto import Ontology, Term

from impc_etl.workflow.config import ImpcConfig
from io import BytesIO
from pyspark.sql import DataFrame, SparkSession
from impc_etl.config import OntologySchema, Constants
from typing import List, Dict, Iterable
from impc_etl.shared.utils import convert_to_row
import pronto
from luigi.contrib.hdfs.hadoopcli_clients import HdfsClient
from luigi.contrib.hdfs.target import HdfsTarget

ONTOLOGIES = [
    {
        "id": "mp",
        "top_level_terms": [
            "MP:0010768",
            "MP:0002873",
            "MP:0001186",
            "MP:0003631",
            "MP:0005367",
            "MP:0005369",
            "MP:0005370",
            "MP:0005371",
            "MP:0005377",
            "MP:0005378",
            "MP:0005375",
            "MP:0005376",
            "MP:0005379",
            "MP:0005380",
            "MP:0005381",
            "MP:0005384",
            "MP:0005385",
            "MP:0005382",
            "MP:0005388",
            "MP:0005389",
            "MP:0005386",
            "MP:0005387",
            "MP:0005391",
            "MP:0005390",
            "MP:0005394",
            "MP:0005397",
            "MP:0010771",
        ],
    },
    # {
    #     "id": "ma",
    #     "top_level_terms": [
    #         "MA:0000004",
    #         "MA:0000007",
    #         "MA:0000009",
    #         "MA:0000010",
    #         "MA:0000012",
    #         "MA:0000014",
    #         "MA:0000016",
    #         "MA:0000017",
    #         "MA:0000325",
    #         "MA:0000326",
    #         "MA:0000327",
    #         "MA:0002411",
    #         "MA:0002418",
    #         "MA:0002431",
    #         "MA:0002711",
    #         "MA:0002887",
    #         "MA:0002405",
    #     ],
    # },
    # {
    #     "id": "emapa",
    #     "top_level_terms": [
    #         "EMAPA:16104",
    #         "EMAPA:16192",
    #         "EMAPA:16246",
    #         "EMAPA:16405",
    #         "EMAPA:16469",
    #         "EMAPA:16727",
    #         "EMAPA:16748",
    #         "EMAPA:16840",
    #         "EMAPA:17524",
    #         "EMAPA:31858",
    #     ],
    # },
    # {"id": "efo", "top_level_terms": []},
    # {"id": "emap", "top_level_terms": []},
    # {"id": "mpath", "top_level_terms": []},
    # {"id": "pato", "top_level_terms": []},
]


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    ontology_df = extract_ontology_terms(spark)
    ontology_df.write.mode("overwrite").parquet(output_path)


def extract_ontology_terms(spark_session: SparkSession) -> DataFrame:
    """

    :param spark_session:
    :param ontologies_path:
    :return:
    """
    ontology_terms = []
    if ImpcConfig().deploy_mode in ["local", "client"]:
        for ontology_desc in ONTOLOGIES:
            ontology = pronto.Ontology.from_obo_library(f"{ontology_desc['id']}.obo")
            top_level_terms = [
                ontology[term] for term in ontology_desc["top_level_terms"]
            ]
            top_level_ancestors = []
            for top_level_term in top_level_terms:
                top_level_ancestors.extend(top_level_term.superclasses(with_self=False))
            top_level_ancestors = set(top_level_ancestors)
            ontology_terms += [
                _parse_ontology_term(term, top_level_terms, top_level_ancestors)
                for term in ontology.terms()
            ]
    ontology_terms_json = spark_session.sparkContext.parallelize(ontology_terms)
    ontology_terms_df = spark_session.read.json(ontology_terms_json)
    return ontology_terms_df


def _parse_ontology_term(
    ontology_term: Term, top_level_terms, top_level_ancestors
) -> Dict:
    children = [
        child_term for child_term in ontology_term.subclasses(1, with_self=False)
    ]
    parents = [
        parent_term for parent_term in ontology_term.superclasses(1, with_self=False)
    ]
    ancestors = [
        ancestor_term for ancestor_term in ontology_term.superclasses(with_self=False)
    ]
    term_top_level_terms = set(top_level_terms).intersection(set(ancestors))
    intermediate_terms = (
        set(ancestors).difference(set(top_level_terms)).difference(top_level_ancestors)
    )
    return {
        "id": ontology_term.id,
        "term": ontology_term.name,
        "definition": str(ontology_term.definition),
        "synonyms": [
            synonym.description
            for synonym in ontology_term.synonyms
            if synonym.type is "EXACT"
        ],
        "alt_ids": list(ontology_term.alternate_ids),
        "child_ids": [child_term.id for child_term in children],
        "child_terms": [child_term.name for child_term in children],
        "child_definitions": [str(child_term.definition) for child_term in children],
        "child_term_synonyms": _get_synonym_list(children),
        "parent_ids": [parent_term.id for parent_term in parents],
        "parent_terms": [parent_term.name for parent_term in parents],
        "parent_definitions": [str(parent_term.definition) for parent_term in parents],
        "parent_term_synonyms": _get_synonym_list(parents),
        "intermediate_ids": [
            intermediate_term.id for intermediate_term in intermediate_terms
        ],
        "intermediate_terms": [
            intermediate_term.name for intermediate_term in intermediate_terms
        ],
        "intermediate_definitions": [
            str(intermediate_term.definition)
            for intermediate_term in intermediate_terms
        ],
        "intermediate_term_synonyms": _get_synonym_list(intermediate_terms),
        "top_level_ids": [
            term_top_level_term.id for term_top_level_term in term_top_level_terms
        ],
        "top_level_terms": [
            term_top_level_term.name for term_top_level_term in term_top_level_terms
        ],
        "top_level_definitions": [
            str(term_top_level_term.definition)
            for term_top_level_term in term_top_level_terms
        ],
        "top_level_synonyms": _get_synonym_list(term_top_level_terms),
    }


def _get_synonym_list(terms: Iterable[Term]):
    flat_list = []
    for term in terms:
        for synonym in term.synonyms:
            flat_list.append(str(synonym))
    return flat_list


if __name__ == "__main__":
    sys.exit(main(sys.argv))
