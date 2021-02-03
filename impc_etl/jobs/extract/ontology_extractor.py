import os
import sys

from pronto import Ontology, Term, Relationship
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from impc_etl.workflow.config import ImpcConfig
from io import BytesIO
from pyspark.sql import DataFrame, SparkSession
from impc_etl.config.ontology_schema import OntologySchema, Constants
from typing import List, Dict, Iterable
from impc_etl.shared.utils import convert_to_row
import pronto
from luigi.contrib.hdfs.hadoopcli_clients import HdfsClient
from luigi.contrib.hdfs.target import HdfsTarget
import unicodedata

ONTOLOGIES = [
    {"id": "mpath", "format": "obo", "top_level_terms": []},
    {
        "id": "mp",
        "format": "obo",
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
    {
        "id": "ma",
        "format": "obo",
        "top_level_terms": [
            "MA:0000004",
            "MA:0000007",
            "MA:0000009",
            "MA:0000010",
            "MA:0000012",
            "MA:0000014",
            "MA:0000016",
            "MA:0000017",
            "MA:0000325",
            "MA:0000326",
            "MA:0000327",
            "MA:0002411",
            "MA:0002418",
            "MA:0002431",
            "MA:0002711",
            "MA:0002887",
            "MA:0002405",
        ],
    },
    {
        "id": "emapa",
        "format": "obo",
        "top_level_terms": [
            "EMAPA:16104",
            "EMAPA:16192",
            "EMAPA:16246",
            "EMAPA:16405",
            "EMAPA:16469",
            "EMAPA:16727",
            "EMAPA:16748",
            "EMAPA:16840",
            "EMAPA:17524",
            "EMAPA:31858",
        ],
    },
    # {"id": "efo", "top_level_terms": []},
    # {"id": "emap", "top_level_terms": []},
    # {"id": "pato", "top_level_terms": []},
]

ONTOLOGY_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("term", StringType(), True),
        StructField("definition", StringType(), True),
        StructField("synonyms", ArrayType(StringType()), True),
        StructField("alt_ids", ArrayType(StringType()), True),
        StructField("child_ids", ArrayType(StringType()), True),
        StructField("child_terms", ArrayType(StringType()), True),
        StructField("child_definitions", ArrayType(StringType()), True),
        StructField("child_term_synonyms", ArrayType(StringType()), True),
        StructField("parent_ids", ArrayType(StringType()), True),
        StructField("parent_terms", ArrayType(StringType()), True),
        StructField("parent_definitions", ArrayType(StringType()), True),
        StructField("parent_term_synonyms", ArrayType(StringType()), True),
        StructField("intermediate_ids", ArrayType(StringType()), True),
        StructField("intermediate_terms", ArrayType(StringType()), True),
        StructField("intermediate_definitions", ArrayType(StringType()), True),
        StructField("intermediate_term_synonyms", ArrayType(StringType()), True),
        StructField("top_level_ids", ArrayType(StringType()), True),
        StructField("top_level_terms", ArrayType(StringType()), True),
        StructField("top_level_definitions", ArrayType(StringType()), True),
        StructField("top_level_synonyms", ArrayType(StringType()), True),
        StructField("top_level_term_id", ArrayType(StringType()), True),
    ]
)


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    input_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    ontology_df = extract_ontology_terms(spark, input_path)
    ontology_df.write.mode("overwrite").parquet(output_path)


def extract_ontology_terms(spark_session: SparkSession, input_path) -> DataFrame:
    """

    :param spark_session:
    :param ontologies_path:
    :return:
    """
    ontology_terms = []
    if ImpcConfig().deploy_mode in ["local", "client"]:
        for ontology_desc in ONTOLOGIES:
            print(f"Processing {ontology_desc['id']}.{ontology_desc['format']}")
            if ontology_desc["id"] == "mpath":
                ontology: Ontology = Ontology(input_path + "mpath.obo")
            else:
                ontology: Ontology = pronto.Ontology.from_obo_library(
                    f"{ontology_desc['id']}.{ontology_desc['format']}"
                )

            part_of_rel: Relationship = None
            for rel in ontology.relationships():
                if rel.id == "part_of":
                    part_of_rel = rel
                    break
            if part_of_rel is not None:
                part_of_rel.transitive = False
                print("Starting to compute super classes from part_of")
                for term in ontology.terms():
                    for super_part_term in term.objects(part_of_rel):
                        if super_part_term.id in ontology.keys():
                            term.superclasses().add(super_part_term)
                print("Finished to compute super classes from part_of")
            top_level_terms = [
                ontology[term] for term in ontology_desc["top_level_terms"]
            ]
            top_level_ancestors = []
            for top_level_term in top_level_terms:
                top_level_ancestors.extend(top_level_term.superclasses(with_self=False))
            top_level_ancestors = set(top_level_ancestors)
            ontology_terms += [
                _parse_ontology_term(
                    term, top_level_terms, top_level_ancestors, part_of_rel
                )
                for term in ontology.terms()
                if term.name is not None
            ]
            print(
                f"Finished processing {ontology_desc['id']}.{ontology_desc['format']}"
            )
    ontology_terms_json = spark_session.sparkContext.parallelize(ontology_terms)
    ontology_terms_df = spark_session.read.json(
        ontology_terms_json, schema=ONTOLOGY_SCHEMA, mode="FAILFAST"
    )
    return ontology_terms_df


def _parse_ontology_term(
    ontology_term: Term, top_level_terms, top_level_ancestors, part_of_rel: Relationship
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

    if part_of_rel is not None:
        ancestors.extend(
            [ancestor_term for ancestor_term in ontology_term.objects(part_of_rel)]
        )
    term_top_level_terms = set(top_level_terms).intersection(set(ancestors))
    intermediate_terms = (
        set(ancestors).difference(set(top_level_terms)).difference(top_level_ancestors)
    )
    return {
        "id": ontology_term.id,
        "term": _parse_text(ontology_term.name),
        "definition": _parse_text(ontology_term.definition)
        if ontology_term.definition is not None
        else "null",
        "synonyms": [
            _parse_text(synonym.description)
            for synonym in ontology_term.synonyms
            if synonym.type is "EXACT"
        ],
        "alt_ids": list(ontology_term.alternate_ids),
        "child_ids": [child_term.id for child_term in children],
        "child_terms": [_parse_text(child_term.name) for child_term in children],
        "child_definitions": [
            _parse_text(child_term.definition)
            for child_term in children
            if child_term.definition is not None
        ],
        "child_term_synonyms": _get_synonym_list(children),
        "parent_ids": [parent_term.id for parent_term in parents],
        "parent_terms": [_parse_text(parent_term.name) for parent_term in parents],
        "parent_definitions": [
            _parse_text(parent_term.definition)
            for parent_term in parents
            if parent_term.definition is not None
        ],
        "parent_term_synonyms": _get_synonym_list(parents),
        "intermediate_ids": [
            intermediate_term.id for intermediate_term in intermediate_terms
        ],
        "intermediate_terms": [
            _parse_text(intermediate_term.name)
            for intermediate_term in intermediate_terms
        ],
        "intermediate_definitions": [
            _parse_text(intermediate_term.definition)
            for intermediate_term in intermediate_terms
            if intermediate_term.definition is not None
        ],
        "intermediate_term_synonyms": _get_synonym_list(intermediate_terms),
        "top_level_ids": [
            term_top_level_term.id for term_top_level_term in term_top_level_terms
        ],
        "top_level_terms": [
            _parse_text(term_top_level_term.name)
            for term_top_level_term in term_top_level_terms
        ],
        "top_level_definitions": [
            _parse_text(term_top_level_term.definition)
            for term_top_level_term in term_top_level_terms
            if term_top_level_term.definition is not None
        ],
        "top_level_synonyms": _get_synonym_list(term_top_level_terms),
        "top_level_term_id": [
            f"{term_top_level_term.id}___{_parse_text(term_top_level_term.name)}"
            for term_top_level_term in term_top_level_terms
        ],
    }


def _get_synonym_list(terms: Iterable[Term]):
    flat_list = []
    for term in terms:
        for synonym in term.synonyms:
            flat_list.append(_parse_text(synonym.description))
    return flat_list


def _parse_text(definition):
    if definition is None:
        return None
    return unicodedata.normalize(
        "NFKD", definition.encode("iso-8859-1").decode("utf-8")
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
