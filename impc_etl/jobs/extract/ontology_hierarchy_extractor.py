"""
    IMPC Ontology Hierarchy Extraction task. It takes in a set of ontologies and returns them in a parquet file that
    represents the hierarchical relations between the terms on those ontologies.
"""
import unicodedata
from io import BytesIO
from typing import Dict, Iterable, List, Any

import luigi
import pronto
from luigi.contrib.spark import PySparkTask
from pronto import Ontology, Term, Relationship
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

from impc_etl.workflow.config import ImpcConfig


class OntologyTermHierarchyExtractor(PySparkTask):
    """
    PySpark Task class to extract the hierarchical relations between terms for the ontologies: MPATH, MA, EMAPA and MP.
    The main goal of this task is to assign to any ontology term a list of:

    - direct children
    - direct parents
    - top level terms
    - intermediate terms (i.e. terms between the given term and the top level ones)
    - synonyms and definitions for all the related terms
    """

    #: Name of the Spark task
    name = "IMPC_Ontology_Term_Hierarchy_Extractor"

    #: Path to the directory containing OBO files for MA, MPATH, EMAPA and MP.
    obo_ontology_input_path: luigi.Parameter = luigi.Parameter()

    #: Path of the output directory where ethe new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    #: List on ontologies to process with their corresponding top level terms
    ONTOLOGIES: List[Dict] = [
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

    #: Schema of the resulting parquet file
    ONTOLOGY_SCHEMA: StructType = StructType(
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

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/impc_ontology_term_hierarchy_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}impc_ontology_term_hierarchy_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.obo_ontology_input_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args: Any):
        """
        DCC Extractor job runner
        """
        input_path = args[0]
        output_path = args[1]

        spark = SparkSession(sc)
        ontology_df = self.extract_ontology_terms(spark, input_path)
        ontology_df.write.mode("overwrite").parquet(output_path)

    def extract_ontology_terms(
        self, spark_session: SparkSession, ontologies_path: str
    ) -> DataFrame:
        """
        Takes in a spark session and the path containing cached OBO files and returns
        a DataFrame that represents Ontology terms hierarchical relationships.
        """

        # List of ontology terms
        ontology_terms = []

        # This process can only be performed on local or client mode
        for ontology_desc in self.ONTOLOGIES:
            print(f"Processing {ontology_desc['id']}.{ontology_desc['format']}")

            # Get the OBO file from the directory if MPATH otherwise get it from OBO foundry
            if ontology_desc["id"] == "mpath":
                if ImpcConfig().deploy_mode in ["local", "client"]:
                    ontology: Ontology = Ontology(ontologies_path + "mpath.obo")
                else:
                    full_ontology_str = spark_session.sparkContext.wholeTextFiles(
                        ontologies_path + "mpath.obo"
                    ).collect()[0][1]
                    ontology: Ontology = Ontology(
                        BytesIO(bytes(full_ontology_str, encoding="utf-8"))
                    )
            else:
                ontology: Ontology = pronto.Ontology.from_obo_library(
                    f"{ontology_desc['id']}.{ontology_desc['format']}"
                )

            part_of_rel: Relationship = None

            # Find the part_of relationship on the current loaded ontology
            for rel in ontology.relationships():
                if rel.id == "part_of":
                    part_of_rel = rel
                    break

            # If a part_of relationship is found, compute the hierarchy of terms using it
            if part_of_rel is not None:
                part_of_rel.transitive = False
                print("Starting to compute super classes from part_of")
                for term in ontology.terms():
                    for super_part_term in term.objects(part_of_rel):
                        if super_part_term.id in ontology.keys():
                            term.superclasses().add(super_part_term)
                print("Finished to compute super classes from part_of")

            # Get the set of ancestors for the top level terms
            top_level_terms = [
                ontology[term] for term in ontology_desc["top_level_terms"]
            ]
            top_level_ancestors = []
            for top_level_term in top_level_terms:
                top_level_ancestors.extend(top_level_term.superclasses(with_self=False))
            top_level_ancestors = set(top_level_ancestors)

            # Iterate over the ontology terms and to get the hierarchy between them and the top level terms
            ontology_terms += [
                self._parse_ontology_term(
                    term, top_level_terms, top_level_ancestors, part_of_rel
                )
                for term in ontology.terms()
                if term.name is not None
            ]
            print(
                f"Finished processing {ontology_desc['id']}.{ontology_desc['format']}"
            )

        # Transform the list of dictionaries representing terms to JSON
        ontology_terms_json = spark_session.sparkContext.parallelize(ontology_terms)

        # Read the JSON RDD to a Spark DataFrame so it can be written to disk as Parquet
        ontology_terms_df = spark_session.read.json(
            ontology_terms_json, schema=self.ONTOLOGY_SCHEMA, mode="FAILFAST"
        )
        return ontology_terms_df

    def _parse_ontology_term(
        self,
        ontology_term: Term,
        top_level_terms,
        top_level_ancestors,
        part_of_rel: Relationship,
    ) -> Dict:
        """
        Takes in an ontology term, a list of top level terms, a list of top level ancestors
        (i.e. the ancestors of the top level terms),
        the relationship used to convey hierarchy and returns a list of dictionaries with all the hierarchical
        relationships between the terms and the top level terms.
        """

        # Gather the direct children of the term
        children = [
            child_term for child_term in ontology_term.subclasses(1, with_self=False)
        ]

        # Get the direct parents of the term
        parents = [
            parent_term
            for parent_term in ontology_term.superclasses(1, with_self=False)
        ]

        # Get all the ancestors of the term
        ancestors = [
            ancestor_term
            for ancestor_term in ontology_term.superclasses(with_self=False)
        ]

        # Get all the ancestors based on the part_of relationship instead of relying on the is_a relationship
        if part_of_rel is not None:
            ancestors.extend(
                [ancestor_term for ancestor_term in ontology_term.objects(part_of_rel)]
            )

        # Get the term top level terms by intersecting its ancestors with the ontology top level terms
        term_top_level_terms = set(top_level_terms).intersection(set(ancestors))

        # Determine the intermediate terms
        intermediate_terms = (
            set(ancestors)
            .difference(set(top_level_terms))
            .difference(top_level_ancestors)
        )
        # Builds and returns the term dictionary containing all the related terms information
        return {
            "id": ontology_term.id,
            "term": self._parse_text(ontology_term.name),
            "definition": self._parse_text(ontology_term.definition)
            if ontology_term.definition is not None
            else "null",
            "synonyms": [
                self._parse_text(synonym.description)
                for synonym in ontology_term.synonyms
                if synonym.type is "EXACT"
            ],
            "alt_ids": list(ontology_term.alternate_ids),
            "child_ids": [child_term.id for child_term in children],
            "child_terms": [
                self._parse_text(child_term.name) for child_term in children
            ],
            "child_definitions": [
                self._parse_text(child_term.definition)
                for child_term in children
                if child_term.definition is not None
            ],
            "child_term_synonyms": self._get_synonym_list(children),
            "parent_ids": [parent_term.id for parent_term in parents],
            "parent_terms": [
                self._parse_text(parent_term.name) for parent_term in parents
            ],
            "parent_definitions": [
                self._parse_text(parent_term.definition)
                for parent_term in parents
                if parent_term.definition is not None
            ],
            "parent_term_synonyms": self._get_synonym_list(parents),
            "intermediate_ids": [
                intermediate_term.id for intermediate_term in intermediate_terms
            ],
            "intermediate_terms": [
                self._parse_text(intermediate_term.name)
                for intermediate_term in intermediate_terms
            ],
            "intermediate_definitions": [
                self._parse_text(intermediate_term.definition)
                for intermediate_term in intermediate_terms
                if intermediate_term.definition is not None
            ],
            "intermediate_term_synonyms": self._get_synonym_list(intermediate_terms),
            "top_level_ids": [
                term_top_level_term.id for term_top_level_term in term_top_level_terms
            ],
            "top_level_terms": [
                self._parse_text(term_top_level_term.name)
                for term_top_level_term in term_top_level_terms
            ],
            "top_level_definitions": [
                self._parse_text(term_top_level_term.definition)
                for term_top_level_term in term_top_level_terms
                if term_top_level_term.definition is not None
            ],
            "top_level_synonyms": self._get_synonym_list(term_top_level_terms),
            "top_level_term_id": [
                f"{term_top_level_term.id}___{self._parse_text(term_top_level_term.name)}"
                for term_top_level_term in term_top_level_terms
            ],
        }

    def _get_synonym_list(self, terms: Iterable[Term]):
        """
        Takes in a list of Terms and returns the list of synonyms for the given terms.
        """
        flat_list = []
        for term in terms:
            for synonym in term.synonyms:
                flat_list.append(self._parse_text(synonym.description))
        return flat_list

    def _parse_text(self, definition: bytes):
        """
        Parse an OBO definition text an return a valid Python str.
        """
        if definition is None:
            return None
        return unicodedata.normalize(
            "NFKD", definition.encode("iso-8859-1").decode("utf-8")
        )
