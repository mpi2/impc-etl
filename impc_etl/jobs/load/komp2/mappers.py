MAPPERS = {
    "allele": {
        "data_sources": [
            {
                "name": "allele2",
                "type": "parquet",
                "join": {
                    "column_a": "symbol",
                    "column_b": "allele_symbol",
                    "how": "left_outer",
                },
            },
            {
                "name": "ontology_term",
                "type": "database",
                "join": {
                    "column_a": "allele2.mutation_type",
                    "column_b": "ontology_term.name",
                    "how": "left_outer",
                },
            },
            {
                "name": "gene",
                "type": "parquet",
                "join": {
                    "column_a": "gene.mutation_type",
                    "column_b": "ontology_term.name",
                    "how": "left_outer",
                },
            },
        ],
        "mappings": [
            {"input_column": "allele2.allele_symbol", "output_column": "symbol"},
            {"input_column": "allele2.allele_name", "output_column": "name"},
            {
                "input_column": "allele2.marker_mgi_accession_id",
                "output_column": "gf_acc",
            },
            {"input_column": "ontology_term.acc", "output_column": "biotype_acc"},
            {"input_column": "ontology_term.db_id", "output_column": "biotype_db_id"},
        ],
        "filter": None,
    }
}
