from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from impc_etl.shared.utils import extract_tsv
import sys


STRAIN_SCHEMA = StructType(
    [
        StructField("mgiStrainID", StringType(), True),
        StructField("strainName", StringType(), True),
        StructField("strainType", StringType(), True),
    ]
)


GENE_PHENO_SCHEMA = StructType(
    [
        StructField("allelicComposition", StringType(), True),
        StructField("alleleSymbol", StringType(), True),
        StructField("mgiAlleleID", StringType(), True),
        StructField("geneticBackground", StringType(), True),
        StructField("mammalianPhenotypeID", StringType(), True),
        StructField("pubMedID", StringType(), True),
        StructField("mgiMarkerAccessionID", StringType(), True),
        StructField("mgiGenotypeAccessionID", StringType(), True),
    ]
)

PHENOTYPIC_ALLELE_SCHEMA = StructType(
    [
        StructField("mgiAlleleID", StringType(), True),
        StructField("alleleSymbol", StringType(), True),
        StructField("alleleName", StringType(), True),
        StructField("alleleType", StringType(), True),
        StructField("alleleAttribute", StringType(), True),
        StructField("pubMedID", StringType(), True),
        StructField("mgiMarkerAccessionID", StringType(), True),
        StructField("markerSymbol", StringType(), True),
        StructField("markerRefSeqID", StringType(), True),
        StructField("markerEnsemblID", StringType(), True),
        StructField("mammalianPhenotypeID", StringType(), True),
        StructField("synonyms", StringType(), True),
        StructField("markerName", StringType(), True),
    ]
)


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
                    [3]: Entity type
    """
    mgi_report_path = argv[1]
    entity_type = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()

    if entity_type == "strain":
        mgi_df = extract_mgi_strain_report(spark, mgi_report_path)
    elif entity_type == "gene_pheno":
        mgi_df = extract_mgi_genes(spark, mgi_report_path)
    elif entity_type == "cross_ref":
        mgi_df = extract_mgi_homologene(spark, mgi_report_path)
    elif entity_type == "mrk_list":
        mgi_df = extract_mgi_mrk_list(spark, mgi_report_path)
    else:
        mgi_df = extract_mgi_alleles(spark, mgi_report_path)

    mgi_df.write.mode("overwrite").parquet(output_path)


def extract_mgi_strain_report(
    spark: SparkSession, strain_report_path: str
) -> DataFrame:
    strain_df = extract_tsv(
        spark, strain_report_path, schema=STRAIN_SCHEMA, header=False
    )
    return strain_df


def extract_mgi_alleles(spark: SparkSession, strain_report_path: str) -> DataFrame:
    gene_pheno_df = extract_tsv(
        spark, strain_report_path, schema=PHENOTYPIC_ALLELE_SCHEMA, header=False
    )
    allele_df = gene_pheno_df.select(
        "alleleSymbol",
        "mgiAlleleID",
        "alleleName",
        "mgiMarkerAccessionID",
        "markerSymbol",
    ).dropDuplicates()
    return allele_df


def extract_mgi_genes(spark: SparkSession, strain_report_path: str) -> DataFrame:
    gene_pheno_df = extract_tsv(
        spark, strain_report_path, schema=GENE_PHENO_SCHEMA, header=False
    )
    return gene_pheno_df


def extract_mgi_homologene(
    spark: SparkSession, homologene_report_path: str
) -> DataFrame:
    # TODO fix empty column on rpt
    homologene_df = extract_tsv(spark, homologene_report_path, header=True)
    for col_name in homologene_df.columns:
        homologene_df = homologene_df.withColumnRenamed(
            col_name, col_name.lower().replace(" ", "_")
        )
    return homologene_df


def extract_mgi_mrk_list(
    spark: SparkSession, mgi_mrk_list_report_path: str
) -> DataFrame:
    mgi_mrk_list_df = extract_tsv(spark, mgi_mrk_list_report_path, header=True)
    for col_name in mgi_mrk_list_df.columns:
        mgi_mrk_list_df = mgi_mrk_list_df.withColumnRenamed(
            col_name,
            col_name.lower().replace(" ", "_").replace("_(pipe-separated)", ""),
        )
    return mgi_mrk_list_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
