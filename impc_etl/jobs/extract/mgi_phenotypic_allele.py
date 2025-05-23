"""

[MGI Phenotypic Allele report](http://www.informatics.jax.org/downloads/reports/MGI_PhenotypicAllele.rpt) extraction Task.
This report contains the list of all the Mouse Phenotypic alleles.

### From [MGI Reports / Alleles and Phenotypes section](http://www.informatics.jax.org/downloads/reports/index.html#pheno):

#### 3. List of All Mouse Phenotypic Alleles (tab-delimited):

| MGI Allele   Accession ID | Allele Symbol| Allele Name| Allele Type | Allele Attribute| PubMed ID for original reference | MGI Marker Accession ID | Marker Symbol | Marker RefSeq ID | Marker Ensembl ID  | High-level Mammalian Phenotype ID   (comma-delimited) | Synonyms (\|-delimited) | Marker Name|
|---------------------------|---------------------------------|-------------------------------------------------------|-------------|----------------------------------------------|----------------------------------|-------------------------|---------------|------------------|--------------------|-------------------------------------------------------|-------------------------|----------------------------|
| MGI:5013777| 0610009B22Rik<tm1a(EUCOMM)Hmgu> | targeted mutation 1a, Helmholtz Zentrum Muenchen GmbH | Targeted    | Reporter\|Null/knockout\|Conditional   ready || MGI:1913300| 0610009B22Rik | XM_006533917     | ENSMUSG00000007777 ||| RIKEN cDNA 0610009B22 gene |
...
"""
import luigi
from luigi.contrib.spark import PySparkTask
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from impc_etl.shared.utils import extract_tsv
from impc_etl.workflow import SmallPySparkTask
from impc_etl.workflow.config import ImpcConfig

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


class MGIPhenotypicAlleleExtractor(SmallPySparkTask):
    """
    PySpark Task class to extract the information from the
    [MGI Phenotypic Allele report](http://www.informatics.jax.org/downloads/reports/MGI_PhenotypicAllele.rpt).
    It also adds a header to the RPT format content (mgiAlleleID, alleleSymbol, alleleName, alleleType, alleleAttribute,
    pubMedID, mgiMarkerAccessionID, markerSymbol, markerRefSeqID,
    markerEnsemblID, mammalianPhenotypeID, synonyms, markerName).
    """

    #: Name of the Spark task
    name: str = "IMPC_MGI_Phenotypic_Allele_Report_Extractor"

    #: Path of the MGI Phenotypic Allele report *.rpt file.
    mgi_phenotypic_allele_report_path: luigi.Parameter = luigi.Parameter()

    #: Path of the output directory where ethe new parquet file will be generated.
    output_path: luigi.Parameter = luigi.Parameter()

    def output(self):
        """
        Returns the full parquet path as an output for the Luigi Task
        (e.g. impc/dr15.2/parquet/mgi_phenotypic_allele_parquet)
        """
        return ImpcConfig().get_target(
            f"{self.output_path}mgi_phenotypic_allele_parquet"
        )

    def app_options(self):
        """
        Generates the options pass to the PySpark job
        """
        return [
            self.mgi_phenotypic_allele_report_path,
            self.output().path,
        ]

    def main(self, sc: SparkContext, *args):
        """
        Takes in a SparkContext and the list of arguments generated by `app_options` and executes the PySpark job.
        """
        mgi_phenotypic_allele_report_path = args[0]
        output_path = args[1]

        spark = SparkSession(sc)

        mgi_phenotypic_allele_df: DataFrame = extract_tsv(
            spark,
            mgi_phenotypic_allele_report_path,
            schema=PHENOTYPIC_ALLELE_SCHEMA,
            header=False,
        )
        allele_df = mgi_phenotypic_allele_df.select(
            "alleleSymbol",
            "mgiAlleleID",
            "alleleName",
            "mgiMarkerAccessionID",
            "markerSymbol",
        ).dropDuplicates()
        allele_df.write.mode("overwrite").parquet(output_path)
