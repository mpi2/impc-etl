"""
IMPC ETL Pipeline
"""
import glob
import os
import sys
import datetime
import argparse

from typing import List, Dict

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('impc_etl.zip'):
    sys.path.insert(0, './impc_etl.zip')
else:
    sys.path.insert(0, '.')

# pylint:disable=E0401,C0412
# try:
#     from pyspark import SparkConf
#     from pyspark.sql import SparkSession
# except ModuleNotFoundError:
import findspark
findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


# pylint:disable=C0413
from impc_etl.jobs.extraction.impress_extractor import extract_impress
from impc_etl.jobs.extraction.imits_extractor import *
from impc_etl.jobs.extraction.dcc_extractor import extract_specimen_files, extract_experiment_files
from impc_etl.jobs.load.prisma_loader import load


import xmlschema

def impc_pipeline(spark_context):
    """
    :param spark_context:
    :return:
    """
    return spark_context


def main():
    """
    http://sandbox.mousephenotype.org/impress
    :return:
    """
    impress_api_url = None
    dcc_xml_file_path = None

    parser = argparse.ArgumentParser()
    parser.add_argument('-i, --impress',
                        metavar='IMPRESS_API_URL',
                        nargs=1,
                        dest='impress_api_url',
                        help='Create the impress.parquet from the DCC impress web service specified in the required IMPRESS_API_URL parameter')
    parser.add_argument('-d, --dcc',
                        metavar='PATH_TO_XML_FILES',
                        nargs=1,
                        dest='dcc_xml_file_path',
                        help="Create the dcc.samples.parquet and dcc.experiments.parquet from the xml files contained in the required PATH_TO_XML_FILES parameter. "
                             "The child directory names are interpreted as datasourceShortNames. The xml files are below the child directories. "
                             "For example, given the directory structure /home/data/IMPC/J/*.xml and /home/data/3i/*.xml, specifying -dcc /home/data "
                             "creates dcc.samples.parquet and dcc.experiments.parquet from /home/dcc/IMPC/J/*.xml with datasourceShortName 'IMPC' and "
                             "/home/dcc/3i/*.xml with datasourceShortName '3i'.")
    args = parser.parse_args()
    print(args)

    impress_api_url = None if args.impress_api_url is None else args.impress_api_url[0]
    print(impress_api_url)

    dcc_xml_file_path = None if args.dcc_xml_file_path is None else args.dcc_xml_file_path[0]
    print(dcc_xml_file_path)

    conf = SparkConf().setAll([('spark.jars.packages', 'com.databricks:spark-xml_2.11:0.5.0'), ('spark.sql.sources.partitionColumnTypeInference.enabled', 'false')])
    spark = SparkSession.builder.appName("IMPC_ETL").config(conf=conf).getOrCreate()





    if impress_api_url is None:
        if os.path.exists('../impress.parquet'):
            print('Impress load from parquet started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
            impress_df = spark.read.parquet('../impress.parquet')
            print('Impress load from parquete ended:  ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        print("Impress load from DCC web service started: ", datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
        impress_pipeline_type = 'pipeline'
        impress_df = extract_impress(spark, impress_api_url, impress_pipeline_type)
        impress_df.write.mode('overwrite').parquet("../impress.parquet")
        print("Impress load from DCC web service ended:   ", datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

    if dcc_xml_file_path is None:
        if os.path.exists('../specimens.parquet'):
            print('Specimen load from parquet started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
            specimens_df = spark.read.parquet('../specimens.parquet')
            print('Specimen load from parquet ended:   ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), '(', specimens_df.count(), ')')
    else:
        print('Specimen load from dcc xml files started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

        #specimen_schema = get_schema('/Users/mrelac/workspace/PhenotypeData/loads/src/main/resources/xsd/specimen_definition.xsd')
        xml_inputs = get_inputs(dcc_xml_file_path)
        specimens_df = extract_specimen_files(spark, xml_inputs)
        specimens_df.write.mode('overwrite').parquet("../specimens.parquet")
        print('Specimen load from dcc xml files ended:   ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), '(', specimens_df.count(), ')')


    df: DataFrame = None


    if 1 == 1:
        return


    specimens_df.createOrReplaceTempView('specimens')
    df = spark.sql("SELECT * FROM specimens WHERE _specimenID IN ('170501-0002F12605-4', '387150', 'JMC300001364')")
    # spark.sql("SELECT _centreID, mouse._specimenID FROM specimens ").take(5)
    # specimens_df[specimens_df._specimenID.isin(['JMC300001364', '387150'])].show()
    spark.sql("SELECT _centreID, mouse._specimenID FROM specimens where '*._specimenID' = '30331892'").take(5)
    df.show()


    if (1 == 1):
        return


    if dcc_xml_file_path is None:
        if os.path.exists('../experiments.parquet'):
            print('Experiment load from parquet started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
            experiments_df = spark.read.parquet('../experiments.parquet')
            print('Experiment load from parquet ended:   ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), '(', specimens_df.count(), ')')
    else:
        print('Experiment load from dcc xml files started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

        experiment_schema = get_schema('/Users/mrelac/workspace/PhenotypeData/loads/src/main/resources/xsd/procedure_definition.xsd')
        #xml_inputs = get_inputs(experiment_schema, dcc_xml_file_path)
        experiments_df = extract_experiment_files(spark, experiment_schema, xml_inputs)
        experiments_df.write.mode('overwrite').parquet("../experiments.parquet")
        print('Experiment load from dcc xml files ended:   ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), '(', specimens_df.count(), ')')


    if (1 == 1):
        return

    print("dcc load start - experiments: ", datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
    dcc_experiments_df = extract_experiments(spark, xml_inputs.get('experiments'))
    dcc_experiments_df.write.parquet("../dcc.experiments.parquet")
    print("dcc load end:   - experiments", datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))


    # genes_df = extract_genes(spark, '../tests/data/imits/allele2Entries.tsv')
    # alleles_df = extract_alleles(spark, '../tests/data/imits/allele2Entries.tsv')
    # products_df = extract_products(spark, '../tests/data/imits/productEntries.tsv')



    experiments_df.printSchema()
    print(experiments_df.count())
    phenotyping_colonies_df = extract_phenotyping_colonies(spark, '../tests/data/imits/mp2_load_phenotyping_colonies_report.tsv')
    #load(genes_df, alleles_df, products_df, phenotyping_colonies_df, samples_df, None, impress_df)


def get_inputs(dcc_xml_file_path):

    xml_files_path = []


    inputs = [{ 'ds_shortname': 'IMPC', 'files': ['/Users/mrelac/dcc/IMPC/latest/J/J.2018-10-05.1.specimen.xml', '/Users/mrelac/dcc/IMPC/latest/J/J.2018-10-05.21.specimen.xml']},
              { 'ds_shortname': '3i',   'files': ['/Users/mrelac/dcc/3i/latest/ags_from_marksheet_libreoffice_version_processed_11102018.specimen.impc.xml', '/Users/mrelac/dcc/3i/latest/bci_from_marksheet_version_downloaded_14052018.specimen.impc.xml']}]





    ds_short_names = os.listdir(dcc_xml_file_path)

    for ds_short_name in ds_short_names:
        xml_path = dcc_xml_file_path + '/' + ds_short_name

        for root, directories, filenames in os.walk(xml_path):
            if len(filenames) > 0:
                thang = {'ds_short_name': ds_short_name, 'file_path': root}
                xml_files_path.append(thang)

    return xml_files_path


def get_inputs2(dcc_xml_file_path):

    xml_specimens = []
    xml_experiments = []

    datasource_short_names = os.listdir(dcc_xml_file_path)

    for datasource_short_name in datasource_short_names:
        xml_path = dcc_xml_file_path + '/' + datasource_short_name

        for root, directories, filenames in os.walk(xml_path):
            for filename in filenames:
                fqfilename = os.path.join(root, filename)
                if "specimen" in filename:
                    xml_specimens.append([('datasourceShortName', datasource_short_name), ('filename', fqfilename)])
                elif "experiment" in filename:
                    xml_experiments.append([('datasourceShortName', datasource_short_name), ('filename', fqfilename)])

    return { 'specimens': xml_specimens, 'experiments': xml_experiments }

def get_schema(xsd_file_path):

    schema = xmlschema.XMLSchema(xsd_file_path)





    return schema


if __name__ == "__main__":
    main()