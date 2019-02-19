"""
IMPC ETL Pipeline
"""
import os
import sys
import datetime
import argparse

from impc_etl.jobs.extraction.impress_extractor import extract_impress
from impc_etl.jobs.extraction.imits_extractor import *
from impc_etl.jobs.extraction.dcc_extractor import extract_specimen_files, get_inputs, extract_procedure_files
from pyspark import SparkConf
import findspark

findspark.init()

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('impc_etl.zip'):
    sys.path.insert(0, './impc_etl.zip')
else:
    sys.path.insert(0, '.')


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
                        help="Create the dcc.specimens.parquet and dcc.procedures.parquet from the xml files contained"
                             " in the required PATH_TO_XML_FILES parameter. The child directory names are interpreted"
                             " as datasourceShortNames. The xml files are below the child directories. For example,"
                             " given the directory structure /home/data/IMPC/J/*.xml and /home/data/3i/*.xml,"
                             " specifying -dcc /home/data creates dcc.specimens.parquet and dcc.procedures.parquet from"
                             " /home/dcc/IMPC/J/*.xml with datasourceShortName 'IMPC' and /home/dcc/3i/*.xml with"
                             " datasourceShortName '3i'.")
    args = parser.parse_args()
    print(f"command-line args: {args}")

    impress_api_url = None if args.impress_api_url is None else args.impress_api_url[0]

    dcc_xml_file_path = None if args.dcc_xml_file_path is None else args.dcc_xml_file_path[0]

    conf = SparkConf().setAll([('spark.jars.packages', 'com.databricks:spark-xml_2.11:0.5.0'),
                               ('spark.sql.sources.partitionColumnTypeInference.enabled', 'false')])
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

    xml_inputs: []
    specimens_df: DataFrame
    procedures_df: DataFrame
    if dcc_xml_file_path is None:
        if os.path.exists('../specimens.parquet'):
            print('Specimen load from parquet started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
            specimens_df = spark.read.parquet('../specimens.parquet')
            print('Specimen load from parquet ended:   ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), '(', specimens_df.count(), ')')
    else:
        print('Specimen load from dcc xml files started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
        xml_inputs = get_inputs(dcc_xml_file_path)
        specimens_df = extract_specimen_files(spark, xml_inputs)
        print('\nspecimens_df schema:')
        specimens_df.printSchema()
        specimens_df.write.mode('overwrite').parquet("../specimens.parquet")
        print('Specimen load from dcc xml files ended:   ',
              datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

    print(f'\nSpecimen count: {specimens_df.count()}')

    spot_check_specimens(specimens_df, spark)

    if dcc_xml_file_path is None:
        if os.path.exists('../procedures.parquet'):
            print('Experiment load from parquet started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
            procedures_df = spark.read.parquet('../experiments.parquet')
            print('Experiment load from parquet ended:   ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"), '(', specimens_df.count(), ')')
    else:
        print('Procedure load from dcc xml files started: ', datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))
        procedures_df = extract_procedure_files(spark, xml_inputs)
        print('\nprocedures_df schema:')
        procedures_df.printSchema()
        procedures_df.write.mode('overwrite').parquet("../procedures.parquet")
        print('Procedure load from dcc xml files ended:   ',
              datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S"))

    experiment_count = procedures_df.select(procedures_df['_type'] == 'experiment').count()
    line_count = procedures_df.select(procedures_df['_type'] == 'line').count()
    print(f'\nExperiment count: {experiment_count}')
    print(f'\nLine count: {line_count}')

    spot_check_experiments(procedures_df, spark)
    spot_check_lines(procedures_df, spark)


    if (1 == 1):
        return




    phenotyping_colonies_df = \
        extract_phenotyping_colonies(spark, '../tests/data/imits/mp2_load_phenotyping_colonies_report.tsv')
    #load(genes_df, alleles_df, products_df, phenotyping_colonies_df, samples_df, None, impress_df)


def spot_check_specimens(specimens_df: DataFrame, spark):
    test_specimen_ids = ['JMC300001364', '387150', '30331892', '170501-0002F12605-4']
    test_specimen_ids_quoted = "'" + "','".join(test_specimen_ids) + "'"
    print(f'Spot-checking specimens. There should be {len(test_specimen_ids)} rows with _specimenIDs'
          f'{test_specimen_ids}')

    # Method 1:
    # specimens_df[specimens_df['_specimenID'].isin(test_specimen_ids)]\
    #     ['_datasourceShortName', '_centreID', '_specimenID', '_type', '_sourceFile']\
    #     .show(len(test_specimen_ids), False)

    # Method 2:
    specimens_df.createOrReplaceTempView('specimens')
    spark.sql(f"SELECT _datasourceShortName, _centreID, _specimenID, _type, _sourceFile FROM specimens"
              f" WHERE _specimenID IN ({test_specimen_ids_quoted})").show(len(test_specimen_ids), False)

show_max_lines = 20
def spot_check_experiments(procedures_df: DataFrame, spark):
    test_experiment_ids = ['3i_674736', '3i_674761', '63983', '31803', '52517', '96178', '75867', '1481',
                           '83_27560_30300005_bw_bw', '84_27561_30300005', '4102319', '281762_29756']
    test_experiment_ids_quoted = "'" + "','".join(test_experiment_ids) + "'"
    print(f'Spot-checking experiments. There should be {len(test_experiment_ids)} unique _experimentIDs'
          f'{test_experiment_ids}')

    # Method 1:
    procedures_df[procedures_df['_experimentID'].isin(test_experiment_ids)]\
        ['_datasourceShortName', '_centreID', '_type', '_experimentID', 'specimenID', '_sourceFile']\
        .show(show_max_lines, False)

    # Method 2:
    procedures_df.createOrReplaceTempView('procedures')
    spark.sql(f"SELECT _datasourceShortName, _centreID, _type, _experimentID, specimenID, _sourceFile FROM procedures"
              f" WHERE _experimentID IN ({test_experiment_ids_quoted})").show(show_max_lines, False)


def spot_check_lines(procedures_df: DataFrame, spark):
    test_line_ids = ['RICOB', 'MIFGB', 'SYNGB', 'GPBRB', 'PRSSB', 'HMGU-HEPD0718_1_B06-1-1', 'GSF-EPD0158_3_A11-1-1', 'H-Clstn3-C09-TM1B']
    test_line_ids_quoted = "'" + "','".join(test_line_ids) + "'"
    print(f'Spot-checking lines. There should be {len(test_line_ids)} unique _colonyIDs'
          f'{test_line_ids}')

    # Method 1:
    procedures_df[procedures_df['_colonyID'].isin(test_line_ids)] and procedures_df[procedures_df['_type'] == 'line']\
        ['_datasourceShortName', '_centreID', '_colonyID', '_type', '_sourceFile']\
        .show(show_max_lines, False)

    # Method 2:
    print(' ')
    procedures_df.createOrReplaceTempView('procedures')
    spark.sql(f"SELECT _datasourceShortName, _centreID, _colonyID, _type, _sourceFile FROM procedures"
              f" WHERE _colonyID IN ({test_line_ids_quoted}) AND _type = 'line'")\
        .show(show_max_lines, False)


if __name__ == "__main__":
    main()
