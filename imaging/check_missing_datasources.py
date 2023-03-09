import json
import os
import sys
from os.path import join

import psycopg2

from imaging import OmeroConstants
from imaging.read_omero_properties import OmeroProperties


def retrieveDatasourcesFromDB(omeroProperties):
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    for dsId in OmeroConstants.DATASOURCE_LIST:
        cur = conn.cursor()
        query = 'Select ds.id, ds.name from dataset ds inner join projectdatasetlink pdsl on ds.id=pdsl.child where pdsl.parent=' + str(
            dsId)
        cur.execute(query)
        for res in cur.fetchall():
            print(res)
    conn.close()


def processPhenoCenter(inputFolder, site, dsData):
    entries = []
    siteFolder = join(inputFolder, site)

    for pipelineKey in os.listdir(siteFolder):
        pipelineFolder = join(siteFolder, pipelineKey)

        for procedureKey in os.listdir(pipelineFolder):
            procedureFolder = join(pipelineFolder, procedureKey)

            for parameterKey in os.listdir(procedureFolder):
                entries.append(site + '-' + pipelineKey + '-' + procedureKey + '-' + parameterKey)

    for entry in entries:
        if not entry in dsData:
            print('{}'.format(entry))


def main(inputFolder, jsonDatasourceFile, omeroDevPropetiesFile):
    with open(jsonDatasourceFile, 'r') as fh:
        dsData = json.load(fh)

    omeroProperties = OmeroProperties(omeroDevPropetiesFile).getProperties()
    omeroDatasources = retrieveDatasourcesFromDB(omeroProperties)

#    for folder in os.listdir(inputFolder):
#        processPhenoCenter(inputFolder, folder, dsData)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
