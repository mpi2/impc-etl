import os
import sys
from os.path import join

import psycopg2

from imaging import OmeroConstants
from imaging.read_omero_properties import OmeroProperties


def retrieveLatestEventId(omeroProperties):
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    cur = conn.cursor()
    selectLastIdQuery = 'SELECT id, type FROM event ORDER BY id DESC limit 1'
    cur.execute(selectLastIdQuery)
    for (x, y) in cur.fetchall():
        last = int(x)
    print(last)
    newEventId = last + 1

    insertNewEventQuery = 'INSERT INTO event(id, permissions, time, experimenter, experimentergroup, session, type)' \
            'SELECT ' + str(newEventId) + ', permissions, time, experimenter, experimentergroup, session, type from event where id=130775809;'
    cur.execute(insertNewEventQuery)

    selectLastIdQuery = 'SELECT id, type FROM event ORDER BY id DESC limit 1'
    cur.execute(selectLastIdQuery)
    for (x, y) in cur.fetchall():
        last = int(x)
    print(last)

    conn.close()


def retrieveDatasourcesFromDB(omeroProperties):
    dsData = {}
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    for dsId in OmeroConstants.DATASOURCE_LIST:
        cur = conn.cursor()
        query = 'SELECT ds.id, ds.name FROM dataset ds INNER JOIN projectdatasetlink pdsl ON ds.id=pdsl.child WHERE pdsl.parent=' + str(
            dsId)
        cur.execute(query)
        for (id, name) in cur.fetchall():
            dsData[name] = int(id)
    conn.close()
    return dsData


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
            print(entry)


def main(inputFolder, omeroDevPropetiesFile):
    omeroProperties = OmeroProperties(omeroDevPropetiesFile).getProperties()
    dsData = retrieveDatasourcesFromDB(omeroProperties)

    retrieveLatestEventId(omeroProperties)


#    for folder in os.listdir(inputFolder):
#        processPhenoCenter(inputFolder, folder, dsData)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
