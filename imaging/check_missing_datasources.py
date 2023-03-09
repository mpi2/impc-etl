import os
import sys
from os.path import join

import psycopg2

from imaging import OmeroConstants
from imaging.omero_util import retrieveDatasourcesFromDB
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
    conn.close()

    return last


def insertNewEvent(omeroProperties, lastEventId):
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    cur = conn.cursor()
    newEventId = lastEventId + 1
    insertNewEventQuery = 'INSERT INTO event(id, permissions, time, experimenter, experimentergroup, session, type)' \
                          'SELECT ' + str(
        newEventId) + ', permissions, time, experimenter, experimentergroup, session, type from event where id=130775809;'
    cur.execute(insertNewEventQuery)
    conn.close()
    return newEventId


def insertNewDataset(dataset, parentId, eventId):
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    cur = conn.cursor()

    insertDatasetQuery = 'INSERT INTO dataset(id, permissions, name, group_id, owner_id, creation_id, update_id)' \
                         'VALUES (<<NEW DS ID>>, -56, ' + dataset + ', 3, 0, ' + str(eventId) + ', ' + str(
        eventId) + ');'
    cur.execute(insertDatasetQuery)
    conn.close()


def processPhenoCenter(inputFolder, site, dsData):
    newEntries = {}
    siteFolder = join(inputFolder, site)

    for pipelineKey in os.listdir(siteFolder):
        pipelineFolder = join(siteFolder, pipelineKey)

        for procedureKey in os.listdir(pipelineFolder):
            procedureFolder = join(pipelineFolder, procedureKey)

            for parameterKey in os.listdir(procedureFolder):
                entryValue = site + '-' + pipelineKey + '-' + procedureKey + '-' + parameterKey
                if not entryValue in dsData:
                    newEntries[entryValue] = site
    return newEntries


def computeLastDatasourceId(dsData):
    max = 0
    for ds in dsData:
        if dsData[ds] > max:
            max = dsData[ds]
    return max


def main(inputFolder, omeroDevPropetiesFile):
    omeroProperties = OmeroProperties(omeroDevPropetiesFile).getProperties()
    dsData = retrieveDatasourcesFromDB(omeroProperties)
    lastDatasourceId = computeLastDatasourceId(dsData)
    print(lastDatasourceId)

    for folder in os.listdir(inputFolder):
        newEntries = processPhenoCenter(inputFolder, folder, dsData)
        print(newEntries)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
