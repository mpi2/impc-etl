import os
import sys
from os.path import join

import psycopg2

from imaging import OmeroConstants
from imaging.OmeroProperties import OmeroProperties
from imaging.omero_util import retrieveDatasourcesFromDB


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
    insertNewEventQuery = 'BEGIN;' \
                          'INSERT INTO event(id, permissions, time, experimenter, experimentergroup, session, type)' \
                          'SELECT ' + str(
        newEventId) + ', permissions, time, experimenter, experimentergroup, session, type from event where id=130775809;' \
                      'COMMIT;'
    cur.execute(insertNewEventQuery)
    conn.close()
    return newEventId


def insertNewDataset(newDatasouceId, dataset, parentId, newEventId_DS, newEventId_DS_LINK, omeroProperties):
    conn = psycopg2.connect(database=omeroProperties[OmeroConstants.OMERO_DB_NAME],
                            user=omeroProperties[OmeroConstants.OMERO_DB_USER],
                            password=omeroProperties[OmeroConstants.OMERO_DB_PASS],
                            host=omeroProperties[OmeroConstants.OMERO_DB_HOST],
                            port=omeroProperties[OmeroConstants.OMERO_DB_PORT])
    cur = conn.cursor()

    insertDatasetQuery = "BEGIN;" \
                         "INSERT INTO dataset(id, permissions, name, group_id, owner_id, creation_id, update_id)" \
                         "VALUES (" + str(newDatasouceId) + ", -56, \'" + dataset + "\', 3, 0, " + str(
        newEventId_DS) + ", " + str(newEventId_DS) + ");" \
                                                     "COMMIT;"
    cur.execute(insertDatasetQuery)

    insertParentLinkQuery = 'BEGIN;' \
                            'INSERT INTO projectdatasetlink(id, permissions, child, parent, group_id, owner_id, creation_id, update_id)' \
                            'VALUES (' + str(newDatasouceId) + ', -56, ' + str(newDatasouceId) + ', ' + str(
        parentId) + ', 0, 0, ' + str(newEventId_DS_LINK) + ', ' + str(newEventId_DS_LINK) + ');' \
                                                                                            'COMMIT;'
    cur.execute(insertParentLinkQuery)
    conn.close()

    dsData = retrieveDatasourcesFromDB(omeroProperties)
    found = False
    if dataset in dsData:
        if dsData[dataset] == newDatasouceId:
            print(' - Datasource <' + dataset + '> successfully created: ' + str(newDatasouceId))
            found = True
    if not found:
        print(' - ERROR: Unable to create datasource <' + dataset + '> with ID: ' + str(newDatasouceId))
    return found


def createNewDataset(dataset, parent, newDatasouceId, omeroProperties):
    parentId = OmeroConstants.getParentDatasourceId(parent)
    if not parentId:
        print(' - ERROR: Something went wrong. Unable to find parent ID for <' + dataset + '> with parent: ' + parent)
        return False

    lastEventId = retrieveLatestEventId(omeroProperties)
    newEventId_DS = insertNewEvent(omeroProperties, lastEventId)

    lastEventId = retrieveLatestEventId(omeroProperties)
    newEventId_DS_LINK = insertNewEvent(omeroProperties, lastEventId)

    return insertNewDataset(newDatasouceId, dataset, parentId, newEventId_DS, newEventId_DS_LINK, omeroProperties)


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


def main(inputFolder, outputFolder, omeroDevPropetiesFile):
    omeroProperties = OmeroProperties(omeroDevPropetiesFile).getProperties()
    dsData = retrieveDatasourcesFromDB(omeroProperties)
    lastDatasourceId = computeLastDatasourceId(dsData)

    missingDSFile = outputFolder + OmeroConstants.FILE_MISSING_DATASOURCES
    if os.path.isfile(missingDSFile):
        os.remove(missingDSFile)

    totalNewEntries = {}
    for folder in os.listdir(inputFolder):
        newEntries = processPhenoCenter(inputFolder, folder, dsData)
        for el in newEntries:
            totalNewEntries[el] = newEntries[el]

    print('Found ' + str(len(totalNewEntries)) + ' new data sources.')
    lines = []
    for el in totalNewEntries:
        print(' - ' + el)
        lines.append(el)

    if len(lines) > 0:
        with open(missingDSFile, 'w') as fh:
            fh.write('\n'.join(lines))

    if len(totalNewEntries) > 0:
        newDatasourceId = lastDatasourceId + 1
        for el in totalNewEntries:
            ok = createNewDataset(el, totalNewEntries[el], newDatasourceId, omeroProperties)
            if not ok:
                break
            newDatasourceId += 1

    print('Running final checks ...')
    dsData = retrieveDatasourcesFromDB(omeroProperties)
    totalNewEntries = {}
    for folder in os.listdir(inputFolder):
        newEntries = processPhenoCenter(inputFolder, folder, dsData)
        for el in newEntries:
            totalNewEntries[el] = newEntries[el]

    print('Found ' + str(len(totalNewEntries)) + ' new data sources.')
    lines = []
    for el in totalNewEntries:
        print(' - ' + el)
        lines.append(el)
    if len(totalNewEntries) > 0:
        print(' - ERROR: Something went wrong since there still are new data sources ...')
        with open(missingDSFile, 'w') as fh:
            fh.write('\n'.join(lines))
    else:
        if os.path.isfile(missingDSFile):
            os.remove(missingDSFile)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
